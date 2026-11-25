//! The `access` feature: many people on one server, through owner-signed memberships and
//! `weirdnet-access` roles.
//!
//! Each [`Access`] names a permission and a resource (an account, a project). The resource's
//! owner is a user; other users join with a `Membership` the owner signed, carrying a role id.
//! [`MembershipAuth`] checks, per request, in `weirdnet-access`'s order:
//!
//! 1. Identity: the caller's presented user `Card` must certify the transport-authenticated
//!    peer (`Profile::check_card`).
//! 2. Owner: a card for the resource owner's user makes the caller the owner.
//! 3. Membership: otherwise a presented membership of this resource, signed by the owner,
//!    unexpired, for the card's user and not revoked (`member::check_presented`).
//! 4. `weirdnet_access::enforce::decide` with the resource's settings and role set.
//!
//! Permissions must be in the catalog; an unknown one is denied. A session-level access (no
//! resource) only checks the card, when one is presented.

use std::collections::HashSet;
use std::sync::Arc;

use weirdnet_access::{Actor, Catalog, Decision, Facts, RoleSet, Settings, decide};
use weirdnet_net::member::{Membership, Presented, Revocations, check_presented};
use weirdnet_net::profile::Profile;
use weirdnet_net::user::{Card, user_id};
use weirdnet_net::{PeerId, PublicKey};

use crate::codec::from_cbor;
use crate::{Access, Admitted, Authorize, Caller, ServiceError};

/// What the server knows about one resource.
#[derive(Clone)]
#[non_exhaustive]
pub struct Resource {
    /// The owner's user key.
    pub owner: PublicKey,
    pub settings: Settings,
    pub roles: Arc<RoleSet>,
    /// The owner's checked membership revocations.
    pub revocations: Arc<Revocations>,
    /// Users on the owner's trust list (needed to see a Private resource).
    pub trusted: Arc<HashSet<String>>,
    /// A valid membership also counts as trust (default on), so members of a Private resource
    /// need no separate trust entry.
    pub trust_members: bool,
}

impl Resource {
    pub fn new(owner: PublicKey, settings: Settings, roles: Arc<RoleSet>) -> Self {
        let revocations = Arc::new(Revocations::new(&user_id(&owner)));
        Self { owner, settings, roles, revocations, trusted: Arc::default(), trust_members: true }
    }

    pub fn with_revocations(mut self, revocations: Arc<Revocations>) -> Self {
        self.revocations = revocations;
        self
    }

    pub fn with_trusted(mut self, trusted: Arc<HashSet<String>>) -> Self {
        self.trusted = trusted;
        self
    }

    pub fn trust_members(mut self, on: bool) -> Self {
        self.trust_members = on;
        self
    }
}

/// Looks up a resource by id (from memory; refresh it in the background).
pub type ResourceView = Arc<dyn Fn(&str) -> Option<Resource> + Send + Sync>;

/// Decides each access with memberships and `weirdnet-access` roles. [`Admitted`] carries the
/// user id, the resource and the role acted under (`owner`, the membership's role id, or
/// `guest`).
pub struct MembershipAuth {
    profile: &'static Profile,
    catalog: Arc<Catalog>,
    resources: ResourceView,
}

impl MembershipAuth {
    pub fn new(profile: &'static Profile, catalog: Arc<Catalog>, resources: ResourceView) -> Self {
        Self { profile, catalog, resources }
    }

    fn card(&self, caller: &Caller) -> Result<Option<Card>, ServiceError> {
        match caller.credentials.card() {
            None => Ok(None),
            Some(bytes) => {
                let card: Card = from_cbor(bytes).map_err(|_| ServiceError::refused("wrong-user"))?;
                self.profile.check_card(&caller.peer, &card).map_err(|_| ServiceError::refused("wrong-user"))?;
                Ok(Some(card))
            }
        }
    }
}

impl Authorize for MembershipAuth {
    fn authorize(&self, caller: &Caller, access: &Access) -> Result<Admitted, ServiceError> {
        let peer: PeerId = caller.peer.parse().map_err(|_| ServiceError::refused("wrong-user"))?;
        let card = self.card(caller)?;
        let mut admitted = Admitted::new(caller.peer.clone());
        if let Some(c) = &card {
            admitted = admitted.with_user(c.id.clone());
        }
        let Some(resource_id) = &access.resource else { return Ok(admitted) };
        let r = (self.resources)(resource_id).ok_or_else(|| ServiceError::not_found(resource_id.clone()))?;
        let owner_id = user_id(&r.owner);
        let is_owner = card.as_ref().is_some_and(|c| c.id == owner_id);
        let mut role = None;
        if let (false, Some(card)) = (is_owner, &card) {
            let now = weirdnet_net::now_ms();
            for bytes in caller.credentials.memberships() {
                let Ok(membership) = from_cbor::<Membership>(bytes) else { continue };
                let p = Presented { membership, card: card.clone() };
                if let Ok(m) = check_presented(self.profile, &p, &peer, &owner_id, resource_id, &r.revocations, now) {
                    role = Some(m.role);
                    break;
                }
            }
        }
        let user = card.as_ref().map(|c| c.id.clone());
        let trusted = user.as_ref().is_some_and(|u| r.trusted.contains(u)) || (r.trust_members && role.is_some());
        let facts = Facts { user, node_revoked: false, is_owner, trusted, membership_role: role };
        match decide(&self.catalog, &r.roles, r.settings, &facts, &access.permission) {
            Decision::Allow { actor, .. } => {
                let role = match actor {
                    Actor::Owner => "owner".to_string(),
                    Actor::Member { role, .. } => role.id,
                    Actor::Guest => "guest".to_string(),
                };
                Ok(admitted.with_resource(resource_id.clone()).with_role(role))
            }
            Decision::NotFound => Err(ServiceError::not_found(resource_id.clone())),
            Decision::JoinFirst => Err(ServiceError::refused("join-first")),
            Decision::Denied(why) => Err(ServiceError::refused(if why.contains("role") { "role" } else { why })),
        }
    }
}
