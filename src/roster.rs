//! The `roster` feature: admit the user's own linked devices (weirdnet-sync roster).
//!
//! Generic over any protocol, with no `weirdnet-agent` dependency. Same checks as
//! `weirdnet-agent`'s `serve::RosterAuth`: `Roster::check_caller` with the
//! transport-authenticated peer and the certificate it presented, then optionally the device
//! roles the user granted it (`DeviceGrant`s, `Roster::roles_of`). Roles are never read from
//! the certificate's label.

use std::collections::HashMap;
use std::sync::Arc;

use weirdnet_net::PublicKey;
use weirdnet_net::profile::Profile;
use weirdnet_net::user::user_id;
use weirdnet_sync::{CallerCheck, Roster};

use crate::{Access, Admitted, Authorize, Caller, ServiceError};

/// The roster as it is now (the app keeps it current from `weirdnet-sync`).
pub type RosterView = Arc<dyn Fn() -> Roster + Send + Sync>;

/// Admits the user's linked devices. Refusals carry the roster's reason (`unknown-device`,
/// `revoked`, `wrong-user`, `needs-admission`), or `role` when a required device role is
/// missing. [`Admitted`] carries the user id and the device's granted roles.
pub struct RosterAuth {
    profile: &'static Profile,
    user: PublicKey,
    user_id: String,
    roster: RosterView,
    any_of: Vec<String>,
    per_permission: HashMap<String, Vec<String>>,
}

impl RosterAuth {
    pub fn new(profile: &'static Profile, user: PublicKey, roster: RosterView) -> Self {
        let user_id = user_id(&user);
        Self { profile, user, user_id, roster, any_of: Vec::new(), per_permission: HashMap::new() }
    }

    /// Every access needs one of these device roles (unless
    /// [`require_roles_for`](Self::require_roles_for) says otherwise for its permission).
    pub fn require_roles(mut self, any_of: &[&str]) -> Self {
        self.any_of = any_of.iter().map(|r| r.to_string()).collect();
        self
    }

    /// Accesses with `permission` need one of these device roles. An empty list needs none.
    pub fn require_roles_for(mut self, permission: &str, any_of: &[&str]) -> Self {
        self.per_permission.insert(permission.to_string(), any_of.iter().map(|r| r.to_string()).collect());
        self
    }

    /// The roster's verdict on the caller.
    pub fn check(&self, roster: &Roster, caller: &Caller) -> CallerCheck {
        let presented = match caller.credentials.node_cert() {
            None => None,
            Some(Ok(c)) => Some(c),
            Some(Err(_)) => return CallerCheck::WrongUser,
        };
        roster.check_caller(self.profile, &self.user, &caller.peer, presented.as_ref())
    }
}

impl Authorize for RosterAuth {
    fn authorize(&self, caller: &Caller, access: &Access) -> Result<Admitted, ServiceError> {
        let roster = (self.roster)();
        if let Some(reason) = self.check(&roster, caller).reason() {
            return Err(ServiceError::refused(reason));
        }
        let roles = roster.roles_of(&caller.peer).map(|r| r.to_vec());
        let need = self.per_permission.get(&access.permission).unwrap_or(&self.any_of);
        if !need.is_empty() && !roles.as_ref().is_some_and(|have| have.iter().any(|r| need.contains(r))) {
            return Err(ServiceError::refused("role"));
        }
        let mut a = Admitted::new(caller.peer.clone()).with_user(self.user_id.clone()).with_device_roles(roles);
        if let Some(r) = &access.resource {
            a = a.with_resource(r.clone());
        }
        Ok(a)
    }
}
