//! Who is calling, what they ask for, and the hook that decides.

use std::sync::Arc;

use serde::{Deserialize, Serialize};
use serde_bytes::ByteBuf;

use crate::ServiceError;

/// What a caller presents once per stream, as opaque CBOR bytes for the [`Authorize`] hook to
/// check. On Weirdnet: the caller's `NodeCert`, its user `Card` and any `Membership`s (see
/// [`with_node_cert`](Self::with_node_cert) and friends with the `net` feature). None of it
/// is trusted on its own: hooks check it against the transport-authenticated peer.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Credentials {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    cert: Option<ByteBuf>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    card: Option<ByteBuf>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    memberships: Vec<ByteBuf>,
}

impl Credentials {
    pub fn new() -> Self {
        Self::default()
    }

    /// A CBOR device certificate (`weirdnet_net::user::NodeCert`).
    pub fn with_cert(mut self, cert: Vec<u8>) -> Self {
        self.cert = Some(ByteBuf::from(cert));
        self
    }

    /// A CBOR user card (`weirdnet_net::user::Card`) certifying the calling device.
    pub fn with_card(mut self, card: Vec<u8>) -> Self {
        self.card = Some(ByteBuf::from(card));
        self
    }

    /// Adds a CBOR membership (`weirdnet_net::member::Membership`).
    pub fn with_membership(mut self, membership: Vec<u8>) -> Self {
        self.memberships.push(ByteBuf::from(membership));
        self
    }

    pub fn cert(&self) -> Option<&[u8]> {
        self.cert.as_deref().map(|b| b.as_slice())
    }

    pub fn card(&self) -> Option<&[u8]> {
        self.card.as_deref().map(|b| b.as_slice())
    }

    pub fn memberships(&self) -> impl Iterator<Item = &[u8]> {
        self.memberships.iter().map(|b| b.as_slice())
    }
}

/// Who is calling. `peer` is the transport-authenticated peer id (Noise on Weirdnet), never
/// one the caller sent.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Caller {
    pub peer: String,
    pub credentials: Credentials,
}

impl Caller {
    pub fn new(peer: impl Into<String>, credentials: Credentials) -> Self {
        Self { peer: peer.into(), credentials }
    }
}

/// What a request or subscription needs: a permission, optionally on a resource (an account,
/// a site, a project).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Access {
    pub permission: String,
    pub resource: Option<String>,
}

impl Access {
    /// The permission checked when a session opens.
    pub const SESSION: &'static str = "session";

    pub fn new(permission: impl Into<String>) -> Self {
        Self { permission: permission.into(), resource: None }
    }

    /// The check when a session opens: no resource, permission [`SESSION`](Self::SESSION).
    pub fn session() -> Self {
        Self::new(Self::SESSION)
    }

    /// The same permission on `resource`.
    pub fn on(mut self, resource: impl Into<String>) -> Self {
        self.resource = Some(resource.into());
        self
    }

    pub fn is_session(&self) -> bool {
        self.permission == Self::SESSION && self.resource.is_none()
    }
}

/// A caller the hook let in, and what it learned about them. Handlers get it with each call.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
#[non_exhaustive]
pub struct Admitted {
    /// The transport-authenticated peer.
    pub peer: String,
    /// The user id the peer resolves to, when the hook knows it.
    pub user: Option<String>,
    /// Device roles granted to the peer (weirdnet-sync grants), when known.
    pub device_roles: Option<Vec<String>>,
    /// The resource the access was on.
    pub resource: Option<String>,
    /// The caller's role on that resource (`owner`, a membership role, `guest`), when known.
    pub role: Option<String>,
}

impl Admitted {
    pub fn new(peer: impl Into<String>) -> Self {
        Self { peer: peer.into(), ..Self::default() }
    }

    pub fn with_user(mut self, user: impl Into<String>) -> Self {
        self.user = Some(user.into());
        self
    }

    pub fn with_device_roles(mut self, roles: Option<Vec<String>>) -> Self {
        self.device_roles = roles;
        self
    }

    pub fn with_resource(mut self, resource: impl Into<String>) -> Self {
        self.resource = Some(resource.into());
        self
    }

    pub fn with_role(mut self, role: impl Into<String>) -> Self {
        self.role = Some(role.into());
        self
    }

    pub fn has_device_role(&self, role: &str) -> bool {
        self.device_roles.as_ref().is_some_and(|r| r.iter().any(|x| x == role))
    }

    /// Fills what `self` does not know from `other` (same peer).
    pub fn merge(mut self, other: Admitted) -> Self {
        self.user = self.user.or(other.user);
        self.device_roles = self.device_roles.or(other.device_roles);
        self.resource = self.resource.or(other.resource);
        self.role = self.role.or(other.role);
        self
    }
}

/// Decides whether a caller may have an [`Access`]. Called when a session opens (with
/// [`Access::session`]), for every call, when a subscription starts and again every
/// [`FeedOptions::recheck`](crate::FeedOptions::recheck) while it runs.
///
/// It is synchronous and should be quick: keep rosters, revocations and role sets in memory
/// and refresh them from storage in the background. A closure works too.
pub trait Authorize: Send + Sync {
    fn authorize(&self, caller: &Caller, access: &Access) -> Result<Admitted, ServiceError>;
}

impl<F> Authorize for F
where
    F: Fn(&Caller, &Access) -> Result<Admitted, ServiceError> + Send + Sync,
{
    fn authorize(&self, caller: &Caller, access: &Access) -> Result<Admitted, ServiceError> {
        self(caller, access)
    }
}

/// Lets everyone in. For tests and genuinely public services only.
#[derive(Debug, Clone, Copy, Default)]
pub struct AllowAll;

impl Authorize for AllowAll {
    fn authorize(&self, caller: &Caller, access: &Access) -> Result<Admitted, ServiceError> {
        let a = Admitted::new(caller.peer.clone());
        Ok(match &access.resource {
            Some(r) => a.with_resource(r.clone()),
            None => a,
        })
    }
}

/// Every hook must agree; the first refusal is the answer, and what they learned is merged.
/// An empty list refuses.
#[derive(Clone)]
pub struct AllOf(pub Vec<Arc<dyn Authorize>>);

impl Authorize for AllOf {
    fn authorize(&self, caller: &Caller, access: &Access) -> Result<Admitted, ServiceError> {
        let mut out: Option<Admitted> = None;
        for a in &self.0 {
            let got = a.authorize(caller, access)?;
            out = Some(match out {
                None => got,
                Some(prev) => prev.merge(got),
            });
        }
        out.ok_or_else(|| ServiceError::refused("no-authorizer"))
    }
}

/// The first hook that lets the caller in wins. When all refuse, the last refusal is the
/// answer. An empty list refuses.
#[derive(Clone)]
pub struct AnyOf(pub Vec<Arc<dyn Authorize>>);

impl Authorize for AnyOf {
    fn authorize(&self, caller: &Caller, access: &Access) -> Result<Admitted, ServiceError> {
        let mut last = ServiceError::refused("no-authorizer");
        for a in &self.0 {
            match a.authorize(caller, access) {
                Ok(got) => return Ok(got),
                Err(e) => last = e,
            }
        }
        Err(last)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::{from_cbor, to_cbor};

    #[test]
    fn combinators_merge_and_refuse() {
        let user = |c: &Caller, _: &Access| Ok(Admitted::new(c.peer.clone()).with_user("u1"));
        let role = |c: &Caller, a: &Access| {
            if a.permission == "write" { Err(ServiceError::refused("role")) } else { Ok(Admitted::new(c.peer.clone()).with_role("viewer")) }
        };
        let caller = Caller::new("p", Credentials::new());
        let all = AllOf(vec![Arc::new(user), Arc::new(role)]);
        let got = all.authorize(&caller, &Access::new("read")).unwrap();
        assert_eq!((got.user.as_deref(), got.role.as_deref()), (Some("u1"), Some("viewer")));
        assert_eq!(all.authorize(&caller, &Access::new("write")), Err(ServiceError::refused("role")));
        assert!(AllOf(vec![]).authorize(&caller, &Access::session()).is_err());
        let any = AnyOf(vec![Arc::new(role), Arc::new(user)]);
        assert_eq!(any.authorize(&caller, &Access::new("write")).unwrap().user.as_deref(), Some("u1"));
        assert!(AnyOf(vec![]).authorize(&caller, &Access::session()).is_err());
        assert_eq!(AllowAll.authorize(&caller, &Access::new("x").on("acct")).unwrap().resource.as_deref(), Some("acct"));
    }

    #[test]
    fn credentials_travel_as_byte_strings() {
        let c = Credentials::new().with_cert(vec![1, 2]).with_membership(vec![3]).with_membership(vec![4, 5]);
        let bytes = to_cbor(&c).unwrap();
        assert!(bytes.len() < 30, "byte strings, not arrays of numbers");
        let back: Credentials = from_cbor(&bytes).unwrap();
        assert_eq!(back, c);
        assert_eq!(back.memberships().collect::<Vec<_>>(), [&[3u8][..], &[4, 5][..]]);
        assert_eq!(from_cbor::<Credentials>(&to_cbor(&Credentials::new()).unwrap()).unwrap(), Credentials::new());
        assert!(Access::session().is_session() && !Access::session().on("x").is_session());
    }
}
