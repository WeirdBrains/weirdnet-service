//! Versioned protocol ids.

use std::fmt;
use std::str::FromStr;

use crate::ServiceError;

/// The envelope version of this crate's messages (`Hello`, `Subscribe` and the frames around
/// bodies). Independent of an application's [`ProtocolId`] version.
pub const WIRE: u8 = 1;

/// A protocol id with a version: `/myapp/api/2` is base `/myapp/api`, version 2.
///
/// Bump the version when a protocol's messages change incompatibly; serve the old and new
/// versions side by side for a while, and let clients try the newest first
/// ([`Client::session_any`](crate::Client::session_any)).
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ProtocolId {
    base: String,
    version: u32,
    id: String,
}

impl ProtocolId {
    /// `base` starts with `/`, has non-empty segments of `a-z A-Z 0-9 . _ -`, and does not end
    /// with `/`; `version` is 1 or more.
    pub fn new(base: &str, version: u32) -> Result<Self, ServiceError> {
        let ok_segment = |s: &str| !s.is_empty() && s.bytes().all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'));
        let Some(rest) = base.strip_prefix('/') else {
            return Err(ServiceError::bad_request(format!("protocol base {base:?} must start with /")));
        };
        if !rest.split('/').all(ok_segment) || base.len() > 200 {
            return Err(ServiceError::bad_request(format!("protocol base {base:?} is not valid")));
        }
        if version == 0 {
            return Err(ServiceError::bad_request("protocol versions start at 1"));
        }
        Ok(Self { base: base.to_string(), version, id: format!("{base}/{version}") })
    }

    /// Parses `/base/N`.
    pub fn parse(id: &str) -> Result<Self, ServiceError> {
        let (base, v) = id.rsplit_once('/').ok_or_else(|| ServiceError::bad_request(format!("{id:?} is not a protocol id")))?;
        if v.is_empty() || !v.bytes().all(|b| b.is_ascii_digit()) || v.starts_with('0') {
            return Err(ServiceError::bad_request(format!("{id:?} does not end in a version")));
        }
        let version = v.parse().map_err(|_| ServiceError::bad_request(format!("{id:?}: version too large")))?;
        Self::new(base, version)
    }

    pub fn base(&self) -> &str {
        &self.base
    }

    pub fn version(&self) -> u32 {
        self.version
    }

    /// The full id, as streams are opened with it.
    pub fn as_str(&self) -> &str {
        &self.id
    }

    /// The same base at another version.
    pub fn with_version(&self, version: u32) -> Result<Self, ServiceError> {
        Self::new(&self.base, version)
    }
}

impl fmt::Display for ProtocolId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.id)
    }
}

impl FromStr for ProtocolId {
    type Err = ServiceError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::parse(s)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ids_parse_print_and_validate() {
        let p = ProtocolId::new("/myapp/api", 2).unwrap();
        assert_eq!((p.as_str(), p.base(), p.version()), ("/myapp/api/2", "/myapp/api", 2));
        assert_eq!(ProtocolId::parse("/myapp/api/2").unwrap(), p);
        assert_eq!(p.with_version(1).unwrap().to_string(), "/myapp/api/1");
        for bad in ["myapp/api/1", "/myapp//api/1", "/myapp/api/", "/myapp/api/0", "/myapp/api/v1", "/my app/1", "/myapp/api/01"] {
            assert!(ProtocolId::parse(bad).is_err(), "{bad}");
        }
        assert!(ProtocolId::new("/x", 0).is_err());
        assert!(ProtocolId::parse("/x/1").unwrap() < ProtocolId::parse("/x/2").unwrap());
    }
}
