//! Errors, as they travel on the wire.

use serde::{Deserialize, Serialize};

/// Why a call or subscription did not complete. Sent to the caller as is, so messages must
/// never carry secrets.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
#[serde(rename_all = "snake_case", tag = "kind")]
#[non_exhaustive]
pub enum ServiceError {
    /// The server could not be reached.
    #[error("unreachable: {message}")]
    Unreachable { message: String },
    /// The [`Authorize`](crate::Authorize) hook refused the caller. `reason` is a short code
    /// such as `unknown-device`, `revoked`, `role` or `join-first`.
    #[error("refused: {reason}")]
    Refused { reason: String },
    /// No such resource or topic, or one the caller may not know about.
    #[error("not found: {message}")]
    NotFound { message: String },
    /// The request could not be decoded or is not valid.
    #[error("bad request: {message}")]
    BadRequest { message: String },
    /// The server does not serve this protocol or wire version.
    #[error("not supported: {message}")]
    Unsupported { message: String },
    /// Too many calls; try again later.
    #[error("busy")]
    Busy,
    /// Known, but not available right now.
    #[error("unavailable: {message}")]
    Unavailable { message: String },
    /// An application error with a stable code.
    #[error("{code}: {message}")]
    App { code: String, message: String },
    /// The handler failed.
    #[error("failed: {message}")]
    Failed { message: String },
    /// The other side broke the protocol.
    #[error("protocol error: {message}")]
    Protocol { message: String },
    /// The stream closed.
    #[error("connection closed")]
    Closed,
    /// No answer in time.
    #[error("timed out")]
    Timeout,
    /// Cancelled by the caller.
    #[error("cancelled")]
    Cancelled,
    /// An error kind from a newer peer.
    #[serde(other)]
    #[error("unknown error")]
    Other,
}

impl ServiceError {
    pub fn refused(reason: impl Into<String>) -> Self {
        Self::Refused { reason: reason.into() }
    }

    pub fn not_found(message: impl Into<String>) -> Self {
        Self::NotFound { message: message.into() }
    }

    pub fn bad_request(message: impl Into<String>) -> Self {
        Self::BadRequest { message: message.into() }
    }

    pub fn unsupported(message: impl Into<String>) -> Self {
        Self::Unsupported { message: message.into() }
    }

    pub fn unavailable(message: impl Into<String>) -> Self {
        Self::Unavailable { message: message.into() }
    }

    pub fn app(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self::App { code: code.into(), message: message.into() }
    }

    pub fn failed(message: impl Into<String>) -> Self {
        Self::Failed { message: message.into() }
    }

    pub fn protocol(message: impl Into<String>) -> Self {
        Self::Protocol { message: message.into() }
    }

    /// Retrying the same thing will get the same answer (refused, not found, bad request,
    /// unsupported, an application error). [`Live`](crate::Live) stops on these and
    /// reconnects on the rest.
    pub fn is_permanent(&self) -> bool {
        matches!(
            self,
            Self::Refused { .. } | Self::NotFound { .. } | Self::BadRequest { .. } | Self::Unsupported { .. } | Self::App { .. } | Self::Other
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::{from_cbor, to_cbor};

    #[test]
    fn errors_round_trip_and_unknown_kinds_decode() {
        for e in [ServiceError::refused("role"), ServiceError::Closed, ServiceError::app("limit", "over the daily limit")] {
            assert_eq!(from_cbor::<ServiceError>(&to_cbor(&e).unwrap()).unwrap(), e);
        }
        #[derive(Serialize)]
        struct Future {
            kind: &'static str,
            detail: u32,
        }
        let newer = to_cbor(&Future { kind: "quota_exceeded", detail: 3 }).unwrap();
        assert_eq!(from_cbor::<ServiceError>(&newer).unwrap(), ServiceError::Other);
        assert!(ServiceError::refused("x").is_permanent() && !ServiceError::Timeout.is_permanent());
    }
}
