//! Weirdnet service framework: one server, many apps.
//!
//! A server exposes typed request/response [`Service`]s and live [`Hub`] feeds. Apps (desktop,
//! terminal, mobile through a Rust bridge, MCP) connect over Weirdnet streams, present their
//! [`Credentials`] once per stream, and every request and subscription is checked by an
//! [`Authorize`] hook before it runs.
//!
//! - [`service`]: request/response sessions. A session is one stream: a `Hello` with the
//!   caller's credentials, then any number of concurrent calls, each with an id, each
//!   authorized on its own [`Access`] (permission and resource), cancellable.
//! - [`feed`]: live topic streams. A [`Hub`] keeps each topic's state and a sequence number;
//!   a subscriber gets a snapshot then deltas, resumes from a [`Position`] after a reconnect
//!   (or gets a fresh snapshot when the gap is too old), and a slow subscriber is coalesced or
//!   re-snapshotted instead of stalling the publisher.
//! - [`auth`]: the [`Authorize`] hook, [`Caller`], [`Access`], [`Admitted`] and combinators.
//!   The `roster` feature adds [`RosterAuth`](roster::RosterAuth) (the user's linked devices
//!   and their device-role grants); the `access` feature adds
//!   [`MembershipAuth`](access::MembershipAuth) (owner-signed memberships and
//!   `weirdnet-access` roles).
//! - [`ProtocolId`]: versioned protocol ids such as `/myapp/api/2`.
//! - The `net` feature (default) serves all of this on a `weirdnet-net` node
//!   ([`Server`](net::Server)) and dials it ([`NetDialer`](net::NetDialer), [`Client`]).
//!
//! Everything below the `net` feature is transport-neutral: a [`Channel`] is any ordered,
//! reliable message channel, and [`memory_pair`] gives two connected ends for tests.
//!
//! Messages are CBOR. The [protocol notes](https://github.com/WeirdBrains/weirdnet-service/blob/main/docs/protocol.md)
//! describe the wire format, and the [design notes](https://github.com/WeirdBrains/weirdnet-service/blob/main/docs/design.md)
//! explain the choices.

pub mod auth;
pub mod channel;
pub mod codec;
pub mod error;
pub mod feed;
pub mod protocol;
pub mod service;

#[cfg(feature = "access")]
pub mod access;
#[cfg(feature = "net")]
pub mod net;
#[cfg(feature = "roster")]
pub mod roster;

mod client;

pub use auth::{Access, Admitted, AllOf, AllowAll, AnyOf, Authorize, Caller, Credentials};
pub use channel::{Channel, Dialer, FrameRx, FrameTx, memory_pair};
pub use client::Client;
pub use error::ServiceError;
pub use feed::{Change, Event, FeedOptions, Hub, HubOptions, Live, Overflow, Position, State, Subscription, Topic, Watcher, serve_feed};
pub use protocol::{ProtocolId, WIRE};
pub use service::{Service, Session, SessionOptions, serve_session};
