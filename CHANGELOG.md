# Changelog

All notable changes to this crate are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the crate uses
[Semantic Versioning](https://semver.org/).

## [Unreleased]

### Added

- `Service` trait for typed request/response services, and sessions with concurrent,
  cancellable calls, deadlines and a bounded queue.
- `Authorize` hook with `Caller`, `Credentials`, `Access` and `Admitted`; `AllOf`, `AnyOf`
  and `AllowAll`.
- `Hub` live feeds: snapshot then deltas, resume from a position, epochs, coalescing,
  overflow handling, throttling, heartbeats and periodic re-authorization.
- `Subscription` and `Live` on the client side.
- Versioned protocol ids (`ProtocolId`).
- `net` feature: `Server`, `NetDialer` and stream channels over weirdnet-net 0.8.
- `roster` feature: `RosterAuth` for linked devices and device-role grants (weirdnet-sync 0.8).
- `access` feature: `MembershipAuth` for owner-signed memberships and weirdnet-access roles.
