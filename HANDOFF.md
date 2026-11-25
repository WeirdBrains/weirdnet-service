# Handoff

Running notes for [weirdnet-service](https://github.com/WeirdBrains/weirdnet-service), newest first.

## 2026-11-25: released v0.1.0

- What changed: the first release, [0.1.0 on crates.io](https://crates.io/crates/weirdnet-service/0.1.0),
  with the API described in the entry below. Tagged
  [v0.1.0](https://github.com/WeirdBrains/weirdnet-service/releases/tag/v0.1.0).
- Commits: `fd13ee0` (`release: v0.1.0`, made by `scripts/weirdnet_release.py`) and this entry.
- Checks: CI passed on `499a297` (Linux checks, macOS and Windows tests with all features);
  the release script's checks (fmt, clippy with all features, tests with all features, docs)
  passed before tagging; `cargo publish` verified the package; crates.io lists 0.1.0.
- Not tested: as in the entry below. docs.rs builds the documentation from the upload.
- Left: nothing for 0.1.0.
- Next: move weirdnet-backends' node and models protocols onto this crate; a client helper
  that reads a node's announcement.

## 2026-11-25: initial crate, unreleased

- What changed: the first implementation, the generic form of the service pattern in
  [weirdnet-backends](https://crates.io/crates/weirdnet-backends) 0.4.5. `Service` with typed
  requests and responses; sessions with concurrent, cancellable calls, deadlines and a bounded
  queue (`Busy`); the `Authorize` hook with `Caller`, `Credentials`, `Access`, `Admitted`,
  `AllOf`, `AnyOf` and `AllowAll`; `Hub` feeds with snapshot then deltas, resume from a
  `Position`, epochs, coalescing, overflow, throttling, heartbeats and rechecks; `Subscription`
  and `Live`; `ProtocolId`; the `net` feature (`Server`, `NetDialer`, stream channels), the
  `roster` feature (`RosterAuth`, [weirdnet-sync](https://crates.io/crates/weirdnet-sync) 0.8)
  and the `access` feature (`MembershipAuth`,
  [weirdnet-access](https://crates.io/crates/weirdnet-access)). No weirdnet-agent dependency.
  The version stays 0.0.0 until the first cut (0.1.0).
- Commits: `5290bfe` (crate), `91c9909` (docs), `8cb1beb` (CI and release config), and this
  entry.
- Checks: `cargo fmt --check`; `cargo clippy --all-targets -D warnings` with default,
  no-default and all features; `cargo test` with default and no-default features (15 unit
  tests plus 1 doc test) and all features (also 2 tests between in-process weirdnet-net nodes:
  linked devices through `RosterAuth`, and owner, viewer, trader and outsider through
  `MembershipAuth`, including revocation ending a live stream); `cargo doc --no-deps
  --all-features` with `-D warnings`; `cargo package --locked`; `weirdnet_release.py validate`.
- Not tested: device-role requirements in `RosterAuth` against real roster grants (the tests
  have no grants); real networks, relays and path switches; heavy load and many subscribers;
  Windows and macOS until the first CI run; the BSDs.
- Left: CI on GitHub; the 0.1.0 release (both done, see above).
- Next: move weirdnet-backends' node and models protocols onto this crate (not started); a
  client helper that reads a node's announcement.
