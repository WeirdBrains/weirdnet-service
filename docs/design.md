# weirdnet-service design notes

Why the crate looks the way it does. The [protocol notes](protocol.md) have the messages;
the [README](../README.md) has the overview.

## Where it comes from

[weirdnet-backends](https://crates.io/crates/weirdnet-backends) serves its model-node
protocol as a transport-neutral service behind an `Authorize` hook, a server on a
weirdnet-net node, a client, CBOR messages carrying the caller's certificate, and a
long-lived subscribe stream. [weirdnet-agent](https://crates.io/crates/weirdnet-agent)
provides the hook (`serve::RosterAuth`) and the stream channels. This crate is that pattern
made generic, for servers that many apps and many people connect to, without depending on
weirdnet-agent. weirdnet-backends keeps its own copy for now.

## Decisions

- **Sessions, not one stream per call.** The backends protocol opens a stream per command.
  A server with interactive clients makes many small calls, so a session presents
  credentials once and multiplexes calls by id. Each call is still authorized on its own,
  because the access (permission and resource) differs per request.
- **Bounded concurrency with an explicit `Busy`.** Running calls and waiting calls are both
  capped. The server keeps reading, so cancellations always get through, and a full queue
  answers `Busy` instead of growing.
- **Bodies as nested CBOR.** The envelope carries the request as a byte string, so a body that
  does not decode is a `bad_request` for that call only, and envelopes can evolve apart from
  application types.
- **A synchronous `Authorize`.** Rosters, revocations and role sets are kept in memory and
  refreshed in the background; a check is a few signature verifications. Combinators
  (`AllOf`, `AnyOf`) build layered policies: a device check and a membership check, for
  example.
- **Identity from the transport.** The peer id always comes from the Noise-authenticated
  stream. Certificates, cards and memberships are presented as bytes and checked against it.
  Device roles come only from user-signed roster grants, never from a certificate label.
- **Owner by card.** `MembershipAuth` treats a caller as a resource's owner when it presents
  the owner's user card certifying the calling device. Members present the same kind of card
  plus an owner-signed membership. Revocations of memberships are the owner's signed
  `MemberRevocation`s; the app keeps them current.
- **One topic per subscription stream.** Each subscription is its own stream with its own
  queue, so one slow topic never holds up another, and resume, throttling and
  re-authorization are per topic. weirdnet-net streams are cheap and survive path switches.
- **The hub owns the state.** Deltas are applied under the hub's lock as they get their
  sequence numbers, so a snapshot is always consistent with its position, and clients apply
  deltas with the same `State::apply`.
- **Epochs.** Every hub has a random epoch, so a client resuming after a server restart gets
  a snapshot instead of deltas from a different history. `Hub::with_epoch` is for servers that
  restore sequence numbers.
- **Backpressure without blocking publishers.** Per-subscriber queues coalesce deltas with the
  same key, then fall back to a fresh snapshot (or `lagged`, so the client resumes).
- **Periodic re-authorization.** A long-lived stream is checked again on an interval, so
  revoking a device or a membership ends its live data too.

