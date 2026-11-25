# weirdnet-service protocol

Messages are CBOR (serde's default enum encoding: a map of the variant name to its fields).
On weirdnet-net streams each message is framed by a big-endian `u32` length (at most 16 MiB
by default). The envelope version is `wire = 1`; it is independent of an application's
protocol id version. See the [README](../README.md) for an overview and the
[design notes](design.md) for the reasons.

Each service and each feed is served on its own stream protocol id, `/<base>/<version>`,
built with `ProtocolId`. A node that does not serve an id refuses the stream; clients see
`unsupported` and can try an older version.

## Credentials

```text
Credentials { cert?: bytes, card?: bytes, memberships?: [bytes] }
```

`cert` is a CBOR `weirdnet_net::user::NodeCert`, `card` a CBOR `weirdnet_net::user::Card`,
each membership a CBOR `weirdnet_net::member::Membership`. They are checked against the
transport-authenticated peer, never trusted alone.

## Sessions

```text
client: { "hello": { wire: 1, credentials } }
server: { "welcome": { wire: 1 } }            or { "failed": error }, then close
client: { "call": { id: u64, body: bytes } }   body = CBOR request
client: { "cancel": { id } }
server: { "ok": { id, body: bytes } }          body = CBOR response
server: { "err": { id, error } }
server: { "failed": error }                    the session ends
```

Calls run concurrently and are answered in completion order. Ids are unique among calls
under way. The client closes its side to end the session; the server answers what is running
first.

## Feeds

```text
client: { wire: 1, credentials, topic, resume?: { epoch, seq } }
server: { "snapshot": { at: { epoch, seq }, state } }
server: { "resumed": { at } }                  the client's copy is current as of `at`
server: { "deltas": { epoch, items: [[seq, delta], ...] } }
server: { "heartbeat": { at } }
server: { "ended": { reason } }                `removed`, or `lagged` (resume)
server: { "failed": error }                    refused (also on a later recheck), not found
```

The first answer is `snapshot`, `resumed` or `failed`. Sequence numbers increase within an
epoch; they may skip when deltas were coalesced. A new epoch means the server's history is
gone and only a snapshot is valid. The client sends nothing after subscribing and closes the
stream to unsubscribe.

## Errors

```text
{ kind: "refused", reason } | { kind: "not_found", message } | { kind: "bad_request", message }
| { kind: "unsupported", message } | { kind: "busy" } | { kind: "unavailable", message }
| { kind: "app", code, message } | { kind: "failed", message } | { kind: "protocol", message }
| { kind: "closed" } | { kind: "timeout" } | { kind: "cancelled" } | { kind: "unreachable", message }
```

An unknown `kind` from a newer peer decodes as `other`.
