# weirdnet-service

A service framework for [Weird Brains](https://weirdbrains.com) servers on Weirdnet: one
server, many apps. A server exposes typed request/response services and live topic feeds;
desktop, terminal, mobile and MCP clients connect over
[weirdnet-net](https://crates.io/crates/weirdnet-net) streams, and every call and
subscription goes through an `Authorize` hook first.

- **Typed services.** Implement `Service` with your own request and response enums (shared by
  server and clients). A session is one stream: credentials once, then concurrent calls with
  ids, cancellation, per-call deadlines, and a bounded queue that answers `Busy` when full.
- **Authorization per request.** Each request names an `Access` (a permission, optionally on
  a resource such as an account). The hook returns who was admitted (user, device roles,
  role on the resource), and the handler gets it.
  - `RosterAuth` (feature `roster`): the user's own linked devices and the device roles the
    user granted them, from the [weirdnet-sync](https://crates.io/crates/weirdnet-sync) 0.8
    roster.
  - `MembershipAuth` (feature `access`): many people on one server. Owner-signed memberships
    from weirdnet-net and roles from [weirdnet-access](https://crates.io/crates/weirdnet-access),
    checked in its enforcement order.
  - `AllOf`, `AnyOf`, `AllowAll`, or a closure.
- **Live feeds.** A `Hub` keeps each topic's state and a sequence number. Subscribers get a
  snapshot, then deltas; after a reconnect they resume from their last position or get a fresh
  snapshot when the gap is too old or the server restarted. Publishing never waits: slow
  subscribers have their deltas coalesced by key and are re-snapshotted (or told to resume)
  when their queue overflows. Optional throttling, heartbeats, and periodic re-authorization,
  so a revoked device loses its stream.
- **`Live`.** A local copy of a topic's state that reconnects and resumes on its own, for a
  UI's background task.
- **Versioned protocol ids.** `ProtocolId` (`/myapp/api/2`), served side by side, with
  `Client::session_any` picking the newest the server has.
- **Transport-neutral core.** Services and feeds run over any `Channel`; the `net` feature
  (default) adds `Server`, `NetDialer` and stream channels for weirdnet-net 0.8.

```rust
use std::sync::Arc;
use weirdnet_service::{Access, Admitted, Service, ServiceError};

struct Echo;

impl Service for Echo {
    type Request = String;
    type Response = String;

    fn access(&self, _request: &String) -> Access {
        Access::new("echo")
    }

    async fn call(&self, _who: Admitted, request: String) -> Result<String, ServiceError> {
        Ok(request)
    }
}

// On the server, inside a Tokio runtime:
// let server = weirdnet_service::net::Server::builder(&node)
//     .service(&ProtocolId::new("/myapp/api", 1)?, Arc::new(Echo), auth.clone(), Default::default())
//     .feed(&ProtocolId::new("/myapp/live", 1)?, hub, |topic| Access::new("read").on(topic.account()), auth, Default::default())
//     .announce("myapp-server")
//     .start()?;
// On a client:
// let session = client.session::<String, String>(&server_peer, &api).await?;
```

The [protocol notes](docs/protocol.md) describe the messages, and the
[design notes](docs/design.md) explain the choices. API documentation is on
[docs.rs](https://docs.rs/weirdnet-service).

## Features

| Feature | Default | Adds |
|---|---|---|
| `net` | yes | `Server`, `NetDialer`, stream channels and credential helpers over weirdnet-net |
| `roster` | no | `RosterAuth` (weirdnet-sync 0.8) |
| `access` | no | `MembershipAuth` (weirdnet-access) |

## Status

The API may change before 1.0. The pattern comes from the model-node protocol in
[weirdnet-backends](https://crates.io/crates/weirdnet-backends), which does not use this
crate yet.

## License

BSD-3-Clause, copyright Weird Brains LLC. See [LICENSE](LICENSE).
