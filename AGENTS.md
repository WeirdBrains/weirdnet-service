# weirdnet-service agents

The collective `AGENTS.md` in `WeirdBrains/weirdnet-agents` is mandatory and wins on conflict.

- Build outside the source checkout, with `CARGO_TARGET_DIR` on a scratch disk; clean targets afterwards.
- The peer id always comes from the transport. Never authorize on anything a caller sent
  without checking it against that peer.
- Device roles come from roster grants only, never from a certificate label.
- Every request, subscription start and recheck goes through `Authorize`; keep it that way.
- Wire changes: keep `docs/protocol.md` in step; bump `WIRE` only for an incompatible envelope.
- Errors travel to callers: never put secrets or internal paths in them.
