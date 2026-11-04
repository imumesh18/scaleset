# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project
follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.2.0](https://github.com/imumesh18/scaleset/compare/v0.1.0...v0.2.0) - 2026-11-04

### Changed

- **Breaking:** `ClientBuilder::allow_http()` takes no argument. Plain HTTP
  is opt-in, so call it only when you need it.
- **Breaking:** `RunnerScaleSetSpec::disable_update()` takes no argument.
  The new `enable_update()` turns runner updates back on.
- **Breaking:** `JobCompleted::runner_id` and `runner_name` are `Option`s,
  `None` when no runner took the job. The wire format is unchanged: the
  service and the emulator still send `runnerId: 0` and `runnerName: ""`.
- **Breaking:** `MessageSession::info()` returns `SessionInfo` instead of
  `Arc<SessionInfo>`.
- **Breaking:** in the `testing` emulator, `FaultResponse::RateLimit` has
  `limit: RateLimitKind` instead of `primary: bool`. In the control API JSON
  this is `"limit": "primary"` or `"limit": "secondary"`.
- **Breaking:** in the `testing` emulator, `Fault`, `EmulatedJob`,
  `EmulatedRunner`, `EmulatedSession`, `EmulatorState` and `JobState` are
  `#[non_exhaustive]`.
- **Breaking:** the minimum supported Rust version is 1.91.
- The `Listener` adds up to 50% random jitter to its backoff, so standby
  listeners do not retry in lockstep.
- `JobTracker::busy_runners` yields runners in job request ID order.
- Error messages keep at most 1 KiB of a non-JSON error body, such as an
  HTML page from a proxy.
- The log line for a rejected admin token shows the request path, never
  the query.

### Added

- `JobTracker::forget_runner`, for a runner whose `JobCompleted` message
  never arrived.
- `FromStr` for the ID types (`ScaleSetId`, `RunnerId`, …).
- `Label::with_type`, for labels whose type is not `System`.
- `Scaler` for `&mut S`.
- `PartialEq` and `Eq` for `ApiError` and `RateLimitError`; `Serialize` and
  `Deserialize` for `RateLimitKind`.
- `# Errors` and `# Panics` sections on every public call.

### Fixed

- A `null` count in a message's statistics, or a `null` message field, no
  longer fails the whole message. Before, the message was never
  acknowledged and kept coming back, and the `Listener` gave up after
  `max_failures`. `null` now reads as 0 or empty, as in Go.
- `examples/autoscaler.rs` no longer overflows on very large job counts,
  and removes a runner's registration when the runner process fails to
  start.

## [0.1.0](https://github.com/imumesh18/scaleset/releases/tag/v0.1.0)

First release.

### Added

- `Client`: scale sets, runner groups, runners and JIT runner configs on
  github.com, GHE.com and GitHub Enterprise Server.
- Credentials: personal access tokens, GitHub Apps with a PEM key, and
  GitHub Apps with an external signer (`Rs256Signer`, `JwtProvider`) for keys
  in a KMS or HSM. Admin tokens are cached, refreshed before expiry, and
  replaced when rejected early.
- `MessageSession`: long polling, acknowledgment, job acquisition, and
  transparent refresh of an expired queue token.
- `RetryPolicy`: jittered exponential backoff, `Retry-After`, a time budget
  per request, rate-limit classification into `RateLimitError`, and
  `RateLimitPolicy` rules; per-call policies through
  `Client::with_retry_policy`.
- `listener`: `Listener` (session lifecycle, job acquisition,
  acknowledgment, backoff, session-conflict and lost-session recovery),
  `Scaler`, `Event`, `Subscription`, `JobTracker`, and a `Scaler` for
  `watch::Sender<ScaleSetStatistics>`.
- `testing` feature: `testing::Emulator`, a stateful emulator of the scale
  set APIs with fault injection, and the `scaleset-emulator` binary.
- Credentials are only sent over HTTPS, unless `ClientBuilder::allow_http`
  permits plain HTTP for a GitHub Enterprise Server lab or a test.
