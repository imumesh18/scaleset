# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project
follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
