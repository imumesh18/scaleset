# AGENTS.md

Guidance for coding agents working in this repository. Read
[docs/DESIGN.md](docs/DESIGN.md) before changing behavior: it explains the
protocol, the public API, and why each part differs from GitHub's Go client.

## Project

`scaleset` is a Rust client for the GitHub Actions Runner Scale Set APIs. It
follows the wire protocol of [`actions/scaleset`](https://github.com/actions/scaleset)
(Go), with an API designed for Rust. Minimum Rust version: see `rust-version`
in `Cargo.toml`.

## Layout

| Path | What |
| --- | --- |
| `src/client.rs` | `Client`, builder, admin token exchange, scale set and runner calls |
| `src/auth.rs` | `Credentials`, GitHub App JWTs, external signers |
| `src/session.rs` | `MessageSession`: poll, acknowledge, acquire jobs, refresh |
| `src/transport.rs` | the retry engine and response-to-error mapping (internal) |
| `src/retry.rs` | `RetryPolicy`, rate-limit policies |
| `src/error.rs` | `Error`, `ApiError`, `RateLimitError` |
| `src/types.rs` | typed IDs and wire types |
| `src/config.rs` | GitHub config URL parsing |
| `src/listener/` | `Listener`, `Scaler`, `Event`, `Subscription`, `JobTracker` |
| `src/testing/` | the emulator (feature `testing`): `state.rs` holds the rules, `server.rs` the HTTP layer |
| `src/bin/scaleset-emulator.rs` | the emulator as a standalone server |
| `examples/autoscaler.rs` | a complete process-based autoscaler |
| `.github/workflows/` | CI (`ci.yml`) and releases with release-plz (`release.yml`) |
| `tests/` | wire tests (`client.rs`, `session.rs`) and scenario tests (`emulator.rs`, `listener.rs`) |

## Commands

Run all of these before proposing a change; CI runs the same:

```sh
cargo fmt --all
cargo clippy --all-targets --all-features
cargo clippy --all-targets --no-default-features --features native-tls
cargo test
RUSTDOCFLAGS="-D warnings" cargo doc --no-deps --all-features
RUSTFLAGS="-D warnings" cargo check --lib   # the library without `testing`
```

`cargo test` enables the `testing` feature for the crate's own tests through a
self-referencing dev-dependency, so the emulator is always available to them.

## Tests

- **Protocol changes** need a wire test in `tests/client.rs` or
  `tests/session.rs` (`wiremock`) that pins the exact request and response.
- **Behavior changes** need a scenario test against `testing::Emulator` in
  `tests/emulator.rs` or `tests/listener.rs`. If the emulator does not model
  the behavior yet, extend it, keeping the protocol rules in
  `src/testing/state.rs`.
- README examples are compiled as doctests; keep them building.
- Never skip, ignore or weaken a test to make CI pass.

## Conventions

- Match the Go client's wire behavior (paths, headers, bodies, status
  handling). Where the Rust API differs on purpose, `docs/DESIGN.md` says
  why; update it when you change a design decision.
- Public types that the service may extend are `#[non_exhaustive]`.
- Secrets (tokens, JIT configs) are `SecretString`; never log them or put
  them in `Debug` output, error messages or tracing fields.
- Credentials travel only over HTTPS unless `allow_http` is set.
- Log through `tracing`; never install a subscriber in the library.
- Keep `axum` and other emulator-only code behind the `testing` feature.
- Write docs and comments in plain, short sentences.
- Pull request titles are Conventional Commits (`feat:`, `fix:`, `docs:`,
  `feat!:` for breaking changes). release-plz builds the version bump and
  `CHANGELOG.md` from them; do not edit `CHANGELOG.md` outside a release
  pull request.
- CI uses only official GitHub actions and the preinstalled `rustup`; do not
  add third-party actions.

## Reviews

Fix review findings that are real bugs on realistic paths: wrong results in
normal use, security exposures, data loss, hangs or panics from real input.
For rare edge cases, misuse of test helpers, or defense in depth with no real
path, reply with the reasoning instead of adding code.
