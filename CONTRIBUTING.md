# Contributing

Thanks for helping. Bug reports, fixes and improvements are all welcome.

## Before you start

For anything beyond a small fix, please open an issue first so we can agree
on the approach. [docs/DESIGN.md](docs/DESIGN.md) explains how the crate is
put together and why; changes should fit it, or update it.

## Development

You need Rust 1.85 or later. The usual commands:

```sh
cargo fmt --all
cargo clippy --all-targets --all-features
cargo test
RUSTDOCFLAGS="-D warnings" cargo doc --no-deps --all-features
```

`cargo test` runs three kinds of test:

- unit tests next to the code;
- wire tests (`tests/client.rs`, `tests/session.rs`) that pin exact requests
  and responses with `wiremock`;
- behavior tests (`tests/emulator.rs`, `tests/listener.rs`) that run whole
  scenarios against `scaleset::testing::Emulator`.

A protocol change needs a wire test. A behavior change needs a scenario
test, and if the emulator does not model it yet, a change to the emulator.

## Pull requests

- Keep each pull request to one change, with tests and docs.
- Add an entry under `[Unreleased]` in [CHANGELOG.md](CHANGELOG.md) for
  anything users would notice.
- CI must pass: formatting, clippy, tests on Linux and macOS, docs, and the
  minimum Rust version.

By contributing, you agree that your contributions are licensed under the
[MIT license](LICENSE).
