# Contributing

Thanks for helping. Bug reports, fixes and improvements are all welcome.

## Before you start

For anything beyond a small fix, please open an issue first so we can agree
on the approach. [docs/DESIGN.md](docs/DESIGN.md) explains how the crate is
put together and why; changes should fit it, or update it.

## Development

You need Rust 1.88 or later. The usual commands:

```sh
cargo fmt --all
cargo clippy --all-targets --all-features
cargo clippy --all-targets --no-default-features --features native-tls
cargo test
RUSTDOCFLAGS="-D warnings" cargo doc --no-deps --all-features
RUSTFLAGS="-D warnings" cargo check --lib   # the library without `testing`
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
- Title it as a [Conventional Commit](https://www.conventionalcommits.org/):
  `feat: ...`, `fix: ...`, `docs: ...`, and `feat!: ...` or a
  `BREAKING CHANGE:` footer for a breaking change. Pull requests are squash
  merged, so the title becomes the commit message, the changelog entry and
  the version bump.
- CI must pass: formatting, clippy, tests on Linux and macOS, docs, the
  minimum Rust version, and a `cargo publish` dry run.

## Releases

Releases are automated with [release-plz](https://release-plz.dev)
(`.github/workflows/release.yml`):

1. Every push to `main` opens or updates a release pull request. It bumps the
   version from the commit messages, checks the public API with
   `cargo-semver-checks`, and adds the commits to
   [CHANGELOG.md](CHANGELOG.md). Edit the changelog in that pull request if
   it needs polish; do not edit it in other pull requests.
2. Merging the release pull request publishes the crate to crates.io, tags
   `vX.Y.Z`, and creates the GitHub release from the changelog.

Maintainers set up once:

- `CARGO_REGISTRY_TOKEN`: a crates.io API token with the `publish-new` and
  `publish-update` scopes.
- Optional `RELEASE_PLZ_TOKEN`: a fine-grained token with contents and pull
  request write access. Pull requests opened with the default `GITHUB_TOKEN`
  do not trigger CI, so without it CI does not run on release pull requests.
- In Settings, Actions, General: allow GitHub Actions to create pull
  requests.
- In Settings, Security: enable private vulnerability reporting, which
  [SECURITY.md](SECURITY.md) points to.

By contributing, you agree that your contributions are licensed under the
[MIT license](LICENSE).
