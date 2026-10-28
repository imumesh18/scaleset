//! A client for the GitHub Actions **Runner Scale Set** APIs.
//!
//! Scale sets let you autoscale self-hosted runners on any infrastructure,
//! without Kubernetes or actions-runner-controller. This crate covers the
//! whole API: scale sets, just-in-time (JIT) runner configs, and the message
//! sessions that tell you how many runners are needed. It follows the
//! protocol of GitHub's Go client,
//! [`actions/scaleset`](https://github.com/actions/scaleset).
//!
//! # Overview
//!
//! 1. Build a [`Client`] with a GitHub App ([`Credentials::github_app`],
//!    preferred) or a personal access token.
//! 2. Create a scale set with [`Client::create_runner_scale_set`]. Its name
//!    is the label workflows target with `runs-on`.
//! 3. Run a [`Listener`](listener::Listener) with your
//!    [`Scaler`](listener::Scaler). It opens the message session, acquires
//!    jobs, acknowledges messages and recovers from failures.
//! 4. In the scaler, keep
//!    [`total_assigned_jobs`](ScaleSetStatistics::total_assigned_jobs)
//!    runners available: start each with a config from
//!    [`Client::generate_jit_config`].
//!
//! ```no_run
//! use scaleset::listener::{Event, Listener, Scaler};
//! use scaleset::{
//!     BoxError, Client, Credentials, ExposeSecret, JitRunnerSetting, RunnerScaleSetSpec,
//!     ScaleSetId,
//! };
//!
//! struct Autoscaler {
//!     client: Client,
//!     scale_set_id: ScaleSetId,
//!     started: u32,
//! }
//!
//! impl Scaler for Autoscaler {
//!     type Error = BoxError;
//!
//!     async fn scale(&mut self, event: Event<'_>) -> Result<(), BoxError> {
//!         while self.started < event.statistics().total_assigned_jobs {
//!             let setting = JitRunnerSetting::new(format!("runner-{}", self.started));
//!             let jit = self.client.generate_jit_config(self.scale_set_id, &setting).await?;
//!             start_runner(jit.encoded_jit_config.expose_secret());
//!             self.started += 1;
//!         }
//!         Ok(())
//!     }
//! }
//!
//! # fn start_runner(_: &str) {}
//! # async fn run() -> Result<(), BoxError> {
//! let pem = std::fs::read_to_string("app.pem")?;
//! let client = Client::builder(
//!     "https://github.com/my-org",
//!     Credentials::github_app("Iv1.abc123", 12345, &pem)?,
//! )
//! .build()?;
//!
//! let scale_set = client
//!     .create_runner_scale_set(&RunnerScaleSetSpec::new("my-scale-set"))
//!     .await?;
//!
//! let mut scaler = Autoscaler { client: client.clone(), scale_set_id: scale_set.id, started: 0 };
//! Listener::new(client.clone(), scale_set.id)
//!     .max_runners(10)
//!     .run(&mut scaler, async { let _ = tokio::signal::ctrl_c().await; })
//!     .await?;
//!
//! client.delete_runner_scale_set(scale_set.id).await?;
//! # Ok(()) }
//! ```
//!
//! # Layers
//!
//! | Layer | Use it to |
//! | --- | --- |
//! | [`Listener`](listener::Listener) + [`Scaler`](listener::Scaler) | run an autoscaler; the usual choice |
//! | [`Subscription`](listener::Subscription) | drive the poll / acknowledge loop yourself |
//! | [`MessageSession`] | call the message queue endpoints directly |
//! | [`Client`] | manage scale sets, runners and JIT configs |
//!
//! # Transport and retries
//!
//! The HTTP client is yours ([`ClientBuilder::http_client`], with defaults
//! from [`http_client_builder`]); retries belong to the SDK
//! ([`RetryPolicy`]). Transport errors, 5xx other than 501, and 429 are
//! retried four times with jittered exponential backoff by default.
//! Rate-limited responses come back as a [`RateLimitError`] (see
//! [`Error::rate_limit`]) and are retried according to [`RateLimitPolicy`].
//! [`Client::with_retry_policy`] changes the policy for some calls only.
//!
//! # Testing
//!
//! With the `testing` feature, [`testing::Emulator`] runs a stateful
//! emulator of GitHub and the Actions service in your test, so an
//! autoscaler can be tested end to end without network access.
//!
//! # Logging
//!
//! The crate logs through [`tracing`]; each HTTP request runs in a
//! `scaleset.request` span. Install a subscriber to see token refreshes,
//! polls and retries.
//!
//! # Features
//!
//! * `rustls` (default): TLS through rustls.
//! * `native-tls`: TLS through the platform's library.
//! * `testing`: [`testing::Emulator`] and the `scaleset-emulator` binary.

#![warn(missing_docs, missing_debug_implementations, rust_2018_idioms)]
#![cfg_attr(docsrs, feature(doc_cfg))]

mod auth;
mod client;
mod config;
mod error;
pub mod listener;
mod retry;
mod session;
#[cfg(feature = "testing")]
#[cfg_attr(docsrs, doc(cfg(feature = "testing")))]
pub mod testing;
mod transport;
mod types;

pub use auth::{
    Credentials, JwtProvider, JwtProviderFn, Rs256Signer, RsaKeyJwtProvider, SignerJwtProvider,
    jwt_provider_fn,
};
pub use client::{
    Client, ClientBuilder, DEFAULT_TIMEOUT, HEADER_SCALE_SET_MAX_CAPACITY, MESSAGE_POLL_TIMEOUT,
    SystemInfo, http_client_builder,
};
pub use config::{FORCE_GHES_ENV, GitHubConfigUrl, Scope};
pub use error::{
    ActionsException, ApiError, ApiErrorKind, BoxError, Error, RateLimitError, RateLimitKind,
    Result,
};
pub use retry::{
    AttemptOutcome, BackoffContext, DEFAULT_MAX_RETRIES, DEFAULT_MAX_WAIT, DEFAULT_MIN_WAIT,
    RateLimitDecision, RateLimitPolicy, RateLimitRule, RetryPolicy,
};
pub use session::MessageSession;
pub use types::*;

/// Re-exported so callers can build an HTTP client without adding the
/// dependency themselves.
pub use reqwest;
/// Re-exported for [`SecretString`] fields such as
/// [`JitRunnerConfig::encoded_jit_config`].
pub use secrecy::{self, ExposeSecret, SecretString};

/// Compiles the README's examples as doctests.
#[cfg(doctest)]
#[doc = include_str!("../README.md")]
pub struct ReadmeDoctests;
