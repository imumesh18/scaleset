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
//! use std::collections::HashSet;
//!
//! use scaleset::listener::{Event, Listener, Scaler};
//! use scaleset::{
//!     BoxError, Client, Credentials, ExposeSecret, JitRunnerSetting, RunnerScaleSetSpec,
//!     ScaleSetId,
//! };
//!
//! /// Keeps one runner for every job assigned to the scale set.
//! struct Autoscaler {
//!     client: Client,
//!     scale_set_id: ScaleSetId,
//!     /// Runners started and not finished yet.
//!     runners: HashSet<String>,
//! }
//!
//! impl Scaler for Autoscaler {
//!     type Error = BoxError;
//!
//!     async fn scale(&mut self, event: Event<'_>) -> Result<(), BoxError> {
//!         // Runners are ephemeral: each one leaves when its job completes.
//!         if let Some(message) = event.message() {
//!             for job in message.jobs_completed() {
//!                 self.runners.remove(&job.runner_name);
//!             }
//!         }
//!         let wanted = event.statistics().total_assigned_jobs as usize;
//!         while self.runners.len() < wanted {
//!             let setting = JitRunnerSetting::unique("runner");
//!             let jit = self.client.generate_jit_config(self.scale_set_id, &setting).await?;
//!             // Start a runner (process, container, VM) with
//!             // ACTIONS_RUNNER_INPUT_JITCONFIG set to this value.
//!             let _config = jit.encoded_jit_config.expose_secret();
//!             self.runners.insert(setting.name);
//!         }
//!         Ok(())
//!     }
//! }
//!
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
//! let mut scaler = Autoscaler { client: client.clone(), scale_set_id: scale_set.id, runners: HashSet::new() };
//! Listener::new(client.clone(), scale_set.id, 10)
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
//!
//! Enable one of the two; without TLS the client cannot connect.
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

pub use auth::{Credentials, JwtProvider, Rs256Signer, SignerJwtProvider};
pub use client::{Client, ClientBuilder, MESSAGE_POLL_TIMEOUT, SystemInfo, http_client_builder};
pub use config::{FORCE_GHES_ENV, GitHubConfigUrl, Scope};
pub use error::{
    ActionsException, ApiError, ApiErrorKind, BoxError, Error, RateLimitError, RateLimitKind,
    Result,
};
pub use retry::{RateLimitPolicy, RateLimitRule, RetryPolicy};
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
