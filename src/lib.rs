//! A client for the GitHub Actions **Runner Scale Set** APIs.
//!
//! Scale sets let you build your own autoscaler for self-hosted runners,
//! without Kubernetes or actions-runner-controller. This crate provides the
//! primitives: create and manage scale sets, open a message session, poll it
//! for scaling signals, and generate just-in-time (JIT) runner configs.
//!
//! It follows the API contract of GitHub's Go client,
//! [`github.com/actions/scaleset`](https://github.com/actions/scaleset).
//!
//! # How it works
//!
//! 1. Build a [`Client`] with a GitHub App ([`Credentials::github_app`],
//!    preferred) or a personal access token.
//! 2. Create a scale set ([`Client::create_runner_scale_set`]). Its name is
//!    the label workflows target with `runs-on`.
//! 3. Open a [`MessageSession`] and poll it. The [`listener`] module does
//!    this for you.
//! 4. Scale on [`ScaleSetStatistics::total_assigned_jobs`]: for each runner
//!    you need, call [`Client::generate_jit_config`] and start a runner with
//!    the result.
//!
//! ```no_run
//! use scaleset::listener::{Event, Listener, Scaler};
//! use scaleset::{
//!     BoxError, Client, Credentials, JitRunnerSetting, MessageSession, RunnerGroupId,
//!     RunnerScaleSetSpec, ScaleSetId,
//! };
//!
//! struct Autoscaler {
//!     client: Client,
//!     session: MessageSession,
//!     scale_set_id: ScaleSetId,
//!     running: u32,
//! }
//!
//! impl Scaler for Autoscaler {
//!     type Error = BoxError;
//!
//!     async fn scale(&mut self, event: Event<'_>) -> Result<(), BoxError> {
//!         if let Event::Message(msg) = event {
//!             let jobs: Vec<_> = msg.jobs_available().map(|j| j.job.runner_request_id).collect();
//!             if !jobs.is_empty() {
//!                 self.session.acquire_jobs(&jobs).await?;
//!             }
//!         }
//!         let Some(stats) = event.statistics() else { return Ok(()) };
//!         while self.running < stats.total_assigned_jobs {
//!             let name = format!("runner-{}", self.running);
//!             let jit = self
//!                 .client
//!                 .generate_jit_config(self.scale_set_id, &JitRunnerSetting::new(name))
//!                 .await?;
//!             // start a runner with jit.encoded_jit_config ...
//!             self.running += 1;
//!         }
//!         Ok(())
//!     }
//! }
//!
//! # async fn run() -> Result<(), BoxError> {
//! let client = Client::builder(
//!     "https://github.com/my-org",
//!     Credentials::github_app("Iv1.abc123", 12345, &std::fs::read_to_string("app.pem")?)?,
//! )
//! .build()?;
//!
//! let scale_set = client
//!     .create_runner_scale_set(
//!         &RunnerScaleSetSpec::new("my-scale-set").runner_group_id(RunnerGroupId::DEFAULT),
//!     )
//!     .await?;
//!
//! let session = client.create_message_session(scale_set.id, "my-host").await?;
//! let mut scaler = Autoscaler {
//!     client: client.clone(),
//!     session: session.clone(),
//!     scale_set_id: scale_set.id,
//!     running: 0,
//! };
//! let result = Listener::new(session.clone(), 10)
//!     .run(&mut scaler, async { let _ = tokio::signal::ctrl_c().await; })
//!     .await;
//!
//! session.close().await?;
//! client.delete_runner_scale_set(scale_set.id).await?;
//! result?;
//! # Ok(()) }
//! ```
//!
//! # The message API
//!
//! * [`MessageSession::get_message`] is a long poll: it returns at once when
//!   there is news, and otherwise returns `None` after about 50 seconds.
//! * Acknowledge each message with [`MessageSession::delete_message`] after
//!   handling it; unacknowledged messages are redelivered.
//! * Pass the last handled message ID to the next poll.
//! * Scale on statistics, not message counts: a batch holds at most 50
//!   messages, and a job can be assigned, canceled and requeued up to three
//!   times, each time with new messages.
//!
//! # Transport and retries
//!
//! The HTTP client is yours ([`ClientBuilder::http_client`], with defaults
//! from [`http_client_builder`]); retries belong to the SDK
//! ([`RetryPolicy`]). By default, transport errors, 5xx except 501, and 429
//! are retried four times with jittered exponential backoff. Rate-limited
//! responses surface as [`RateLimitError`] through [`Error::rate_limit`]
//! and are retried according to [`RateLimitPolicy`].
//!
//! # Logging
//!
//! The crate logs through [`tracing`]. Install a subscriber to see token
//! refreshes, polls and retries.
//!
//! # Features
//!
//! * `rustls` (default): TLS through rustls.
//! * `native-tls`: TLS through the platform's library.

#![warn(missing_docs, missing_debug_implementations, rust_2018_idioms)]
#![cfg_attr(docsrs, feature(doc_cfg))]

mod auth;
mod client;
mod config;
mod error;
pub mod listener;
mod retry;
mod session;
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

/// Compiles the README's examples as doctests.
#[cfg(doctest)]
#[doc = include_str!("../README.md")]
pub struct ReadmeDoctests;
