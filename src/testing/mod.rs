//! A stateful emulator of the scale set APIs, for tests.
//!
//! Enable the `testing` feature:
//!
//! ```toml
//! [dev-dependencies]
//! scaleset = { version = "0.1", features = ["testing"] }
//! ```
//!
//! [`Emulator`] runs an in-process HTTP server that plays GitHub and the
//! Actions service: the token exchange, scale sets, runners, JIT configs,
//! message sessions with long polling, redelivery until acknowledgment, and
//! a job lifecycle with statistics computed from state. Tests drive it
//! (queue a job, start or complete it on a runner, expire a token, inject a
//! fault) and inspect it.
//!
//! ```
//! use std::time::Duration;
//! use scaleset::testing::{Emulator, JobState};
//! use scaleset::{JitRunnerSetting, RunnerScaleSetSpec};
//!
//! # #[tokio::main(flavor = "current_thread")]
//! # async fn main() -> scaleset::Result<()> {
//! let emu = Emulator::builder().long_poll(Duration::from_millis(100)).start().await;
//! let client = emu.client();
//!
//! let set = client.create_runner_scale_set(&RunnerScaleSetSpec::new("linux")).await?;
//! let session = client.create_message_session(set.id, "test").await?;
//!
//! let job = emu.queue_job(set.id, ["linux"]);
//! let msg = session.get_message(None, 10).await?.expect("a message");
//! assert_eq!(msg.jobs_available().next().unwrap().job.runner_request_id, job);
//!
//! session.acquire_jobs(&[job]).await?;
//! session.delete_message(msg.id).await?;
//! client.generate_jit_config(set.id, &JitRunnerSetting::new("r1")).await?;
//! emu.start_job(job, Some("r1"));
//! assert_eq!(emu.state().jobs[0].state, JobState::Running);
//! # Ok(()) }
//! ```
//!
//! # Fidelity
//!
//! The emulator follows the protocol this crate and GitHub's Go client
//! implement. Where the real service's behavior is undocumented, it makes a
//! reasonable choice:
//!
//! * Acquired jobs are assigned to the scale set at once.
//! * Runners from JIT configs are ephemeral: completing their job removes
//!   them.
//! * A poll returns the oldest unacknowledged message newer than
//!   `lastMessageId`, else a new batch of up to 50 job messages, else waits
//!   for [`long_poll`](EmulatorBuilder::long_poll) and returns 202.
//! * Error statuses and exception names for conflicts are approximations.
//! * App JWTs are checked for shape, `RS256`, an issuer and expiry, but not
//!   for their signature.
//!
//! The `scaleset-emulator` binary serves the same emulator for clients in
//! other languages, driven through a JSON control API under `/_emulator`.

mod server;
mod state;

use std::fmt;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tokio::sync::Notify;

use crate::auth::Credentials;
use crate::client::{Client, ClientBuilder};
use crate::retry::RetryPolicy;
use crate::types::{JobRequestId, RunnerId, ScaleSetId, ScaleSetStatistics};

pub use state::{EmulatedJob, EmulatedRunner, EmulatorState, JobState};

/// Personal access token the emulator accepts by default.
pub const DEFAULT_PAT: &str = "ghp_emulator";

/// Organization the emulator's config URL points at.
pub const ORG: &str = "org";

/// Settings of an [`Emulator`].
#[derive(Debug, Clone)]
pub(crate) struct EmulatorConfig {
    pub pat: String,
    pub app_installation_id: Option<i64>,
    pub admin_token_ttl: Duration,
    pub long_poll: Duration,
    pub auto_run: Option<Duration>,
    pub runner_groups: Vec<String>,
}

impl Default for EmulatorConfig {
    fn default() -> Self {
        Self {
            pat: DEFAULT_PAT.into(),
            app_installation_id: None,
            admin_token_ttl: Duration::from_secs(3600),
            long_poll: Duration::from_secs(50),
            auto_run: None,
            runner_groups: Vec::new(),
        }
    }
}

/// Configures an [`Emulator`]. See [`Emulator::builder`].
#[derive(Debug, Clone, Default)]
#[must_use]
pub struct EmulatorBuilder {
    config: EmulatorConfig,
    addr: Option<SocketAddr>,
}

impl EmulatorBuilder {
    /// The personal access token to accept. Defaults to [`DEFAULT_PAT`].
    pub fn pat(mut self, token: impl Into<String>) -> Self {
        self.config.pat = token.into();
        self
    }

    /// Accepts GitHub App JWTs for this installation ID.
    pub fn github_app(mut self, installation_id: i64) -> Self {
        self.config.app_installation_id = Some(installation_id);
        self
    }

    /// Lifetime of issued admin tokens. Under a minute makes the client
    /// refresh on every call. Defaults to an hour.
    pub fn admin_token_ttl(mut self, ttl: Duration) -> Self {
        self.config.admin_token_ttl = ttl;
        self
    }

    /// How long an idle poll is held before a 202. The service holds it for
    /// about 50 seconds, the default; tests usually want much less.
    pub fn long_poll(mut self, hold: Duration) -> Self {
        self.config.long_poll = hold;
        self
    }

    /// Plays the runners: assigned jobs start on idle runners as soon as
    /// both exist, and complete successfully after `duration`.
    pub fn auto_run_jobs(mut self, duration: Duration) -> Self {
        self.config.auto_run = Some(duration);
        self
    }

    /// Adds a runner group besides `Default` (ID 1). Groups get IDs from 2
    /// in the order added.
    pub fn runner_group(mut self, name: impl Into<String>) -> Self {
        self.config.runner_groups.push(name.into());
        self
    }

    /// The address to listen on. Defaults to an ephemeral port on
    /// `127.0.0.1`.
    pub fn bind(mut self, addr: SocketAddr) -> Self {
        self.addr = Some(addr);
        self
    }

    /// Starts the emulator.
    ///
    /// # Panics
    ///
    /// If the address cannot be bound.
    pub async fn start(self) -> Emulator {
        let addr = self
            .addr
            .unwrap_or_else(|| SocketAddr::from(([127, 0, 0, 1], 0)));
        let listener = tokio::net::TcpListener::bind(addr)
            .await
            .expect("failed to bind the emulator");
        let addr = listener.local_addr().expect("listener address");
        let base_url = format!("http://{addr}");
        let inner = Arc::new(Inner {
            state: Mutex::new(state::State::new(self.config, base_url)),
            changed: Notify::new(),
            requests: Mutex::new(Vec::new()),
        });
        let server = tokio::spawn(server::serve(listener, Arc::clone(&inner)));
        Emulator {
            inner,
            addr,
            server,
        }
    }
}

pub(crate) struct Inner {
    pub state: Mutex<state::State>,
    /// Woken on every state change, for long polls.
    pub changed: Notify,
    pub requests: Mutex<Vec<RecordedRequest>>,
}

impl Inner {
    pub fn lock(&self) -> MutexGuard<'_, state::State> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Wakes long polls and, when runners are emulated, starts jobs.
    pub fn on_change(self: &Arc<Self>) {
        let (started, duration) = {
            let mut state = self.lock();
            match state.config.auto_run {
                Some(d) => (state.match_jobs(), d),
                None => (Vec::new(), Duration::ZERO),
            }
        };
        for job in started {
            let inner = Arc::clone(self);
            tokio::spawn(async move {
                tokio::time::sleep(duration).await;
                let _ = inner.lock().complete_job(job, None);
                inner.on_change();
            });
        }
        self.changed.notify_waiters();
    }
}

/// A running emulator. The server stops when this is dropped.
pub struct Emulator {
    inner: Arc<Inner>,
    addr: SocketAddr,
    server: tokio::task::JoinHandle<()>,
}

impl fmt::Debug for Emulator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Emulator")
            .field("addr", &self.addr)
            .finish_non_exhaustive()
    }
}

impl Drop for Emulator {
    fn drop(&mut self) {
        self.server.abort();
    }
}

impl Emulator {
    /// Starts an emulator with default settings.
    pub async fn start() -> Self {
        Self::builder().start().await
    }

    /// Configures an emulator.
    pub fn builder() -> EmulatorBuilder {
        EmulatorBuilder::default()
    }

    /// The server's base URL, such as `http://127.0.0.1:41234`.
    pub fn uri(&self) -> String {
        format!("http://{}", self.addr)
    }

    /// The address the server listens on.
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    /// The organization config URL to register scale sets at. The host is
    /// not github.com, so clients treat it as GitHub Enterprise Server.
    pub fn config_url(&self) -> String {
        format!("{}/{ORG}", self.uri())
    }

    /// Credentials the emulator accepts.
    pub fn credentials(&self) -> Credentials {
        Credentials::personal_access_token(self.inner.lock().config.pat.clone())
            .expect("non-empty PAT")
    }

    /// A client builder pointed at the emulator, with retries that wait
    /// milliseconds instead of seconds.
    pub fn client_builder(&self) -> ClientBuilder {
        Client::builder(self.config_url(), self.credentials()).retry_policy(
            RetryPolicy::default()
                .min_wait(Duration::from_millis(1))
                .max_wait(Duration::from_millis(10)),
        )
    }

    /// A client pointed at the emulator. See [`client_builder`](Self::client_builder).
    pub fn client(&self) -> Client {
        self.client_builder()
            .build()
            .expect("valid emulator client")
    }

    /// A copy of the current state.
    pub fn state(&self) -> EmulatorState {
        self.inner.lock().snapshot()
    }

    /// Statistics of a scale set, as the service would report them.
    pub fn statistics(&self, scale_set_id: ScaleSetId) -> Option<ScaleSetStatistics> {
        self.state().statistics(scale_set_id)
    }

    /// Queues a job for a scale set; empty `labels` uses the scale set name.
    ///
    /// # Panics
    ///
    /// If the scale set does not exist.
    pub fn queue_job<I, S>(&self, scale_set_id: ScaleSetId, labels: I) -> JobRequestId
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let labels = labels.into_iter().map(Into::into).collect();
        let id = self
            .inner
            .lock()
            .queue_job(scale_set_id, labels)
            .unwrap_or_else(|e| panic!("queue_job: {}", e.message));
        self.inner.on_change();
        id
    }

    /// Starts an assigned job on the named runner, or on any idle runner of
    /// its scale set.
    ///
    /// # Panics
    ///
    /// If the job is not assigned or no matching runner is idle.
    pub fn start_job(&self, job: JobRequestId, runner_name: Option<&str>) -> RunnerId {
        let runner = self
            .inner
            .lock()
            .start_job(job, runner_name)
            .unwrap_or_else(|e| panic!("start_job: {}", e.message));
        self.inner.on_change();
        runner
    }

    /// Completes a job with `result` (default `succeeded`, or `canceled` if
    /// it never started), removing its runner.
    ///
    /// # Panics
    ///
    /// If the job does not exist.
    pub fn complete_job(&self, job: JobRequestId, result: Option<&str>) {
        self.inner
            .lock()
            .complete_job(job, result)
            .unwrap_or_else(|e| panic!("complete_job: {}", e.message));
        self.inner.on_change();
    }

    /// Makes every admin token stop working, as if they expired.
    pub fn expire_admin_tokens(&self) {
        self.inner.lock().expire_admin_tokens();
    }

    /// Makes the queue token of the scale set's session stop working. The
    /// next queue call gets a 401 until the session is refreshed.
    ///
    /// # Panics
    ///
    /// If the scale set has no session.
    pub fn expire_queue_token(&self, scale_set_id: ScaleSetId) {
        assert!(
            self.inner.lock().expire_queue_token(scale_set_id),
            "scale set {scale_set_id} has no session"
        );
    }

    /// Adds a fault. Faults are checked in the order added.
    pub fn inject(&self, fault: Fault) {
        self.inner.lock().faults.push(fault);
    }

    /// Removes all faults.
    pub fn clear_faults(&self) {
        self.inner.lock().faults.clear();
    }

    /// Every request served so far.
    pub fn requests(&self) -> Vec<RecordedRequest> {
        self.inner
            .requests
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    /// Requests served for one endpoint.
    pub fn requests_to(&self, endpoint: Endpoint) -> Vec<RecordedRequest> {
        self.requests()
            .into_iter()
            .filter(|r| r.endpoint == endpoint)
            .collect()
    }

    /// Clears all state, faults and recorded requests. Issued tokens stop
    /// working.
    pub fn reset(&self) {
        {
            let mut state = self.inner.lock();
            let config = state.config.clone();
            let base = state.base_url.clone();
            *state = state::State::new(config, base);
        }
        self.inner
            .requests
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clear();
        self.inner.on_change();
    }

    /// Waits until `condition` holds for the state, checking after every
    /// change, and returns that state.
    ///
    /// # Panics
    ///
    /// If `timeout` passes first. The message includes the last state.
    pub async fn wait_until<F>(&self, timeout: Duration, mut condition: F) -> EmulatorState
    where
        F: FnMut(&EmulatorState) -> bool,
    {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let notified = self.inner.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            let state = self.state();
            if condition(&state) {
                return state;
            }
            // Also re-check periodically, in case a change raced the check.
            let tick = tokio::time::sleep(Duration::from_millis(50));
            tokio::select! {
                () = notified => {}
                () = tick => {}
                () = tokio::time::sleep_until(deadline) => {
                    panic!("condition not met within {timeout:?}; state: {state:#?}");
                }
            }
        }
    }
}

/// A group of emulator endpoints, for faults and request inspection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub enum Endpoint {
    /// Matches every endpoint (faults only).
    Any,
    /// `POST /app/installations/{id}/access_tokens`
    AccessToken,
    /// `POST …/actions/runners/registration-token`
    RegistrationToken,
    /// `POST /actions/runner-registration`
    AdminConnection,
    /// Runner group lookups.
    RunnerGroups,
    /// Scale set create, read, update, delete.
    ScaleSets,
    /// `POST …/generatejitconfig`
    GenerateJitConfig,
    /// Runner lookups and removal.
    Runners,
    /// Session create, refresh, delete.
    Sessions,
    /// Message poll.
    GetMessage,
    /// Message acknowledgment.
    DeleteMessage,
    /// `POST …/acquirejobs`
    AcquireJobs,
    /// The `/_emulator` control API.
    Control,
    /// Anything else.
    Unknown,
}

/// What a fault does.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "camelCase")]
#[non_exhaustive]
pub enum FaultResponse {
    /// Respond with this status and an empty JSON error.
    Status {
        /// HTTP status.
        status: u16,
    },
    /// Respond as a rate limit: 429, or 403 with headers and message.
    #[serde(rename_all = "camelCase")]
    RateLimit {
        /// 429 or 403.
        status: u16,
        /// `Retry-After` in seconds.
        retry_after_secs: Option<u64>,
        /// Primary (`X-RateLimit-Remaining: 0`) rather than secondary.
        primary: bool,
    },
    /// Wait, then handle the request normally.
    #[serde(rename_all = "camelCase")]
    Delay {
        /// Milliseconds to wait.
        millis: u64,
    },
}

/// A failure the emulator injects into matching requests.
///
/// ```
/// use std::time::Duration;
/// use scaleset::testing::{Endpoint, Fault};
///
/// let five_hundreds = Fault::status(Endpoint::GetMessage, 503).times(2);
/// let slow = Fault::delay(Endpoint::GenerateJitConfig, Duration::from_millis(200));
/// let limited = Fault::secondary_rate_limit(Endpoint::ScaleSets, Some(Duration::from_secs(1)));
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Fault {
    /// Requests the fault applies to.
    pub endpoint: Endpoint,
    /// What happens to them.
    pub response: FaultResponse,
    /// How many requests it applies to; `None` for all.
    #[serde(default)]
    pub times: Option<u32>,
}

impl Fault {
    /// Respond with `status`.
    pub fn status(endpoint: Endpoint, status: u16) -> Self {
        Self::new(endpoint, FaultResponse::Status { status })
    }

    /// Respond 403 with a spent primary quota.
    pub fn primary_rate_limit(endpoint: Endpoint, retry_after: Option<Duration>) -> Self {
        Self::new(
            endpoint,
            FaultResponse::RateLimit {
                status: 403,
                retry_after_secs: retry_after.map(|d| d.as_secs()),
                primary: true,
            },
        )
    }

    /// Respond 403 with a secondary-rate-limit message.
    pub fn secondary_rate_limit(endpoint: Endpoint, retry_after: Option<Duration>) -> Self {
        Self::new(
            endpoint,
            FaultResponse::RateLimit {
                status: 403,
                retry_after_secs: retry_after.map(|d| d.as_secs()),
                primary: false,
            },
        )
    }

    /// Respond 429.
    pub fn too_many_requests(endpoint: Endpoint, retry_after: Option<Duration>) -> Self {
        Self::new(
            endpoint,
            FaultResponse::RateLimit {
                status: 429,
                retry_after_secs: retry_after.map(|d| d.as_secs()),
                primary: false,
            },
        )
    }

    /// Wait before handling the request.
    pub fn delay(endpoint: Endpoint, delay: Duration) -> Self {
        Self::new(
            endpoint,
            FaultResponse::Delay {
                millis: u64::try_from(delay.as_millis()).unwrap_or(u64::MAX),
            },
        )
    }

    fn new(endpoint: Endpoint, response: FaultResponse) -> Self {
        Self {
            endpoint,
            response,
            times: None,
        }
    }

    /// Applies to the next `n` matching requests only.
    #[must_use]
    pub fn times(mut self, n: u32) -> Self {
        self.times = Some(n);
        self
    }
}

/// A request the emulator served.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RecordedRequest {
    /// HTTP method.
    pub method: String,
    /// Path.
    pub path: String,
    /// Query string, if any.
    pub query: Option<String>,
    /// The endpoint group.
    pub endpoint: Endpoint,
    /// The status returned.
    pub status: u16,
    /// The `User-Agent` header.
    pub user_agent: Option<String>,
}
