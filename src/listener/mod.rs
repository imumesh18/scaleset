//! Turning a scale set's message queue into scaling decisions.
//!
//! * [`Listener`] runs the whole loop for you. It opens the message
//!   session, acquires every available job, hands each event to your
//!   [`Scaler`], acknowledges it, and survives failures: it retries with
//!   backoff, waits out a session conflict after a restart, and opens a new
//!   session when the old one disappears.
//! * [`Subscription`] is the pull API underneath, for driving the loop
//!   yourself.
//! * [`JobTracker`] follows individual jobs, to tell busy runners from idle
//!   ones.
//!
//! # Scaling
//!
//! Every [`Event`] carries the latest [`ScaleSetStatistics`]. Keep
//! [`total_assigned_jobs`](ScaleSetStatistics::total_assigned_jobs) runners
//! available (plus any warm minimum you want). Never count job messages
//! instead: a batch holds at most 50, and a job can be assigned and
//! canceled several times before it runs.
//!
//! ```no_run
//! use scaleset::listener::{Event, Listener, Scaler};
//! use scaleset::{BoxError, Client, ScaleSetId};
//!
//! struct MyScaler;
//!
//! impl Scaler for MyScaler {
//!     type Error = BoxError;
//!
//!     async fn scale(&mut self, event: Event<'_>) -> Result<(), BoxError> {
//!         let wanted = event.statistics().total_assigned_jobs;
//!         // start runners until `wanted` are running or starting ...
//!         Ok(())
//!     }
//! }
//!
//! # async fn run(client: Client, id: ScaleSetId) -> Result<(), scaleset::listener::Error> {
//! Listener::new(client, id, 10)
//!     .run(&mut MyScaler, async { let _ = tokio::signal::ctrl_c().await; })
//!     .await
//! # }
//! ```
//!
//! # Slow provisioning
//!
//! The listener waits for `scale` before it polls again. If starting a
//! runner takes long (booting a VM), do it elsewhere: a
//! [`tokio::sync::watch::Sender`] is a scaler that publishes the latest
//! statistics, so a separate task can reconcile at its own pace while the
//! listener keeps polling.
//!
//! ```no_run
//! # async fn run(client: scaleset::Client, id: scaleset::ScaleSetId) {
//! use scaleset::ScaleSetStatistics;
//! use scaleset::listener::Listener;
//!
//! let (mut statistics, mut desired) = tokio::sync::watch::channel(ScaleSetStatistics::default());
//! tokio::spawn(async move {
//!     while desired.changed().await.is_ok() {
//!         let wanted = desired.borrow_and_update().total_assigned_jobs;
//!         // reconcile toward `wanted`, however long it takes ...
//!     }
//! });
//! Listener::new(client, id, 10).run(&mut statistics, std::future::pending()).await.unwrap();
//! # }
//! ```

mod subscription;
mod tracker;

use std::convert::Infallible;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

pub use subscription::Subscription;
pub use tracker::{JobStatus, JobTracker};

use crate::client::Client;
use crate::error::BoxError;
use crate::session::MessageSession;
use crate::types::{ScaleSetId, ScaleSetMessage, ScaleSetStatistics};

/// Consecutive failures a [`Listener`] tolerates by default.
pub const DEFAULT_MAX_FAILURES: u32 = 10;

/// Extra time a [`Listener`] gives work that must finish on shutdown.
const CLOSE_TIMEOUT: Duration = Duration::from_secs(10);

/// What a [`Scaler`] is asked to handle. Every event carries the latest
/// statistics.
#[derive(Debug, Clone, Copy)]
#[non_exhaustive]
pub enum Event<'a> {
    /// A session was opened. Sent before the first poll of each session, so
    /// the scaler converges before any message arrives.
    Initial {
        /// The session's statistics.
        statistics: &'a ScaleSetStatistics,
    },
    /// A batch of job messages.
    Message {
        /// The message.
        message: &'a ScaleSetMessage,
        /// The message's statistics, or the last known ones if it had none.
        statistics: &'a ScaleSetStatistics,
    },
    /// A poll ended without news (after about 50 seconds).
    Idle {
        /// The last known statistics.
        statistics: &'a ScaleSetStatistics,
    },
}

impl<'a> Event<'a> {
    /// The latest statistics. Scale on these.
    pub fn statistics(&self) -> &'a ScaleSetStatistics {
        match self {
            Self::Initial { statistics }
            | Self::Message { statistics, .. }
            | Self::Idle { statistics } => statistics,
        }
    }

    /// The message, for [`Event::Message`].
    pub fn message(&self) -> Option<&'a ScaleSetMessage> {
        match self {
            Self::Message { message, .. } => Some(message),
            _ => None,
        }
    }
}

/// Decides how many runners to run. See the [module docs](self).
///
/// `scale` is called once per event and never concurrently, which is why it
/// takes `&mut self`: keep whatever state you need without locks. Implement
/// it with an `async fn`.
///
/// When run by a [`Listener`], available jobs are already acquired, and a
/// message is acknowledged only after `scale` returns `Ok`. An error leaves
/// the message unacknowledged, so it is delivered again: make each step safe
/// to repeat.
pub trait Scaler: Send {
    /// The error `scale` returns: anything that converts to [`BoxError`],
    /// including `anyhow::Error`.
    type Error: Into<BoxError>;

    /// Handles one event.
    fn scale(&mut self, event: Event<'_>) -> impl Future<Output = Result<(), Self::Error>> + Send;
}

/// Publishes the latest statistics, so another task can reconcile at its
/// own pace. See [Slow provisioning](self#slow-provisioning).
impl Scaler for tokio::sync::watch::Sender<ScaleSetStatistics> {
    type Error = Infallible;

    async fn scale(&mut self, event: Event<'_>) -> Result<(), Infallible> {
        self.send_if_modified(|current| {
            let changed = current != event.statistics();
            *current = *event.statistics();
            changed
        });
        Ok(())
    }
}

/// A shared handle to the capacity reported on each poll. Clones share the
/// value, so it can be changed while a listener runs.
#[derive(Debug, Clone)]
pub struct MaxRunners(Arc<AtomicU32>);

impl MaxRunners {
    /// A handle with an initial capacity.
    pub fn new(max_runners: u32) -> Self {
        Self(Arc::new(AtomicU32::new(max_runners)))
    }

    /// Sets the capacity reported on the next poll.
    pub fn set(&self, max_runners: u32) {
        self.0.store(max_runners, Ordering::Relaxed);
    }

    /// The current capacity.
    pub fn get(&self) -> u32 {
        self.0.load(Ordering::Relaxed)
    }
}

impl From<u32> for MaxRunners {
    fn from(max_runners: u32) -> Self {
        Self::new(max_runners)
    }
}

/// Why a [`Listener`] gave up, after more than its
/// [`max_failures`](Listener::max_failures) failures in a row.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// The service kept failing: opening the session, polling, acquiring
    /// jobs or acknowledging.
    #[error("the message session kept failing")]
    Session(#[source] crate::Error),
    /// The scaler kept failing.
    #[error("the scaler kept failing")]
    Scale(#[source] BoxError),
}

/// Runs a scale set's message loop, with recovery. See the
/// [module docs](self).
///
/// On each event the listener acquires available jobs, calls the scaler, and
/// acknowledges the message. When a step fails, it backs off and tries
/// again; a message whose handling failed is delivered again, and so is the
/// initial event. It gives up after more than
/// [`max_failures`](Self::max_failures) failures in a row.
///
/// It opens the session itself. While another session is open, for example
/// one left by a previous run that crashed, it waits on standby, backing
/// off, until that session closes or expires; this does not count as a
/// failure. If its own session disappears, it opens a new one, starting
/// again with [`Event::Initial`]. On shutdown or error it closes the session.
/// Dropping the `run` future instead of completing `shutdown` skips that, so
/// the session stays open until the service expires it.
#[derive(Debug)]
#[must_use]
pub struct Listener {
    client: Client,
    scale_set_id: ScaleSetId,
    owner: String,
    max_runners: MaxRunners,
    max_failures: u32,
    min_backoff: Duration,
    max_backoff: Duration,
}

/// How a session ended.
enum Exit {
    Shutdown,
    GaveUp(Error),
    /// The session is gone; open a new one.
    SessionLost(crate::Error),
}

impl Listener {
    /// A listener for a scale set that can run up to `max_runners` runners.
    /// The capacity is reported on each poll, so the service does not
    /// assign jobs you cannot take. Pass a number, or a [`MaxRunners`]
    /// handle to change it while running (see also
    /// [`max_runners`](Self::max_runners)).
    pub fn new(
        client: Client,
        scale_set_id: ScaleSetId,
        max_runners: impl Into<MaxRunners>,
    ) -> Self {
        Self {
            client,
            scale_set_id,
            owner: default_owner(),
            max_runners: max_runners.into(),
            max_failures: DEFAULT_MAX_FAILURES,
            min_backoff: Duration::from_secs(1),
            max_backoff: Duration::from_secs(60),
        }
    }

    /// The session owner name, shown by the service. Defaults to the host
    /// name.
    pub fn owner(mut self, owner: impl Into<String>) -> Self {
        self.owner = owner.into();
        self
    }

    /// How many failures in a row to tolerate before giving up. Zero gives
    /// up on the first failure. Defaults to [`DEFAULT_MAX_FAILURES`].
    pub fn max_failures(mut self, failures: u32) -> Self {
        self.max_failures = failures;
        self
    }

    /// The wait after a failure: `min` doubled for each further failure in a
    /// row, up to `max`. Defaults to 1 s and 60 s.
    pub fn backoff(mut self, min: Duration, max: Duration) -> Self {
        self.min_backoff = min;
        self.max_backoff = max.max(min);
        self
    }

    /// A handle for changing the capacity while the listener runs.
    pub fn max_runners(&self) -> MaxRunners {
        self.max_runners.clone()
    }

    /// Runs until `shutdown` completes, or until it gives up.
    ///
    /// `shutdown` interrupts a poll, a backoff or a `scale` call. Work that
    /// has to finish so nothing is left behind, acknowledging a handled
    /// message, or closing a session that was being opened, gets up to ten
    /// more seconds. Shutdown returns `Ok(())`.
    pub async fn run<S: Scaler>(
        &self,
        scaler: &mut S,
        shutdown: impl Future<Output = ()>,
    ) -> Result<(), Error> {
        tokio::pin!(shutdown);
        let mut failures = 0;
        let mut conflicts = 0;
        loop {
            let create = self
                .client
                .create_message_session(self.scale_set_id, self.owner.clone());
            tokio::pin!(create);
            let opened = tokio::select! {
                biased;
                result = &mut create => result,
                () = shutdown.as_mut() => {
                    // The service may already have created the session; finish
                    // creating it so it can be closed rather than left behind.
                    if let Ok(Ok(session)) = tokio::time::timeout(CLOSE_TIMEOUT, create).await {
                        close(&session).await;
                    }
                    return Ok(());
                }
            };

            let exit = match opened {
                Ok(session) => {
                    conflicts = 0;
                    tracing::info!(scale_set_id = %self.scale_set_id, session_id = %session.info().session_id, "message session opened");
                    let exit = self
                        .serve(&session, scaler, shutdown.as_mut(), &mut failures)
                        .await;
                    close(&session).await;
                    exit
                }
                Err(e) if e.is_conflict() => {
                    conflicts += 1;
                    let wait = self.backoff_for(conflicts);
                    tracing::warn!(
                        ?wait,
                        "another session is open for this scale set; waiting on standby"
                    );
                    if self.sleep(wait, shutdown.as_mut()).await {
                        return Ok(());
                    }
                    continue;
                }
                Err(e) => Exit::SessionLost(e),
            };

            match exit {
                Exit::Shutdown => return Ok(()),
                Exit::GaveUp(e) => return Err(e),
                Exit::SessionLost(e) => {
                    match self
                        .failed(&mut failures, Error::Session(e), shutdown.as_mut())
                        .await
                    {
                        Some(Exit::GaveUp(e)) => return Err(e),
                        Some(_) => return Ok(()),
                        None => {}
                    }
                }
            }
        }
    }

    /// Runs one session's loop.
    async fn serve<S: Scaler, F: Future<Output = ()>>(
        &self,
        session: &MessageSession,
        scaler: &mut S,
        mut shutdown: Pin<&mut F>,
        failures: &mut u32,
    ) -> Exit {
        let mut events = session.subscribe(self.max_runners.clone());
        loop {
            let event = tokio::select! {
                biased;
                () = shutdown.as_mut() => return Exit::Shutdown,
                result = events.next() => result,
            };
            let event = match event {
                Ok(event) => event,
                Err(e) if e.is_not_found() => return Exit::SessionLost(e),
                Err(e) => match self
                    .failed(failures, Error::Session(e), shutdown.as_mut())
                    .await
                {
                    Some(exit) => return exit,
                    None => continue,
                },
            };
            let initial = matches!(event, Event::Initial { .. });

            if let Some(message) = event.message() {
                let jobs: Vec<_> = message
                    .jobs_available()
                    .map(|j| j.job.runner_request_id)
                    .collect();
                if !jobs.is_empty() {
                    let acquired = tokio::select! {
                        biased;
                        () = shutdown.as_mut() => return Exit::Shutdown,
                        result = session.acquire_jobs(&jobs) => result,
                    };
                    match acquired {
                        Ok(_) => {}
                        Err(e) if e.is_not_found() => return Exit::SessionLost(e),
                        Err(e) => match self
                            .failed(failures, Error::Session(e), shutdown.as_mut())
                            .await
                        {
                            Some(exit) => return exit,
                            None => continue,
                        },
                    }
                }
            }

            // The scaler goes first: once it has finished, its result counts
            // even if shutdown arrived at the same moment.
            let scaled = tokio::select! {
                biased;
                result = scaler.scale(event) => result.map_err(Into::<BoxError>::into),
                () = shutdown.as_mut() => return Exit::Shutdown,
            };
            if let Err(e) = scaled {
                if initial {
                    events.replay_initial();
                }
                match self
                    .failed(failures, Error::Scale(e), shutdown.as_mut())
                    .await
                {
                    Some(exit) => return exit,
                    None => continue,
                }
            }

            // The message is handled: acknowledge it even if shutdown comes
            // meanwhile, within a bound, or it would only be delivered again.
            let acked = {
                let ack = events.ack();
                tokio::pin!(ack);
                tokio::select! {
                    biased;
                    result = &mut ack => result,
                    () = shutdown.as_mut() => {
                        let _ = tokio::time::timeout(CLOSE_TIMEOUT, ack).await;
                        return Exit::Shutdown;
                    }
                }
            };
            match acked {
                Ok(()) => *failures = 0,
                Err(e) => {
                    if let Some(exit) = self
                        .failed(failures, Error::Session(e), shutdown.as_mut())
                        .await
                    {
                        return exit;
                    }
                }
            }
        }
    }

    /// Counts a failure. Gives up past the limit; otherwise waits out the
    /// backoff, unless shutdown comes first.
    async fn failed<F: Future<Output = ()>>(
        &self,
        failures: &mut u32,
        error: Error,
        shutdown: Pin<&mut F>,
    ) -> Option<Exit> {
        *failures += 1;
        if *failures > self.max_failures {
            tracing::error!(error = %error, failures = *failures, "listener giving up");
            return Some(Exit::GaveUp(error));
        }
        let wait = self.backoff_for(*failures);
        tracing::warn!(error = %error, failures = *failures, max_failures = self.max_failures, ?wait, "listener step failed, retrying");
        self.sleep(wait, shutdown).await.then_some(Exit::Shutdown)
    }

    /// The backoff after the `n`th failure in a row.
    fn backoff_for(&self, n: u32) -> Duration {
        let factor = 1u32.checked_shl(n.saturating_sub(1)).unwrap_or(u32::MAX);
        self.min_backoff
            .saturating_mul(factor)
            .min(self.max_backoff)
    }

    /// Sleeps for `wait`. Returns `true` if shutdown came first.
    async fn sleep<F: Future<Output = ()>>(&self, wait: Duration, shutdown: Pin<&mut F>) -> bool {
        tokio::select! {
            biased;
            () = shutdown => true,
            () = tokio::time::sleep(wait) => false,
        }
    }
}

/// Closes a session, bounded in time; failures are only logged.
async fn close(session: &MessageSession) {
    match tokio::time::timeout(CLOSE_TIMEOUT, session.close()).await {
        Ok(Ok(())) => {
            tracing::info!(session_id = %session.info().session_id, "message session closed")
        }
        Ok(Err(e)) if e.is_not_found() => {}
        Ok(Err(e)) => tracing::warn!(error = %e, "failed to close the message session"),
        Err(_) => tracing::warn!("timed out closing the message session"),
    }
}

fn default_owner() -> String {
    ["HOSTNAME", "COMPUTERNAME"]
        .iter()
        .find_map(|var| std::env::var(var).ok().filter(|v| !v.is_empty()))
        .unwrap_or_else(|| "scaleset".to_owned())
}
