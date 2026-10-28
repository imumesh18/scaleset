//! A ready-made poll → scale → acknowledge loop.
//!
//! [`Listener`] owns the message loop. You implement [`Scaler`], which
//! decides how many runners to run.
//!
//! ```no_run
//! use scaleset::listener::{Event, Listener, Scaler};
//! use scaleset::{BoxError, Client, MessageSession};
//!
//! struct MyScaler {
//!     session: MessageSession,
//! }
//!
//! impl Scaler for MyScaler {
//!     type Error = BoxError;
//!
//!     async fn scale(&mut self, event: Event<'_>) -> Result<(), BoxError> {
//!         if let Event::Message(msg) = event {
//!             let wanted: Vec<_> = msg.jobs_available().map(|j| j.job.runner_request_id).collect();
//!             if !wanted.is_empty() {
//!                 self.session.acquire_jobs(&wanted).await?;
//!             }
//!         }
//!         if let Some(stats) = event.statistics() {
//!             // start or stop runners to reach stats.total_assigned_jobs
//!         }
//!         Ok(())
//!     }
//! }
//!
//! # async fn run(client: Client, scale_set_id: scaleset::ScaleSetId) -> Result<(), BoxError> {
//! let session = client.create_message_session(scale_set_id, "my-host").await?;
//! let listener = Listener::new(session.clone(), 10);
//! let mut scaler = MyScaler { session: session.clone() };
//!
//! let result = listener.run(&mut scaler, async { let _ = tokio::signal::ctrl_c().await; }).await;
//! session.close().await?;
//! result?;
//! # Ok(()) }
//! ```

use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use crate::error::BoxError;
use crate::session::MessageSession;
use crate::types::{MessageId, ScaleSetMessage, ScaleSetStatistics};

/// What the listener hands to [`Scaler::scale`].
#[derive(Debug, Clone, Copy)]
#[non_exhaustive]
pub enum Event<'a> {
    /// The session's statistics, delivered once before the first poll, so
    /// the scaler can converge before any message arrives.
    Initial(&'a ScaleSetStatistics),
    /// A batch of messages. It is acknowledged once `scale` returns `Ok`.
    Message(&'a ScaleSetMessage),
    /// The long poll ended without news. The listener does not repeat the
    /// last statistics; keep them if you want to keep converging while idle.
    Idle,
}

impl<'a> Event<'a> {
    /// The statistics carried by the event, if any.
    pub fn statistics(&self) -> Option<&'a ScaleSetStatistics> {
        match self {
            Self::Initial(stats) => Some(stats),
            Self::Message(msg) => msg.statistics.as_ref(),
            Self::Idle => None,
        }
    }
}

/// Decides how many runners to run. See the [module docs](self).
///
/// `scale` is called once per poll and never concurrently, which is why it
/// takes `&mut self`. It must:
///
/// * handle [`Event::Initial`] and [`Event::Idle`] as well as messages;
/// * pass every job it wants from [`ScaleSetMessage::jobs_available`] to
///   [`MessageSession::acquire_jobs`], or the job stays unassigned;
/// * scale on [`ScaleSetStatistics::total_assigned_jobs`], not on message
///   counts.
///
/// A message is acknowledged only after `scale` returns `Ok`. An error stops
/// the listener without acknowledging, so the message is redelivered when
/// polling resumes: make each step safe to repeat.
pub trait Scaler: Send {
    /// The error `scale` returns. Anything that converts to [`BoxError`],
    /// including `anyhow::Error`.
    type Error: Into<BoxError>;

    /// Handles one event.
    fn scale(&mut self, event: Event<'_>) -> impl Future<Output = Result<(), Self::Error>> + Send;
}

/// The message queue the listener reads. [`MessageSession`] implements it;
/// implement it yourself to test a scaler without HTTP, or to wrap the
/// session.
pub trait MessageSource: Send + Sync {
    /// Statistics when the session was created.
    fn initial_statistics(&self) -> Option<ScaleSetStatistics>;

    /// See [`MessageSession::get_message`].
    fn get_message(
        &self,
        last_message_id: Option<MessageId>,
        max_capacity: u32,
    ) -> impl Future<Output = crate::Result<Option<ScaleSetMessage>>> + Send;

    /// See [`MessageSession::delete_message`].
    fn delete_message(
        &self,
        message_id: MessageId,
    ) -> impl Future<Output = crate::Result<()>> + Send;
}

impl MessageSource for MessageSession {
    fn initial_statistics(&self) -> Option<ScaleSetStatistics> {
        self.info().statistics
    }

    fn get_message(
        &self,
        last_message_id: Option<MessageId>,
        max_capacity: u32,
    ) -> impl Future<Output = crate::Result<Option<ScaleSetMessage>>> + Send {
        MessageSession::get_message(self, last_message_id, max_capacity)
    }

    fn delete_message(
        &self,
        message_id: MessageId,
    ) -> impl Future<Output = crate::Result<()>> + Send {
        MessageSession::delete_message(self, message_id)
    }
}

/// Why [`Listener::run`] stopped.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// The session carried no statistics.
    #[error("session has no statistics")]
    MissingStatistics,
    /// Polling or acknowledging failed.
    #[error("message queue error: {0}")]
    Session(#[source] crate::Error),
    /// The scaler failed. The message, if any, was not acknowledged.
    #[error("scaler failed: {0}")]
    Scale(#[source] BoxError),
}

/// A handle for changing a listener's capacity while it runs.
#[derive(Debug, Clone)]
pub struct MaxRunners(Arc<AtomicU32>);

impl MaxRunners {
    /// Sets the capacity reported on the next poll.
    pub fn set(&self, max_runners: u32) {
        self.0.store(max_runners, Ordering::Relaxed);
    }

    /// The current capacity.
    pub fn get(&self) -> u32 {
        self.0.load(Ordering::Relaxed)
    }
}

/// Polls a message session and feeds a [`Scaler`].
#[derive(Debug)]
pub struct Listener<S = MessageSession> {
    source: S,
    max_runners: MaxRunners,
}

impl<S: MessageSource> Listener<S> {
    /// A listener reporting `max_runners` as the scale set's capacity.
    pub fn new(source: S, max_runners: u32) -> Self {
        Self {
            source,
            max_runners: MaxRunners(Arc::new(AtomicU32::new(max_runners))),
        }
    }

    /// Sets the capacity reported on the next poll.
    pub fn set_max_runners(&self, max_runners: u32) {
        self.max_runners.set(max_runners);
    }

    /// A handle for changing the capacity from elsewhere.
    pub fn max_runners(&self) -> MaxRunners {
        self.max_runners.clone()
    }

    /// The message source.
    pub fn source(&self) -> &S {
        &self.source
    }

    /// Runs until `shutdown` completes or an error occurs.
    ///
    /// First hands the session's statistics to the scaler as
    /// [`Event::Initial`]; then polls, calls the scaler, and acknowledges
    /// each message after the scaler succeeds.
    ///
    /// `shutdown` interrupts a poll or a `scale` call, but not an
    /// acknowledgment: once the scaler has handled a message, the listener
    /// acknowledges it before stopping. A shutdown returns `Ok(())`.
    ///
    /// The listener does not close the session; call
    /// [`MessageSession::close`] afterwards.
    pub async fn run<C>(
        &self,
        scaler: &mut C,
        shutdown: impl Future<Output = ()>,
    ) -> Result<(), Error>
    where
        C: Scaler,
    {
        tokio::pin!(shutdown);

        let stats = self
            .source
            .initial_statistics()
            .ok_or(Error::MissingStatistics)?;
        tracing::info!(
            total_assigned_jobs = stats.total_assigned_jobs,
            "handling initial session statistics"
        );
        tokio::select! {
            biased;
            () = &mut shutdown => return Ok(()),
            result = scaler.scale(Event::Initial(&stats)) => {
                result.map_err(|e| Error::Scale(e.into()))?;
            }
        }

        let mut last_message_id = None;
        loop {
            let max_capacity = self.max_runners.get();
            tracing::debug!(?last_message_id, max_capacity, "getting next message");

            let message = tokio::select! {
                biased;
                () = &mut shutdown => return Ok(()),
                result = self.source.get_message(last_message_id, max_capacity) => {
                    result.map_err(Error::Session)?
                }
            };

            let event = match &message {
                Some(msg) => Event::Message(msg),
                None => Event::Idle,
            };
            tokio::select! {
                biased;
                () = &mut shutdown => return Ok(()),
                result = scaler.scale(event) => {
                    result.map_err(|e| Error::Scale(e.into()))?;
                }
            }

            if let Some(msg) = message {
                last_message_id = Some(msg.id);
                // Not raced against shutdown: the message is handled, so
                // leaving it unacknowledged would only cause a redelivery.
                self.source
                    .delete_message(msg.id)
                    .await
                    .map_err(Error::Session)?;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::VecDeque;
    use std::sync::Mutex;

    #[derive(Default)]
    struct FakeSource {
        stats: Option<ScaleSetStatistics>,
        messages: Mutex<VecDeque<Option<ScaleSetMessage>>>,
        polls: Mutex<Vec<(Option<MessageId>, u32)>>,
        deleted: Mutex<Vec<MessageId>>,
    }

    impl MessageSource for FakeSource {
        fn initial_statistics(&self) -> Option<ScaleSetStatistics> {
            self.stats
        }

        async fn get_message(
            &self,
            last: Option<MessageId>,
            cap: u32,
        ) -> crate::Result<Option<ScaleSetMessage>> {
            self.polls.lock().unwrap().push((last, cap));
            let next = self.messages.lock().unwrap().pop_front();
            match next {
                Some(m) => Ok(m),
                None => std::future::pending().await,
            }
        }

        async fn delete_message(&self, id: MessageId) -> crate::Result<()> {
            self.deleted.lock().unwrap().push(id);
            Ok(())
        }
    }

    #[derive(Default)]
    struct Recorder {
        events: Vec<String>,
        fail_on: Option<i64>,
    }

    impl Scaler for Recorder {
        type Error = BoxError;

        async fn scale(&mut self, event: Event<'_>) -> Result<(), BoxError> {
            let label = match event {
                Event::Initial(s) => format!("initial:{}", s.total_assigned_jobs),
                Event::Message(m) => format!("message:{}", m.id),
                Event::Idle => "idle".into(),
            };
            self.events.push(label);
            if let (Event::Message(m), Some(fail)) = (event, self.fail_on) {
                if m.id.get() == fail {
                    return Err("boom".into());
                }
            }
            Ok(())
        }
    }

    fn msg(id: i64) -> Option<ScaleSetMessage> {
        Some(ScaleSetMessage {
            id: MessageId(id),
            statistics: Some(ScaleSetStatistics::default()),
            jobs: vec![],
        })
    }

    fn source(messages: Vec<Option<ScaleSetMessage>>) -> FakeSource {
        FakeSource {
            stats: Some(ScaleSetStatistics {
                total_assigned_jobs: 2,
                ..Default::default()
            }),
            messages: Mutex::new(messages.into()),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn polls_scales_and_acks() {
        let listener = Listener::new(source(vec![msg(1), None, msg(2)]), 5);
        let mut scaler = Recorder::default();

        // Stops once the source runs dry and the shutdown future completes.
        let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        let run = listener.run(&mut scaler, async {
            let _ = rx.await;
        });
        let stop = async {
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            let _ = tx.send(());
        };
        let (result, ()) = tokio::join!(run, stop);
        result.unwrap();

        assert_eq!(
            scaler.events,
            ["initial:2", "message:1", "idle", "message:2"]
        );
        let src = listener.source();
        assert_eq!(*src.deleted.lock().unwrap(), [MessageId(1), MessageId(2)]);
        let polls = src.polls.lock().unwrap();
        assert_eq!(polls[0], (None, 5));
        assert_eq!(polls[1], (Some(MessageId(1)), 5));
        assert_eq!(polls[2], (Some(MessageId(1)), 5));
        assert_eq!(polls[3], (Some(MessageId(2)), 5));
    }

    #[tokio::test]
    async fn scaler_error_skips_ack() {
        let listener = Listener::new(source(vec![msg(1), msg(2)]), 1);
        let mut scaler = Recorder {
            fail_on: Some(2),
            ..Default::default()
        };
        let err = listener
            .run(&mut scaler, std::future::pending())
            .await
            .unwrap_err();
        assert!(matches!(err, Error::Scale(_)));
        assert_eq!(*listener.source().deleted.lock().unwrap(), [MessageId(1)]);
    }

    #[tokio::test]
    async fn missing_statistics() {
        let listener = Listener::new(FakeSource::default(), 1);
        let err = listener
            .run(&mut Recorder::default(), std::future::pending())
            .await
            .unwrap_err();
        assert!(matches!(err, Error::MissingStatistics));
    }

    #[tokio::test]
    async fn capacity_handle_updates_polls() {
        let listener = Listener::new(source(vec![None, None]), 1);
        let handle = listener.max_runners();

        struct SetCap(MaxRunners);
        impl Scaler for SetCap {
            type Error = BoxError;
            async fn scale(&mut self, _: Event<'_>) -> Result<(), BoxError> {
                self.0.set(9);
                Ok(())
            }
        }

        let _ = tokio::time::timeout(
            std::time::Duration::from_millis(50),
            listener.run(&mut SetCap(handle), std::future::pending()),
        )
        .await;
        let polls = listener.source().polls.lock().unwrap();
        assert!(polls.iter().all(|&(_, cap)| cap == 9));
    }
}
