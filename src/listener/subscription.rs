//! The pull API: poll a session yourself and acknowledge explicitly.

use crate::error::Result;
use crate::session::MessageSession;
use crate::types::{MessageId, ScaleSetMessage, ScaleSetStatistics};

use super::{Event, MaxRunners};

/// Events from one message session, pulled one at a time.
///
/// This is the layer [`Listener`](super::Listener) is built on. Use it when
/// you want to drive the loop yourself, for example to acquire only some
/// jobs. Unlike the listener, it does not acquire jobs, retry failures or
/// manage the session.
///
/// ```no_run
/// use scaleset::listener::Event;
///
/// # async fn f(session: scaleset::MessageSession) -> scaleset::Result<()> {
/// let mut events = session.subscribe(10);
/// loop {
///     match events.next().await? {
///         Event::Message { message, statistics } => {
///             let jobs: Vec<_> = message.jobs_available().map(|j| j.job.runner_request_id).collect();
///             events.session().acquire_jobs(&jobs).await?;
///             // scale to statistics.total_assigned_jobs ...
///         }
///         event => { /* Initial or Idle: scale to event.statistics() */ }
///     }
///     events.ack().await?;
/// }
/// # }
/// ```
///
/// A message is acknowledged only by [`ack`](Self::ack). Calling
/// [`next`](Self::next) without acknowledging the previous message gives it
/// up: the service delivers it again.
#[derive(Debug)]
pub struct Subscription {
    session: MessageSession,
    max_runners: MaxRunners,
    statistics: ScaleSetStatistics,
    started: bool,
    current: Option<ScaleSetMessage>,
    last_acked: Option<MessageId>,
}

impl Subscription {
    pub(crate) fn new(session: MessageSession, max_runners: MaxRunners) -> Self {
        let statistics = session.info().statistics.unwrap_or_default();
        Self {
            session,
            max_runners,
            statistics,
            started: false,
            current: None,
            last_acked: None,
        }
    }

    /// The session being read.
    pub fn session(&self) -> &MessageSession {
        &self.session
    }

    /// The capacity reported on each poll.
    pub fn max_runners(&self) -> &MaxRunners {
        &self.max_runners
    }

    /// The most recent statistics.
    pub fn statistics(&self) -> &ScaleSetStatistics {
        &self.statistics
    }

    /// Waits for the next event. The first is always [`Event::Initial`],
    /// with the session's statistics; then each poll yields a message or
    /// [`Event::Idle`].
    ///
    /// Cancel safe: dropping the future loses nothing.
    pub async fn next(&mut self) -> Result<Event<'_>> {
        if !self.started {
            self.started = true;
            return Ok(Event::Initial {
                statistics: &self.statistics,
            });
        }

        if let Some(skipped) = self.current.take() {
            tracing::debug!(message_id = %skipped.id, "message not acknowledged; it will be redelivered");
        }

        let message = self
            .session
            .get_message(self.last_acked, self.max_runners.get())
            .await?;
        let Some(message) = message else {
            return Ok(Event::Idle {
                statistics: &self.statistics,
            });
        };
        if let Some(statistics) = message.statistics {
            self.statistics = statistics;
        }
        let message = self.current.insert(message);
        Ok(Event::Message {
            message,
            statistics: &self.statistics,
        })
    }

    /// Makes the next [`next`](Self::next) yield [`Event::Initial`] again,
    /// for when handling it failed.
    pub(crate) fn replay_initial(&mut self) {
        self.started = false;
    }

    /// Acknowledges the message from the last [`next`](Self::next), so it is
    /// not delivered again. Does nothing after other events.
    ///
    /// If the acknowledgment fails, the message stays unacknowledged.
    pub async fn ack(&mut self) -> Result<()> {
        let Some(id) = self.current.as_ref().map(|m| m.id) else {
            return Ok(());
        };
        self.session.delete_message(id).await?;
        self.current = None;
        self.last_acked = Some(id);
        Ok(())
    }
}
