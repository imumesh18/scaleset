//! Following individual jobs through their messages.

use std::collections::HashMap;

use crate::types::{JobMessage, JobRequestId, RunnerId, ScaleSetMessage};

/// Where a tracked job is.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum JobStatus {
    /// Assigned to the scale set, waiting for a runner.
    Assigned,
    /// Running on a runner.
    Running {
        /// The runner's ID.
        runner_id: RunnerId,
        /// The runner's name.
        runner_name: String,
    },
}

/// Follows jobs through their messages, to tell busy runners from idle ones.
///
/// Statistics say how many runners are needed; they do not say which
/// runners are running a job. Before stopping a runner to scale down, check
/// [`is_runner_busy`](Self::is_runner_busy), the way actions-runner-controller
/// uses `JobStarted` to protect busy pods.
///
/// It only knows jobs it has seen since it was created: after a restart, a
/// runner already running a job looks idle until its next job message. The
/// service still refuses to remove a runner with a running job.
///
/// A job can be assigned, canceled and assigned again up to three times
/// before a runner takes it. The tracker keys jobs by request ID, so each
/// job is counted once however often it is reassigned.
///
/// ```
/// use scaleset::listener::{Event, JobTracker};
///
/// fn on_event(tracker: &mut JobTracker, event: Event<'_>) {
///     if let Some(message) = event.message() {
///         tracker.observe(message);
///     }
///     let can_stop = |runner: &str| !tracker.is_runner_busy(runner);
///     # let _ = can_stop;
/// }
/// ```
#[derive(Debug, Clone, Default)]
pub struct JobTracker {
    jobs: HashMap<JobRequestId, JobStatus>,
}

impl JobTracker {
    /// An empty tracker.
    pub fn new() -> Self {
        Self::default()
    }

    /// Updates the tracker from a message's job messages, in order.
    pub fn observe(&mut self, message: &ScaleSetMessage) {
        for job in &message.jobs {
            match job {
                JobMessage::Assigned(m) => {
                    self.jobs
                        .entry(m.job.runner_request_id)
                        .or_insert(JobStatus::Assigned);
                }
                JobMessage::Started(m) => {
                    self.jobs.insert(
                        m.job.runner_request_id,
                        JobStatus::Running {
                            runner_id: m.runner_id,
                            runner_name: m.runner_name.clone(),
                        },
                    );
                }
                JobMessage::Completed(m) => {
                    self.jobs.remove(&m.job.runner_request_id);
                }
                _ => {}
            }
        }
    }

    /// The status of a job, if it is assigned or running.
    pub fn status(&self, job: JobRequestId) -> Option<&JobStatus> {
        self.jobs.get(&job)
    }

    /// Whether the named runner is running a job.
    pub fn is_runner_busy(&self, runner_name: &str) -> bool {
        self.busy_runners().any(|name| name == runner_name)
    }

    /// Names of the runners running a job.
    pub fn busy_runners(&self) -> impl Iterator<Item = &str> {
        self.jobs.values().filter_map(|status| match status {
            JobStatus::Running { runner_name, .. } => Some(runner_name.as_str()),
            JobStatus::Assigned => None,
        })
    }

    /// Jobs waiting for a runner.
    pub fn waiting(&self) -> usize {
        self.jobs
            .values()
            .filter(|s| **s == JobStatus::Assigned)
            .count()
    }

    /// Jobs running on a runner.
    pub fn running(&self) -> usize {
        self.jobs.len() - self.waiting()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{JobAssigned, JobCompleted, JobInfo, JobStarted, MessageId};

    fn info(id: i64) -> JobInfo {
        JobInfo {
            runner_request_id: JobRequestId(id),
            ..JobInfo::default()
        }
    }

    fn message(jobs: Vec<JobMessage>) -> ScaleSetMessage {
        ScaleSetMessage {
            id: MessageId(1),
            statistics: None,
            jobs,
        }
    }

    fn started(id: i64, runner: &str) -> JobMessage {
        JobMessage::Started(JobStarted {
            runner_id: RunnerId(id),
            runner_name: runner.into(),
            job: info(id),
        })
    }

    fn completed(id: i64, result: &str) -> JobMessage {
        JobMessage::Completed(JobCompleted {
            result: result.into(),
            job: info(id),
            ..JobCompleted::default()
        })
    }

    #[test]
    fn follows_jobs() {
        let mut tracker = JobTracker::new();
        tracker.observe(&message(vec![
            JobMessage::Assigned(JobAssigned { job: info(1) }),
            JobMessage::Assigned(JobAssigned { job: info(2) }),
            started(1, "r1"),
        ]));
        assert!(tracker.is_runner_busy("r1"));
        assert!(!tracker.is_runner_busy("r2"));
        assert_eq!((tracker.waiting(), tracker.running()), (1, 1));

        tracker.observe(&message(vec![completed(1, "succeeded")]));
        assert!(!tracker.is_runner_busy("r1"));
        assert_eq!(tracker.status(JobRequestId(2)), Some(&JobStatus::Assigned));
    }

    #[test]
    fn reassigned_job_counts_once() {
        let mut tracker = JobTracker::new();
        for _ in 0..3 {
            tracker.observe(&message(vec![
                JobMessage::Assigned(JobAssigned { job: info(7) }),
                completed(7, "canceled"),
            ]));
        }
        tracker.observe(&message(vec![JobMessage::Assigned(JobAssigned {
            job: info(7),
        })]));
        assert_eq!(tracker.waiting(), 1);

        // A late duplicate assignment does not undo the start.
        tracker.observe(&message(vec![
            started(7, "r"),
            JobMessage::Assigned(JobAssigned { job: info(7) }),
        ]));
        assert_eq!(tracker.running(), 1);
    }
}
