//! Data types exchanged with the scale set APIs.

use std::fmt;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Deserializer, Serialize};
use uuid::Uuid;

macro_rules! id_type {
    ($(#[$meta:meta])* $name:ident) => {
        $(#[$meta])*
        #[derive(
            Debug, Default, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize,
        )]
        #[serde(transparent)]
        pub struct $name(pub i64);

        impl $name {
            /// Returns the raw value.
            pub const fn get(self) -> i64 {
                self.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                fmt::Display::fmt(&self.0, f)
            }
        }

        impl From<i64> for $name {
            fn from(value: i64) -> Self {
                Self(value)
            }
        }

        impl From<$name> for i64 {
            fn from(value: $name) -> Self {
                value.0
            }
        }
    };
}

id_type!(
    /// Identifies a runner scale set.
    ScaleSetId
);
id_type!(
    /// Identifies a runner group.
    RunnerGroupId
);
id_type!(
    /// Identifies a registered runner.
    RunnerId
);
id_type!(
    /// Identifies a message in a session's message queue.
    MessageId
);
id_type!(
    /// Identifies a job request (`runnerRequestId`), as passed to
    /// [`MessageSession::acquire_jobs`](crate::MessageSession::acquire_jobs).
    JobRequestId
);

impl RunnerGroupId {
    /// The ID of the `Default` runner group.
    pub const DEFAULT: Self = Self(1);
}

/// Name of the default runner group.
pub const DEFAULT_RUNNER_GROUP: &str = "default";

/// Label type the service assigns to labels created through this API.
pub const LABEL_TYPE_SYSTEM: &str = "System";

/// A label workflows use in `runs-on` to target a scale set.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Label {
    /// The label text.
    pub name: String,
    /// The label type. Empty values are sent as [`LABEL_TYPE_SYSTEM`].
    #[serde(rename = "type", default)]
    pub label_type: String,
}

impl Label {
    /// A label of type [`LABEL_TYPE_SYSTEM`].
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            label_type: LABEL_TYPE_SYSTEM.to_owned(),
        }
    }
}

impl From<&str> for Label {
    fn from(name: &str) -> Self {
        Self::new(name)
    }
}

impl From<String> for Label {
    fn from(name: String) -> Self {
        Self::new(name)
    }
}

/// A runner group.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct RunnerGroup {
    /// Runner group ID.
    pub id: RunnerGroupId,
    /// Runner group name.
    pub name: String,
    /// Number of runners in the group.
    pub size: i64,
    /// Whether this is the default group.
    #[serde(rename = "isDefaultGroup")]
    pub is_default: bool,
}

/// Runner settings of a scale set.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct RunnerSetting {
    /// Stops runners from updating themselves.
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub disable_update: bool,
}

/// Job and runner counts of a scale set.
///
/// Scale on [`total_assigned_jobs`](Self::total_assigned_jobs): it is the
/// number of jobs assigned to the scale set, waiting or running, and is always
/// current. Do not count job messages instead; batches are capped at 50
/// messages and jobs can be reassigned.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct ScaleSetStatistics {
    /// Jobs available to the scale set.
    pub total_available_jobs: u32,
    /// Jobs acquired by the scale set.
    pub total_acquired_jobs: u32,
    /// Jobs assigned to the scale set, including running ones. This is the
    /// number of runners the scale set needs.
    pub total_assigned_jobs: u32,
    /// Jobs running on the scale set's runners.
    pub total_running_jobs: u32,
    /// Runners registered with the scale set.
    pub total_registered_runners: u32,
    /// Runners running a job.
    pub total_busy_runners: u32,
    /// Runners waiting for a job.
    pub total_idle_runners: u32,
}

/// A runner scale set as returned by the service.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct RunnerScaleSet {
    /// Scale set ID.
    pub id: ScaleSetId,
    /// Scale set name.
    pub name: String,
    /// ID of the runner group the scale set belongs to.
    pub runner_group_id: RunnerGroupId,
    /// Name of the runner group the scale set belongs to.
    pub runner_group_name: String,
    /// Labels workflows can target.
    pub labels: Vec<Label>,
    /// Runner settings.
    #[serde(rename = "RunnerSetting")]
    pub runner_setting: RunnerSetting,
    /// When the scale set was created.
    #[serde(deserialize_with = "optional_time")]
    pub created_on: Option<DateTime<Utc>>,
    /// URL for generating JIT runner configs.
    pub runner_jit_config_url: Option<String>,
    /// Current statistics, when included.
    pub statistics: Option<ScaleSetStatistics>,
}

/// The desired state of a scale set, for
/// [`Client::create_runner_scale_set`](crate::Client::create_runner_scale_set)
/// and [`Client::update_runner_scale_set`](crate::Client::update_runner_scale_set).
///
/// ```
/// use scaleset::{RunnerGroupId, RunnerScaleSetSpec};
///
/// let spec = RunnerScaleSetSpec::new("my-scale-set")
///     .runner_group_id(RunnerGroupId::DEFAULT)
///     .labels(["linux", "x64"]);
/// ```
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RunnerScaleSetSpec {
    /// Scale set name. Required on create.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// Runner group to place the scale set in.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub runner_group_id: Option<RunnerGroupId>,
    /// Runner group name.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub runner_group_name: Option<String>,
    /// Labels. On create, an empty list means "use the name as the label".
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub labels: Vec<Label>,
    /// Runner settings.
    #[serde(rename = "RunnerSetting")]
    pub runner_setting: RunnerSetting,
}

impl RunnerScaleSetSpec {
    /// A spec with the given name.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: Some(name.into()),
            ..Self::default()
        }
    }

    /// Sets the name.
    #[must_use]
    pub fn name(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }

    /// Sets the runner group ID.
    #[must_use]
    pub fn runner_group_id(mut self, id: RunnerGroupId) -> Self {
        self.runner_group_id = Some(id);
        self
    }

    /// Sets the runner group name.
    #[must_use]
    pub fn runner_group_name(mut self, name: impl Into<String>) -> Self {
        self.runner_group_name = Some(name.into());
        self
    }

    /// Adds a label.
    #[must_use]
    pub fn label(mut self, label: impl Into<Label>) -> Self {
        self.labels.push(label.into());
        self
    }

    /// Adds labels.
    ///
    /// Multiple labels on GitHub Enterprise Server need 3.18 or later with
    /// the `DistributedTask.AllowRunnerScaleSetCustomLabels` feature flag.
    #[must_use]
    pub fn labels<I, L>(mut self, labels: I) -> Self
    where
        I: IntoIterator<Item = L>,
        L: Into<Label>,
    {
        self.labels.extend(labels.into_iter().map(Into::into));
        self
    }

    /// Sets whether runners may update themselves.
    #[must_use]
    pub fn disable_update(mut self, disable: bool) -> Self {
        self.runner_setting.disable_update = disable;
        self
    }

    /// Fills in label types, and on create, the default label.
    pub(crate) fn normalized(&self, creating: bool) -> Result<Self, String> {
        let mut spec = self.clone();
        if creating && spec.labels.is_empty() {
            match spec.name.as_deref() {
                Some(name) if !name.is_empty() => spec.labels.push(Label::new(name)),
                _ => return Err("runner scale set must have a name or at least one label".into()),
            }
        }
        for label in &mut spec.labels {
            if label.label_type.is_empty() {
                LABEL_TYPE_SYSTEM.clone_into(&mut label.label_type);
            }
        }
        Ok(spec)
    }
}

/// Settings for a just-in-time runner.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct JitRunnerSetting {
    /// Runner name. Must be unique within the scale set.
    pub name: String,
    /// Runner work folder. Empty uses the runner's default.
    pub work_folder: String,
}

impl JitRunnerSetting {
    /// Settings for a runner with the given name.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            work_folder: String::new(),
        }
    }

    /// Sets the work folder.
    #[must_use]
    pub fn work_folder(mut self, folder: impl Into<String>) -> Self {
        self.work_folder = folder.into();
        self
    }
}

/// A registered runner.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct RunnerReference {
    /// Runner ID.
    pub id: RunnerId,
    /// Runner name.
    pub name: String,
    /// Scale set the runner belongs to.
    pub runner_scale_set_id: ScaleSetId,
}

/// A just-in-time runner configuration.
///
/// Treat [`encoded_jit_config`](Self::encoded_jit_config) as a secret until
/// the runner has used it. It is redacted from `Debug` output.
#[derive(Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct JitRunnerConfig {
    /// The runner registered for this config.
    pub runner: Option<RunnerReference>,
    /// Pass to the runner as `ACTIONS_RUNNER_INPUT_JITCONFIG`, or as
    /// `run.sh --jitconfig <value>`.
    #[serde(rename = "encodedJITConfig")]
    pub encoded_jit_config: String,
}

impl fmt::Debug for JitRunnerConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("JitRunnerConfig")
            .field("runner", &self.runner)
            .field("encoded_jit_config", &"<redacted>")
            .finish()
    }
}

/// A message session, as created or refreshed by the service.
///
/// The message queue token is redacted from `Debug` output.
#[derive(Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct SessionInfo {
    /// Session ID.
    pub session_id: Uuid,
    /// Owner name given when the session was created.
    pub owner_name: String,
    /// The scale set the session belongs to.
    pub runner_scale_set: Option<RunnerScaleSet>,
    /// URL of the message queue.
    pub message_queue_url: String,
    /// Token for the message queue.
    pub message_queue_access_token: String,
    /// Statistics when the session was created.
    pub statistics: Option<ScaleSetStatistics>,
}

impl fmt::Debug for SessionInfo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SessionInfo")
            .field("session_id", &self.session_id)
            .field("owner_name", &self.owner_name)
            .field("runner_scale_set", &self.runner_scale_set)
            .field("message_queue_url", &self.message_queue_url)
            .field("message_queue_access_token", &"<redacted>")
            .field("statistics", &self.statistics)
            .finish()
    }
}

/// A batch of job messages from the message queue.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ScaleSetMessage {
    /// Message ID. Acknowledge it with
    /// [`MessageSession::delete_message`](crate::MessageSession::delete_message)
    /// and pass it as `last_message_id` on the next poll.
    pub id: MessageId,
    /// Current statistics. Scale on these.
    pub statistics: Option<ScaleSetStatistics>,
    /// Job messages, in delivery order. At most 50 per batch.
    pub jobs: Vec<JobMessage>,
}

impl ScaleSetMessage {
    /// `JobAvailable` messages. Acquire the ones you want with
    /// [`MessageSession::acquire_jobs`](crate::MessageSession::acquire_jobs).
    pub fn jobs_available(&self) -> impl Iterator<Item = &JobAvailable> {
        self.jobs.iter().filter_map(|m| match m {
            JobMessage::Available(j) => Some(j),
            _ => None,
        })
    }

    /// `JobAssigned` messages.
    pub fn jobs_assigned(&self) -> impl Iterator<Item = &JobAssigned> {
        self.jobs.iter().filter_map(|m| match m {
            JobMessage::Assigned(j) => Some(j),
            _ => None,
        })
    }

    /// `JobStarted` messages.
    pub fn jobs_started(&self) -> impl Iterator<Item = &JobStarted> {
        self.jobs.iter().filter_map(|m| match m {
            JobMessage::Started(j) => Some(j),
            _ => None,
        })
    }

    /// `JobCompleted` messages.
    pub fn jobs_completed(&self) -> impl Iterator<Item = &JobCompleted> {
        self.jobs.iter().filter_map(|m| match m {
            JobMessage::Completed(j) => Some(j),
            _ => None,
        })
    }
}

/// One job message in a [`ScaleSetMessage`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "messageType")]
#[non_exhaustive]
pub enum JobMessage {
    /// A job is available to the scale set.
    #[serde(rename = "JobAvailable")]
    Available(JobAvailable),
    /// A job was assigned to the scale set.
    #[serde(rename = "JobAssigned")]
    Assigned(JobAssigned),
    /// A runner started a job.
    #[serde(rename = "JobStarted")]
    Started(JobStarted),
    /// A job finished, or its assignment was canceled.
    #[serde(rename = "JobCompleted")]
    Completed(JobCompleted),
    /// A message type this crate does not know.
    #[serde(other)]
    Unknown,
}

impl JobMessage {
    /// The job details, unless the message type is unknown.
    pub fn job(&self) -> Option<&JobInfo> {
        match self {
            Self::Available(m) => Some(&m.job),
            Self::Assigned(m) => Some(&m.job),
            Self::Started(m) => Some(&m.job),
            Self::Completed(m) => Some(&m.job),
            Self::Unknown => None,
        }
    }
}

/// Details shared by all job messages.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct JobInfo {
    /// Job request ID, for [`MessageSession::acquire_jobs`](crate::MessageSession::acquire_jobs).
    pub runner_request_id: JobRequestId,
    /// Repository name.
    pub repository_name: String,
    /// Repository owner.
    pub owner_name: String,
    /// Job ID.
    pub job_id: String,
    /// Workflow reference.
    pub job_workflow_ref: String,
    /// Job display name.
    pub job_display_name: String,
    /// Workflow run ID.
    pub workflow_run_id: i64,
    /// Event that triggered the workflow.
    pub event_name: String,
    /// Labels the job requested.
    pub request_labels: Vec<String>,
    /// When the job was queued.
    #[serde(deserialize_with = "optional_time")]
    pub queue_time: Option<DateTime<Utc>>,
    /// When the job was assigned to the scale set.
    #[serde(deserialize_with = "optional_time")]
    pub scale_set_assign_time: Option<DateTime<Utc>>,
    /// When the job was assigned to a runner.
    #[serde(deserialize_with = "optional_time")]
    pub runner_assign_time: Option<DateTime<Utc>>,
    /// When the job finished.
    #[serde(deserialize_with = "optional_time")]
    pub finish_time: Option<DateTime<Utc>>,
}

/// A job is available to the scale set.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct JobAvailable {
    /// URL for acquiring the job.
    #[serde(default)]
    pub acquire_job_url: String,
    /// Job details.
    #[serde(flatten)]
    pub job: JobInfo,
}

/// A job was assigned to the scale set.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct JobAssigned {
    /// Job details.
    #[serde(flatten)]
    pub job: JobInfo,
}

/// A runner started a job.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct JobStarted {
    /// The runner's ID.
    #[serde(default)]
    pub runner_id: RunnerId,
    /// The runner's name.
    #[serde(default)]
    pub runner_name: String,
    /// Job details.
    #[serde(flatten)]
    pub job: JobInfo,
}

/// A job finished, or its assignment was canceled before a runner took it.
///
/// A job can be assigned and canceled up to three times before it runs; each
/// attempt produces its own messages.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct JobCompleted {
    /// Result, such as `succeeded`, `failed` or `canceled`.
    #[serde(default)]
    pub result: String,
    /// The runner's ID.
    #[serde(default)]
    pub runner_id: RunnerId,
    /// The runner's name.
    #[serde(default)]
    pub runner_name: String,
    /// Job details.
    #[serde(flatten)]
    pub job: JobInfo,
}

/// Wire shape of a message queue response.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct RawMessage {
    #[serde(default)]
    pub message_id: MessageId,
    #[serde(default)]
    pub message_type: String,
    #[serde(default)]
    pub body: String,
    #[serde(default)]
    pub statistics: Option<ScaleSetStatistics>,
}

pub(crate) const MESSAGE_TYPE_JOB_MESSAGES: &str = "RunnerScaleSetJobMessages";

impl ScaleSetMessage {
    pub(crate) fn from_raw(raw: RawMessage) -> Result<Self, String> {
        if raw.message_type != MESSAGE_TYPE_JOB_MESSAGES {
            return Err(format!("unsupported message type: {}", raw.message_type));
        }
        let jobs = if raw.body.is_empty() {
            Vec::new()
        } else {
            serde_json::from_str(&raw.body)
                .map_err(|e| format!("failed to decode job messages: {e}"))?
        };
        Ok(Self {
            id: raw.message_id,
            statistics: raw.statistics,
            jobs,
        })
    }
}

/// `{ "count": n, "value": [...] }`
#[derive(Debug, Deserialize)]
pub(crate) struct ListResponse<T> {
    #[serde(default)]
    pub count: usize,
    #[serde(default = "Vec::new")]
    pub value: Vec<T>,
}

/// Deserializes an optional timestamp, mapping Go's zero time to `None`.
fn optional_time<'de, D>(deserializer: D) -> Result<Option<DateTime<Utc>>, D::Error>
where
    D: Deserializer<'de>,
{
    let value = Option::<DateTime<Utc>>::deserialize(deserializer)?;
    Ok(value.filter(|t| t.timestamp() > GO_ZERO_TIME_SECS))
}

/// `0001-01-01T00:00:00Z` as a Unix timestamp.
const GO_ZERO_TIME_SECS: i64 = -62_135_596_800;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decodes_job_messages_in_order() {
        let body = serde_json::json!([
            {"messageType": "JobAvailable", "acquireJobUrl": "u", "runnerRequestId": 1,
             "queueTime": "2024-01-01T00:00:00Z", "finishTime": "0001-01-01T00:00:00Z"},
            {"messageType": "JobStarted", "runnerRequestId": 2, "runnerId": 7, "runnerName": "r"},
            {"messageType": "SomethingNew", "x": 1},
            {"messageType": "JobCompleted", "runnerRequestId": 3, "result": "canceled"},
            {"messageType": "JobAssigned", "runnerRequestId": 4, "requestLabels": ["a"]},
        ]);
        let raw = RawMessage {
            message_id: MessageId(5),
            message_type: MESSAGE_TYPE_JOB_MESSAGES.into(),
            body: body.to_string(),
            statistics: Some(ScaleSetStatistics {
                total_assigned_jobs: 3,
                ..Default::default()
            }),
        };
        let msg = ScaleSetMessage::from_raw(raw).unwrap();
        assert_eq!(msg.id, MessageId(5));
        assert_eq!(msg.jobs.len(), 5);
        assert!(matches!(msg.jobs[2], JobMessage::Unknown));

        let available: Vec<_> = msg.jobs_available().collect();
        assert_eq!(available[0].acquire_job_url, "u");
        assert_eq!(available[0].job.runner_request_id, JobRequestId(1));
        assert!(available[0].job.queue_time.is_some());
        assert_eq!(available[0].job.finish_time, None);

        let started: Vec<_> = msg.jobs_started().collect();
        assert_eq!(started[0].runner_id, RunnerId(7));
        assert_eq!(msg.jobs_completed().next().unwrap().result, "canceled");
        assert_eq!(
            msg.jobs_assigned().next().unwrap().job.request_labels,
            vec!["a"]
        );
    }

    #[test]
    fn rejects_unknown_envelope() {
        let raw = RawMessage {
            message_id: MessageId(1),
            message_type: "Other".into(),
            body: String::new(),
            statistics: None,
        };
        assert!(ScaleSetMessage::from_raw(raw).is_err());
    }

    #[test]
    fn spec_defaults_label_to_name() {
        let spec = RunnerScaleSetSpec::new("set").normalized(true).unwrap();
        assert_eq!(spec.labels, vec![Label::new("set")]);

        let spec = RunnerScaleSetSpec::default()
            .label(Label {
                name: "x".into(),
                label_type: String::new(),
            })
            .normalized(false)
            .unwrap();
        assert_eq!(spec.labels[0].label_type, LABEL_TYPE_SYSTEM);

        assert!(RunnerScaleSetSpec::default().normalized(true).is_err());
    }

    #[test]
    fn spec_serialization() {
        let spec = RunnerScaleSetSpec::new("set")
            .runner_group_id(RunnerGroupId(3))
            .normalized(true)
            .unwrap();
        assert_eq!(
            serde_json::to_value(&spec).unwrap(),
            serde_json::json!({
                "name": "set",
                "runnerGroupId": 3,
                "labels": [{"name": "set", "type": "System"}],
                "RunnerSetting": {}
            })
        );
    }

    #[test]
    fn secrets_are_redacted() {
        let jit = JitRunnerConfig {
            runner: None,
            encoded_jit_config: "secret".into(),
        };
        assert!(!format!("{jit:?}").contains("secret"));

        let session = SessionInfo {
            message_queue_access_token: "secret".into(),
            ..Default::default()
        };
        assert!(!format!("{session:?}").contains("secret"));
    }
}
