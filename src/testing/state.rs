//! The emulator's state and the rules of the scale set protocol.
//!
//! Everything here is synchronous and free of HTTP, so the server stays a
//! thin translation layer.

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use base64::Engine;
use base64::engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD};
use chrono::Utc;
use secrecy::ExposeSecret;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::{EmulatorConfig, Fault};
use crate::types::{
    JobAssigned, JobAvailable, JobCompleted, JobInfo, JobMessage, JobRequestId, JobStarted, Label,
    MessageId, RunnerGroup, RunnerGroupId, RunnerId, RunnerReference, RunnerScaleSet,
    RunnerSetting, ScaleSetId, ScaleSetStatistics, SessionInfo,
};

/// Most job messages the service puts in one batch.
pub(crate) const MAX_BATCH: usize = 50;

/// Where a job is in its lifecycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum JobState {
    /// Queued; a `JobAvailable` message was sent. Waiting to be acquired.
    Available,
    /// Acquired and assigned to the scale set; waiting for an idle runner.
    Assigned,
    /// Running on a runner.
    Running,
    /// Finished or canceled.
    Completed,
}

/// A job in the emulator.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EmulatedJob {
    /// The job request ID.
    pub id: JobRequestId,
    /// The scale set the job targets.
    pub scale_set_id: ScaleSetId,
    /// Labels the job requested.
    pub labels: Vec<String>,
    /// Lifecycle state.
    pub state: JobState,
    /// The runner running or that ran the job.
    pub runner_id: Option<RunnerId>,
    /// Result, once completed.
    pub result: Option<String>,
}

/// A runner registered through a JIT config.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EmulatedRunner {
    /// Runner ID.
    pub id: RunnerId,
    /// Runner name.
    pub name: String,
    /// The scale set it belongs to.
    pub scale_set_id: ScaleSetId,
    /// The job it is running, if busy.
    pub job: Option<JobRequestId>,
}

/// An open message session.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EmulatedSession {
    /// Session ID.
    pub session_id: Uuid,
    /// The scale set it belongs to.
    pub scale_set_id: ScaleSetId,
    /// Owner name given at creation.
    pub owner_name: String,
}

/// A point-in-time copy of the emulator's state.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EmulatorState {
    /// Scale sets, with current statistics.
    pub scale_sets: Vec<RunnerScaleSet>,
    /// Open sessions.
    pub sessions: Vec<EmulatedSession>,
    /// Registered runners.
    pub runners: Vec<EmulatedRunner>,
    /// All jobs, including completed ones.
    pub jobs: Vec<EmulatedJob>,
    /// Messages delivered but not yet acknowledged, by scale set.
    pub unacknowledged: BTreeMap<ScaleSetId, Vec<MessageId>>,
    /// Job messages not yet delivered by a poll, by scale set.
    pub undelivered: BTreeMap<ScaleSetId, usize>,
}

impl EmulatorState {
    /// Whether every message was delivered and acknowledged.
    pub fn queues_drained(&self) -> bool {
        self.unacknowledged.is_empty() && self.undelivered.is_empty()
    }

    /// The scale set with this ID.
    pub fn scale_set(&self, id: ScaleSetId) -> Option<&RunnerScaleSet> {
        self.scale_sets.iter().find(|s| s.id == id)
    }

    /// Statistics of a scale set.
    pub fn statistics(&self, id: ScaleSetId) -> Option<ScaleSetStatistics> {
        self.scale_set(id).and_then(|s| s.statistics)
    }

    /// Jobs of a scale set in the given state.
    pub fn jobs_in(&self, id: ScaleSetId, state: JobState) -> impl Iterator<Item = &EmulatedJob> {
        self.jobs
            .iter()
            .filter(move |j| j.scale_set_id == id && j.state == state)
    }
}

/// An error the service would return.
#[derive(Debug, Clone)]
pub(crate) struct ServiceError {
    pub status: u16,
    pub type_name: Option<&'static str>,
    pub message: String,
}

impl ServiceError {
    pub fn new(status: u16, message: impl Into<String>) -> Self {
        Self {
            status,
            type_name: None,
            message: message.into(),
        }
    }

    pub fn exception(status: u16, type_name: &'static str, message: impl Into<String>) -> Self {
        Self {
            status,
            type_name: Some(type_name),
            message: message.into(),
        }
    }

    pub fn unauthorized() -> Self {
        Self::new(401, "Bad credentials")
    }

    pub fn not_found(what: &str) -> Self {
        Self::new(404, format!("{what} not found"))
    }
}

pub(crate) type ServiceResult<T> = Result<T, ServiceError>;

struct ScaleSetEntry {
    set: RunnerScaleSet,
    session: Option<Session>,
    queue: Queue,
}

struct Session {
    info: SessionInfo,
    /// Whether the current queue token is still accepted.
    token_valid: bool,
}

#[derive(Default)]
struct Queue {
    /// Job messages not yet put in a batch.
    pending: VecDeque<JobMessage>,
    /// Delivered batches waiting for acknowledgment.
    unacked: BTreeMap<i64, Vec<JobMessage>>,
    next_message_id: i64,
}

struct Job {
    public: EmulatedJob,
    info: JobInfo,
}

/// The emulator's mutable state.
pub(crate) struct State {
    pub config: EmulatorConfig,
    pub base_url: String,
    next_id: i64,
    installation_tokens: HashSet<String>,
    registration_tokens: HashSet<String>,
    admin_tokens: HashMap<String, SystemTime>,
    groups: Vec<RunnerGroup>,
    scale_sets: BTreeMap<ScaleSetId, ScaleSetEntry>,
    runners: BTreeMap<RunnerId, EmulatedRunner>,
    jobs: BTreeMap<JobRequestId, Job>,
    pub faults: Vec<Fault>,
}

impl State {
    pub fn new(config: EmulatorConfig, base_url: String) -> Self {
        let mut groups = vec![RunnerGroup {
            id: RunnerGroupId::DEFAULT,
            name: "Default".into(),
            size: 0,
            is_default: true,
        }];
        for (i, name) in config.runner_groups.iter().enumerate() {
            groups.push(RunnerGroup {
                id: RunnerGroupId(2 + i64::try_from(i).unwrap_or(0)),
                name: name.clone(),
                size: 0,
                is_default: false,
            });
        }
        Self {
            config,
            base_url,
            next_id: 1000,
            installation_tokens: HashSet::new(),
            registration_tokens: HashSet::new(),
            admin_tokens: HashMap::new(),
            groups,
            scale_sets: BTreeMap::new(),
            runners: BTreeMap::new(),
            jobs: BTreeMap::new(),
            faults: Vec::new(),
        }
    }

    fn id(&mut self) -> i64 {
        self.next_id += 1;
        self.next_id
    }

    fn token(prefix: &str) -> String {
        format!("{prefix}_{:032x}", fastrand::u128(..))
    }

    // ---------------------------------------------------------------------
    // Authentication

    /// Exchanges an app JWT for an installation token.
    pub fn installation_token(
        &mut self,
        installation_id: i64,
        bearer: Option<&str>,
    ) -> ServiceResult<String> {
        let Some(expected) = self.config.app_installation_id else {
            return Err(ServiceError::not_found("installation"));
        };
        if installation_id != expected {
            return Err(ServiceError::not_found("installation"));
        }
        let jwt = bearer.ok_or_else(ServiceError::unauthorized)?;
        if !valid_app_jwt(jwt) {
            return Err(ServiceError::new(
                401,
                "A JSON web token could not be decoded",
            ));
        }
        let token = Self::token("ghs");
        self.installation_tokens.insert(token.clone());
        Ok(token)
    }

    /// Issues a runner registration token for a PAT or installation token.
    pub fn registration_token(&mut self, bearer: Option<&str>) -> ServiceResult<String> {
        let bearer = bearer.ok_or_else(ServiceError::unauthorized)?;
        if bearer != self.config.pat && !self.installation_tokens.contains(bearer) {
            return Err(ServiceError::unauthorized());
        }
        let token = Self::token("reg");
        self.registration_tokens.insert(token.clone());
        Ok(token)
    }

    /// Exchanges a registration token for the Actions service URL and an
    /// admin token.
    pub fn admin_connection(
        &mut self,
        remote_auth: Option<&str>,
    ) -> ServiceResult<(String, String)> {
        let token = remote_auth.ok_or_else(ServiceError::unauthorized)?;
        if !self.registration_tokens.contains(token) {
            return Err(ServiceError::unauthorized());
        }
        let expires = SystemTime::now() + self.config.admin_token_ttl;
        let exp = expires
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or_default();
        let header = URL_SAFE_NO_PAD.encode(br#"{"alg":"none","typ":"JWT"}"#);
        let payload = URL_SAFE_NO_PAD.encode(
            serde_json::json!({ "exp": exp, "nonce": format!("{:x}", fastrand::u64(..)) })
                .to_string(),
        );
        let jwt = format!("{header}.{payload}.");
        self.admin_tokens.insert(jwt.clone(), expires);
        Ok((format!("{}/tenant/", self.base_url), jwt))
    }

    /// Checks an admin token.
    pub fn check_admin(&self, bearer: Option<&str>) -> ServiceResult<()> {
        match bearer.and_then(|b| self.admin_tokens.get(b)) {
            Some(exp) if *exp > SystemTime::now() => Ok(()),
            _ => Err(ServiceError::unauthorized()),
        }
    }

    /// Revokes every admin token.
    pub fn expire_admin_tokens(&mut self) {
        self.admin_tokens.clear();
    }

    // ---------------------------------------------------------------------
    // Runner groups and scale sets

    pub fn runner_groups(&self, name: Option<&str>) -> Vec<RunnerGroup> {
        self.groups
            .iter()
            .filter(|g| name.is_none_or(|n| g.name.eq_ignore_ascii_case(n)))
            .cloned()
            .collect()
    }

    fn group(&self, id: RunnerGroupId) -> ServiceResult<&RunnerGroup> {
        self.groups
            .iter()
            .find(|g| g.id == id)
            .ok_or_else(|| ServiceError::not_found("runner group"))
    }

    fn statistics(&self, id: ScaleSetId) -> ScaleSetStatistics {
        let mut stats = ScaleSetStatistics::default();
        for job in self.jobs.values().filter(|j| j.public.scale_set_id == id) {
            match job.public.state {
                JobState::Available => stats.total_available_jobs += 1,
                JobState::Assigned => {
                    stats.total_acquired_jobs += 1;
                    stats.total_assigned_jobs += 1;
                }
                JobState::Running => {
                    stats.total_acquired_jobs += 1;
                    stats.total_assigned_jobs += 1;
                    stats.total_running_jobs += 1;
                }
                JobState::Completed => {}
            }
        }
        for runner in self.runners.values().filter(|r| r.scale_set_id == id) {
            stats.total_registered_runners += 1;
            if runner.job.is_some() {
                stats.total_busy_runners += 1;
            } else {
                stats.total_idle_runners += 1;
            }
        }
        stats
    }

    fn with_statistics(&self, set: &RunnerScaleSet) -> RunnerScaleSet {
        let mut set = set.clone();
        set.statistics = Some(self.statistics(set.id));
        set
    }

    pub fn scale_sets(
        &self,
        group: Option<RunnerGroupId>,
        name: Option<&str>,
    ) -> Vec<RunnerScaleSet> {
        self.scale_sets
            .values()
            .filter(|e| group.is_none_or(|g| e.set.runner_group_id == g))
            .filter(|e| name.is_none_or(|n| e.set.name == n))
            .map(|e| self.with_statistics(&e.set))
            .collect()
    }

    pub fn scale_set(&self, id: ScaleSetId) -> ServiceResult<RunnerScaleSet> {
        self.scale_sets
            .get(&id)
            .map(|e| self.with_statistics(&e.set))
            .ok_or_else(|| ServiceError::not_found("runner scale set"))
    }

    pub fn create_scale_set(&mut self, spec: ScaleSetBody) -> ServiceResult<RunnerScaleSet> {
        let name = spec
            .name
            .filter(|n| !n.is_empty())
            .ok_or_else(|| ServiceError::new(400, "name is required"))?;
        let group_id = spec
            .runner_group_id
            .filter(|id| id.get() > 0)
            .unwrap_or(RunnerGroupId::DEFAULT);
        let group_name = self.group(group_id)?.name.clone();
        if self
            .scale_sets
            .values()
            .any(|e| e.set.runner_group_id == group_id && e.set.name == name)
        {
            return Err(ServiceError::exception(
                409,
                "RunnerScaleSetExistsException",
                format!("A runner scale set named {name} already exists in the runner group"),
            ));
        }
        let mut labels = spec.labels.unwrap_or_default();
        if labels.is_empty() {
            labels.push(Label::new(&name));
        }
        let id = ScaleSetId(self.id());
        let set = RunnerScaleSet {
            id,
            name,
            runner_group_id: group_id,
            runner_group_name: group_name,
            labels,
            runner_setting: spec.runner_setting.unwrap_or_default(),
            created_on: Some(Utc::now()),
            runner_jit_config_url: Some(format!(
                "{}/tenant/_apis/runtime/runnerscalesets/{id}/generatejitconfig",
                self.base_url
            )),
            statistics: None,
        };
        self.scale_sets.insert(
            id,
            ScaleSetEntry {
                set: set.clone(),
                session: None,
                queue: Queue {
                    next_message_id: 1,
                    ..Queue::default()
                },
            },
        );
        Ok(self.with_statistics(&set))
    }

    pub fn update_scale_set(
        &mut self,
        id: ScaleSetId,
        spec: ScaleSetBody,
    ) -> ServiceResult<RunnerScaleSet> {
        let group = match spec.runner_group_id.filter(|g| g.get() > 0) {
            Some(g) => Some((g, self.group(g)?.name.clone())),
            None => None,
        };
        let entry = self
            .scale_sets
            .get_mut(&id)
            .ok_or_else(|| ServiceError::not_found("runner scale set"))?;
        if let Some(name) = spec.name.filter(|n| !n.is_empty()) {
            entry.set.name = name;
        }
        if let Some((gid, gname)) = group {
            entry.set.runner_group_id = gid;
            entry.set.runner_group_name = gname;
        }
        if let Some(labels) = spec.labels.filter(|l| !l.is_empty()) {
            entry.set.labels = labels;
        }
        if let Some(setting) = spec.runner_setting {
            entry.set.runner_setting = setting;
        }
        let set = entry.set.clone();
        Ok(self.with_statistics(&set))
    }

    pub fn delete_scale_set(&mut self, id: ScaleSetId) -> ServiceResult<()> {
        self.scale_sets
            .remove(&id)
            .ok_or_else(|| ServiceError::not_found("runner scale set"))?;
        self.runners.retain(|_, r| r.scale_set_id != id);
        self.jobs.retain(|_, j| j.public.scale_set_id != id);
        Ok(())
    }

    fn entry(&mut self, id: ScaleSetId) -> ServiceResult<&mut ScaleSetEntry> {
        self.scale_sets
            .get_mut(&id)
            .ok_or_else(|| ServiceError::not_found("runner scale set"))
    }

    // ---------------------------------------------------------------------
    // Runners

    pub fn generate_jit_config(
        &mut self,
        scale_set_id: ScaleSetId,
        name: &str,
    ) -> ServiceResult<(RunnerReference, String)> {
        self.entry(scale_set_id)?;
        if name.is_empty() {
            return Err(ServiceError::new(400, "runner name is required"));
        }
        if self.runners.values().any(|r| r.name == name) {
            return Err(ServiceError::exception(
                409,
                "GitHub.DistributedTask.WebApi.AgentExistsException",
                format!("A runner with the name {name} already exists"),
            ));
        }
        let id = RunnerId(self.id());
        self.runners.insert(
            id,
            EmulatedRunner {
                id,
                name: name.to_owned(),
                scale_set_id,
                job: None,
            },
        );
        let reference = RunnerReference {
            id,
            name: name.to_owned(),
            runner_scale_set_id: scale_set_id,
        };
        let config = STANDARD.encode(
            serde_json::json!({
                "runnerId": id,
                "runnerName": name,
                "scaleSetId": scale_set_id,
                "emulator": self.base_url,
            })
            .to_string(),
        );
        Ok((reference, config))
    }

    pub fn runner(&self, id: RunnerId) -> ServiceResult<RunnerReference> {
        self.runners
            .get(&id)
            .map(reference)
            .ok_or_else(runner_not_found)
    }

    pub fn runners_named(&self, name: &str) -> Vec<RunnerReference> {
        self.runners
            .values()
            .filter(|r| r.name == name)
            .map(reference)
            .collect()
    }

    pub fn remove_runner(&mut self, id: RunnerId) -> ServiceResult<()> {
        let runner = self.runners.get(&id).ok_or_else(runner_not_found)?;
        if runner.job.is_some() {
            return Err(ServiceError::exception(
                400,
                "GitHub.DistributedTask.WebApi.JobStillRunningException",
                format!("Runner {} is still running a job", runner.name),
            ));
        }
        self.runners.remove(&id);
        Ok(())
    }

    // ---------------------------------------------------------------------
    // Sessions and the message queue

    pub fn create_session(
        &mut self,
        scale_set_id: ScaleSetId,
        owner: String,
    ) -> ServiceResult<SessionInfo> {
        let stats = self.statistics(scale_set_id);
        let base = self.base_url.clone();
        let entry = self.entry(scale_set_id)?;
        if entry.session.is_some() {
            return Err(ServiceError::exception(
                409,
                "GitHub.DistributedTask.WebApi.TaskAgentSessionConflictException",
                format!(
                    "The scale set {} already has an active session",
                    entry.set.name
                ),
            ));
        }
        let session_id = random_uuid();
        let info = SessionInfo {
            session_id,
            owner_name: owner,
            runner_scale_set: Some(entry.set.clone()),
            message_queue_url: format!("{base}/queue/{scale_set_id}/{session_id}"),
            message_queue_access_token: Self::token("queue").into(),
            statistics: Some(stats),
        };
        entry.session = Some(Session {
            info: info.clone(),
            token_valid: true,
        });
        Ok(info)
    }

    fn session_mut(
        &mut self,
        scale_set_id: ScaleSetId,
        session_id: Uuid,
    ) -> ServiceResult<&mut Session> {
        self.entry(scale_set_id)?
            .session
            .as_mut()
            .filter(|s| s.info.session_id == session_id)
            .ok_or_else(|| ServiceError::not_found("session"))
    }

    pub fn refresh_session(
        &mut self,
        scale_set_id: ScaleSetId,
        session_id: Uuid,
    ) -> ServiceResult<SessionInfo> {
        let stats = self.statistics(scale_set_id);
        let session = self.session_mut(scale_set_id, session_id)?;
        session.info.message_queue_access_token = Self::token("queue").into();
        session.info.statistics = Some(stats);
        session.token_valid = true;
        Ok(session.info.clone())
    }

    pub fn delete_session(
        &mut self,
        scale_set_id: ScaleSetId,
        session_id: Uuid,
    ) -> ServiceResult<()> {
        self.session_mut(scale_set_id, session_id)?;
        self.entry(scale_set_id)?.session = None;
        Ok(())
    }

    /// Drops a scale set's session, as if it timed out on the service.
    pub fn expire_session(&mut self, scale_set_id: ScaleSetId) -> bool {
        self.scale_sets
            .get_mut(&scale_set_id)
            .and_then(|e| e.session.take())
            .is_some()
    }

    /// Makes the queue token of a scale set's session stop working, as if
    /// it expired.
    pub fn expire_queue_token(&mut self, scale_set_id: ScaleSetId) -> bool {
        match self
            .scale_sets
            .get_mut(&scale_set_id)
            .and_then(|e| e.session.as_mut())
        {
            Some(s) => {
                s.token_valid = false;
                true
            }
            None => false,
        }
    }

    /// Checks a queue token against the scale set's session.
    pub fn check_queue(
        &self,
        scale_set_id: ScaleSetId,
        session_id: Option<Uuid>,
        bearer: Option<&str>,
    ) -> ServiceResult<()> {
        let session = self
            .scale_sets
            .get(&scale_set_id)
            .and_then(|e| e.session.as_ref())
            .filter(|s| session_id.is_none_or(|id| id == s.info.session_id))
            .ok_or_else(|| ServiceError::not_found("session"))?;
        if session.token_valid
            && bearer == Some(session.info.message_queue_access_token.expose_secret())
        {
            Ok(())
        } else {
            Err(ServiceError::unauthorized())
        }
    }

    /// The next message for a poll: an unacknowledged batch newer than
    /// `last`, or a new batch of pending job messages.
    pub fn next_message(
        &mut self,
        scale_set_id: ScaleSetId,
        last: i64,
    ) -> ServiceResult<Option<(i64, Vec<JobMessage>, ScaleSetStatistics)>> {
        let stats = self.statistics(scale_set_id);
        let queue = &mut self.entry(scale_set_id)?.queue;
        if let Some((id, jobs)) = queue.unacked.range(last + 1..).next() {
            return Ok(Some((*id, jobs.clone(), stats)));
        }
        if queue.pending.is_empty() {
            return Ok(None);
        }
        let take = queue.pending.len().min(MAX_BATCH);
        let jobs: Vec<_> = queue.pending.drain(..take).collect();
        let id = queue.next_message_id;
        queue.next_message_id += 1;
        queue.unacked.insert(id, jobs.clone());
        Ok(Some((id, jobs, stats)))
    }

    pub fn ack(&mut self, scale_set_id: ScaleSetId, message_id: i64) -> ServiceResult<()> {
        self.entry(scale_set_id)?.queue.unacked.remove(&message_id);
        Ok(())
    }

    fn push(&mut self, scale_set_id: ScaleSetId, message: JobMessage) {
        if let Some(entry) = self.scale_sets.get_mut(&scale_set_id) {
            entry.queue.pending.push_back(message);
        }
    }

    // ---------------------------------------------------------------------
    // Jobs

    /// Queues a job and announces it with `JobAvailable`.
    pub fn queue_job(
        &mut self,
        scale_set_id: ScaleSetId,
        labels: Vec<String>,
    ) -> ServiceResult<JobRequestId> {
        let set_name = self.entry(scale_set_id)?.set.name.clone();
        let id = JobRequestId(self.id());
        let run_id = self.id();
        let labels = if labels.is_empty() {
            vec![set_name]
        } else {
            labels
        };
        let info = JobInfo {
            runner_request_id: id,
            repository_name: "repo".into(),
            owner_name: "org".into(),
            job_id: random_uuid().to_string(),
            job_workflow_ref: "org/repo/.github/workflows/ci.yml@refs/heads/main".into(),
            job_display_name: format!("job-{id}"),
            workflow_run_id: run_id,
            event_name: "push".into(),
            request_labels: labels.clone(),
            queue_time: Some(Utc::now()),
            ..JobInfo::default()
        };
        self.jobs.insert(
            id,
            Job {
                public: EmulatedJob {
                    id,
                    scale_set_id,
                    labels,
                    state: JobState::Available,
                    runner_id: None,
                    result: None,
                },
                info: info.clone(),
            },
        );
        self.push(
            scale_set_id,
            JobMessage::Available(JobAvailable {
                acquire_job_url: format!(
                    "{}/tenant/_apis/runtime/runnerscalesets/{scale_set_id}/acquirejobs",
                    self.base_url
                ),
                job: info,
            }),
        );
        Ok(id)
    }

    /// Acquires available jobs and assigns them to the scale set at once.
    pub fn acquire_jobs(
        &mut self,
        scale_set_id: ScaleSetId,
        ids: &[JobRequestId],
    ) -> Vec<JobRequestId> {
        let mut acquired = Vec::new();
        for id in ids {
            let Some(job) = self.jobs.get_mut(id) else {
                continue;
            };
            if job.public.scale_set_id != scale_set_id {
                continue;
            }
            match job.public.state {
                JobState::Available => {
                    job.public.state = JobState::Assigned;
                    job.info.scale_set_assign_time = Some(Utc::now());
                    let info = job.info.clone();
                    acquired.push(*id);
                    self.push(
                        scale_set_id,
                        JobMessage::Assigned(JobAssigned { job: info }),
                    );
                }
                // Acquiring again is harmless.
                JobState::Assigned | JobState::Running => acquired.push(*id),
                JobState::Completed => {}
            }
        }
        acquired
    }

    /// Starts an assigned job on a runner: the named one, or any idle
    /// runner of the scale set.
    pub fn start_job(
        &mut self,
        id: JobRequestId,
        runner_name: Option<&str>,
    ) -> ServiceResult<RunnerId> {
        let job = self
            .jobs
            .get(&id)
            .ok_or_else(|| ServiceError::not_found("job"))?;
        if job.public.state != JobState::Assigned {
            return Err(ServiceError::new(
                409,
                format!("job {id} is {:?}, not assigned", job.public.state),
            ));
        }
        let scale_set_id = job.public.scale_set_id;
        let runner_id = self
            .runners
            .values()
            .find(|r| {
                r.scale_set_id == scale_set_id
                    && r.job.is_none()
                    && runner_name.is_none_or(|n| r.name == n)
            })
            .map(|r| r.id)
            .ok_or_else(|| ServiceError::new(409, "no idle runner to run the job"))?;

        let runner = self.runners.get_mut(&runner_id).expect("runner exists");
        runner.job = Some(id);
        let runner_name = runner.name.clone();
        let job = self.jobs.get_mut(&id).expect("job exists");
        job.public.state = JobState::Running;
        job.public.runner_id = Some(runner_id);
        job.info.runner_assign_time = Some(Utc::now());
        let info = job.info.clone();
        self.push(
            scale_set_id,
            JobMessage::Started(JobStarted {
                runner_id,
                runner_name,
                job: info,
            }),
        );
        Ok(runner_id)
    }

    /// Completes a job. A running job's runner is removed, as ephemeral
    /// runners exit after one job. A job that has not started is canceled.
    pub fn complete_job(&mut self, id: JobRequestId, result: Option<&str>) -> ServiceResult<()> {
        let job = self
            .jobs
            .get_mut(&id)
            .ok_or_else(|| ServiceError::not_found("job"))?;
        if job.public.state == JobState::Completed {
            return Ok(());
        }
        let was_running = job.public.state == JobState::Running;
        let result = result
            .unwrap_or(if was_running { "succeeded" } else { "canceled" })
            .to_owned();
        job.public.state = JobState::Completed;
        job.public.result = Some(result.clone());
        job.info.finish_time = Some(Utc::now());
        let info = job.info.clone();
        let scale_set_id = job.public.scale_set_id;
        let runner_id = job.public.runner_id;

        let (runner_id, runner_name) = match runner_id.and_then(|r| self.runners.remove(&r)) {
            Some(r) => (r.id, r.name),
            None => (RunnerId::default(), String::new()),
        };
        self.push(
            scale_set_id,
            JobMessage::Completed(JobCompleted {
                result,
                runner_id,
                runner_name,
                job: info,
            }),
        );
        Ok(())
    }

    /// Starts assigned jobs on idle runners, oldest first. Returns the jobs
    /// started.
    pub fn match_jobs(&mut self) -> Vec<JobRequestId> {
        let assigned: Vec<JobRequestId> = self
            .jobs
            .values()
            .filter(|j| j.public.state == JobState::Assigned)
            .map(|j| j.public.id)
            .collect();
        assigned
            .into_iter()
            .filter(|id| self.start_job(*id, None).is_ok())
            .collect()
    }

    pub fn snapshot(&self) -> EmulatorState {
        EmulatorState {
            scale_sets: self.scale_sets(None, None),
            sessions: self
                .scale_sets
                .values()
                .filter_map(|e| {
                    e.session.as_ref().map(|s| EmulatedSession {
                        session_id: s.info.session_id,
                        scale_set_id: e.set.id,
                        owner_name: s.info.owner_name.clone(),
                    })
                })
                .collect(),
            runners: self.runners.values().cloned().collect(),
            jobs: self.jobs.values().map(|j| j.public.clone()).collect(),
            unacknowledged: self
                .scale_sets
                .iter()
                .filter(|(_, e)| !e.queue.unacked.is_empty())
                .map(|(id, e)| (*id, e.queue.unacked.keys().map(|k| MessageId(*k)).collect()))
                .collect(),
            undelivered: self
                .scale_sets
                .iter()
                .filter(|(_, e)| !e.queue.pending.is_empty())
                .map(|(id, e)| (*id, e.queue.pending.len()))
                .collect(),
        }
    }
}

/// The body of a scale set create or update.
#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub(crate) struct ScaleSetBody {
    pub name: Option<String>,
    pub runner_group_id: Option<RunnerGroupId>,
    pub labels: Option<Vec<Label>>,
    #[serde(rename = "RunnerSetting")]
    pub runner_setting: Option<RunnerSetting>,
}

fn random_uuid() -> Uuid {
    uuid::Builder::from_random_bytes(fastrand::u128(..).to_le_bytes()).into_uuid()
}

fn reference(r: &EmulatedRunner) -> RunnerReference {
    RunnerReference {
        id: r.id,
        name: r.name.clone(),
        runner_scale_set_id: r.scale_set_id,
    }
}

fn runner_not_found() -> ServiceError {
    ServiceError::exception(
        404,
        "GitHub.DistributedTask.WebApi.AgentNotFoundException",
        "No runner found",
    )
}

/// Accepts an RS256 JWT with an issuer that has not expired. The signature
/// is not checked: the emulator does not know the app's public key.
fn valid_app_jwt(jwt: &str) -> bool {
    #[derive(Deserialize)]
    struct Claims {
        #[serde(default)]
        iss: serde_json::Value,
        exp: u64,
    }
    let mut parts = jwt.split('.');
    let (Some(header), Some(payload), Some(_sig), None) =
        (parts.next(), parts.next(), parts.next(), parts.next())
    else {
        return false;
    };
    let decode = |s: &str| URL_SAFE_NO_PAD.decode(s).ok();
    let header_ok = decode(header)
        .and_then(|h| serde_json::from_slice::<serde_json::Value>(&h).ok())
        .is_some_and(|h| h["alg"] == "RS256");
    let claims = decode(payload).and_then(|p| serde_json::from_slice::<Claims>(&p).ok());
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
        .as_secs();
    header_ok && claims.is_some_and(|c| c.exp > now && !(c.iss.is_null() || c.iss == ""))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn state() -> State {
        State::new(EmulatorConfig::default(), "http://emu".into())
    }

    fn with_set(state: &mut State) -> ScaleSetId {
        state
            .create_scale_set(ScaleSetBody {
                name: Some("set".into()),
                ..Default::default()
            })
            .unwrap()
            .id
    }

    #[test]
    fn job_lifecycle_and_statistics() {
        let mut s = state();
        let id = with_set(&mut s);
        let job = s.queue_job(id, vec![]).unwrap();
        assert_eq!(s.statistics(id).total_available_jobs, 1);
        assert_eq!(s.statistics(id).total_assigned_jobs, 0);

        assert_eq!(s.acquire_jobs(id, &[job]), vec![job]);
        assert_eq!(s.statistics(id).total_assigned_jobs, 1);
        assert!(s.start_job(job, None).is_err(), "no runner yet");

        let (runner, _) = s.generate_jit_config(id, "r1").unwrap();
        assert_eq!(s.statistics(id).total_idle_runners, 1);
        assert_eq!(s.match_jobs(), vec![job]);
        let stats = s.statistics(id);
        assert_eq!(
            (
                stats.total_running_jobs,
                stats.total_busy_runners,
                stats.total_assigned_jobs
            ),
            (1, 1, 1)
        );
        assert!(s.remove_runner(runner.id).is_err(), "busy");

        s.complete_job(job, None).unwrap();
        assert_eq!(s.statistics(id), ScaleSetStatistics::default());
        assert!(s.runner(runner.id).is_err(), "ephemeral runner removed");

        // Available, Assigned, Started, Completed, in order.
        let (_, jobs, _) = s.next_message(id, 0).unwrap().unwrap();
        let kinds: Vec<_> = jobs
            .iter()
            .map(|m| match m {
                JobMessage::Available(_) => "available",
                JobMessage::Assigned(_) => "assigned",
                JobMessage::Started(_) => "started",
                JobMessage::Completed(_) => "completed",
                _ => "other",
            })
            .collect();
        assert_eq!(kinds, ["available", "assigned", "started", "completed"]);
    }

    #[test]
    fn redelivers_until_acknowledged() {
        let mut s = state();
        let id = with_set(&mut s);
        s.queue_job(id, vec![]).unwrap();
        let (m1, ..) = s.next_message(id, 0).unwrap().unwrap();
        // Not acknowledged: delivered again to a poll that has not seen it.
        assert_eq!(s.next_message(id, 0).unwrap().unwrap().0, m1);
        // A poll that has seen it gets nothing new.
        assert!(s.next_message(id, m1).unwrap().is_none());
        s.ack(id, m1).unwrap();
        assert!(s.next_message(id, 0).unwrap().is_none());
    }

    #[test]
    fn batches_are_capped() {
        let mut s = state();
        let id = with_set(&mut s);
        for _ in 0..60 {
            s.queue_job(id, vec![]).unwrap();
        }
        let (m1, jobs, stats) = s.next_message(id, 0).unwrap().unwrap();
        assert_eq!(jobs.len(), MAX_BATCH);
        assert_eq!(stats.total_available_jobs, 60);
        let (_, rest, _) = s.next_message(id, m1).unwrap().unwrap();
        assert_eq!(rest.len(), 10);
    }

    #[test]
    fn one_session_per_scale_set() {
        let mut s = state();
        let id = with_set(&mut s);
        let session = s.create_session(id, "a".into()).unwrap();
        let err = s.create_session(id, "b".into()).unwrap_err();
        assert_eq!(err.status, 409);
        s.delete_session(id, session.session_id).unwrap();
        s.create_session(id, "b".into()).unwrap();
    }

    #[test]
    fn duplicate_names_conflict() {
        let mut s = state();
        let id = with_set(&mut s);
        let err = s
            .create_scale_set(ScaleSetBody {
                name: Some("set".into()),
                ..Default::default()
            })
            .unwrap_err();
        assert_eq!(err.status, 409);
        s.generate_jit_config(id, "r").unwrap();
        let err = s.generate_jit_config(id, "r").unwrap_err();
        assert_eq!(err.status, 409);
        assert!(err.type_name.unwrap().contains("AgentExistsException"));
    }

    #[test]
    fn canceling_unstarted_job() {
        let mut s = state();
        let id = with_set(&mut s);
        let job = s.queue_job(id, vec![]).unwrap();
        s.complete_job(job, None).unwrap();
        let snap = s.snapshot();
        assert_eq!(snap.jobs[0].result.as_deref(), Some("canceled"));
    }
}
