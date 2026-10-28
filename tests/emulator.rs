//! Behavior tests against the stateful emulator: whole scenarios rather than
//! single requests. Byte-level wire checks live in `client.rs` and
//! `session.rs` (wiremock).

use std::collections::HashSet;
use std::time::Duration;

use scaleset::listener::{Event, Listener, Scaler};
use scaleset::testing::{Emulator, Endpoint, Fault, JobState};
use scaleset::{
    BoxError, Client, Credentials, JitRunnerSetting, MessageSession, RetryPolicy, RunnerGroupId,
    RunnerScaleSetSpec, ScaleSetId,
};

const WAIT: Duration = Duration::from_secs(10);

async fn emulator() -> Emulator {
    Emulator::builder()
        .long_poll(Duration::from_millis(100))
        .start()
        .await
}

async fn scale_set(client: &Client, name: &str) -> ScaleSetId {
    client
        .create_runner_scale_set(&RunnerScaleSetSpec::new(name))
        .await
        .unwrap()
        .id
}

/// A scaler that keeps `total_assigned_jobs` runners registered, the way a
/// real autoscaler would.
struct Autoscaler {
    client: Client,
    session: MessageSession,
    scale_set_id: ScaleSetId,
    started: u32,
    peak_assigned: u32,
}

impl Scaler for Autoscaler {
    type Error = BoxError;

    async fn scale(&mut self, event: Event<'_>) -> Result<(), BoxError> {
        if let Event::Message(msg) = event {
            let jobs: Vec<_> = msg
                .jobs_available()
                .map(|j| j.job.runner_request_id)
                .collect();
            if !jobs.is_empty() {
                self.session.acquire_jobs(&jobs).await?;
            }
            // Runners are ephemeral: one leaves with each completed job.
            self.started -= u32::try_from(
                msg.jobs_completed()
                    .filter(|j| !j.runner_name.is_empty())
                    .count(),
            )?;
        }
        let Some(stats) = event.statistics() else {
            return Ok(());
        };
        self.peak_assigned = self.peak_assigned.max(stats.total_assigned_jobs);
        while self.started < stats.total_assigned_jobs {
            let name = format!("runner-{:08x}", fastrand::u32(..));
            self.client
                .generate_jit_config(self.scale_set_id, &JitRunnerSetting::new(name))
                .await?;
            self.started += 1;
        }
        Ok(())
    }
}

#[tokio::test]
async fn autoscaler_runs_every_job() {
    let emu = Emulator::builder()
        .long_poll(Duration::from_millis(100))
        .auto_run_jobs(Duration::from_millis(50))
        .start()
        .await;
    let client = emu.client();
    let id = scale_set(&client, "linux").await;
    let session = client.create_message_session(id, "host").await.unwrap();

    let mut scaler = Autoscaler {
        client: client.clone(),
        session: session.clone(),
        scale_set_id: id,
        started: 0,
        peak_assigned: 0,
    };
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    let listener = Listener::new(session.clone(), 10);
    let run = tokio::spawn(async move {
        listener
            .run(&mut scaler, async {
                let _ = stopped.await;
            })
            .await
            .map(|()| scaler)
    });

    for _ in 0..5 {
        emu.queue_job(id, ["linux"]);
    }
    let state = emu
        .wait_until(WAIT, |s| {
            s.jobs.len() == 5 && s.jobs.iter().all(|j| j.state == JobState::Completed)
        })
        .await;
    assert!(
        state
            .jobs
            .iter()
            .all(|j| j.result.as_deref() == Some("succeeded"))
    );

    // Everything was acknowledged and no runner is left behind.
    let state = emu
        .wait_until(WAIT, |s| s.queues_drained() && s.runners.is_empty())
        .await;
    assert_eq!(state.statistics(id).unwrap().total_assigned_jobs, 0);

    stop.send(()).unwrap();
    let scaler = run.await.unwrap().unwrap();
    assert!(scaler.peak_assigned >= 1);
    assert_eq!(scaler.started, 0);
    session.close().await.unwrap();
    assert!(emu.state().sessions.is_empty());
}

#[tokio::test]
async fn unacknowledged_message_is_redelivered() {
    let emu = emulator().await;
    let client = emu.client();
    let id = scale_set(&client, "set").await;
    let session = client.create_message_session(id, "host").await.unwrap();
    let job = emu.queue_job(id, Vec::<String>::new());

    let first = session.get_message(None, 1).await.unwrap().unwrap();
    // A crash before acknowledging: the next listener starts from scratch.
    let again = session.get_message(None, 1).await.unwrap().unwrap();
    assert_eq!(first, again);
    assert_eq!(
        again.jobs_available().next().unwrap().job.runner_request_id,
        job
    );

    session.delete_message(first.id).await.unwrap();
    assert!(session.get_message(None, 1).await.unwrap().is_none());
}

#[tokio::test]
async fn scaler_error_leaves_message_for_the_next_run() {
    let emu = emulator().await;
    let client = emu.client();
    let id = scale_set(&client, "set").await;
    let session = client.create_message_session(id, "host").await.unwrap();
    emu.queue_job(id, ["set"]);

    struct FailOnMessage;
    impl Scaler for FailOnMessage {
        type Error = BoxError;
        async fn scale(&mut self, event: Event<'_>) -> Result<(), BoxError> {
            match event {
                Event::Message(_) => Err("provisioning failed".into()),
                _ => Ok(()),
            }
        }
    }
    let err = Listener::new(session.clone(), 1)
        .run(&mut FailOnMessage, std::future::pending())
        .await
        .unwrap_err();
    assert!(matches!(err, scaleset::listener::Error::Scale(_)));
    assert_eq!(emu.state().unacknowledged[&id].len(), 1);

    // The next run sees the same message and acknowledges it.
    struct Ack(Option<tokio::sync::oneshot::Sender<()>>);
    impl Scaler for Ack {
        type Error = BoxError;
        async fn scale(&mut self, event: Event<'_>) -> Result<(), BoxError> {
            if matches!(event, Event::Idle) {
                if let Some(tx) = self.0.take() {
                    let _ = tx.send(());
                }
            }
            Ok(())
        }
    }
    let (tx, rx) = tokio::sync::oneshot::channel();
    Listener::new(session.clone(), 1)
        .run(&mut Ack(Some(tx)), async {
            let _ = rx.await;
        })
        .await
        .unwrap();
    assert!(emu.state().unacknowledged.is_empty());
}

#[tokio::test]
async fn queue_token_expiry_is_transparent() {
    let emu = emulator().await;
    let client = emu.client();
    let id = scale_set(&client, "set").await;
    let session = client.create_message_session(id, "host").await.unwrap();
    let token = session.info().message_queue_access_token.clone();

    emu.expire_queue_token(id);
    assert!(session.get_message(None, 1).await.unwrap().is_none());
    assert_ne!(session.info().message_queue_access_token, token);

    emu.expire_queue_token(id);
    let job = emu.queue_job(id, ["set"]);
    assert_eq!(session.acquire_jobs(&[job]).await.unwrap(), vec![job]);

    let refreshes = emu
        .requests_to(Endpoint::Sessions)
        .into_iter()
        .filter(|r| r.method == "PATCH")
        .count();
    assert_eq!(refreshes, 2);
}

#[tokio::test]
async fn revoked_admin_token_is_replaced() {
    let emu = emulator().await;
    let client = emu.client();
    let id = scale_set(&client, "set").await;

    emu.expire_admin_tokens();
    client.get_runner_scale_set_by_id(id).await.unwrap();
    assert_eq!(emu.requests_to(Endpoint::AdminConnection).len(), 2);
}

#[tokio::test]
async fn short_lived_admin_tokens_refresh_before_expiry() {
    let emu = Emulator::builder()
        .admin_token_ttl(Duration::from_secs(30))
        .start()
        .await;
    let client = emu.client();
    let id = scale_set(&client, "set").await;
    client.get_runner_scale_set_by_id(id).await.unwrap();
    // Within a minute of expiry: refreshed before each call, never a 401.
    assert_eq!(emu.requests_to(Endpoint::AdminConnection).len(), 2);
    assert!(emu.requests().iter().all(|r| r.status != 401));
}

#[tokio::test]
async fn one_session_per_scale_set() {
    let emu = emulator().await;
    let client = emu.client();
    let id = scale_set(&client, "set").await;
    let first = client.create_message_session(id, "a").await.unwrap();

    let err = client.create_message_session(id, "b").await.unwrap_err();
    assert!(err.is_conflict(), "{err}");

    first.close().await.unwrap();
    client.create_message_session(id, "b").await.unwrap();
}

#[tokio::test]
async fn busy_runner_cannot_be_removed() {
    let emu = emulator().await;
    let client = emu.client();
    let id = scale_set(&client, "set").await;
    let session = client.create_message_session(id, "host").await.unwrap();
    let runner = client
        .generate_jit_config(id, &JitRunnerSetting::new("r1"))
        .await
        .unwrap()
        .runner
        .unwrap();

    let dup = client
        .generate_jit_config(id, &JitRunnerSetting::new("r1"))
        .await
        .unwrap_err();
    assert!(dup.is_runner_exists(), "{dup}");

    let job = emu.queue_job(id, ["set"]);
    session.acquire_jobs(&[job]).await.unwrap();
    emu.start_job(job, Some("r1"));

    let err = client.remove_runner(runner.id).await.unwrap_err();
    assert!(err.is_job_still_running(), "{err}");

    emu.complete_job(job, None);
    let err = client.remove_runner(runner.id).await.unwrap_err();
    assert!(err.is_runner_not_found(), "{err}");
}

#[tokio::test]
async fn faults_are_retried_or_surfaced() {
    let emu = emulator().await;
    let client = emu.client();
    let id = scale_set(&client, "set").await;
    let session = client.create_message_session(id, "host").await.unwrap();

    // Two 503s, then success: absorbed by retries.
    emu.inject(Fault::status(Endpoint::GetMessage, 503).times(2));
    assert!(session.get_message(None, 1).await.unwrap().is_none());
    let statuses: Vec<_> = emu
        .requests_to(Endpoint::GetMessage)
        .iter()
        .map(|r| r.status)
        .collect();
    assert_eq!(statuses, [503, 503, 202]);

    // A secondary rate limit is not retried by default and comes back typed.
    emu.inject(
        Fault::secondary_rate_limit(Endpoint::GenerateJitConfig, Some(Duration::from_secs(7)))
            .times(1),
    );
    let err = client
        .generate_jit_config(id, &JitRunnerSetting::new("r"))
        .await
        .unwrap_err();
    let rl = err.rate_limit().unwrap();
    assert_eq!(rl.retry_after, Some(Duration::from_secs(7)));
    assert_eq!(rl.kind, scaleset::RateLimitKind::Secondary);

    // A slow endpoint trips the per-request budget.
    emu.inject(Fault::delay(Endpoint::ScaleSets, Duration::from_secs(2)).times(1));
    let err = client
        .with_retry_policy(RetryPolicy::default().max_elapsed(Duration::from_millis(200)))
        .get_runner_scale_set_by_id(id)
        .await
        .unwrap_err();
    assert!(err.is_timeout(), "{err}");
}

#[tokio::test]
async fn statistics_follow_the_job_lifecycle() {
    let emu = emulator().await;
    let client = emu.client();
    let id = scale_set(&client, "set").await;
    let session = client.create_message_session(id, "host").await.unwrap();

    let jobs: Vec<_> = (0..3).map(|_| emu.queue_job(id, ["set"])).collect();
    let msg = session.get_message(None, 3).await.unwrap().unwrap();
    assert_eq!(msg.statistics.unwrap().total_available_jobs, 3);
    session.acquire_jobs(&jobs[..2]).await.unwrap();
    session.delete_message(msg.id).await.unwrap();

    let msg = session.get_message(Some(msg.id), 3).await.unwrap().unwrap();
    let stats = msg.statistics.unwrap();
    assert_eq!(
        (stats.total_available_jobs, stats.total_assigned_jobs),
        (1, 2)
    );
    assert_eq!(msg.jobs_assigned().count(), 2);

    client
        .generate_jit_config(id, &JitRunnerSetting::new("r1"))
        .await
        .unwrap();
    emu.start_job(jobs[0], None);
    let stats = emu.statistics(id).unwrap();
    assert_eq!(
        (
            stats.total_running_jobs,
            stats.total_busy_runners,
            stats.total_idle_runners
        ),
        (1, 1, 0)
    );
}

#[tokio::test]
async fn github_app_credentials() {
    let emu = Emulator::builder().github_app(42).start().await;
    let pem = test_key_pem();
    let client = Client::builder(
        emu.config_url(),
        Credentials::github_app("Iv1.emulated", 42, &pem).unwrap(),
    )
    .build()
    .unwrap();
    scale_set(&client, "set").await;
    assert_eq!(emu.requests_to(Endpoint::AccessToken)[0].status, 201);

    let wrong = Client::builder(
        emu.config_url(),
        Credentials::github_app("Iv1.emulated", 7, &pem).unwrap(),
    )
    .build()
    .unwrap();
    let err = wrong
        .create_runner_scale_set(&RunnerScaleSetSpec::new("x"))
        .await
        .unwrap_err();
    assert!(err.is_not_found(), "{err}");
}

#[tokio::test]
async fn bad_pat_is_rejected() {
    let emu = emulator().await;
    let client = Client::builder(
        emu.config_url(),
        Credentials::personal_access_token("ghp_wrong").unwrap(),
    )
    .build()
    .unwrap();
    let err = client
        .get_runner_group_by_name("Default")
        .await
        .unwrap_err();
    assert!(err.is_unauthorized(), "{err}");
}

#[tokio::test]
async fn runner_groups_and_lookups() {
    let emu = Emulator::builder().runner_group("gpu").start().await;
    let client = emu.client();
    let gpu = client
        .get_runner_group_by_name("gpu")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(gpu.id, RunnerGroupId(2));

    let set = client
        .create_runner_scale_set(&RunnerScaleSetSpec::new("big").runner_group_id(gpu.id))
        .await
        .unwrap();
    assert_eq!(set.runner_group_name, "gpu");
    let found = client
        .get_runner_scale_set(gpu.id, "big")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(found.id, set.id);
    assert!(
        client
            .get_runner_scale_set(RunnerGroupId::DEFAULT, "big")
            .await
            .unwrap()
            .is_none()
    );

    let names: HashSet<_> = client
        .list_runner_scale_sets(gpu.id)
        .await
        .unwrap()
        .into_iter()
        .map(|s| s.name)
        .collect();
    assert_eq!(names, HashSet::from(["big".to_owned()]));

    client.delete_runner_scale_set(set.id).await.unwrap();
    assert!(
        client
            .get_runner_scale_set_by_id(set.id)
            .await
            .unwrap_err()
            .is_not_found()
    );
}

#[tokio::test]
async fn reset_clears_everything() {
    let emu = emulator().await;
    let client = emu.client();
    scale_set(&client, "set").await;
    emu.inject(Fault::status(Endpoint::Any, 500));
    emu.reset();
    assert_eq!(emu.state(), Default::default());
    assert!(emu.requests().is_empty());
    // Old tokens no longer work; a fresh client starts over.
    emu.client()
        .create_runner_scale_set(&RunnerScaleSetSpec::new("set"))
        .await
        .unwrap();
}

fn test_key_pem() -> String {
    use aws_lc_rs::encoding::AsDer;
    use base64::Engine;
    let key = aws_lc_rs::rsa::KeyPair::generate(aws_lc_rs::rsa::KeySize::Rsa2048).unwrap();
    let der = key.as_der().unwrap();
    let b64 = base64::engine::general_purpose::STANDARD.encode(der.as_ref());
    let mut pem = String::from("-----BEGIN PRIVATE KEY-----\n");
    for chunk in b64.as_bytes().chunks(64) {
        pem.push_str(std::str::from_utf8(chunk).unwrap());
        pem.push('\n');
    }
    pem + "-----END PRIVATE KEY-----\n"
}
