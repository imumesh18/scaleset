//! Behavior tests against the stateful emulator: whole scenarios rather than
//! single requests. Byte-level wire checks live in `client.rs` and
//! `session.rs` (wiremock).

mod fixtures;

use std::collections::HashSet;
use std::time::{Duration, Instant};

use fixtures::{fast_retries, test_key_pem};
use scaleset::testing::{Emulator, Endpoint, Fault, JobState};
use scaleset::{
    Client, Credentials, Error, ExposeSecret, JitRunnerSetting, MessageId, RateLimitKind,
    RateLimitPolicy, RateLimitRule, RetryPolicy, RunnerGroupId, RunnerId, RunnerScaleSetSpec,
    ScaleSetId,
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

/// Statuses of the scale set reads served so far.
fn scale_set_reads(emu: &Emulator) -> Vec<u16> {
    emu.requests_to(Endpoint::ScaleSets)
        .into_iter()
        .filter(|r| r.method == "GET")
        .map(|r| r.status)
        .collect()
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
async fn queue_token_expiry_is_transparent() {
    let emu = emulator().await;
    let client = emu.client();
    let id = scale_set(&client, "set").await;
    let session = client.create_message_session(id, "host").await.unwrap();
    let token = session
        .info()
        .message_queue_access_token
        .expose_secret()
        .to_owned();

    emu.expire_queue_token(id);
    assert!(session.get_message(None, 1).await.unwrap().is_none());
    assert_ne!(
        session.info().message_queue_access_token.expose_secret(),
        token
    );

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
async fn concurrent_rejections_share_one_refresh() {
    let emu = emulator().await;
    let client = emu.client();
    let id = scale_set(&client, "set").await;

    emu.expire_admin_tokens();
    let calls: Vec<_> = (0..8)
        .map(|_| {
            let client = client.clone();
            tokio::spawn(async move { client.get_runner_scale_set_by_id(id).await })
        })
        .collect();
    for call in calls {
        call.await.unwrap().unwrap();
    }
    assert_eq!(emu.requests_to(Endpoint::AdminConnection).len(), 2);
}

#[tokio::test]
async fn concurrent_first_calls_share_one_token_exchange() {
    let emu = emulator().await;
    let client = emu.client();
    let calls: Vec<_> = (0..8)
        .map(|_| {
            let client = client.clone();
            tokio::spawn(async move { client.get_runner_group_by_name("Default").await })
        })
        .collect();
    for call in calls {
        call.await.unwrap().unwrap().unwrap();
    }
    assert_eq!(emu.requests_to(Endpoint::RegistrationToken).len(), 1);
    assert_eq!(emu.requests_to(Endpoint::AdminConnection).len(), 1);
}

#[tokio::test]
async fn admin_connection_retries_unauthorized() {
    let emu = emulator().await;
    // A fresh registration token is briefly rejected.
    emu.inject(Fault::status(Endpoint::AdminConnection, 401).times(1));
    emu.client()
        .get_runner_group_by_name("Default")
        .await
        .unwrap()
        .unwrap();
    let statuses: Vec<_> = emu
        .requests_to(Endpoint::AdminConnection)
        .iter()
        .map(|r| r.status)
        .collect();
    assert_eq!(statuses, [401, 200]);

    // With retries disabled, the 401 is returned.
    emu.inject(Fault::status(Endpoint::AdminConnection, 401));
    let err = emu
        .client()
        .with_retry_policy(RetryPolicy::none())
        .get_runner_group_by_name("Default")
        .await
        .unwrap_err();
    assert!(err.is_unauthorized(), "{err}");
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
    assert!(matches!(err, Error::MaxElapsedExceeded { .. }), "{err}");
    assert!(err.is_timeout(), "{err}");
}

#[tokio::test]
async fn server_errors_are_retried_up_to_the_limit() {
    let emu = emulator().await;
    let client = emu
        .client_builder()
        .retry_policy(fast_retries().max_retries(2))
        .build()
        .unwrap();
    let id = scale_set(&client, "set").await;

    emu.inject(Fault::status(Endpoint::ScaleSets, 500));
    let err = client.get_runner_scale_set_by_id(id).await.unwrap_err();
    assert_eq!(err.status().map(|s| s.as_u16()), Some(500), "{err}");
    assert_eq!(scale_set_reads(&emu), [500, 500, 500]);

    // Not retried: 501 and 4xx.
    for status in [501, 400] {
        emu.clear_faults();
        emu.inject(Fault::status(Endpoint::ScaleSets, status));
        let err = client.get_runner_scale_set_by_id(id).await.unwrap_err();
        assert_eq!(err.status().map(|s| s.as_u16()), Some(status), "{err}");
    }
    assert_eq!(scale_set_reads(&emu), [500, 500, 500, 501, 400]);
}

#[tokio::test]
async fn primary_rate_limit_is_not_retried_by_default() {
    let emu = emulator().await;
    let client = emu.client();
    let id = scale_set(&client, "set").await;

    emu.inject(Fault::primary_rate_limit(
        Endpoint::ScaleSets,
        Some(Duration::from_secs(30)),
    ));
    let err = client.get_runner_scale_set_by_id(id).await.unwrap_err();
    let rl = err.rate_limit().expect("rate limit error");
    assert_eq!(rl.kind, RateLimitKind::Primary);
    assert_eq!(rl.retry_after, Some(Duration::from_secs(30)));
    assert_eq!(rl.retries, 0);
    assert_eq!(rl.response.message, "API rate limit exceeded");
    assert_eq!(scale_set_reads(&emu), [403]);
}

#[tokio::test]
async fn too_many_requests_is_retried() {
    let emu = emulator().await;
    let client = emu.client();
    let id = scale_set(&client, "set").await;

    emu.inject(Fault::too_many_requests(Endpoint::ScaleSets, Some(Duration::ZERO)).times(1));
    client.get_runner_scale_set_by_id(id).await.unwrap();
    assert_eq!(scale_set_reads(&emu), [429, 200]);
}

#[tokio::test]
async fn rate_limit_rules_count_retries() {
    let emu = emulator().await;
    let client = emu
        .client()
        .with_retry_policy(fast_retries().rate_limit(RateLimitPolicy::rules(
            RateLimitRule::give_up(),
            RateLimitRule::retries(2),
        )));
    let id = scale_set(&client, "set").await;

    emu.inject(Fault::secondary_rate_limit(Endpoint::ScaleSets, None));
    let err = client.get_runner_scale_set_by_id(id).await.unwrap_err();
    let rl = err.rate_limit().expect("rate limit error");
    assert_eq!(rl.kind, RateLimitKind::Secondary);
    assert_eq!(rl.retries, 2);
    assert_eq!(scale_set_reads(&emu), [403, 403, 403]);
}

#[tokio::test]
async fn budget_returns_last_response_instead_of_sleeping() {
    let emu = emulator().await;
    let client = emu
        .client()
        .with_retry_policy(RetryPolicy::default().max_elapsed(Duration::from_secs(5)));
    let id = scale_set(&client, "set").await;

    // The 60 s wait cannot fit the 5 s budget.
    emu.inject(Fault::too_many_requests(
        Endpoint::ScaleSets,
        Some(Duration::from_secs(60)),
    ));
    let started = Instant::now();
    let err = client.get_runner_scale_set_by_id(id).await.unwrap_err();
    assert!(started.elapsed() < Duration::from_secs(2));
    assert_eq!(
        err.rate_limit().expect("rate limit error").retry_after,
        Some(Duration::from_secs(60))
    );
    assert_eq!(scale_set_reads(&emu), [429]);
}

#[tokio::test]
async fn polls_ignore_the_time_budget() {
    // The service holds an idle poll open, here longer than the budget.
    let emu = Emulator::builder()
        .long_poll(Duration::from_millis(300))
        .start()
        .await;
    let client = emu
        .client()
        .with_retry_policy(fast_retries().max_elapsed(Duration::from_millis(100)));
    let id = scale_set(&client, "set").await;
    let session = client.create_message_session(id, "host").await.unwrap();

    assert!(session.get_message(None, 10).await.unwrap().is_none());
    assert_eq!(emu.requests_to(Endpoint::GetMessage).len(), 1);
}

#[tokio::test]
async fn acknowledging_a_gone_message_succeeds() {
    let emu = emulator().await;
    let client = emu.client();
    let id = scale_set(&client, "set").await;
    let session = client.create_message_session(id, "host").await.unwrap();

    emu.inject(Fault::status(Endpoint::DeleteMessage, 404).times(1));
    session.delete_message(MessageId(1)).await.unwrap();
    assert_eq!(emu.requests_to(Endpoint::DeleteMessage)[0].status, 404);
}

#[tokio::test]
async fn persistent_queue_401_is_reported() {
    let emu = emulator().await;
    let client = emu.client();
    let id = scale_set(&client, "set").await;
    let session = client.create_message_session(id, "host").await.unwrap();

    emu.inject(Fault::status(Endpoint::DeleteMessage, 401));
    let err = session.delete_message(MessageId(1)).await.unwrap_err();
    assert!(err.is_message_queue_token_expired(), "{err}");

    // One refresh, then one more attempt.
    let refreshes = emu
        .requests_to(Endpoint::Sessions)
        .into_iter()
        .filter(|r| r.method == "PATCH")
        .count();
    assert_eq!(refreshes, 1);
    let statuses: Vec<_> = emu
        .requests_to(Endpoint::DeleteMessage)
        .iter()
        .map(|r| r.status)
        .collect();
    assert_eq!(statuses, [401, 401]);
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
async fn github_app_works_only_for_its_installation() {
    let emu = Emulator::builder().github_app(42).start().await;
    let pem = test_key_pem();
    let client = Client::builder(
        emu.config_url(),
        Credentials::github_app("Iv1.emulated", 42, &pem).unwrap(),
    )
    .allow_http(true)
    .build()
    .unwrap();
    scale_set(&client, "set").await;
    assert_eq!(emu.requests_to(Endpoint::AccessToken)[0].status, 201);

    let wrong = Client::builder(
        emu.config_url(),
        Credentials::github_app("Iv1.emulated", 7, &pem).unwrap(),
    )
    .allow_http(true)
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
    .allow_http(true)
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
        .get_runner_scale_set_by_name(gpu.id, "big")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(found.id, set.id);
    assert!(
        client
            .get_runner_scale_set_by_name(RunnerGroupId::DEFAULT, "big")
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
async fn runners_by_id_and_name() {
    let emu = emulator().await;
    let client = emu.client();
    let id = scale_set(&client, "set").await;
    let runner = client
        .generate_jit_config(id, &JitRunnerSetting::new("r1"))
        .await
        .unwrap()
        .runner
        .unwrap();

    let by_id = client.get_runner(runner.id).await.unwrap();
    assert_eq!((by_id.name.as_str(), by_id.runner_scale_set_id), ("r1", id));
    let by_name = client.get_runner_by_name("r1").await.unwrap().unwrap();
    assert_eq!(by_name.id, runner.id);

    // Unknown names and IDs.
    assert!(client.get_runner_by_name("r2").await.unwrap().is_none());
    let err = client.get_runner(RunnerId(999)).await.unwrap_err();
    assert!(err.is_runner_not_found(), "{err}");

    // A removed runner is gone under both.
    client.remove_runner(runner.id).await.unwrap();
    let err = client.get_runner(runner.id).await.unwrap_err();
    assert!(err.is_runner_not_found(), "{err}");
    assert!(client.get_runner_by_name("r1").await.unwrap().is_none());
}

#[tokio::test]
async fn zero_time_faults_never_apply() {
    let emu = emulator().await;
    let client = emu.client();
    emu.inject(Fault::status(Endpoint::ScaleSets, 500).times(0));
    scale_set(&client, "set").await;
    assert!(
        emu.requests_to(Endpoint::ScaleSets)
            .iter()
            .all(|r| r.status == 200)
    );
}

#[tokio::test]
async fn reset_cancels_pending_completions() {
    const RUN: Duration = Duration::from_secs(2);
    let emu = Emulator::builder()
        .long_poll(Duration::from_millis(50))
        .auto_run_jobs(RUN)
        .start()
        .await;
    let run_one = || async {
        let client = emu.client();
        let id = scale_set(&client, "set").await;
        let session = client.create_message_session(id, "host").await.unwrap();
        let job = emu.queue_job(id, ["set"]);
        session.acquire_jobs(&[job]).await.unwrap();
        client
            .generate_jit_config(id, &JitRunnerSetting::new("r1"))
            .await
            .unwrap();
        job
    };

    let before = run_one().await;
    // The old job runs, so its completion is pending.
    assert_eq!(emu.state().jobs[0].state, JobState::Running);
    tokio::time::sleep(RUN / 2).await;
    emu.reset();
    let new_start = Instant::now();
    let after = run_one().await;
    assert_eq!(before, after, "IDs restart after a reset");
    assert_eq!(emu.state().jobs[0].state, JobState::Running);

    // The old completion is due about RUN / 2 from now. If it leaked, it
    // would finish the new job early; the new job's own completion comes no
    // sooner than RUN after it started.
    emu.wait_until(WAIT, |s| s.jobs[0].state == JobState::Completed)
        .await;
    assert!(
        new_start.elapsed() >= RUN,
        "the new job completed after {:?}",
        new_start.elapsed()
    );
}

#[tokio::test]
async fn reset_clears_everything() {
    let emu = emulator().await;
    let client = emu.client();
    scale_set(&client, "set").await;
    emu.inject(Fault::status(Endpoint::Any, 500));
    emu.reset();
    assert_eq!(emu.state(), scaleset::testing::EmulatorState::default());
    assert!(emu.requests().is_empty());
    // Old tokens no longer work; a fresh client starts over.
    emu.client()
        .create_runner_scale_set(&RunnerScaleSetSpec::new("set"))
        .await
        .unwrap();
}
