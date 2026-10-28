mod common;

use std::time::Duration;

use common::{Fake, PAT, fast_retries};
use scaleset::{
    Client, Credentials, Error, ExposeSecret, JitRunnerSetting, RateLimitKind, RateLimitPolicy,
    RateLimitRule, RetryPolicy, RunnerGroupId, RunnerId, RunnerScaleSetSpec, ScaleSetId,
    SystemInfo,
};
use serde_json::json;
use wiremock::matchers::{body_json, header, header_exists, method, path, query_param};
use wiremock::{Mock, ResponseTemplate};

fn scale_set_json(id: i64, name: &str) -> serde_json::Value {
    json!({
        "id": id,
        "name": name,
        "runnerGroupId": 1,
        "runnerGroupName": "Default",
        "labels": [{"name": name, "type": "System"}],
        "RunnerSetting": {},
        "createdOn": "2024-05-01T10:00:00Z",
        "statistics": {"totalAssignedJobs": 3}
    })
}

#[tokio::test]
async fn pat_token_exchange_and_lookup() {
    let fake = Fake::start().await;
    Mock::given(method("GET"))
        .and(path("/tenant/_apis/runtime/runnerscalesets"))
        .and(query_param("runnerGroupId", "1"))
        .and(query_param("name", "my set"))
        .and(query_param("api-version", "6.0-preview"))
        .and(header("authorization", fake.admin_auth().as_str()))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(json!({"count": 1, "value": [scale_set_json(7, "my set")]})),
        )
        .expect(1)
        .mount(&fake.server)
        .await;

    let client = fake.client();
    let set = client
        .get_runner_scale_set(RunnerGroupId::DEFAULT, "my set")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(set.id, ScaleSetId(7));
    assert_eq!(set.statistics.unwrap().total_assigned_jobs, 3);
    assert!(set.created_on.is_some());

    let requests = fake.server.received_requests().await.unwrap();
    let registration = &requests[0];
    assert_eq!(
        registration.headers.get("authorization").unwrap(),
        &format!("Bearer {PAT}")
    );
    let ua: serde_json::Value =
        serde_json::from_slice(registration.headers.get("user-agent").unwrap().as_bytes()).unwrap();
    assert_eq!(ua["kind"], "scaleset");

    let admin = &requests[1];
    let body: serde_json::Value = serde_json::from_slice(&admin.body).unwrap();
    assert_eq!(
        body,
        json!({"url": fake.config_url(), "runner_event": "register"})
    );
}

#[tokio::test]
async fn github_app_token_exchange() {
    let fake = Fake::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v3/app/installations/42/access_tokens"))
        .respond_with(ResponseTemplate::new(201).set_body_json(json!({
            "token": "ghs_installation",
            "expires_at": "2099-01-01T00:00:00Z"
        })))
        .expect(1)
        .mount(&fake.server)
        .await;
    Mock::given(method("GET"))
        .and(path("/tenant/_apis/runtime/runnerscalesets/7"))
        .respond_with(ResponseTemplate::new(200).set_body_json(scale_set_json(7, "s")))
        .mount(&fake.server)
        .await;

    let pem = test_key_pem();
    let client = Client::builder(
        fake.config_url(),
        Credentials::github_app("Iv1.app", 42, &pem).unwrap(),
    )
    .build()
    .unwrap();
    client
        .get_runner_scale_set_by_id(ScaleSetId(7))
        .await
        .unwrap();

    let requests = fake.server.received_requests().await.unwrap();
    let jwt = requests[0]
        .headers
        .get("authorization")
        .unwrap()
        .to_str()
        .unwrap();
    assert!(jwt.starts_with("Bearer eyJ"), "{jwt}");
    assert_eq!(
        requests[1].headers.get("authorization").unwrap(),
        "Bearer ghs_installation"
    );
}

#[tokio::test]
async fn admin_token_is_cached_until_near_expiry() {
    let fake = Fake::start().await;
    Mock::given(method("DELETE"))
        .and(path("/tenant/_apis/distributedtask/pools/0/agents/5"))
        .respond_with(ResponseTemplate::new(204))
        .mount(&fake.server)
        .await;
    let client = fake.client();
    client.remove_runner(RunnerId(5)).await.unwrap();
    client.clone().remove_runner(RunnerId(5)).await.unwrap();
    assert_eq!(count(&fake, "/api/v3/actions/runner-registration").await, 1);

    // A token within a minute of expiry is refreshed on every call.
    let fake = Fake::start_with_ttl(Duration::from_secs(30)).await;
    Mock::given(method("DELETE"))
        .and(path("/tenant/_apis/distributedtask/pools/0/agents/5"))
        .respond_with(ResponseTemplate::new(204))
        .mount(&fake.server)
        .await;
    let client = fake.client();
    client.remove_runner(RunnerId(5)).await.unwrap();
    client.remove_runner(RunnerId(5)).await.unwrap();
    assert_eq!(count(&fake, "/api/v3/actions/runner-registration").await, 2);
}

#[tokio::test]
async fn concurrent_calls_share_one_refresh() {
    let fake = Fake::start().await;
    Mock::given(method("DELETE"))
        .respond_with(ResponseTemplate::new(204))
        .mount(&fake.server)
        .await;
    let client = fake.client();
    let calls = (0..8).map(|i| {
        let client = client.clone();
        tokio::spawn(async move { client.remove_runner(RunnerId(i)).await })
    });
    for call in calls {
        call.await.unwrap().unwrap();
    }
    assert_eq!(count(&fake, "/api/v3/actions/runner-registration").await, 1);
}

#[tokio::test]
async fn create_scale_set_sends_default_label() {
    let fake = Fake::start().await;
    Mock::given(method("POST"))
        .and(path("/tenant/_apis/runtime/runnerscalesets"))
        .and(body_json(json!({
            "name": "set",
            "runnerGroupId": 1,
            "labels": [{"name": "set", "type": "System"}],
            "RunnerSetting": {"disableUpdate": true}
        })))
        .respond_with(ResponseTemplate::new(200).set_body_json(scale_set_json(9, "set")))
        .expect(1)
        .mount(&fake.server)
        .await;

    let set = fake
        .client()
        .create_runner_scale_set(
            &RunnerScaleSetSpec::new("set")
                .runner_group_id(RunnerGroupId::DEFAULT)
                .disable_update(true),
        )
        .await
        .unwrap();
    assert_eq!(set.id, ScaleSetId(9));

    let err = fake
        .client()
        .create_runner_scale_set(&RunnerScaleSetSpec::default())
        .await
        .unwrap_err();
    assert!(matches!(err, Error::InvalidInput(_)), "{err}");
}

#[tokio::test]
async fn jit_config() {
    let fake = Fake::start().await;
    Mock::given(method("POST"))
        .and(path(
            "/tenant/_apis/runtime/runnerscalesets/3/generatejitconfig",
        ))
        .and(body_json(json!({"name": "r1", "workFolder": ""})))
        .respond_with(
            // The Actions service sometimes prefixes bodies with a BOM.
            ResponseTemplate::new(200).set_body_raw(
                [
                    b"\xEF\xBB\xBF".as_slice(),
                    json!({"runner": {"id": 11, "name": "r1", "runnerScaleSetId": 3},
                           "encodedJITConfig": "c2VjcmV0"})
                    .to_string()
                    .as_bytes(),
                ]
                .concat(),
                "application/json",
            ),
        )
        .mount(&fake.server)
        .await;

    let jit = fake
        .client()
        .generate_jit_config(ScaleSetId(3), &JitRunnerSetting::new("r1"))
        .await
        .unwrap();
    assert_eq!(jit.encoded_jit_config.expose_secret(), "c2VjcmV0");
    assert_eq!(jit.runner.unwrap().id, RunnerId(11));
}

#[tokio::test]
async fn service_exceptions_map_to_kinds() {
    let fake = Fake::start().await;
    Mock::given(method("DELETE"))
        .and(path("/tenant/_apis/distributedtask/pools/0/agents/1"))
        .respond_with(
            ResponseTemplate::new(404)
                .insert_header("ActivityId", "act-1")
                .set_body_json(json!({
                    "typeName": "GitHub.DistributedTask.WebApi.AgentNotFoundException, GitHub.DistributedTask.WebApi",
                    "message": "No agent found"
                })),
        )
        .mount(&fake.server)
        .await;
    Mock::given(method("DELETE"))
        .and(path("/tenant/_apis/distributedtask/pools/0/agents/2"))
        .respond_with(ResponseTemplate::new(400).set_body_json(json!({
            "typeName": "JobStillRunningException",
            "message": "busy"
        })))
        .mount(&fake.server)
        .await;
    Mock::given(method("DELETE"))
        .and(path("/tenant/_apis/distributedtask/pools/0/agents/3"))
        .respond_with(ResponseTemplate::new(409).set_body_string("plain conflict"))
        .mount(&fake.server)
        .await;

    let client = fake.client();
    let err = client.remove_runner(RunnerId(1)).await.unwrap_err();
    assert!(err.is_not_found() && err.is_runner_not_found(), "{err}");
    let api = err.api_error().unwrap();
    assert_eq!(api.activity_id.as_deref(), Some("act-1"));
    assert_eq!(api.message, "No agent found");
    assert!(err.to_string().contains("activity_id=act-1"), "{err}");

    let err = client.remove_runner(RunnerId(2)).await.unwrap_err();
    assert!(err.is_bad_request() && err.is_job_still_running(), "{err}");

    let err = client.remove_runner(RunnerId(3)).await.unwrap_err();
    assert!(err.is_conflict(), "{err}");
    assert_eq!(err.api_error().unwrap().message, "plain conflict");
}

#[tokio::test]
async fn lookups_return_none_or_reject_duplicates() {
    let fake = Fake::start().await;
    Mock::given(method("GET"))
        .and(path("/tenant/_apis/runtime/runnergroups/"))
        .and(query_param("groupName", "none"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"count": 0, "value": []})))
        .mount(&fake.server)
        .await;
    Mock::given(method("GET"))
        .and(path("/tenant/_apis/runtime/runnergroups/"))
        .and(query_param("groupName", "dup"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(json!({"count": 2, "value": [
                {"id": 1, "name": "dup"}, {"id": 2, "name": "dup"}
            ]})),
        )
        .mount(&fake.server)
        .await;
    Mock::given(method("GET"))
        .and(path("/tenant/_apis/runtime/runnergroups/"))
        .and(query_param("groupName", "one"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(json!({"count": 1, "value": [
                {"id": 4, "name": "one", "isDefaultGroup": false, "size": 2}
            ]})),
        )
        .mount(&fake.server)
        .await;

    let client = fake.client();
    assert!(
        client
            .get_runner_group_by_name("none")
            .await
            .unwrap()
            .is_none()
    );
    assert!(matches!(
        client.get_runner_group_by_name("dup").await,
        Err(Error::UnexpectedResponse(_))
    ));
    let group = client
        .get_runner_group_by_name("one")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(group.id, RunnerGroupId(4));
}

#[tokio::test]
async fn retries_server_errors() {
    let fake = Fake::start().await;
    Mock::given(method("GET"))
        .and(path("/tenant/_apis/runtime/runnerscalesets/1"))
        .respond_with(ResponseTemplate::new(502))
        .up_to_n_times(2)
        .with_priority(1)
        .mount(&fake.server)
        .await;
    Mock::given(method("GET"))
        .and(path("/tenant/_apis/runtime/runnerscalesets/1"))
        .respond_with(ResponseTemplate::new(200).set_body_json(scale_set_json(1, "s")))
        .mount(&fake.server)
        .await;

    fake.client()
        .get_runner_scale_set_by_id(ScaleSetId(1))
        .await
        .unwrap();
    assert_eq!(
        count(&fake, "/tenant/_apis/runtime/runnerscalesets/1").await,
        3
    );
}

#[tokio::test]
async fn gives_up_after_max_retries() {
    let fake = Fake::start().await;
    Mock::given(method("GET"))
        .and(path("/tenant/_apis/runtime/runnerscalesets/1"))
        .respond_with(ResponseTemplate::new(500).set_body_string("down"))
        .mount(&fake.server)
        .await;

    let client = fake
        .client()
        .with_retry_policy(fast_retries().max_retries(2));
    let err = client
        .get_runner_scale_set_by_id(ScaleSetId(1))
        .await
        .unwrap_err();
    assert_eq!(
        err.status(),
        Some(reqwest::StatusCode::INTERNAL_SERVER_ERROR)
    );
    assert_eq!(
        count(&fake, "/tenant/_apis/runtime/runnerscalesets/1").await,
        3
    );

    // Not retried: 501 and 4xx.
    let fake = Fake::start().await;
    Mock::given(method("GET"))
        .and(path("/tenant/_apis/runtime/runnerscalesets/1"))
        .respond_with(ResponseTemplate::new(501))
        .mount(&fake.server)
        .await;
    fake.client()
        .get_runner_scale_set_by_id(ScaleSetId(1))
        .await
        .unwrap_err();
    assert_eq!(
        count(&fake, "/tenant/_apis/runtime/runnerscalesets/1").await,
        1
    );
}

#[tokio::test]
async fn rate_limited_403_is_not_retried_by_default() {
    let fake = Fake::start().await;
    Mock::given(method("GET"))
        .and(path("/tenant/_apis/runtime/runnerscalesets/1"))
        .respond_with(
            ResponseTemplate::new(403)
                .insert_header("X-RateLimit-Remaining", "0")
                .insert_header("Retry-After", "30")
                .set_body_json(json!({"message": "API rate limit exceeded"})),
        )
        .mount(&fake.server)
        .await;

    let err = fake
        .client()
        .get_runner_scale_set_by_id(ScaleSetId(1))
        .await
        .unwrap_err();
    let rl = err.rate_limit().expect("rate limit error");
    assert_eq!(rl.kind, RateLimitKind::Primary);
    assert_eq!(rl.retry_after, Some(Duration::from_secs(30)));
    assert_eq!(rl.retries, 0);
    assert_eq!(rl.response.message, "API rate limit exceeded");
    assert_eq!(
        count(&fake, "/tenant/_apis/runtime/runnerscalesets/1").await,
        1
    );
}

#[tokio::test]
async fn too_many_requests_is_retried() {
    let fake = Fake::start().await;
    Mock::given(method("GET"))
        .and(path("/tenant/_apis/runtime/runnerscalesets/1"))
        .respond_with(ResponseTemplate::new(429).insert_header("Retry-After", "0"))
        .up_to_n_times(1)
        .with_priority(1)
        .mount(&fake.server)
        .await;
    Mock::given(method("GET"))
        .and(path("/tenant/_apis/runtime/runnerscalesets/1"))
        .respond_with(ResponseTemplate::new(200).set_body_json(scale_set_json(1, "s")))
        .mount(&fake.server)
        .await;
    fake.client()
        .get_runner_scale_set_by_id(ScaleSetId(1))
        .await
        .unwrap();
}

#[tokio::test]
async fn rate_limit_rules_count_retries() {
    let fake = Fake::start().await;
    Mock::given(method("GET"))
        .and(path("/tenant/_apis/runtime/runnerscalesets/1"))
        .respond_with(
            ResponseTemplate::new(403)
                .set_body_json(json!({"message": "You have exceeded a secondary rate limit."})),
        )
        .mount(&fake.server)
        .await;

    let client =
        fake.client()
            .with_retry_policy(fast_retries().rate_limit(RateLimitPolicy::rules(
                RateLimitRule::give_up(),
                RateLimitRule::retries(2),
            )));
    let err = client
        .get_runner_scale_set_by_id(ScaleSetId(1))
        .await
        .unwrap_err();
    let rl = err.rate_limit().unwrap();
    assert_eq!(rl.kind, RateLimitKind::Secondary);
    assert_eq!(rl.retries, 2);
    assert_eq!(
        count(&fake, "/tenant/_apis/runtime/runnerscalesets/1").await,
        3
    );
}

#[tokio::test]
async fn budget_returns_last_response_instead_of_sleeping() {
    let fake = Fake::start().await;
    Mock::given(method("GET"))
        .and(path("/tenant/_apis/runtime/runnerscalesets/1"))
        .respond_with(ResponseTemplate::new(429).insert_header("Retry-After", "60"))
        .mount(&fake.server)
        .await;

    let client = fake
        .client()
        .with_retry_policy(RetryPolicy::default().max_elapsed(Duration::from_secs(5)));
    let started = std::time::Instant::now();
    let err = client
        .get_runner_scale_set_by_id(ScaleSetId(1))
        .await
        .unwrap_err();
    assert!(started.elapsed() < Duration::from_secs(2));
    assert_eq!(
        err.rate_limit().unwrap().retry_after,
        Some(Duration::from_secs(60))
    );
}

#[tokio::test]
async fn budget_cuts_off_slow_attempts() {
    let fake = Fake::start().await;
    Mock::given(method("GET"))
        .and(path("/tenant/_apis/runtime/runnerscalesets/1"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(scale_set_json(1, "s"))
                .set_delay(Duration::from_secs(2)),
        )
        .mount(&fake.server)
        .await;

    // Fetch the admin token first, so the budget only covers the slow call.
    let client = fake.client();
    client.get_runner_by_name("warm-up").await.ok();

    let err = client
        .with_retry_policy(RetryPolicy::default().max_elapsed(Duration::from_millis(200)))
        .get_runner_scale_set_by_id(ScaleSetId(1))
        .await
        .unwrap_err();
    assert!(matches!(err, Error::MaxElapsedExceeded { .. }), "{err}");
    assert!(err.is_timeout());
}

#[tokio::test]
async fn admin_connection_retries_unauthorized() {
    let fake = Fake::start().await;
    // A fresh registration token is briefly rejected.
    Mock::given(method("POST"))
        .and(path("/api/v3/actions/runner-registration"))
        .respond_with(ResponseTemplate::new(401))
        .up_to_n_times(1)
        .with_priority(1)
        .mount(&fake.server)
        .await;
    Mock::given(method("DELETE"))
        .respond_with(ResponseTemplate::new(204))
        .mount(&fake.server)
        .await;

    fake.client().remove_runner(RunnerId(1)).await.unwrap();
    assert_eq!(count(&fake, "/api/v3/actions/runner-registration").await, 2);

    // With retries disabled, the 401 is returned.
    let fake = Fake::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v3/actions/runner-registration"))
        .respond_with(ResponseTemplate::new(401))
        .with_priority(1)
        .mount(&fake.server)
        .await;
    let err = fake
        .client()
        .with_retry_policy(RetryPolicy::none())
        .remove_runner(RunnerId(1))
        .await
        .unwrap_err();
    assert!(err.is_unauthorized(), "{err}");
}

#[tokio::test]
async fn system_info_updates_user_agent() {
    let fake = Fake::start().await;
    Mock::given(method("DELETE"))
        .and(header_exists("user-agent"))
        .respond_with(ResponseTemplate::new(204))
        .mount(&fake.server)
        .await;
    let client = fake.client();
    client.set_system_info(SystemInfo::new("my-system").subsystem("controller"));
    client.remove_runner(RunnerId(1)).await.unwrap();

    let requests = fake.server.received_requests().await.unwrap();
    let last = requests.last().unwrap();
    let ua: serde_json::Value =
        serde_json::from_slice(last.headers.get("user-agent").unwrap().as_bytes()).unwrap();
    assert_eq!(ua["system"], "my-system");
    assert_eq!(ua["subsystem"], "controller");
}

#[test]
fn rejects_bad_config_url() {
    let err = Client::builder(
        "https://github.com",
        Credentials::personal_access_token(PAT).unwrap(),
    )
    .build()
    .unwrap_err();
    assert!(matches!(err, Error::InvalidConfigUrl { .. }));
}

async fn count(fake: &Fake, p: &str) -> usize {
    fake.server
        .received_requests()
        .await
        .unwrap()
        .iter()
        .filter(|r| r.url.path() == p)
        .count()
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
