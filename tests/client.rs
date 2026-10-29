mod fixtures;

use std::time::Duration;

use fixtures::server::{Fake, PAT};
use fixtures::test_key_pem;
use scaleset::{
    Client, Credentials, Error, ExposeSecret, JitRunnerSetting, RetryPolicy, RunnerGroupId,
    RunnerId, RunnerScaleSetSpec, ScaleSetId, SystemInfo,
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
        .get_runner_scale_set_by_name(RunnerGroupId::DEFAULT, "my set")
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
    .allow_http(true)
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
async fn generate_jit_config_body_and_bom_response() {
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
async fn huge_retry_after_and_budget_do_not_overflow() {
    let fake = Fake::start().await;
    Mock::given(method("GET"))
        .and(path("/tenant/_apis/runtime/runnerscalesets/1"))
        .respond_with(
            ResponseTemplate::new(429).insert_header("Retry-After", "9223372036854775807"),
        )
        .mount(&fake.server)
        .await;

    // The wait cannot fit any budget, so the 429 comes back at once.
    let err = fake
        .client()
        .with_retry_policy(RetryPolicy::default().max_elapsed(Duration::from_secs(20)))
        .get_runner_scale_set_by_id(ScaleSetId(1))
        .await
        .unwrap_err();
    assert!(err.rate_limit().is_some(), "{err}");

    // An unbounded budget is accepted.
    Mock::given(method("GET"))
        .and(path("/tenant/_apis/runtime/runnerscalesets/2"))
        .respond_with(ResponseTemplate::new(200).set_body_json(scale_set_json(2, "s")))
        .mount(&fake.server)
        .await;
    fake.client()
        .with_retry_policy(RetryPolicy::default().max_elapsed(Duration::MAX))
        .get_runner_scale_set_by_id(ScaleSetId(2))
        .await
        .unwrap();
}

#[tokio::test]
async fn update_sends_only_what_changed() {
    let fake = Fake::start().await;
    Mock::given(method("PATCH"))
        .and(path("/tenant/_apis/runtime/runnerscalesets/4"))
        .and(body_json(json!({
            "labels": [{"name": "gpu", "type": "System"}],
            "RunnerSetting": {"disableUpdate": false}
        })))
        .respond_with(ResponseTemplate::new(200).set_body_json(scale_set_json(4, "s")))
        .expect(1)
        .mount(&fake.server)
        .await;
    fake.client()
        .update_runner_scale_set(
            ScaleSetId(4),
            &RunnerScaleSetSpec::default()
                .label("gpu")
                .disable_update(false),
        )
        .await
        .unwrap();
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

#[tokio::test]
async fn redirects_are_not_followed() {
    let fake = Fake::start().await;
    Mock::given(method("GET"))
        .and(path("/tenant/_apis/runtime/runnerscalesets/1"))
        .respond_with(
            ResponseTemplate::new(302).insert_header("location", "https://elsewhere.example/steal"),
        )
        .mount(&fake.server)
        .await;
    let err = fake
        .client()
        .get_runner_scale_set_by_id(ScaleSetId(1))
        .await
        .unwrap_err();
    assert_eq!(err.status(), Some(reqwest::StatusCode::FOUND), "{err}");
}

#[test]
fn credentials_need_https() {
    let pat = || Credentials::personal_access_token(PAT).unwrap();
    let insecure = |url: &str| {
        matches!(
            Client::builder(url, pat()).build(),
            Err(Error::InsecureUrl { .. })
        )
    };
    assert!(insecure("http://ghes.example.com/org"));
    // github.com and GHE.com never accept HTTP, even when allowed.
    let allowed = |url: &str| Client::builder(url, pat()).allow_http(true).build();
    assert!(matches!(
        allowed("http://github.com/org"),
        Err(Error::InsecureUrl { .. })
    ));
    assert!(matches!(
        allowed("http://acme.ghe.com/org"),
        Err(Error::InsecureUrl { .. })
    ));
    assert!(allowed("http://ghes.example.com/org").is_ok());
    assert!(
        Client::builder("https://github.com/org", pat())
            .build()
            .is_ok()
    );
}

#[tokio::test]
async fn insecure_service_urls_are_refused() {
    // URLs the service hands back carry tokens too, so they get the same
    // check: here a queue URL that is neither HTTPS nor allowed HTTP.
    let fake = Fake::start().await;
    Mock::given(method("POST"))
        .and(path("/tenant/_apis/runtime/runnerscalesets/5/sessions"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "sessionId": "6f1d4b5e-0c39-4b27-9c43-2a4f1d0a1b2c",
            "messageQueueUrl": "ftp://queue.example/msgs",
            "messageQueueAccessToken": "t",
        })))
        .mount(&fake.server)
        .await;
    let session = fake
        .client()
        .create_message_session(ScaleSetId(5), "o")
        .await
        .unwrap();
    let err = session.get_message(None, 1).await.unwrap_err();
    assert!(matches!(err, Error::InsecureUrl { .. }), "{err}");
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
