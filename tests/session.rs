//! Wire tests for `MessageSession`: polling, acknowledging, acquiring jobs and refreshing, against wiremock.

mod fixtures;

use fixtures::server::Fake;
use scaleset::{ExposeSecret, JobRequestId, MessageId, MessageSession, RunnerId, ScaleSetId};
use serde_json::json;
use wiremock::matchers::{body_json, header, method, path, query_param, query_param_is_missing};
use wiremock::{Mock, ResponseTemplate};

const SESSION_ID: &str = "6f1d4b5e-0c39-4b27-9c43-2a4f1d0a1b2c";

fn session_json(server: &str, token: &str) -> serde_json::Value {
    json!({
        "sessionId": SESSION_ID,
        "ownerName": "host-1",
        "messageQueueUrl": format!("{server}/queue/msgs?q=1"),
        "messageQueueAccessToken": token,
        "statistics": {"totalAssignedJobs": 1, "totalRunningJobs": 0}
    })
}

fn message_json(id: i64) -> serde_json::Value {
    let jobs = json!([
        {"messageType": "JobAvailable", "runnerRequestId": 100 + id, "acquireJobUrl": "x"},
        {"messageType": "JobStarted", "runnerRequestId": 50, "runnerId": 3, "runnerName": "r3"}
    ]);
    json!({
        "messageId": id,
        "messageType": "RunnerScaleSetJobMessages",
        "body": jobs.to_string(),
        "statistics": {"totalAssignedJobs": 4}
    })
}

async fn session(fake: &Fake) -> MessageSession {
    Mock::given(method("POST"))
        .and(path("/tenant/_apis/runtime/runnerscalesets/5/sessions"))
        .and(body_json(json!({
            "sessionId": "00000000-0000-0000-0000-000000000000",
            "ownerName": "host-1"
        })))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(session_json(&fake.server.uri(), "old")),
        )
        .expect(1)
        .mount(&fake.server)
        .await;
    fake.client()
        .create_message_session(ScaleSetId(5), "host-1")
        .await
        .expect("session opens")
}

#[tokio::test]
async fn polls_and_acknowledges() {
    let fake = Fake::start().await;
    let session = session(&fake).await;
    assert_eq!(session.info().session_id.to_string(), SESSION_ID);
    assert_eq!(session.info().statistics.unwrap().total_assigned_jobs, 1);

    Mock::given(method("GET"))
        .and(path("/queue/msgs"))
        .and(query_param("lastMessageId", "6"))
        .respond_with(ResponseTemplate::new(202))
        .mount(&fake.server)
        .await;
    Mock::given(method("GET"))
        .and(path("/queue/msgs"))
        .and(query_param("q", "1"))
        .and(query_param_is_missing("lastMessageId"))
        .and(header("authorization", "Bearer old"))
        .and(header("x-scalesetmaxcapacity", "10"))
        .and(header(
            "accept",
            "application/json; api-version=6.0-preview",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(message_json(6)))
        .mount(&fake.server)
        .await;
    Mock::given(method("DELETE"))
        .and(path("/queue/msgs/6"))
        .and(query_param("q", "1"))
        .and(header("authorization", "Bearer old"))
        .respond_with(ResponseTemplate::new(204))
        .expect(1)
        .mount(&fake.server)
        .await;

    let msg = session.get_message(None, 10).await.unwrap().unwrap();
    assert_eq!(msg.id, MessageId(6));
    assert_eq!(msg.statistics.unwrap().total_assigned_jobs, 4);
    assert_eq!(
        msg.jobs_available().next().unwrap().job.runner_request_id,
        JobRequestId(106)
    );
    assert_eq!(msg.jobs_started().next().unwrap().runner_name, "r3");

    session.delete_message(msg.id).await.unwrap();
    assert!(
        session
            .get_message(Some(msg.id), 10)
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn refreshes_expired_queue_token_once() {
    let fake = Fake::start().await;
    let session = session(&fake).await;

    Mock::given(method("GET"))
        .and(path("/queue/msgs"))
        .and(header("authorization", "Bearer old"))
        .respond_with(ResponseTemplate::new(401))
        .mount(&fake.server)
        .await;
    Mock::given(method("GET"))
        .and(path("/queue/msgs"))
        .and(header("authorization", "Bearer new"))
        .respond_with(ResponseTemplate::new(202))
        .mount(&fake.server)
        .await;
    Mock::given(method("PATCH"))
        .and(path(format!(
            "/tenant/_apis/runtime/runnerscalesets/5/sessions/{SESSION_ID}"
        )))
        .and(header("authorization", fake.admin_auth().as_str()))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(session_json(&fake.server.uri(), "new")),
        )
        .expect(1)
        .mount(&fake.server)
        .await;

    // Concurrent polls hitting the expired token share one refresh.
    let (a, b) = tokio::join!(session.get_message(None, 1), session.get_message(None, 1));
    assert!(a.unwrap().is_none());
    assert!(b.unwrap().is_none());
    assert_eq!(
        session.info().message_queue_access_token.expose_secret(),
        "new"
    );
}

#[tokio::test]
async fn acquires_jobs_with_queue_token() {
    let fake = Fake::start().await;
    let session = session(&fake).await;
    Mock::given(method("POST"))
        .and(path("/tenant/_apis/runtime/runnerscalesets/5/acquirejobs"))
        .and(query_param("api-version", "6.0-preview"))
        .and(header("authorization", "Bearer old"))
        .and(body_json(json!([1, 2])))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"count": 1, "value": [1]})))
        .expect(1)
        .mount(&fake.server)
        .await;

    let acquired = session
        .acquire_jobs(&[JobRequestId(1), JobRequestId(2)])
        .await
        .unwrap();
    assert_eq!(acquired, vec![JobRequestId(1)]);
}

/// Serves one queue message with `envelope` and expects it acknowledged.
async fn serve_once(fake: &Fake, envelope: serde_json::Value) {
    Mock::given(method("GET"))
        .and(path("/queue/msgs"))
        .and(query_param_is_missing("lastMessageId"))
        .respond_with(ResponseTemplate::new(200).set_body_json(envelope))
        .mount(&fake.server)
        .await;
    Mock::given(method("DELETE"))
        .and(path("/queue/msgs/7"))
        .respond_with(ResponseTemplate::new(204))
        .expect(1)
        .mount(&fake.server)
        .await;
}

#[tokio::test]
async fn null_counts_read_as_zero() {
    let fake = Fake::start().await;
    let session = session(&fake).await;
    serve_once(
        &fake,
        json!({
            "messageId": 7,
            "messageType": "RunnerScaleSetJobMessages",
            "body": null,
            "statistics": {"totalAssignedJobs": null, "totalRunningJobs": 2}
        }),
    )
    .await;

    let msg = session.get_message(None, 10).await.unwrap().unwrap();
    assert_eq!(msg.id, MessageId(7));
    assert!(msg.jobs.is_empty());
    let statistics = msg.statistics.unwrap();
    assert_eq!(statistics.total_assigned_jobs, 0);
    assert_eq!(statistics.total_running_jobs, 2);
    session.delete_message(msg.id).await.unwrap();
}

#[tokio::test]
async fn canceled_job_has_no_runner() {
    let fake = Fake::start().await;
    let session = session(&fake).await;
    // The service sends Go's zero values when no runner took the job.
    let jobs = json!([
        {"messageType": "JobCompleted", "runnerRequestId": 9, "result": "canceled",
         "runnerId": 0, "runnerName": ""},
        {"messageType": "JobCompleted", "runnerRequestId": 10, "result": "succeeded",
         "runnerId": 3, "runnerName": "r3"}
    ]);
    serve_once(
        &fake,
        json!({
            "messageId": 7,
            "messageType": "RunnerScaleSetJobMessages",
            "body": jobs.to_string(),
            "statistics": {}
        }),
    )
    .await;

    let msg = session.get_message(None, 10).await.unwrap().unwrap();
    let completed: Vec<_> = msg
        .jobs_completed()
        .map(|j| (j.result.as_str(), j.runner_id, j.runner_name.as_deref()))
        .collect();
    assert_eq!(
        completed,
        [
            ("canceled", None, None),
            ("succeeded", Some(RunnerId(3)), Some("r3")),
        ]
    );
    session.delete_message(msg.id).await.unwrap();
}
