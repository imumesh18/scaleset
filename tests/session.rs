mod common;

use std::time::Duration;

use common::Fake;
use scaleset::listener::{Event, Listener, Scaler};
use scaleset::{BoxError, JobRequestId, MessageId, MessageSession, ScaleSetId};
use serde_json::json;
use wiremock::matchers::{body_json, header, method, path, query_param};
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
        .unwrap()
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
    assert_eq!(session.info().message_queue_access_token, "new");
}

#[tokio::test]
async fn persistent_401_is_reported() {
    let fake = Fake::start().await;
    let session = session(&fake).await;
    Mock::given(method("DELETE"))
        .and(path("/queue/msgs/1"))
        .respond_with(ResponseTemplate::new(401))
        .mount(&fake.server)
        .await;
    Mock::given(method("PATCH"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(session_json(&fake.server.uri(), "new")),
        )
        .mount(&fake.server)
        .await;

    let err = session.delete_message(MessageId(1)).await.unwrap_err();
    assert!(err.is_message_queue_token_expired(), "{err}");
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

#[tokio::test]
async fn close_deletes_session() {
    let fake = Fake::start().await;
    let session = session(&fake).await;
    Mock::given(method("DELETE"))
        .and(path(format!(
            "/tenant/_apis/runtime/runnerscalesets/5/sessions/{SESSION_ID}"
        )))
        .respond_with(ResponseTemplate::new(204))
        .expect(1)
        .mount(&fake.server)
        .await;
    session.close().await.unwrap();
}

#[tokio::test]
async fn session_conflict_is_reported() {
    let fake = Fake::start().await;
    Mock::given(method("POST"))
        .and(path("/tenant/_apis/runtime/runnerscalesets/5/sessions"))
        .respond_with(ResponseTemplate::new(409).set_body_json(json!({
            "typeName": "TaskAgentSessionConflictException",
            "message": "already has an active session"
        })))
        .mount(&fake.server)
        .await;
    let err = fake
        .client()
        .create_message_session(ScaleSetId(5), "host")
        .await
        .unwrap_err();
    assert!(err.is_conflict(), "{err}");
}

/// Runs the listener against the fake service: initial statistics, one
/// message (acquired and acknowledged), then idle polls until shutdown.
#[tokio::test]
async fn listener_end_to_end() {
    let fake = Fake::start().await;
    let session = session(&fake).await;

    Mock::given(method("GET"))
        .and(path("/queue/msgs"))
        .and(query_param("lastMessageId", "6"))
        .respond_with(ResponseTemplate::new(202).set_delay(Duration::from_millis(20)))
        .with_priority(1)
        .mount(&fake.server)
        .await;
    Mock::given(method("GET"))
        .and(path("/queue/msgs"))
        .respond_with(ResponseTemplate::new(200).set_body_json(message_json(6)))
        .mount(&fake.server)
        .await;
    Mock::given(method("DELETE"))
        .and(path("/queue/msgs/6"))
        .respond_with(ResponseTemplate::new(204))
        .expect(1)
        .mount(&fake.server)
        .await;
    Mock::given(method("POST"))
        .and(path("/tenant/_apis/runtime/runnerscalesets/5/acquirejobs"))
        .and(body_json(json!([106])))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"count": 1, "value": [106]})))
        .expect(1)
        .mount(&fake.server)
        .await;

    struct Recorder {
        session: MessageSession,
        desired: Vec<u32>,
        idle: usize,
        stop: Option<tokio::sync::oneshot::Sender<()>>,
    }
    impl Scaler for Recorder {
        type Error = BoxError;
        async fn scale(&mut self, event: Event<'_>) -> Result<(), BoxError> {
            if let Event::Message(msg) = event {
                let ids: Vec<_> = msg
                    .jobs_available()
                    .map(|j| j.job.runner_request_id)
                    .collect();
                self.session.acquire_jobs(&ids).await?;
            }
            match event.statistics() {
                Some(stats) => self.desired.push(stats.total_assigned_jobs),
                None => {
                    self.idle += 1;
                    if self.idle == 2 {
                        let _ = self.stop.take().unwrap().send(());
                    }
                }
            }
            Ok(())
        }
    }

    let (tx, rx) = tokio::sync::oneshot::channel();
    let mut scaler = Recorder {
        session: session.clone(),
        desired: vec![],
        idle: 0,
        stop: Some(tx),
    };
    Listener::new(session.clone(), 3)
        .run(&mut scaler, async {
            let _ = rx.await;
        })
        .await
        .unwrap();

    assert_eq!(scaler.desired, [1, 4]);
    assert_eq!(scaler.idle, 2);
}
