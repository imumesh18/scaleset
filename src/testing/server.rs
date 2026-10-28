//! HTTP layer of the emulator: routing, auth headers, faults, recording.

use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::body::{Body, to_bytes};
use axum::http::{HeaderMap, HeaderValue, Method, Request, StatusCode, header};
use axum::response::{IntoResponse, Response};
use secrecy::ExposeSecret;
use serde::Deserialize;
use serde_json::{Value, json};
use uuid::Uuid;

use super::state::{ScaleSetBody, ServiceError, ServiceResult};
use super::{Endpoint, Fault, FaultResponse, Inner, RecordedRequest};
use crate::types::{JobRequestId, MessageId, RunnerGroupId, RunnerId, ScaleSetId, SessionInfo};

const MAX_BODY: usize = 1 << 20;

pub(crate) async fn serve(listener: tokio::net::TcpListener, inner: Arc<Inner>) {
    let app = Router::new().fallback(handle).with_state(inner);
    if let Err(e) = axum::serve(listener, app).await {
        tracing::error!(error = %e, "emulator server failed");
    }
}

/// A parsed route.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Route {
    AccessToken(i64),
    RegistrationToken,
    AdminConnection,
    RunnerGroups,
    ScaleSets,
    ScaleSet(ScaleSetId),
    Jit(ScaleSetId),
    Acquire(ScaleSetId),
    Sessions(ScaleSetId),
    Session(ScaleSetId, Uuid),
    Runners,
    Runner(RunnerId),
    Queue(ScaleSetId, Uuid),
    QueueMessage(ScaleSetId, Uuid, i64),
    Control(Vec<String>),
    Unknown,
}

impl Route {
    fn parse(path: &str) -> Self {
        let segs: Vec<&str> = path.trim_matches('/').split('/').collect();
        let id = |s: &str| s.parse::<i64>().ok();
        let uuid = |s: &str| s.parse::<Uuid>().ok();
        match segs.as_slice() {
            ["api", "v3", "app", "installations", inst, "access_tokens"] => {
                id(inst).map_or(Self::Unknown, Self::AccessToken)
            }
            ["api", "v3", .., "actions", "runners", "registration-token"] => {
                Self::RegistrationToken
            }
            ["api", "v3", "actions", "runner-registration"] => Self::AdminConnection,
            ["tenant", "_apis", "runtime", "runnergroups"] => Self::RunnerGroups,
            ["tenant", "_apis", "runtime", "runnerscalesets"] => Self::ScaleSets,
            ["tenant", "_apis", "runtime", "runnerscalesets", s] => {
                id(s).map_or(Self::Unknown, |s| Self::ScaleSet(ScaleSetId(s)))
            }
            [
                "tenant",
                "_apis",
                "runtime",
                "runnerscalesets",
                s,
                "generatejitconfig",
            ] => id(s).map_or(Self::Unknown, |s| Self::Jit(ScaleSetId(s))),
            [
                "tenant",
                "_apis",
                "runtime",
                "runnerscalesets",
                s,
                "acquirejobs",
            ] => id(s).map_or(Self::Unknown, |s| Self::Acquire(ScaleSetId(s))),
            [
                "tenant",
                "_apis",
                "runtime",
                "runnerscalesets",
                s,
                "sessions",
            ] => id(s).map_or(Self::Unknown, |s| Self::Sessions(ScaleSetId(s))),
            [
                "tenant",
                "_apis",
                "runtime",
                "runnerscalesets",
                s,
                "sessions",
                sid,
            ] => match (id(s), uuid(sid)) {
                (Some(s), Some(sid)) => Self::Session(ScaleSetId(s), sid),
                _ => Self::Unknown,
            },
            ["tenant", "_apis", "distributedtask", "pools", "0", "agents"] => Self::Runners,
            [
                "tenant",
                "_apis",
                "distributedtask",
                "pools",
                "0",
                "agents",
                r,
            ] => id(r).map_or(Self::Unknown, |r| Self::Runner(RunnerId(r))),
            ["queue", s, sid] => match (id(s), uuid(sid)) {
                (Some(s), Some(sid)) => Self::Queue(ScaleSetId(s), sid),
                _ => Self::Unknown,
            },
            ["queue", s, sid, m] => match (id(s), uuid(sid), id(m)) {
                (Some(s), Some(sid), Some(m)) => Self::QueueMessage(ScaleSetId(s), sid, m),
                _ => Self::Unknown,
            },
            ["_emulator", rest @ ..] => {
                Self::Control(rest.iter().map(|s| (*s).to_owned()).collect())
            }
            _ => Self::Unknown,
        }
    }

    fn endpoint(&self, method: &Method) -> Endpoint {
        match self {
            Self::AccessToken(_) => Endpoint::AccessToken,
            Self::RegistrationToken => Endpoint::RegistrationToken,
            Self::AdminConnection => Endpoint::AdminConnection,
            Self::RunnerGroups => Endpoint::RunnerGroups,
            Self::ScaleSets | Self::ScaleSet(_) => Endpoint::ScaleSets,
            Self::Jit(_) => Endpoint::GenerateJitConfig,
            Self::Acquire(_) => Endpoint::AcquireJobs,
            Self::Sessions(_) | Self::Session(..) => Endpoint::Sessions,
            Self::Runners | Self::Runner(_) => Endpoint::Runners,
            Self::Queue(..) => Endpoint::GetMessage,
            Self::QueueMessage(..) if method == Method::DELETE => Endpoint::DeleteMessage,
            Self::QueueMessage(..) => Endpoint::Unknown,
            Self::Control(_) => Endpoint::Control,
            Self::Unknown => Endpoint::Unknown,
        }
    }

    /// Actions service endpoints need an admin token and an API version.
    fn is_actions(&self) -> bool {
        matches!(
            self,
            Self::RunnerGroups
                | Self::ScaleSets
                | Self::ScaleSet(_)
                | Self::Jit(_)
                | Self::Acquire(_)
                | Self::Sessions(_)
                | Self::Session(..)
                | Self::Runners
                | Self::Runner(_)
        )
    }
}

struct Req {
    method: Method,
    route: Route,
    query: Vec<(String, String)>,
    headers: HeaderMap,
    body: bytes::Bytes,
}

impl Req {
    fn query(&self, key: &str) -> Option<&str> {
        self.query
            .iter()
            .find(|(k, _)| k == key)
            .map(|(_, v)| v.as_str())
    }

    fn auth(&self, scheme: &str) -> Option<&str> {
        self.headers
            .get(header::AUTHORIZATION)?
            .to_str()
            .ok()?
            .strip_prefix(scheme)?
            .strip_prefix(' ')
    }

    fn bearer(&self) -> Option<&str> {
        self.auth("Bearer")
    }

    fn json<T: for<'de> Deserialize<'de>>(&self) -> ServiceResult<T> {
        serde_json::from_slice(&self.body)
            .map_err(|e| ServiceError::new(400, format!("invalid request body: {e}")))
    }
}

async fn handle(
    axum::extract::State(inner): axum::extract::State<Arc<Inner>>,
    request: Request<Body>,
) -> Response {
    let (parts, body) = request.into_parts();
    let path = parts.uri.path().to_owned();
    let raw_query = parts.uri.query().map(str::to_owned);
    let route = Route::parse(&path);
    let endpoint = route.endpoint(&parts.method);
    let user_agent = parts
        .headers
        .get(header::USER_AGENT)
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned);
    let body = to_bytes(body, MAX_BODY).await.unwrap_or_default();
    let req = Req {
        method: parts.method.clone(),
        route,
        query: url::form_urlencoded::parse(raw_query.as_deref().unwrap_or("").as_bytes())
            .into_owned()
            .collect(),
        headers: parts.headers,
        body,
    };

    let response = match take_fault(&inner, endpoint) {
        Some(FaultResponse::Delay { millis }) => {
            tokio::time::sleep(Duration::from_millis(millis)).await;
            dispatch(&inner, req).await
        }
        Some(fault) => fault_response(&fault),
        None => dispatch(&inner, req).await,
    };

    inner
        .requests
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .push(RecordedRequest {
            method: parts.method.to_string(),
            path,
            query: raw_query,
            endpoint,
            status: response.status().as_u16(),
            user_agent,
        });
    response
}

/// Takes the first fault matching `endpoint`, using up one of its times.
fn take_fault(inner: &Inner, endpoint: Endpoint) -> Option<FaultResponse> {
    if endpoint == Endpoint::Control {
        return None;
    }
    let mut state = inner.lock();
    let index = state
        .faults
        .iter()
        .position(|f| f.endpoint == endpoint || f.endpoint == Endpoint::Any)?;
    let fault: &mut Fault = &mut state.faults[index];
    let response = fault.response.clone();
    match &mut fault.times {
        Some(1) => {
            state.faults.remove(index);
        }
        Some(n) => *n -= 1,
        None => {}
    }
    Some(response)
}

fn fault_response(fault: &FaultResponse) -> Response {
    match fault {
        FaultResponse::Status { status } => {
            let status = StatusCode::from_u16(*status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
            (status, axum::Json(json!({ "message": "injected fault" }))).into_response()
        }
        FaultResponse::RateLimit {
            status,
            retry_after_secs,
            primary,
        } => {
            let status = StatusCode::from_u16(*status).unwrap_or(StatusCode::TOO_MANY_REQUESTS);
            let message = if *primary {
                "API rate limit exceeded"
            } else {
                "You have exceeded a secondary rate limit. Please wait a few minutes before you try again."
            };
            let mut response = (status, axum::Json(json!({ "message": message }))).into_response();
            let headers = response.headers_mut();
            if let Some(secs) = retry_after_secs {
                headers.insert(header::RETRY_AFTER, HeaderValue::from(*secs));
            }
            if *primary {
                headers.insert("x-ratelimit-remaining", HeaderValue::from_static("0"));
            }
            response
        }
        FaultResponse::Delay { .. } => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
}

async fn dispatch(inner: &Arc<Inner>, req: Req) -> Response {
    let result = if req.route.is_actions() {
        match actions_preconditions(inner, &req) {
            Ok(()) => route(inner, &req).await,
            Err(e) => Err(e),
        }
    } else {
        route(inner, &req).await
    };
    match result {
        Ok(response) => response,
        Err(e) => error_response(&req.route, e),
    }
}

fn actions_preconditions(inner: &Inner, req: &Req) -> ServiceResult<()> {
    if req.query("api-version").is_none() {
        return Err(ServiceError::new(400, "api-version is required"));
    }
    // acquirejobs is authorized with the message queue token.
    if matches!(req.route, Route::Acquire(_)) {
        return Ok(());
    }
    inner.lock().check_admin(req.bearer())
}

fn error_response(route: &Route, e: ServiceError) -> Response {
    let status = StatusCode::from_u16(e.status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
    let body = if route.is_actions() || e.type_name.is_some() {
        json!({ "typeName": e.type_name.unwrap_or("VssServiceException"), "message": e.message })
    } else {
        json!({ "message": e.message, "documentation_url": "https://docs.github.com/rest" })
    };
    (status, axum::Json(body)).into_response()
}

fn ok(status: StatusCode, body: Value) -> ServiceResult<Response> {
    Ok((status, axum::Json(body)).into_response())
}

fn no_content() -> ServiceResult<Response> {
    Ok(StatusCode::NO_CONTENT.into_response())
}

fn list<T: serde::Serialize>(items: &[T]) -> Value {
    json!({ "count": items.len(), "value": items })
}

fn not_allowed() -> ServiceResult<Response> {
    Err(ServiceError::new(405, "method not allowed"))
}

#[allow(clippy::too_many_lines)]
async fn route(inner: &Arc<Inner>, req: &Req) -> ServiceResult<Response> {
    let m = &req.method;
    match &req.route {
        Route::AccessToken(id) if m == Method::POST => {
            let token = inner.lock().installation_token(*id, req.bearer())?;
            ok(
                StatusCode::CREATED,
                json!({ "token": token, "expires_at": in_an_hour() }),
            )
        }
        Route::RegistrationToken if m == Method::POST => {
            let token = inner.lock().registration_token(req.bearer())?;
            ok(
                StatusCode::CREATED,
                json!({ "token": token, "expires_at": in_an_hour() }),
            )
        }
        Route::AdminConnection if m == Method::POST => {
            #[derive(Deserialize)]
            struct Body {
                url: String,
                runner_event: String,
            }
            let body: Body = req.json()?;
            if body.url.is_empty() || body.runner_event != "register" {
                return Err(ServiceError::new(
                    400,
                    "url and runner_event=register are required",
                ));
            }
            let (url, token) = inner.lock().admin_connection(req.auth("RemoteAuth"))?;
            ok(StatusCode::OK, json!({ "url": url, "token": token }))
        }
        Route::RunnerGroups if m == Method::GET => {
            let groups = inner.lock().runner_groups(req.query("groupName"));
            ok(StatusCode::OK, list(&groups))
        }
        Route::ScaleSets if m == Method::GET => {
            let group = req
                .query("runnerGroupId")
                .and_then(|g| g.parse().ok())
                .map(RunnerGroupId);
            let sets = inner.lock().scale_sets(group, req.query("name"));
            ok(StatusCode::OK, list(&sets))
        }
        Route::ScaleSets if m == Method::POST => {
            let body: ScaleSetBody = req.json()?;
            let set = inner.lock().create_scale_set(body)?;
            ok(StatusCode::OK, json!(set))
        }
        Route::ScaleSet(id) => match *m {
            Method::GET => ok(StatusCode::OK, json!(inner.lock().scale_set(*id)?)),
            Method::PATCH => {
                let body: ScaleSetBody = req.json()?;
                let set = inner.lock().update_scale_set(*id, body)?;
                ok(StatusCode::OK, json!(set))
            }
            Method::DELETE => {
                inner.lock().delete_scale_set(*id)?;
                inner.on_change();
                no_content()
            }
            _ => not_allowed(),
        },
        Route::Jit(id) if m == Method::POST => {
            #[derive(Deserialize)]
            struct Body {
                name: String,
            }
            let body: Body = req.json()?;
            let (runner, config) = inner.lock().generate_jit_config(*id, &body.name)?;
            inner.on_change();
            ok(
                StatusCode::OK,
                json!({ "runner": runner, "encodedJITConfig": config }),
            )
        }
        Route::Acquire(id) if m == Method::POST => {
            inner.lock().check_queue(*id, None, req.bearer())?;
            let ids: Vec<JobRequestId> = req.json()?;
            let acquired = inner.lock().acquire_jobs(*id, &ids);
            inner.on_change();
            ok(StatusCode::OK, list(&acquired))
        }
        Route::Sessions(id) if m == Method::POST => {
            #[derive(Deserialize)]
            #[serde(rename_all = "camelCase")]
            struct Body {
                #[serde(default)]
                owner_name: String,
            }
            let body: Body = req.json()?;
            let session = inner.lock().create_session(*id, body.owner_name)?;
            ok(StatusCode::OK, session_json(&session))
        }
        Route::Session(id, sid) => match *m {
            Method::PATCH => ok(
                StatusCode::OK,
                session_json(&inner.lock().refresh_session(*id, *sid)?),
            ),
            Method::DELETE => {
                inner.lock().delete_session(*id, *sid)?;
                no_content()
            }
            _ => not_allowed(),
        },
        Route::Runners if m == Method::GET => {
            let name = req.query("agentName").unwrap_or_default();
            ok(StatusCode::OK, list(&inner.lock().runners_named(name)))
        }
        Route::Runner(id) => match *m {
            Method::GET => ok(StatusCode::OK, json!(inner.lock().runner(*id)?)),
            Method::DELETE => {
                inner.lock().remove_runner(*id)?;
                inner.on_change();
                no_content()
            }
            _ => not_allowed(),
        },
        Route::Queue(id, sid) if m == Method::GET => poll(inner, req, *id, *sid).await,
        Route::QueueMessage(id, sid, message) if m == Method::DELETE => {
            let mut state = inner.lock();
            state.check_queue(*id, Some(*sid), req.bearer())?;
            state.ack(*id, *message)?;
            no_content()
        }
        Route::Control(path) => control(inner, req, path),
        Route::Unknown => Err(ServiceError::new(404, "no such endpoint in the emulator")),
        _ => not_allowed(),
    }
}

/// The long poll: answer with the next message, or hold until one arrives
/// or the hold time passes.
async fn poll(inner: &Arc<Inner>, req: &Req, id: ScaleSetId, sid: Uuid) -> ServiceResult<Response> {
    let last = req
        .query("lastMessageId")
        .and_then(|v| v.parse::<i64>().ok())
        .unwrap_or(0);
    let hold = inner.lock().config.long_poll;
    let deadline = tokio::time::Instant::now() + hold;
    loop {
        let notified = inner.changed.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        {
            let mut state = inner.lock();
            state.check_queue(id, Some(sid), req.bearer())?;
            if let Some((message_id, jobs, stats)) = state.next_message(id, last)? {
                let body = serde_json::to_string(&jobs)
                    .map_err(|e| ServiceError::new(500, e.to_string()))?;
                return ok(
                    StatusCode::OK,
                    json!({
                        "messageId": MessageId(message_id),
                        "messageType": "RunnerScaleSetJobMessages",
                        "body": body,
                        "statistics": stats,
                    }),
                );
            }
        }
        tokio::select! {
            () = notified => {}
            () = tokio::time::sleep_until(deadline) => {
                return Ok(StatusCode::ACCEPTED.into_response());
            }
        }
    }
}

/// The `/_emulator` control API, for clients in other languages.
fn control(inner: &Arc<Inner>, req: &Req, path: &[String]) -> ServiceResult<Response> {
    let segs: Vec<&str> = path.iter().map(String::as_str).collect();
    let m = &req.method;
    let job_id = |s: &str| {
        s.parse::<i64>()
            .map(JobRequestId)
            .map_err(|_| ServiceError::new(400, "invalid job ID"))
    };
    match segs.as_slice() {
        ["state"] if m == Method::GET => ok(StatusCode::OK, json!(inner.lock().snapshot())),
        ["jobs"] if m == Method::POST => {
            #[derive(Deserialize)]
            #[serde(rename_all = "camelCase")]
            struct Body {
                scale_set_id: ScaleSetId,
                #[serde(default)]
                labels: Vec<String>,
            }
            let body: Body = req.json()?;
            let id = inner.lock().queue_job(body.scale_set_id, body.labels)?;
            inner.on_change();
            ok(StatusCode::CREATED, json!({ "runnerRequestId": id }))
        }
        ["jobs", id, "start"] if m == Method::POST => {
            #[derive(Deserialize, Default)]
            #[serde(rename_all = "camelCase", default)]
            struct Body {
                runner_name: Option<String>,
            }
            let body: Body = if req.body.is_empty() {
                Body::default()
            } else {
                req.json()?
            };
            let runner = inner
                .lock()
                .start_job(job_id(id)?, body.runner_name.as_deref())?;
            inner.on_change();
            ok(StatusCode::OK, json!({ "runnerId": runner }))
        }
        ["jobs", id, "complete"] if m == Method::POST => {
            #[derive(Deserialize, Default)]
            #[serde(default)]
            struct Body {
                result: Option<String>,
            }
            let body: Body = if req.body.is_empty() {
                Body::default()
            } else {
                req.json()?
            };
            inner
                .lock()
                .complete_job(job_id(id)?, body.result.as_deref())?;
            inner.on_change();
            no_content()
        }
        ["faults"] if m == Method::POST => {
            let fault: Fault = req.json()?;
            inner.lock().faults.push(fault);
            no_content()
        }
        ["faults"] if m == Method::DELETE => {
            inner.lock().faults.clear();
            no_content()
        }
        ["expire", "admin-tokens"] if m == Method::POST => {
            inner.lock().expire_admin_tokens();
            no_content()
        }
        ["expire", "queue-token", id] if m == Method::POST => {
            let id = id
                .parse()
                .map(ScaleSetId)
                .map_err(|_| ServiceError::new(400, "invalid scale set ID"))?;
            if inner.lock().expire_queue_token(id) {
                no_content()
            } else {
                Err(ServiceError::not_found("session"))
            }
        }
        ["expire", "session", id] if m == Method::POST => {
            let id = id
                .parse()
                .map(ScaleSetId)
                .map_err(|_| ServiceError::new(400, "invalid scale set ID"))?;
            if inner.lock().expire_session(id) {
                inner.on_change();
                no_content()
            } else {
                Err(ServiceError::not_found("session"))
            }
        }
        ["reset"] if m == Method::POST => {
            {
                let mut state = inner.lock();
                let config = state.config.clone();
                let base = state.base_url.clone();
                *state = super::state::State::new(config, base);
            }
            inner
                .requests
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clear();
            inner.on_change();
            no_content()
        }
        _ => Err(ServiceError::new(404, "no such control endpoint")),
    }
}

/// The wire form of a session, token included.
fn session_json(session: &SessionInfo) -> Value {
    json!({
        "sessionId": session.session_id,
        "ownerName": session.owner_name,
        "runnerScaleSet": session.runner_scale_set,
        "messageQueueUrl": session.message_queue_url,
        "messageQueueAccessToken": session.message_queue_access_token.expose_secret(),
        "statistics": session.statistics,
    })
}

fn in_an_hour() -> String {
    (chrono::Utc::now() + Duration::from_secs(3600)).to_rfc3339()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn routes() {
        let sid = "6f1d4b5e-0c39-4b27-9c43-2a4f1d0a1b2c";
        assert_eq!(
            Route::parse("/api/v3/repos/o/r/actions/runners/registration-token"),
            Route::RegistrationToken
        );
        assert_eq!(
            Route::parse("/api/v3/enterprises/e/actions/runners/registration-token"),
            Route::RegistrationToken
        );
        assert_eq!(
            Route::parse("/tenant/_apis/runtime/runnergroups/"),
            Route::RunnerGroups
        );
        assert_eq!(
            Route::parse("/tenant/_apis/runtime/runnerscalesets/5/acquirejobs"),
            Route::Acquire(ScaleSetId(5))
        );
        assert_eq!(
            Route::parse(&format!("/queue/5/{sid}/9")),
            Route::QueueMessage(ScaleSetId(5), sid.parse().unwrap(), 9)
        );
        assert_eq!(Route::parse("/nope"), Route::Unknown);
    }
}
