//! Message sessions: polling a scale set's message queue.

use std::fmt;
use std::future::Future;
use std::sync::{Arc, RwLock};

use reqwest::header::{ACCEPT, AUTHORIZATION, CONTENT_TYPE, HeaderValue, USER_AGENT};
use reqwest::{Method, Request, StatusCode};
use secrecy::ExposeSecret;
use serde::Serialize;
use url::Url;
use uuid::Uuid;

use crate::client::{
    Client, HEADER_SCALE_SET_MAX_CAPACITY, MESSAGE_POLL_TIMEOUT, SCALE_SET_ENDPOINT, sensitive,
    to_json,
};
use crate::error::{ApiErrorKind, Error, Result};
use crate::listener::{MaxRunners, Subscription};
use crate::transport::RawResponse;
use crate::types::{
    JobRequestId, ListResponse, MessageId, RawMessage, ScaleSetId, ScaleSetMessage, SessionInfo,
};

/// A message session on a scale set's message queue.
///
/// Create one with [`Client::create_message_session`]. It is cheap to clone
/// and safe to share between tasks; clones use the same session.
///
/// When the message queue token expires, each method refreshes the session
/// once and retries. Concurrent callers share one refresh.
///
/// Rust has no async drop, so call [`close`](Self::close) when done.
/// Otherwise the service keeps the session until it times out, and a new
/// listener for the same scale set fails with a conflict meanwhile.
#[derive(Clone)]
pub struct MessageSession {
    inner: Arc<Inner>,
}

struct Inner {
    client: Client,
    scale_set_id: ScaleSetId,
    info: RwLock<Arc<SessionInfo>>,
    refresh: tokio::sync::Mutex<()>,
}

impl fmt::Debug for MessageSession {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MessageSession")
            .field("scale_set_id", &self.inner.scale_set_id)
            .field("info", &self.info())
            .finish()
    }
}

impl MessageSession {
    pub(crate) async fn create(
        client: Client,
        scale_set_id: ScaleSetId,
        owner: String,
    ) -> Result<Self> {
        #[derive(Serialize)]
        #[serde(rename_all = "camelCase")]
        struct NewSession {
            session_id: Uuid,
            owner_name: String,
        }
        let body = to_json(&NewSession {
            session_id: Uuid::nil(),
            owner_name: owner,
        })?;
        let path = format!("/{SCALE_SET_ENDPOINT}/{scale_set_id}/sessions");

        let info: SessionInfo = client
            .actions(Method::POST, &path, &[], Some(body), client.retry_policy())
            .await?
            .expect(StatusCode::OK)?
            .json()?;

        Ok(Self {
            inner: Arc::new(Inner {
                client,
                scale_set_id,
                info: RwLock::new(Arc::new(info)),
                refresh: tokio::sync::Mutex::new(()),
            }),
        })
    }

    /// The current session, as last created or refreshed.
    pub fn info(&self) -> Arc<SessionInfo> {
        Arc::clone(&self.inner.info.read().expect("lock poisoned"))
    }

    /// The scale set this session belongs to.
    pub fn scale_set_id(&self) -> ScaleSetId {
        self.inner.scale_set_id
    }

    /// The client the session uses for its requests.
    pub fn client(&self) -> &Client {
        &self.inner.client
    }

    /// Long-polls for the next message.
    ///
    /// Pass the ID of the last message you processed, or `None` at first.
    /// `max_capacity` is the most runners the scale set can run; the service
    /// uses it to avoid assigning jobs you cannot take.
    ///
    /// Returns `None` when the poll ends without news (after about 50
    /// seconds). A message is redelivered until you acknowledge it with
    /// [`delete_message`](Self::delete_message). Poll again right after
    /// handling each result.
    ///
    /// Each attempt may take up to [`MESSAGE_POLL_TIMEOUT`], whatever the
    /// HTTP client's own timeout, and each retry gets a new window.
    pub async fn get_message(
        &self,
        last_message_id: Option<MessageId>,
        max_capacity: u32,
    ) -> Result<Option<ScaleSetMessage>> {
        self.with_refresh(|info| self.get_message_once(info, last_message_id, max_capacity))
            .await
    }

    async fn get_message_once(
        &self,
        info: Arc<SessionInfo>,
        last_message_id: Option<MessageId>,
        max_capacity: u32,
    ) -> Result<Option<ScaleSetMessage>> {
        let mut url = queue_url(&info)?;
        if let Some(id) = last_message_id.filter(|id| id.get() > 0) {
            set_query(&mut url, "lastMessageId", &id.to_string());
        }

        let mut req = Request::new(Method::GET, url);
        let headers = req.headers_mut();
        headers.insert(
            ACCEPT,
            HeaderValue::from_static("application/json; api-version=6.0-preview"),
        );
        headers.insert(AUTHORIZATION, queue_auth(&info)?);
        headers.insert(USER_AGENT, self.inner.client.user_agent());
        headers.insert(
            HEADER_SCALE_SET_MAX_CAPACITY,
            HeaderValue::from(max_capacity),
        );

        let mut policy = self.inner.client.retry_policy().clone();
        policy.attempt_timeout = Some(MESSAGE_POLL_TIMEOUT);

        let resp = self.inner.client.send(req, &policy).await?;
        match resp.status {
            StatusCode::ACCEPTED => Ok(None),
            StatusCode::OK => {
                let raw: RawMessage = resp.json()?;
                ScaleSetMessage::from_raw(raw)
                    .map(Some)
                    .map_err(|reason| Error::Decode {
                        method: resp.method,
                        url: resp.url,
                        reason,
                    })
            }
            _ => Err(queue_error(resp)),
        }
    }

    /// Acknowledges a message, so it is not delivered again. Call it after
    /// the message is handled.
    pub async fn delete_message(&self, message_id: MessageId) -> Result<()> {
        self.with_refresh(|info| self.delete_message_once(info, message_id))
            .await
    }

    async fn delete_message_once(
        &self,
        info: Arc<SessionInfo>,
        message_id: MessageId,
    ) -> Result<()> {
        let mut url = queue_url(&info)?;
        url.path_segments_mut()
            .map_err(|()| Error::UnexpectedResponse("message queue URL cannot be a base".into()))?
            .pop_if_empty()
            .push(&message_id.to_string());

        let mut req = Request::new(Method::DELETE, url);
        let headers = req.headers_mut();
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        headers.insert(AUTHORIZATION, queue_auth(&info)?);
        headers.insert(USER_AGENT, self.inner.client.user_agent());

        let resp = self
            .inner
            .client
            .send(req, self.inner.client.retry_policy())
            .await?;
        if resp.status == StatusCode::NO_CONTENT {
            Ok(())
        } else {
            Err(queue_error(resp))
        }
    }

    /// Acquires jobs for this scale set, from the `runner_request_id`s of
    /// [`JobAvailable`](crate::JobAvailable) messages. Returns the IDs that
    /// were acquired.
    ///
    /// A job you do not acquire stays unassigned. Acquiring a job twice is
    /// harmless.
    pub async fn acquire_jobs(&self, request_ids: &[JobRequestId]) -> Result<Vec<JobRequestId>> {
        self.with_refresh(|info| self.acquire_jobs_once(info, request_ids))
            .await
    }

    async fn acquire_jobs_once(
        &self,
        info: Arc<SessionInfo>,
        request_ids: &[JobRequestId],
    ) -> Result<Vec<JobRequestId>> {
        let path = format!(
            "/{SCALE_SET_ENDPOINT}/{}/acquirejobs",
            self.inner.scale_set_id
        );
        let mut req = self
            .inner
            .client
            .actions_request(Method::POST, &path, &[], Some(to_json(&request_ids)?))
            .await?;
        req.headers_mut().insert(AUTHORIZATION, queue_auth(&info)?);

        let resp = self
            .inner
            .client
            .send(req, self.inner.client.retry_policy())
            .await?;
        if resp.status != StatusCode::OK {
            return Err(queue_error(resp));
        }
        let list: ListResponse<JobRequestId> = resp.json()?;
        Ok(list.value)
    }

    /// Pulls events from this session one at a time. See
    /// [`Subscription`](crate::listener::Subscription); most users want a
    /// [`Listener`](crate::listener::Listener) instead.
    pub fn subscribe(&self, max_runners: impl Into<MaxRunners>) -> Subscription {
        Subscription::new(self.clone(), max_runners.into())
    }

    /// Deletes the session.
    pub async fn close(&self) -> Result<()> {
        let info = self.info();
        let path = format!(
            "/{SCALE_SET_ENDPOINT}/{}/sessions/{}",
            self.inner.scale_set_id, info.session_id
        );

        self.inner
            .client
            .actions(
                Method::DELETE,
                &path,
                &[],
                None,
                self.inner.client.retry_policy(),
            )
            .await?
            .expect(StatusCode::NO_CONTENT)?;
        Ok(())
    }

    /// Runs `op` with the current session; if the queue token has expired,
    /// refreshes the session and runs it once more.
    async fn with_refresh<'a, T, F, Fut>(&'a self, op: F) -> Result<T>
    where
        F: Fn(Arc<SessionInfo>) -> Fut + 'a,
        Fut: Future<Output = Result<T>> + 'a,
    {
        let info = self.info();
        match op(Arc::clone(&info)).await {
            Err(e) if e.is_message_queue_token_expired() => {
                tracing::debug!(session_id = %info.session_id, "message queue token expired, refreshing session");
                self.refresh(&info).await?;
                op(self.info()).await
            }
            result => result,
        }
    }

    /// Refreshes the session, unless another task already replaced `expired`.
    async fn refresh(&self, expired: &SessionInfo) -> Result<()> {
        let _guard = self.inner.refresh.lock().await;
        let current = self.info();
        if current.session_id != expired.session_id
            || current.message_queue_access_token.expose_secret()
                != expired.message_queue_access_token.expose_secret()
        {
            return Ok(());
        }

        let path = format!(
            "/{SCALE_SET_ENDPOINT}/{}/sessions/{}",
            self.inner.scale_set_id, current.session_id
        );

        let refreshed: SessionInfo = self
            .inner
            .client
            .actions(
                Method::PATCH,
                &path,
                &[],
                None,
                self.inner.client.retry_policy(),
            )
            .await?
            .expect(StatusCode::OK)?
            .json()?;
        *self.inner.info.write().expect("lock poisoned") = Arc::new(refreshed);
        Ok(())
    }
}

fn queue_url(info: &SessionInfo) -> Result<Url> {
    Url::parse(&info.message_queue_url).map_err(|e| {
        Error::UnexpectedResponse(format!(
            "invalid message queue URL {:?}: {e}",
            info.message_queue_url
        ))
    })
}

fn queue_auth(info: &SessionInfo) -> Result<HeaderValue> {
    sensitive(&format!(
        "Bearer {}",
        info.message_queue_access_token.expose_secret()
    ))
}

/// A 401 from the queue means its token expired.
fn queue_error(resp: RawResponse) -> Error {
    let kind = if resp.status == StatusCode::UNAUTHORIZED {
        ApiErrorKind::MessageQueueTokenExpired
    } else {
        ApiErrorKind::Other
    };
    resp.into_error(kind)
}

/// Sets `key` to `value`, replacing existing values.
fn set_query(url: &mut Url, key: &str, value: &str) {
    let pairs: Vec<(String, String)> = url
        .query_pairs()
        .filter(|(k, _)| k != key)
        .map(|(k, v)| (k.into_owned(), v.into_owned()))
        .collect();
    let mut q = url.query_pairs_mut();
    q.clear();
    for (k, v) in &pairs {
        q.append_pair(k, v);
    }
    q.append_pair(key, value);
}
