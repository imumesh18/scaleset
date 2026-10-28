//! The scale set API client.

use std::fmt;
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime};

use reqwest::header::{ACCEPT, AUTHORIZATION, CONTENT_TYPE, HeaderValue, USER_AGENT};
use reqwest::{Method, Request, StatusCode};
use secrecy::ExposeSecret;
use serde::{Deserialize, Serialize};
use url::Url;

use crate::auth::{Credentials, CredentialsKind, unverified_expiry};
use crate::config::GitHubConfigUrl;
use crate::error::{Error, Result};
use crate::retry::RetryPolicy;
use crate::session::MessageSession;
use crate::transport::{self, RawResponse};
use crate::types::{
    JitRunnerConfig, JitRunnerSetting, ListResponse, RunnerGroup, RunnerGroupId, RunnerId,
    RunnerReference, RunnerScaleSet, RunnerScaleSetSpec, ScaleSetId,
};

/// Bounds a single HTTP attempt on the default HTTP client.
pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(5 * 60);

/// How long one message poll attempt may take. The service holds an idle
/// poll for about 50 seconds; this leaves room above that. Each retry gets a
/// new window.
pub const MESSAGE_POLL_TIMEOUT: Duration = Duration::from_secs(2 * 60);

/// Header carrying the scale set's capacity on message polls.
pub const HEADER_SCALE_SET_MAX_CAPACITY: &str = "X-ScaleSetMaxCapacity";

pub(crate) const SCALE_SET_ENDPOINT: &str = "_apis/runtime/runnerscalesets";
const RUNNER_ENDPOINT: &str = "_apis/distributedtask/pools/0/agents";
const RUNNER_GROUP_ENDPOINT: &str = "/_apis/runtime/runnergroups/";
const API_VERSION: &str = "6.0-preview";

/// Refresh the admin token this long before it expires.
const ADMIN_TOKEN_REFRESH_MARGIN: Duration = Duration::from_secs(60);

/// Returns a [`reqwest::ClientBuilder`] with the SDK's defaults: a 5-minute
/// timeout per attempt, a 30-second connect timeout, a 90-second idle pool
/// timeout, TCP keepalive, and TLS 1.2 or later.
///
/// Customize it and pass the result to [`ClientBuilder::http_client`]:
///
/// ```no_run
/// let pem = std::fs::read("corp-ca.pem")?;
/// let http = scaleset::http_client_builder()
///     .tls_certs_merge([reqwest::Certificate::from_pem(&pem)?])
///     .build()?;
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
///
/// The SDK sets its own per-request timeout on message polls (see
/// [`MESSAGE_POLL_TIMEOUT`]), so a shorter client timeout does not cut long
/// polls off. Avoid a `read_timeout` under a minute for the same reason.
pub fn http_client_builder() -> reqwest::ClientBuilder {
    let builder = reqwest::Client::builder()
        .timeout(DEFAULT_TIMEOUT)
        .connect_timeout(Duration::from_secs(30))
        .pool_idle_timeout(Duration::from_secs(90))
        .tcp_keepalive(Duration::from_secs(30));
    #[cfg(any(feature = "rustls", feature = "native-tls"))]
    let builder = builder.tls_version_min(reqwest::tls::Version::TLS_1_2);
    builder
}

/// Describes the system using the client. It is sent, with the crate
/// version, as a JSON `User-Agent`.
///
/// For example, actions-runner-controller sets `system` to
/// `"actions-runner-controller"` and `subsystem` to `"listener"` or
/// `"controller"`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SystemInfo {
    /// Name of the scale set implementation.
    pub system: String,
    /// Version of the implementation.
    pub version: String,
    /// Git commit of the implementation.
    pub commit_sha: String,
    /// The scale set the client works on, if one.
    pub scale_set_id: ScaleSetId,
    /// Component, such as `listener` or `controller`.
    pub subsystem: String,
}

impl SystemInfo {
    /// System info with the given system name.
    pub fn new(system: impl Into<String>) -> Self {
        Self {
            system: system.into(),
            ..Self::default()
        }
    }

    /// Sets the version.
    #[must_use]
    pub fn version(mut self, version: impl Into<String>) -> Self {
        self.version = version.into();
        self
    }

    /// Sets the commit SHA.
    #[must_use]
    pub fn commit_sha(mut self, sha: impl Into<String>) -> Self {
        self.commit_sha = sha.into();
        self
    }

    /// Sets the scale set ID.
    #[must_use]
    pub fn scale_set_id(mut self, id: ScaleSetId) -> Self {
        self.scale_set_id = id;
        self
    }

    /// Sets the subsystem.
    #[must_use]
    pub fn subsystem(mut self, subsystem: impl Into<String>) -> Self {
        self.subsystem = subsystem.into();
        self
    }

    fn user_agent(&self) -> HeaderValue {
        #[derive(Serialize)]
        struct UserAgent<'a> {
            #[serde(flatten)]
            info: &'a SystemInfo,
            build_version: &'a str,
            build_commit_sha: &'a str,
            kind: &'a str,
        }
        let json = serde_json::to_string(&UserAgent {
            info: self,
            build_version: env!("CARGO_PKG_VERSION"),
            build_commit_sha: "unknown",
            kind: "scaleset",
        })
        .unwrap_or_default();
        HeaderValue::from_str(&json).unwrap_or_else(|_| HeaderValue::from_static("scaleset"))
    }
}

/// Builds a [`Client`]. See [`Client::builder`].
#[derive(Debug)]
#[must_use]
pub struct ClientBuilder {
    config_url: String,
    credentials: Credentials,
    system_info: SystemInfo,
    http: Option<reqwest::Client>,
    retry: RetryPolicy,
}

impl ClientBuilder {
    /// Sets the system info sent in the `User-Agent`.
    pub fn system_info(mut self, info: SystemInfo) -> Self {
        self.system_info = info;
        self
    }

    /// Sends every request through `http`.
    ///
    /// The SDK never changes the client. TLS, proxies, connection pooling
    /// and timeouts stay under your control; see [`http_client_builder`]
    /// for the defaults. Retries are handled by the SDK, above the client.
    pub fn http_client(mut self, http: reqwest::Client) -> Self {
        self.http = Some(http);
        self
    }

    /// Sets the retry policy. See [`Client::with_retry_policy`] to change it
    /// for some calls only.
    pub fn retry_policy(mut self, policy: RetryPolicy) -> Self {
        self.retry = policy;
        self
    }

    /// Builds the client. No request is sent until the first call.
    pub fn build(self) -> Result<Client> {
        let config = GitHubConfigUrl::parse(&self.config_url)?;
        let http = match self.http {
            Some(http) => http,
            None => http_client_builder().build().map_err(|e| {
                Error::InvalidInput(format!("failed to build the default HTTP client: {e}"))
            })?,
        };
        let user_agent = self.system_info.user_agent();
        Ok(Client {
            shared: Arc::new(Shared {
                http,
                config,
                credentials: self.credentials,
                system: RwLock::new((self.system_info, user_agent)),
                admin: RwLock::new(None),
                refresh: tokio::sync::Mutex::new(()),
            }),
            retry: Arc::new(self.retry),
        })
    }
}

/// A client for the GitHub Actions Runner Scale Set APIs.
///
/// `Client` is cheap to clone: clones share the HTTP connection pool, the
/// admin token and the system info. It is `Send + Sync`.
///
/// ```no_run
/// use scaleset::{Client, Credentials, RunnerGroupId, RunnerScaleSetSpec};
///
/// # async fn run() -> scaleset::Result<()> {
/// let client = Client::builder(
///     "https://github.com/my-org",
///     Credentials::personal_access_token("ghp_...")?,
/// )
/// .build()?;
///
/// let scale_set = client
///     .create_runner_scale_set(
///         &RunnerScaleSetSpec::new("my-scale-set").runner_group_id(RunnerGroupId::DEFAULT),
///     )
///     .await?;
/// # Ok(()) }
/// ```
#[derive(Clone)]
pub struct Client {
    shared: Arc<Shared>,
    retry: Arc<RetryPolicy>,
}

struct Shared {
    http: reqwest::Client,
    config: GitHubConfigUrl,
    credentials: Credentials,
    system: RwLock<(SystemInfo, HeaderValue)>,
    admin: RwLock<Option<Arc<AdminToken>>>,
    /// Held while refreshing the admin token, so only one task refreshes.
    refresh: tokio::sync::Mutex<()>,
}

struct AdminToken {
    authorization: HeaderValue,
    expires_at: SystemTime,
    url: String,
}

impl AdminToken {
    fn is_fresh(&self) -> bool {
        SystemTime::now() + ADMIN_TOKEN_REFRESH_MARGIN < self.expires_at
    }
}

impl fmt::Debug for Client {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Client")
            .field("config_url", &self.shared.config.as_str())
            .field("credentials", &self.shared.credentials)
            .field("retry", &self.retry)
            .finish_non_exhaustive()
    }
}

impl Client {
    /// Starts building a client for `config_url`: an enterprise
    /// (`https://github.com/enterprises/acme`), organization
    /// (`https://github.com/acme`) or repository
    /// (`https://github.com/acme/app`) URL on github.com, GHE.com or GitHub
    /// Enterprise Server.
    pub fn builder(config_url: impl Into<String>, credentials: Credentials) -> ClientBuilder {
        ClientBuilder {
            config_url: config_url.into(),
            credentials,
            system_info: SystemInfo::default(),
            http: None,
            retry: RetryPolicy::default(),
        }
    }

    /// Returns a client that shares everything with this one but retries
    /// under `policy`.
    ///
    /// Use it to change the policy for some calls, for example to give up on
    /// a rate limit at once and requeue the work:
    ///
    /// ```no_run
    /// use std::time::Duration;
    /// use scaleset::{JitRunnerSetting, RateLimitPolicy, RateLimitRule, RetryPolicy};
    ///
    /// # async fn f(client: scaleset::Client, id: scaleset::ScaleSetId) -> scaleset::Result<()> {
    /// let jit = client
    ///     .with_retry_policy(
    ///         RetryPolicy::default()
    ///             .max_elapsed(Duration::from_secs(20))
    ///             .rate_limit(RateLimitPolicy::rules(
    ///                 RateLimitRule::give_up(),
    ///                 RateLimitRule::retries(2).max_wait(Duration::from_secs(5)),
    ///             )),
    ///     )
    ///     .generate_jit_config(id, &JitRunnerSetting::new("runner-1"))
    ///     .await;
    ///
    /// if let Err(e) = &jit {
    ///     if let Some(rl) = e.rate_limit() {
    ///         let requeue_after = rl.retry_after.unwrap_or(Duration::from_secs(60));
    ///         // requeue...
    ///     }
    /// }
    /// # Ok(()) }
    /// ```
    ///
    /// A [`MessageSession`] created from the returned client keeps its
    /// policy.
    #[must_use]
    pub fn with_retry_policy(&self, policy: RetryPolicy) -> Self {
        Self {
            shared: Arc::clone(&self.shared),
            retry: Arc::new(policy),
        }
    }

    /// The retry policy this client uses.
    pub fn retry_policy(&self) -> &RetryPolicy {
        &self.retry
    }

    /// The configuration URL.
    pub fn config_url(&self) -> &GitHubConfigUrl {
        &self.shared.config
    }

    /// The current system info.
    pub fn system_info(&self) -> SystemInfo {
        self.shared.system.read().expect("lock poisoned").0.clone()
    }

    /// Replaces the system info, for this client and all its clones.
    pub fn set_system_info(&self, info: SystemInfo) {
        let ua = info.user_agent();
        *self.shared.system.write().expect("lock poisoned") = (info, ua);
    }

    pub(crate) fn user_agent(&self) -> HeaderValue {
        self.shared.system.read().expect("lock poisoned").1.clone()
    }

    pub(crate) async fn send(&self, request: Request, policy: &RetryPolicy) -> Result<RawResponse> {
        transport::send(&self.shared.http, request, policy).await
    }

    // ---------------------------------------------------------------------
    // Scale sets

    /// Finds a scale set by name within a runner group.
    pub async fn get_runner_scale_set(
        &self,
        runner_group_id: RunnerGroupId,
        name: &str,
    ) -> Result<Option<RunnerScaleSet>> {
        let list: ListResponse<RunnerScaleSet> = self
            .actions(
                Method::GET,
                SCALE_SET_ENDPOINT,
                &[
                    ("runnerGroupId", runner_group_id.to_string()),
                    ("name", name.to_owned()),
                ],
                None,
                &self.retry,
            )
            .await?
            .expect(StatusCode::OK)?
            .json()?;
        single(list, || {
            format!("multiple runner scale sets found with name {name:?}")
        })
    }

    /// Lists the scale sets in a runner group.
    pub async fn list_runner_scale_sets(
        &self,
        runner_group_id: RunnerGroupId,
    ) -> Result<Vec<RunnerScaleSet>> {
        let list: ListResponse<RunnerScaleSet> = self
            .actions(
                Method::GET,
                SCALE_SET_ENDPOINT,
                &[("runnerGroupId", runner_group_id.to_string())],
                None,
                &self.retry,
            )
            .await?
            .expect(StatusCode::OK)?
            .json()?;
        Ok(list.value)
    }

    /// Gets a scale set by ID.
    pub async fn get_runner_scale_set_by_id(&self, id: ScaleSetId) -> Result<RunnerScaleSet> {
        let path = format!("/{SCALE_SET_ENDPOINT}/{id}");

        self.actions(Method::GET, &path, &[], None, &self.retry)
            .await?
            .expect(StatusCode::OK)?
            .json()
    }

    /// Creates a scale set. Names are unique within a runner group.
    ///
    /// Without labels, the name becomes the only label. Labels without a
    /// type get type `System`.
    pub async fn create_runner_scale_set(
        &self,
        spec: &RunnerScaleSetSpec,
    ) -> Result<RunnerScaleSet> {
        let spec = spec.normalized(true).map_err(Error::InvalidInput)?;
        let body = to_json(&spec)?;

        self.actions(
            Method::POST,
            SCALE_SET_ENDPOINT,
            &[],
            Some(body),
            &self.retry,
        )
        .await?
        .expect(StatusCode::OK)?
        .json()
    }

    /// Updates a scale set. Fields left `None` or empty in `spec` are not
    /// sent.
    pub async fn update_runner_scale_set(
        &self,
        id: ScaleSetId,
        spec: &RunnerScaleSetSpec,
    ) -> Result<RunnerScaleSet> {
        let spec = spec.normalized(false).map_err(Error::InvalidInput)?;
        let body = to_json(&spec)?;
        let path = format!("{SCALE_SET_ENDPOINT}/{id}");

        self.actions(Method::PATCH, &path, &[], Some(body), &self.retry)
            .await?
            .expect(StatusCode::OK)?
            .json()
    }

    /// Deletes a scale set.
    pub async fn delete_runner_scale_set(&self, id: ScaleSetId) -> Result<()> {
        let path = format!("/{SCALE_SET_ENDPOINT}/{id}");

        self.actions(Method::DELETE, &path, &[], None, &self.retry)
            .await?
            .expect(StatusCode::NO_CONTENT)?;
        Ok(())
    }

    /// Finds a runner group by name.
    pub async fn get_runner_group_by_name(&self, name: &str) -> Result<Option<RunnerGroup>> {
        let list: ListResponse<RunnerGroup> = self
            .actions(
                Method::GET,
                RUNNER_GROUP_ENDPOINT,
                &[("groupName", name.to_owned())],
                None,
                &self.retry,
            )
            .await?
            .expect(StatusCode::OK)?
            .json()?;
        single(list, || {
            format!("multiple runner groups found with name {name:?}")
        })
    }

    // ---------------------------------------------------------------------
    // Runners

    /// Generates a just-in-time runner configuration. Start a runner with it
    /// to add one runner to the scale set.
    ///
    /// Runner names are unique: a name in use fails with an error for which
    /// [`Error::is_runner_exists`] holds. That can also happen when an
    /// earlier attempt of this call registered the runner but its response
    /// was lost and the request was retried. The JIT config of that attempt
    /// cannot be recovered, so remove the runner
    /// ([`get_runner_by_name`](Self::get_runner_by_name), then
    /// [`remove_runner`](Self::remove_runner)) or pick a new name.
    pub async fn generate_jit_config(
        &self,
        scale_set_id: ScaleSetId,
        setting: &JitRunnerSetting,
    ) -> Result<JitRunnerConfig> {
        let path = format!("/{SCALE_SET_ENDPOINT}/{scale_set_id}/generatejitconfig");

        self.actions(
            Method::POST,
            &path,
            &[],
            Some(to_json(setting)?),
            &self.retry,
        )
        .await?
        .expect(StatusCode::OK)?
        .json()
    }

    /// Gets a runner by ID. A missing runner is an error for which
    /// [`Error::is_not_found`] or [`Error::is_runner_not_found`] holds.
    pub async fn get_runner(&self, id: RunnerId) -> Result<RunnerReference> {
        let path = format!("/{RUNNER_ENDPOINT}/{id}");

        self.actions(Method::GET, &path, &[], None, &self.retry)
            .await?
            .expect(StatusCode::OK)?
            .json()
    }

    /// Finds a runner by name.
    pub async fn get_runner_by_name(&self, name: &str) -> Result<Option<RunnerReference>> {
        let list: ListResponse<RunnerReference> = self
            .actions(
                Method::GET,
                RUNNER_ENDPOINT,
                &[("agentName", name.to_owned())],
                None,
                &self.retry,
            )
            .await?
            .expect(StatusCode::OK)?
            .json()?;
        single(list, || {
            format!("multiple runners found with name {name:?}")
        })
    }

    /// Removes a runner.
    pub async fn remove_runner(&self, id: RunnerId) -> Result<()> {
        let path = format!("/{RUNNER_ENDPOINT}/{id}");

        self.actions(Method::DELETE, &path, &[], None, &self.retry)
            .await?
            .expect(StatusCode::NO_CONTENT)?;
        Ok(())
    }

    // ---------------------------------------------------------------------
    // Sessions

    /// Opens a message session for a scale set. Only one session per scale
    /// set can be open; a second one fails with a conflict.
    ///
    /// Call [`MessageSession::close`] when done, so another listener can
    /// take over without waiting for the session to time out.
    pub async fn create_message_session(
        &self,
        scale_set_id: ScaleSetId,
        owner: impl Into<String>,
    ) -> Result<MessageSession> {
        MessageSession::create(self.clone(), scale_set_id, owner.into()).await
    }

    // ---------------------------------------------------------------------
    // Requests

    /// Sends a request to the Actions service with the admin token.
    ///
    /// If the service rejects a token that has not expired yet (it was
    /// revoked, or the clocks disagree), the token is dropped and the
    /// request is sent once more with a fresh one.
    pub(crate) async fn actions(
        &self,
        method: Method,
        path: &str,
        query: &[(&str, String)],
        body: Option<Vec<u8>>,
        policy: &RetryPolicy,
    ) -> Result<RawResponse> {
        let token = self.admin_token().await?;
        let req = self.actions_request_with(&token, method.clone(), path, query, body.clone())?;
        let resp = self.send(req, policy).await?;
        if resp.status != StatusCode::UNAUTHORIZED {
            return Ok(resp);
        }

        tracing::info!(url = %resp.url, "admin token rejected, refreshing it");
        self.invalidate_admin_token(&token);
        let token = self.admin_token().await?;
        let req = self.actions_request_with(&token, method, path, query, body)?;
        self.send(req, policy).await
    }

    /// Builds a request to the Actions service, refreshing the admin token
    /// if needed.
    pub(crate) async fn actions_request(
        &self,
        method: Method,
        path: &str,
        query: &[(&str, String)],
        body: Option<Vec<u8>>,
    ) -> Result<Request> {
        let token = self.admin_token().await?;
        self.actions_request_with(&token, method, path, query, body)
    }

    fn actions_request_with(
        &self,
        token: &AdminToken,
        method: Method,
        path: &str,
        query: &[(&str, String)],
        body: Option<Vec<u8>>,
    ) -> Result<Request> {
        let url = actions_url(&token.url, path, query)?;
        let mut req = Request::new(method, url);
        let headers = req.headers_mut();
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        headers.insert(AUTHORIZATION, token.authorization.clone());
        headers.insert(USER_AGENT, self.user_agent());
        if let Some(body) = body {
            *req.body_mut() = Some(body.into());
        }
        Ok(req)
    }

    /// Forgets `token` unless another task already replaced it.
    fn invalidate_admin_token(&self, token: &Arc<AdminToken>) {
        let mut cached = self.shared.admin.write().expect("lock poisoned");
        if cached.as_ref().is_some_and(|t| Arc::ptr_eq(t, token)) {
            *cached = None;
        }
    }

    fn github_request(
        &self,
        path: &str,
        authorization: &str,
        content_type: &'static str,
        body: Vec<u8>,
    ) -> Result<Request> {
        let mut req = Request::new(Method::POST, self.shared.config.api_url(path));
        let headers = req.headers_mut();
        headers.insert(USER_AGENT, self.user_agent());
        headers.insert(CONTENT_TYPE, HeaderValue::from_static(content_type));
        headers.insert(
            ACCEPT,
            HeaderValue::from_static("application/vnd.github+json"),
        );
        headers.insert(AUTHORIZATION, sensitive(authorization)?);
        *req.body_mut() = Some(body.into());
        Ok(req)
    }

    async fn admin_token(&self) -> Result<Arc<AdminToken>> {
        if let Some(token) = self.cached_admin_token() {
            return Ok(token);
        }
        let _guard = self.shared.refresh.lock().await;
        if let Some(token) = self.cached_admin_token() {
            return Ok(token);
        }

        tracing::info!(config_url = %self.shared.config, "refreshing admin token");
        let registration_token = self.registration_token().await?;
        let token = Arc::new(self.admin_connection(&registration_token).await?);
        *self.shared.admin.write().expect("lock poisoned") = Some(Arc::clone(&token));
        Ok(token)
    }

    fn cached_admin_token(&self) -> Option<Arc<AdminToken>> {
        self.shared
            .admin
            .read()
            .expect("lock poisoned")
            .as_ref()
            .filter(|t| t.is_fresh())
            .cloned()
    }

    async fn registration_token(&self) -> Result<String> {
        let bearer = match &self.shared.credentials.kind {
            CredentialsKind::Token(token) => format!("Bearer {}", token.expose_secret()),
            CredentialsKind::App {
                installation_id,
                provider,
            } => {
                let token = self.installation_token(*installation_id, provider).await?;
                format!("Bearer {token}")
            }
        };

        let path = self.shared.config.registration_token_path();
        let req =
            self.github_request(&path, &bearer, "application/vnd.github.v3+json", Vec::new())?;
        tracing::info!(url = %req.url(), "getting runner registration token");

        #[derive(Deserialize)]
        struct RegistrationToken {
            token: String,
        }
        let resp: RegistrationToken = self
            .send(req, &self.retry)
            .await?
            .expect(StatusCode::CREATED)?
            .json()?;
        Ok(resp.token)
    }

    async fn installation_token(
        &self,
        installation_id: i64,
        provider: &Arc<dyn crate::auth::DynJwtProvider>,
    ) -> Result<String> {
        let jwt = CredentialsKind::jwt(provider).await?;
        let path = format!("/app/installations/{installation_id}/access_tokens");
        let req = self.github_request(
            &path,
            &format!("Bearer {jwt}"),
            "application/vnd.github+json",
            Vec::new(),
        )?;
        tracing::info!(url = %req.url(), "getting access token for GitHub App auth");

        #[derive(Deserialize)]
        struct AccessToken {
            token: String,
        }
        let resp: AccessToken = self
            .send(req, &self.retry)
            .await?
            .expect(StatusCode::CREATED)?
            .json()?;
        Ok(resp.token)
    }

    async fn admin_connection(&self, registration_token: &str) -> Result<AdminToken> {
        #[derive(Serialize)]
        struct Body<'a> {
            url: &'a str,
            runner_event: &'a str,
        }
        let body = to_json(&Body {
            url: self.shared.config.as_str(),
            runner_event: "register",
        })?;
        let req = self.github_request(
            "/actions/runner-registration",
            &format!("RemoteAuth {registration_token}"),
            "application/json",
            body,
        )?;
        tracing::info!(url = %req.url(), "getting Actions service URL and admin token");

        // A fresh registration token is not always usable at once, so this
        // request also retries 401 and 403.
        let mut policy = (*self.retry).clone();
        policy.also_retry = &[StatusCode::UNAUTHORIZED, StatusCode::FORBIDDEN];

        #[derive(Deserialize)]
        struct Connection {
            #[serde(default)]
            url: String,
            #[serde(default)]
            token: String,
        }
        let resp = self.send(req, &policy).await?.expect_success()?;
        let conn: Connection = resp.json()?;
        if conn.url.is_empty() {
            return Err(Error::UnexpectedResponse(
                "Actions service admin connection is missing the URL".into(),
            ));
        }
        if conn.token.is_empty() {
            return Err(Error::UnexpectedResponse(
                "Actions service admin connection is missing the token".into(),
            ));
        }
        let expires_at = unverified_expiry(&conn.token).ok_or_else(|| {
            Error::UnexpectedResponse("admin token is not a JWT with an exp claim".into())
        })?;
        Ok(AdminToken {
            authorization: sensitive(&format!("Bearer {}", conn.token))?,
            expires_at,
            url: conn.url,
        })
    }
}

/// Joins the Actions service URL and `path`, adding `query` and the API
/// version.
fn actions_url(base: &str, path: &str, query: &[(&str, String)]) -> Result<Url> {
    let (path, path_query) = path.split_once('?').unwrap_or((path, ""));
    let base = base.trim_end_matches('/');
    let joined = match path {
        "" => base.to_owned(),
        p if p.starts_with('/') => format!("{base}{p}"),
        p => format!("{base}/{p}"),
    };
    let mut url = Url::parse(&joined).map_err(|e| {
        Error::UnexpectedResponse(format!("invalid Actions service URL {joined:?}: {e}"))
    })?;
    {
        let mut pairs = url.query_pairs_mut();
        if !path_query.is_empty() {
            for (k, v) in url::form_urlencoded::parse(path_query.as_bytes()) {
                pairs.append_pair(&k, &v);
            }
        }
        for (k, v) in query {
            pairs.append_pair(k, v);
        }
    }
    let has_version = url.query_pairs().any(|(k, _)| k == "api-version");
    if !has_version {
        url.query_pairs_mut()
            .append_pair("api-version", API_VERSION);
    }
    Ok(url)
}

fn single<T>(list: ListResponse<T>, multiple: impl FnOnce() -> String) -> Result<Option<T>> {
    match (list.count, list.value.len()) {
        (0, _) | (_, 0) => Ok(None),
        (1, _) => Ok(list.value.into_iter().next()),
        _ => Err(Error::UnexpectedResponse(multiple())),
    }
}

pub(crate) fn to_json<T: Serialize>(value: &T) -> Result<Vec<u8>> {
    serde_json::to_vec(value)
        .map_err(|e| Error::InvalidInput(format!("failed to encode body: {e}")))
}

pub(crate) fn sensitive(value: &str) -> Result<HeaderValue> {
    let mut value = HeaderValue::from_str(value)
        .map_err(|_| Error::InvalidCredentials("token contains invalid characters".into()))?;
    value.set_sensitive(true);
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn actions_urls() {
        let url = actions_url(
            "https://pipelines.example/abc/",
            "_apis/runtime/runnerscalesets",
            &[("name", "a b".into())],
        )
        .unwrap();
        assert_eq!(
            url.as_str(),
            "https://pipelines.example/abc/_apis/runtime/runnerscalesets?name=a+b&api-version=6.0-preview"
        );

        let url = actions_url("https://p.example/abc", "/x/1?api-version=7.0", &[]).unwrap();
        assert_eq!(url.as_str(), "https://p.example/abc/x/1?api-version=7.0");
    }

    #[test]
    fn user_agent_shape() {
        let ua = SystemInfo::new("sys")
            .subsystem("listener")
            .scale_set_id(ScaleSetId(3))
            .user_agent();
        let v: serde_json::Value = serde_json::from_slice(ua.as_bytes()).unwrap();
        assert_eq!(v["system"], "sys");
        assert_eq!(v["subsystem"], "listener");
        assert_eq!(v["scale_set_id"], 3);
        assert_eq!(v["kind"], "scaleset");
        assert_eq!(v["build_version"], env!("CARGO_PKG_VERSION"));
    }
}
