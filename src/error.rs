//! Error types.

use std::fmt;
use std::time::{Duration, SystemTime};

use reqwest::{Method, StatusCode};
use serde::Deserialize;

/// A boxed error, as returned by user-supplied hooks such as a
/// [`JwtProvider`](crate::JwtProvider).
pub type BoxError = Box<dyn std::error::Error + Send + Sync + 'static>;

/// `Result` with this crate's [`Error`].
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Errors returned by this crate.
///
/// Common checks have predicates, so most callers never need to match
/// nested variants:
///
/// ```no_run
/// # async fn f(client: scaleset::Client) -> scaleset::Result<()> {
/// match client.remove_runner(scaleset::RunnerId(42)).await {
///     Ok(()) => {}
///     Err(e) if e.is_runner_not_found() => {} // already gone
///     Err(e) if e.is_job_still_running() => { /* try again later */ }
///     Err(e) => return Err(e),
/// }
/// # Ok(()) }
/// ```
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// The GitHub config URL is not an enterprise, organization or repository
    /// URL.
    #[error("invalid GitHub config URL {url:?}: {reason}")]
    InvalidConfigUrl {
        /// The URL as given.
        url: String,
        /// Why it was rejected.
        reason: String,
    },

    /// The credentials are incomplete or malformed.
    #[error("invalid credentials: {0}")]
    InvalidCredentials(String),

    /// An argument is invalid.
    #[error("invalid input: {0}")]
    InvalidInput(String),

    /// The service returned an unsuccessful response.
    #[error(transparent)]
    Api(Box<ApiError>),

    /// The service rate limited the request.
    #[error(transparent)]
    RateLimited(Box<RateLimitError>),

    /// The request could not be sent, or its response could not be read,
    /// after any retries.
    #[error("request {method} {url} failed: {source}")]
    Transport {
        /// Request method.
        method: Method,
        /// Request URL.
        url: String,
        /// The underlying error.
        #[source]
        source: reqwest::Error,
    },

    /// A request ran out of its [`RetryPolicy::max_elapsed`](crate::RetryPolicy::max_elapsed)
    /// budget while an attempt was in progress.
    #[error("request {method} {url} exceeded its retry budget of {budget:?}")]
    MaxElapsedExceeded {
        /// Request method.
        method: Method,
        /// Request URL.
        url: String,
        /// The configured budget.
        budget: Duration,
    },

    /// A successful response could not be decoded.
    #[error("failed to decode response of {method} {url}: {reason}")]
    Decode {
        /// Request method.
        method: Method,
        /// Request URL.
        url: String,
        /// What went wrong.
        reason: String,
    },

    /// The service returned something the protocol does not allow, such as
    /// several scale sets with the same name.
    #[error("unexpected response: {0}")]
    UnexpectedResponse(String),

    /// The [`JwtProvider`](crate::JwtProvider) failed to produce a token.
    #[error("failed to create GitHub App JWT: {0}")]
    Jwt(#[source] BoxError),
}

impl Error {
    /// The HTTP status of an [`Api`](Self::Api) or
    /// [`RateLimited`](Self::RateLimited) error.
    pub fn status(&self) -> Option<StatusCode> {
        match self {
            Self::Api(e) => Some(e.status),
            Self::RateLimited(e) => Some(e.status),
            _ => None,
        }
    }

    /// The API error, for [`Api`](Self::Api) and
    /// [`RateLimited`](Self::RateLimited) errors.
    pub fn api_error(&self) -> Option<&ApiError> {
        match self {
            Self::Api(e) => Some(e),
            Self::RateLimited(e) => Some(&e.response),
            _ => None,
        }
    }

    /// The rate limit details, if the request was rate limited.
    pub fn rate_limit(&self) -> Option<&RateLimitError> {
        match self {
            Self::RateLimited(e) => Some(e),
            _ => None,
        }
    }

    fn kind(&self) -> Option<ApiErrorKind> {
        self.api_error().map(|e| e.kind)
    }

    /// HTTP 400.
    pub fn is_bad_request(&self) -> bool {
        self.status() == Some(StatusCode::BAD_REQUEST)
    }

    /// HTTP 401.
    pub fn is_unauthorized(&self) -> bool {
        self.status() == Some(StatusCode::UNAUTHORIZED)
    }

    /// HTTP 404.
    pub fn is_not_found(&self) -> bool {
        self.status() == Some(StatusCode::NOT_FOUND)
    }

    /// HTTP 409.
    pub fn is_conflict(&self) -> bool {
        self.status() == Some(StatusCode::CONFLICT)
    }

    /// The runner does not exist (`AgentNotFoundException`).
    pub fn is_runner_not_found(&self) -> bool {
        self.kind() == Some(ApiErrorKind::RunnerNotFound)
    }

    /// A runner with this name already exists (`AgentExistsException`).
    pub fn is_runner_exists(&self) -> bool {
        self.kind() == Some(ApiErrorKind::RunnerExists)
    }

    /// The runner is still running a job (`JobStillRunningException`).
    pub fn is_job_still_running(&self) -> bool {
        self.kind() == Some(ApiErrorKind::JobStillRunning)
    }

    /// The message queue token expired, and refreshing the session did not
    /// help.
    pub fn is_message_queue_token_expired(&self) -> bool {
        self.kind() == Some(ApiErrorKind::MessageQueueTokenExpired)
    }

    /// The request timed out: an attempt timed out, or the
    /// [`max_elapsed`](crate::RetryPolicy::max_elapsed) budget ran out.
    pub fn is_timeout(&self) -> bool {
        match self {
            Self::Transport { source, .. } => source.is_timeout(),
            Self::MaxElapsedExceeded { .. } => true,
            _ => false,
        }
    }
}

impl From<ApiError> for Error {
    fn from(e: ApiError) -> Self {
        Self::Api(Box::new(e))
    }
}

impl From<RateLimitError> for Error {
    fn from(e: RateLimitError) -> Self {
        Self::RateLimited(Box::new(e))
    }
}

/// What an [`ApiError`] means, beyond its HTTP status.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum ApiErrorKind {
    /// `AgentExistsException`.
    RunnerExists,
    /// `AgentNotFoundException`.
    RunnerNotFound,
    /// `JobStillRunningException`.
    JobStillRunning,
    /// A 401 from the message queue.
    MessageQueueTokenExpired,
    /// Anything else.
    Other,
}

/// An exception returned by the Actions service.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
pub struct ActionsException {
    /// Exception type, such as `AgentNotFoundException`.
    #[serde(rename = "typeName", default)]
    pub type_name: String,
    /// Exception message.
    #[serde(default)]
    pub message: String,
}

/// An unsuccessful response from GitHub or the Actions service.
#[derive(Debug, Clone)]
pub struct ApiError {
    /// Request method.
    pub method: Method,
    /// Request URL.
    pub url: String,
    /// Response status.
    pub status: StatusCode,
    /// What the error means.
    pub kind: ApiErrorKind,
    /// The `ActivityId` response header, for support requests.
    pub activity_id: Option<String>,
    /// The `X-GitHub-Request-Id` response header, for support requests.
    pub github_request_id: Option<String>,
    /// The exception the Actions service returned, if any.
    pub exception: Option<ActionsException>,
    /// The error message: the exception message, or the response body.
    pub message: String,
}

impl fmt::Display for ApiError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "request {} {} failed (status={}",
            self.method, self.url, self.status
        )?;
        if let Some(id) = &self.activity_id {
            write!(f, ", activity_id={id}")?;
        }
        if let Some(id) = &self.github_request_id {
            write!(f, ", github_request_id={id}")?;
        }
        f.write_str(")")?;
        if let Some(e) = &self.exception {
            if !e.type_name.is_empty() {
                write!(f, ": {}", e.type_name)?;
            }
        }
        if !self.message.is_empty() {
            write!(f, ": {}", self.message)?;
        }
        Ok(())
    }
}

impl std::error::Error for ApiError {}

/// Which rate limit rejected a request.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RateLimitKind {
    /// The primary quota is spent (`X-RateLimit-Remaining: 0`, no
    /// secondary-limit message).
    Primary,
    /// Any other rate limit.
    Secondary,
}

impl fmt::Display for RateLimitKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Primary => "primary",
            Self::Secondary => "secondary",
        })
    }
}

/// A rate-limited response: any 429, or a 403 with `Retry-After`,
/// `X-RateLimit-Remaining: 0`, or a secondary-rate-limit message.
///
/// When requeuing rate-limited work, wait for
/// [`retry_after`](Self::retry_after) and fall back to a positive delay (for
/// example one minute) when it is `None`.
#[derive(Debug, Clone)]
pub struct RateLimitError {
    /// Which limit was hit.
    pub kind: RateLimitKind,
    /// 429 or 403.
    pub status: StatusCode,
    /// How long the server asked to wait, from `Retry-After`, or from
    /// `X-RateLimit-Reset` when the primary quota is spent.
    pub retry_after: Option<Duration>,
    /// When the rate limit window resets, from `X-RateLimit-Reset`.
    pub reset: Option<SystemTime>,
    /// Rate-limited retries already made for this request, of either kind.
    pub retries: u32,
    /// The response.
    pub response: ApiError,
}

impl fmt::Display for RateLimitError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "rate limited ({})", self.kind)?;
        if let Some(wait) = self.retry_after {
            write!(f, " (retry after {wait:?})")?;
        }
        write!(f, ": {}", self.response)
    }
}

impl std::error::Error for RateLimitError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.response)
    }
}
