//! Retry policy.
//!
//! Retries live in the SDK rather than in the HTTP client, so a policy can
//! change for one call (see [`Client::with_retry_policy`](crate::Client::with_retry_policy))
//! without touching shared state.

use std::fmt;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use reqwest::StatusCode;
use reqwest::header::{HeaderMap, RETRY_AFTER};

use crate::error::{RateLimitError, RateLimitKind};

/// Default number of retries after the first attempt.
pub const DEFAULT_MAX_RETRIES: u32 = 4;
/// Default base delay between attempts.
pub const DEFAULT_MIN_WAIT: Duration = Duration::from_secs(1);
/// Default cap on the computed backoff.
pub const DEFAULT_MAX_WAIT: Duration = Duration::from_secs(30);

/// The result of one attempt, as seen by [`RetryPolicy::should_retry`].
#[derive(Debug, Clone, Copy)]
#[non_exhaustive]
pub enum AttemptOutcome<'a> {
    /// A response arrived and its body was read.
    Response {
        /// Response status.
        status: StatusCode,
        /// Response headers.
        headers: &'a HeaderMap,
    },
    /// The request could not be sent, or its body could not be read.
    Error(&'a reqwest::Error),
}

impl AttemptOutcome<'_> {
    /// The response status, if there was a response.
    pub fn status(&self) -> Option<StatusCode> {
        match self {
            Self::Response { status, .. } => Some(*status),
            Self::Error(_) => None,
        }
    }
}

/// Input to a [`RetryPolicy::backoff`] function.
#[derive(Debug, Clone, Copy)]
#[non_exhaustive]
pub struct BackoffContext<'a> {
    /// Zero-based index of the attempt that just failed.
    pub attempt: u32,
    /// [`RetryPolicy::min_wait`].
    pub min_wait: Duration,
    /// [`RetryPolicy::max_wait`].
    pub max_wait: Duration,
    /// The response status, if there was a response.
    pub status: Option<StatusCode>,
    /// The response headers, if there was a response.
    pub headers: Option<&'a HeaderMap>,
}

/// What to do with a rate-limited response.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RateLimitDecision {
    /// Return the [`RateLimitError`] now.
    GiveUp,
    /// Retry after the [`RetryPolicy::backoff`] delay.
    Retry,
    /// Retry after this delay.
    RetryAfter(Duration),
}

/// Limits the retries for one kind of rate limit. See
/// [`RateLimitPolicy::rules`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RateLimitRule {
    /// Retry while the request's rate-limited retry count (both kinds) is
    /// below this. Zero gives up at once.
    pub max_retries: u32,
    /// The longest server-requested wait to accept. A longer one gives up
    /// rather than being shortened. `None` accepts any wait.
    pub max_wait: Option<Duration>,
}

impl RateLimitRule {
    /// Give up on the first rate-limited response.
    pub const fn give_up() -> Self {
        Self {
            max_retries: 0,
            max_wait: None,
        }
    }

    /// Retry up to `n` times.
    pub const fn retries(n: u32) -> Self {
        Self {
            max_retries: n,
            max_wait: None,
        }
    }

    /// Give up when the server asks to wait longer than `wait`.
    #[must_use]
    pub const fn max_wait(mut self, wait: Duration) -> Self {
        self.max_wait = Some(wait);
        self
    }

    fn decide(&self, rl: &RateLimitError) -> RateLimitDecision {
        if rl.retries >= self.max_retries {
            return RateLimitDecision::GiveUp;
        }
        match (rl.retry_after, self.max_wait) {
            (Some(wait), Some(max)) if wait > max => RateLimitDecision::GiveUp,
            (Some(wait), _) if !wait.is_zero() => RateLimitDecision::RetryAfter(wait),
            _ => RateLimitDecision::Retry,
        }
    }
}

type RateLimitFn = Arc<dyn Fn(&RateLimitError) -> RateLimitDecision + Send + Sync>;

/// How rate-limited responses are retried. These responses never reach
/// [`RetryPolicy::should_retry`].
#[derive(Clone, Default)]
#[non_exhaustive]
pub enum RateLimitPolicy {
    /// Retry a 429 after the backoff delay (which honors `Retry-After` in
    /// full); return a rate-limited 403 at once.
    #[default]
    Default,
    /// Apply `primary` to a spent primary quota and `secondary` to any other
    /// rate limit. A rule's wait is the server's `Retry-After`, or the backoff
    /// delay when it gave none.
    Rules {
        /// Rule for [`RateLimitKind::Primary`].
        primary: RateLimitRule,
        /// Rule for [`RateLimitKind::Secondary`].
        secondary: RateLimitRule,
    },
    /// A custom decision. It may be called concurrently.
    Custom(RateLimitFn),
}

impl RateLimitPolicy {
    /// See [`RateLimitPolicy::Rules`].
    ///
    /// A controller can give up at once on a spent quota, absorb short
    /// secondary limits, and requeue the rest:
    ///
    /// ```
    /// use std::time::Duration;
    /// use scaleset::{RateLimitPolicy, RateLimitRule, RetryPolicy};
    ///
    /// let policy = RetryPolicy::default().rate_limit(RateLimitPolicy::rules(
    ///     RateLimitRule::give_up(),
    ///     RateLimitRule::retries(2).max_wait(Duration::from_secs(5)),
    /// ));
    /// ```
    pub fn rules(primary: RateLimitRule, secondary: RateLimitRule) -> Self {
        Self::Rules { primary, secondary }
    }

    /// See [`RateLimitPolicy::Custom`].
    pub fn custom<F>(f: F) -> Self
    where
        F: Fn(&RateLimitError) -> RateLimitDecision + Send + Sync + 'static,
    {
        Self::Custom(Arc::new(f))
    }

    pub(crate) fn decide(&self, rl: &RateLimitError) -> RateLimitDecision {
        match self {
            Self::Default => {
                if rl.status == StatusCode::TOO_MANY_REQUESTS {
                    RateLimitDecision::Retry
                } else {
                    RateLimitDecision::GiveUp
                }
            }
            Self::Rules { primary, secondary } => match rl.kind {
                RateLimitKind::Primary => primary.decide(rl),
                RateLimitKind::Secondary => secondary.decide(rl),
            },
            Self::Custom(f) => f(rl),
        }
    }
}

impl fmt::Debug for RateLimitPolicy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Default => f.write_str("Default"),
            Self::Rules { primary, secondary } => f
                .debug_struct("Rules")
                .field("primary", primary)
                .field("secondary", secondary)
                .finish(),
            Self::Custom(_) => f.write_str("Custom(..)"),
        }
    }
}

type ShouldRetryFn = Arc<dyn Fn(&AttemptOutcome<'_>) -> bool + Send + Sync>;
type BackoffFn = Arc<dyn Fn(&BackoffContext<'_>) -> Duration + Send + Sync>;

/// How the SDK retries a request.
///
/// Every field has a default, so start from [`RetryPolicy::default`] and
/// change what you need:
///
/// ```
/// use std::time::Duration;
/// use scaleset::RetryPolicy;
///
/// let policy = RetryPolicy::default()
///     .max_retries(8)
///     .max_elapsed(Duration::from_secs(60));
/// ```
#[derive(Clone)]
pub struct RetryPolicy {
    pub(crate) max_retries: u32,
    pub(crate) min_wait: Duration,
    pub(crate) max_wait: Duration,
    pub(crate) max_elapsed: Option<Duration>,
    pub(crate) should_retry: Option<ShouldRetryFn>,
    pub(crate) backoff: Option<BackoffFn>,
    pub(crate) rate_limit: RateLimitPolicy,
    /// Bounds each attempt on its own. Set by the SDK for message polls.
    pub(crate) attempt_timeout: Option<Duration>,
    /// Statuses retried on top of the policy. Set by the SDK for the admin
    /// token exchange.
    pub(crate) also_retry: &'static [StatusCode],
}

impl Default for RetryPolicy {
    /// Four retries with jittered exponential backoff from 1 s to 30 s, no
    /// total budget. Transport errors, 429, and 5xx other than 501 are
    /// retried.
    fn default() -> Self {
        Self {
            max_retries: DEFAULT_MAX_RETRIES,
            min_wait: DEFAULT_MIN_WAIT,
            max_wait: DEFAULT_MAX_WAIT,
            max_elapsed: None,
            should_retry: None,
            backoff: None,
            rate_limit: RateLimitPolicy::Default,
            attempt_timeout: None,
            also_retry: &[],
        }
    }
}

impl fmt::Debug for RetryPolicy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RetryPolicy")
            .field("max_retries", &self.max_retries)
            .field("min_wait", &self.min_wait)
            .field("max_wait", &self.max_wait)
            .field("max_elapsed", &self.max_elapsed)
            .field("should_retry", &self.should_retry.as_ref().map(|_| ".."))
            .field("backoff", &self.backoff.as_ref().map(|_| ".."))
            .field("rate_limit", &self.rate_limit)
            .finish_non_exhaustive()
    }
}

impl RetryPolicy {
    /// A policy that never retries.
    pub fn none() -> Self {
        Self::default().max_retries(0)
    }

    /// Retries after the first attempt. Zero disables retries.
    #[must_use]
    pub fn max_retries(mut self, n: u32) -> Self {
        self.max_retries = n;
        self
    }

    /// Base delay for exponential backoff.
    #[must_use]
    pub fn min_wait(mut self, wait: Duration) -> Self {
        self.min_wait = wait;
        self
    }

    /// Cap on the exponential backoff. It does not cap a `Retry-After` the
    /// server sends.
    #[must_use]
    pub fn max_wait(mut self, wait: Duration) -> Self {
        self.max_wait = wait;
        self
    }

    /// Total time budget for one HTTP request: every attempt, the waits
    /// between them, and reading the final response.
    ///
    /// When the next wait would reach the budget, the SDK stops and returns
    /// the last response (for example a [`RateLimitError`] with its
    /// `retry_after`) instead of sleeping into a timeout. If the budget runs
    /// out during an attempt, the call fails with
    /// [`Error::MaxElapsedExceeded`](crate::Error::MaxElapsedExceeded).
    ///
    /// The budget applies per HTTP request. A call that first refreshes the
    /// admin token makes several. To bound a whole call, wrap it in
    /// [`tokio::time::timeout`].
    #[must_use]
    pub fn max_elapsed(mut self, budget: Duration) -> Self {
        self.max_elapsed = Some(budget);
        self
    }

    /// Decides whether a non-rate-limited attempt is retried. Replaces
    /// [`RetryPolicy::default_should_retry`]. It may be called concurrently.
    #[must_use]
    pub fn should_retry<F>(mut self, f: F) -> Self
    where
        F: Fn(&AttemptOutcome<'_>) -> bool + Send + Sync + 'static,
    {
        self.should_retry = Some(Arc::new(f));
        self
    }

    /// Computes the delay before the next attempt. Replaces
    /// [`RetryPolicy::default_backoff`]. It may be called concurrently.
    #[must_use]
    pub fn backoff<F>(mut self, f: F) -> Self
    where
        F: Fn(&BackoffContext<'_>) -> Duration + Send + Sync + 'static,
    {
        self.backoff = Some(Arc::new(f));
        self
    }

    /// Sets how rate-limited responses are retried.
    #[must_use]
    pub fn rate_limit(mut self, policy: RateLimitPolicy) -> Self {
        self.rate_limit = policy;
        self
    }

    /// The configured number of retries.
    pub fn retries(&self) -> u32 {
        self.max_retries
    }

    /// The configured budget, if any.
    pub fn budget(&self) -> Option<Duration> {
        self.max_elapsed
    }

    /// The default retry decision: retry transport errors, 429, and 5xx other
    /// than 501. TLS certificate failures are not retried.
    pub fn default_should_retry(outcome: &AttemptOutcome<'_>) -> bool {
        match outcome {
            AttemptOutcome::Error(e) => !is_certificate_error(e),
            AttemptOutcome::Response { status, .. } => {
                *status == StatusCode::TOO_MANY_REQUESTS
                    || (status.is_server_error() && *status != StatusCode::NOT_IMPLEMENTED)
            }
        }
    }

    /// The default backoff: the full `Retry-After` of a 429 or 503 when
    /// present; otherwise `min_wait * 2^attempt`, plus up to half of that
    /// again as random jitter, capped at `max_wait`.
    pub fn default_backoff(ctx: &BackoffContext<'_>) -> Duration {
        if let (Some(status), Some(headers)) = (ctx.status, ctx.headers) {
            if status == StatusCode::TOO_MANY_REQUESTS || status == StatusCode::SERVICE_UNAVAILABLE
            {
                if let Some(wait) = retry_after(headers, SystemTime::now()) {
                    return wait;
                }
            }
        }

        let exp = 2f64.powi(i32::try_from(ctx.attempt).unwrap_or(i32::MAX));
        let backoff = ctx.min_wait.as_secs_f64() * exp;
        if !backoff.is_finite() || backoff >= ctx.max_wait.as_secs_f64() {
            return ctx.max_wait;
        }
        let wait = Duration::from_secs_f64(backoff);
        let spread = wait.as_nanos() / 2;
        let jitter = if spread > 0 {
            let spread = u64::try_from(spread).unwrap_or(u64::MAX);
            Duration::from_nanos(fastrand::u64(0..=spread))
        } else {
            Duration::ZERO
        };
        (wait + jitter).min(ctx.max_wait)
    }

    pub(crate) fn wants_retry(&self, outcome: &AttemptOutcome<'_>) -> bool {
        if let Some(status) = outcome.status() {
            if self.also_retry.contains(&status) {
                return true;
            }
        }
        match &self.should_retry {
            Some(f) => f(outcome),
            None => Self::default_should_retry(outcome),
        }
    }

    pub(crate) fn rate_limit_decision(&self, rl: &RateLimitError) -> RateLimitDecision {
        // Statuses the SDK insists on retrying are retried under the default
        // policy. A caller who set a policy owns every rate-limited response.
        if matches!(self.rate_limit, RateLimitPolicy::Default)
            && self.also_retry.contains(&rl.status)
        {
            return RateLimitDecision::Retry;
        }
        self.rate_limit.decide(rl)
    }

    pub(crate) fn wait(&self, ctx: &BackoffContext<'_>) -> Duration {
        match &self.backoff {
            Some(f) => f(ctx),
            None => Self::default_backoff(ctx),
        }
    }
}

/// Reports whether a transport error is a TLS certificate failure, which a
/// retry cannot fix.
///
/// `reqwest` does not expose the TLS error type, so this looks for the
/// wording rustls and native TLS stacks use in the error chain.
fn is_certificate_error(err: &reqwest::Error) -> bool {
    let mut source: Option<&(dyn std::error::Error + 'static)> = Some(err);
    while let Some(e) = source {
        let msg = e.to_string().to_ascii_lowercase();
        if msg.contains("certificate") || msg.contains("unknownissuer") {
            return true;
        }
        source = e.source();
    }
    false
}

/// Parses `Retry-After` as seconds or an HTTP date.
pub(crate) fn retry_after(headers: &HeaderMap, now: SystemTime) -> Option<Duration> {
    let value = headers.get(RETRY_AFTER)?.to_str().ok()?.trim();
    parse_retry_after(value, now)
}

pub(crate) fn parse_retry_after(value: &str, now: SystemTime) -> Option<Duration> {
    if value.is_empty() {
        return None;
    }
    if let Ok(seconds) = value.parse::<i64>() {
        return u64::try_from(seconds).ok().map(Duration::from_secs);
    }
    let when = httpdate::parse_http_date(value).ok()?;
    when.duration_since(now).ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use reqwest::header::HeaderValue;

    #[test]
    fn retry_after_forms() {
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1_000_000_000);
        assert_eq!(parse_retry_after("5", now), Some(Duration::from_secs(5)));
        assert_eq!(parse_retry_after("0", now), Some(Duration::ZERO));
        assert_eq!(parse_retry_after("-1", now), None);
        assert_eq!(parse_retry_after("", now), None);
        assert_eq!(parse_retry_after("soon", now), None);

        let later = httpdate::fmt_http_date(now + Duration::from_secs(30));
        assert_eq!(
            parse_retry_after(&later, now),
            Some(Duration::from_secs(30))
        );
        let earlier = httpdate::fmt_http_date(now - Duration::from_secs(30));
        assert_eq!(parse_retry_after(&earlier, now), None);
    }

    #[test]
    fn default_should_retry_statuses() {
        let headers = HeaderMap::new();
        let check = |code: u16| {
            RetryPolicy::default_should_retry(&AttemptOutcome::Response {
                status: StatusCode::from_u16(code).unwrap(),
                headers: &headers,
            })
        };
        assert!(check(429));
        assert!(check(500));
        assert!(check(502));
        assert!(check(503));
        assert!(!check(501));
        assert!(!check(400));
        assert!(!check(401));
        assert!(!check(403));
        assert!(!check(404));
    }

    #[test]
    fn default_backoff_grows_and_caps() {
        let ctx = |attempt| BackoffContext {
            attempt,
            min_wait: Duration::from_secs(1),
            max_wait: Duration::from_secs(30),
            status: None,
            headers: None,
        };
        for attempt in 0..4 {
            let base = Duration::from_secs(1 << attempt);
            let wait = RetryPolicy::default_backoff(&ctx(attempt));
            assert!(
                wait >= base && wait <= base + base / 2,
                "{attempt}: {wait:?}"
            );
        }
        assert_eq!(
            RetryPolicy::default_backoff(&ctx(10)),
            Duration::from_secs(30)
        );
        assert_eq!(
            RetryPolicy::default_backoff(&ctx(u32::MAX)),
            Duration::from_secs(30)
        );
    }

    #[test]
    fn default_backoff_honors_retry_after() {
        let mut headers = HeaderMap::new();
        headers.insert(RETRY_AFTER, HeaderValue::from_static("120"));
        let wait = RetryPolicy::default_backoff(&BackoffContext {
            attempt: 0,
            min_wait: Duration::from_secs(1),
            max_wait: Duration::from_secs(30),
            status: Some(StatusCode::TOO_MANY_REQUESTS),
            headers: Some(&headers),
        });
        assert_eq!(wait, Duration::from_secs(120));
    }

    fn rate_limit(kind: RateLimitKind, retries: u32, after: Option<u64>) -> RateLimitError {
        RateLimitError {
            kind,
            status: StatusCode::FORBIDDEN,
            retry_after: after.map(Duration::from_secs),
            reset: None,
            retries,
            response: crate::error::ApiError {
                method: reqwest::Method::GET,
                url: "u".into(),
                status: StatusCode::FORBIDDEN,
                kind: crate::ApiErrorKind::Other,
                activity_id: None,
                github_request_id: None,
                exception: None,
                message: String::new(),
            },
        }
    }

    #[test]
    fn rules() {
        let policy = RateLimitPolicy::rules(
            RateLimitRule::give_up(),
            RateLimitRule::retries(2).max_wait(Duration::from_secs(5)),
        );
        use RateLimitDecision::*;
        use RateLimitKind::*;
        assert_eq!(policy.decide(&rate_limit(Primary, 0, Some(1))), GiveUp);
        assert_eq!(
            policy.decide(&rate_limit(Secondary, 0, Some(3))),
            RetryAfter(Duration::from_secs(3))
        );
        assert_eq!(policy.decide(&rate_limit(Secondary, 1, None)), Retry);
        assert_eq!(policy.decide(&rate_limit(Secondary, 2, Some(1))), GiveUp);
        assert_eq!(policy.decide(&rate_limit(Secondary, 0, Some(6))), GiveUp);
    }

    #[test]
    fn default_rate_limit_policy() {
        let mut rl = rate_limit(RateLimitKind::Secondary, 0, None);
        assert_eq!(
            RateLimitPolicy::Default.decide(&rl),
            RateLimitDecision::GiveUp
        );
        rl.status = StatusCode::TOO_MANY_REQUESTS;
        assert_eq!(
            RateLimitPolicy::Default.decide(&rl),
            RateLimitDecision::Retry
        );
    }

    #[test]
    fn also_retry_overrides_default_only() {
        let policy = RetryPolicy {
            also_retry: &[StatusCode::FORBIDDEN],
            ..RetryPolicy::default()
        };
        let rl = rate_limit(RateLimitKind::Secondary, 0, None);
        assert_eq!(policy.rate_limit_decision(&rl), RateLimitDecision::Retry);

        let mut policy = policy.rate_limit(RateLimitPolicy::rules(
            RateLimitRule::give_up(),
            RateLimitRule::give_up(),
        ));
        policy.also_retry = &[StatusCode::FORBIDDEN];
        assert_eq!(policy.rate_limit_decision(&rl), RateLimitDecision::GiveUp);
    }
}
