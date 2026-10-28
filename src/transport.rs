//! The retry engine and response-to-error mapping.

use std::time::{Duration, SystemTime};

use bytes::Bytes;
use reqwest::header::{CONTENT_TYPE, HeaderMap};
use reqwest::{Method, Request, StatusCode};
use serde::de::DeserializeOwned;
use tokio::time::Instant;

use crate::error::{
    ActionsException, ApiError, ApiErrorKind, Error, RateLimitError, RateLimitKind, Result,
};
use crate::retry::{AttemptOutcome, BackoffContext, RateLimitDecision, RetryPolicy, retry_after};

const HEADER_ACTIVITY_ID: &str = "ActivityId";
const HEADER_GITHUB_REQUEST_ID: &str = "X-GitHub-Request-Id";
const HEADER_RATE_LIMIT_REMAINING: &str = "X-RateLimit-Remaining";
const HEADER_RATE_LIMIT_RESET: &str = "X-RateLimit-Reset";

/// A fully read response.
#[derive(Debug)]
pub(crate) struct RawResponse {
    pub method: Method,
    pub url: String,
    pub status: StatusCode,
    pub headers: HeaderMap,
    pub body: Bytes,
    /// Rate-limited retries made before this response.
    pub rate_limit_retries: u32,
}

impl RawResponse {
    /// Fails unless the status is `expected`.
    pub fn expect(self, expected: StatusCode) -> Result<Self> {
        if self.status == expected {
            Ok(self)
        } else {
            Err(self.into_error(ApiErrorKind::Other))
        }
    }

    /// Fails unless the status is 2xx.
    pub fn expect_success(self) -> Result<Self> {
        if self.status.is_success() {
            Ok(self)
        } else {
            Err(self.into_error(ApiErrorKind::Other))
        }
    }

    /// Decodes the body as JSON.
    pub fn json<T: DeserializeOwned>(&self) -> Result<T> {
        serde_json::from_slice(&self.body).map_err(|e| Error::Decode {
            method: self.method.clone(),
            url: self.url.clone(),
            reason: e.to_string(),
        })
    }

    /// Builds the error for an unsuccessful response. `fallback` is the kind
    /// used when the body does not name a known exception.
    pub fn into_error(self, fallback: ApiErrorKind) -> Error {
        let api = self.api_error(fallback);
        match classify_rate_limit(self.status, &self.headers, &api.message, SystemTime::now()) {
            Some((kind, retry_after, reset)) => Error::from(RateLimitError {
                kind,
                status: self.status,
                retry_after,
                reset,
                retries: self.rate_limit_retries,
                response: api,
            }),
            None => Error::from(api),
        }
    }

    fn api_error(&self, fallback: ApiErrorKind) -> ApiError {
        let header = |name: &str| {
            self.headers
                .get(name)
                .and_then(|v| v.to_str().ok())
                .filter(|v| !v.is_empty())
                .map(str::to_owned)
        };
        let (exception, message) = parse_error_body(&self.headers, &self.body);
        let kind = exception
            .as_ref()
            .and_then(|e| exception_kind(&e.type_name))
            .unwrap_or(fallback);
        ApiError {
            method: self.method.clone(),
            url: self.url.clone(),
            status: self.status,
            kind,
            activity_id: header(HEADER_ACTIVITY_ID),
            github_request_id: header(HEADER_GITHUB_REQUEST_ID),
            exception,
            message,
        }
    }
}

fn exception_kind(type_name: &str) -> Option<ApiErrorKind> {
    if type_name.contains("AgentExistsException") {
        Some(ApiErrorKind::RunnerExists)
    } else if type_name.contains("AgentNotFoundException") {
        Some(ApiErrorKind::RunnerNotFound)
    } else if type_name.contains("JobStillRunningException") {
        Some(ApiErrorKind::JobStillRunning)
    } else {
        None
    }
}

/// Reads an error body: an Actions exception (`{"typeName", "message"}`),
/// a GitHub error (`{"message"}`), or plain text.
fn parse_error_body(headers: &HeaderMap, body: &[u8]) -> (Option<ActionsException>, String) {
    let text = String::from_utf8_lossy(body).trim().to_owned();
    if text.is_empty() {
        return (None, String::new());
    }
    let is_plain = headers
        .get(CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.contains("text/plain"));
    if is_plain {
        return (None, text);
    }
    match serde_json::from_str::<ActionsException>(&text) {
        Ok(e) if !e.message.is_empty() || !e.type_name.is_empty() => {
            let message = e.message.clone();
            (Some(e), message)
        }
        _ => (None, text),
    }
}

/// Classifies a rate-limited response. Returns its kind, the delay the
/// server asked for, and when the window resets.
pub(crate) fn classify_rate_limit(
    status: StatusCode,
    headers: &HeaderMap,
    message: &str,
    now: SystemTime,
) -> Option<(RateLimitKind, Option<Duration>, Option<SystemTime>)> {
    let header = |name: &str| headers.get(name).and_then(|v| v.to_str().ok());
    let primary = header(HEADER_RATE_LIMIT_REMAINING).map(str::trim) == Some("0");
    let secondary = mentions_secondary_rate(message);
    let has_retry_after = header(reqwest::header::RETRY_AFTER.as_str()).is_some();

    match status {
        StatusCode::TOO_MANY_REQUESTS => {}
        StatusCode::FORBIDDEN if primary || secondary || has_retry_after => {}
        _ => return None,
    }

    let kind = if primary && !secondary {
        RateLimitKind::Primary
    } else {
        RateLimitKind::Secondary
    };

    let reset = header(HEADER_RATE_LIMIT_RESET)
        .and_then(|v| v.trim().parse::<u64>().ok())
        .filter(|&s| s > 0)
        .map(|s| SystemTime::UNIX_EPOCH + Duration::from_secs(s));

    let wait = retry_after(headers, now).or_else(|| {
        if primary {
            reset.and_then(|r| r.duration_since(now).ok())
        } else {
            None
        }
    });

    Some((kind, wait, reset))
}

/// Matches the word-bounded phrase "secondary rate", case-insensitively.
fn mentions_secondary_rate(message: &str) -> bool {
    let lower = message.to_ascii_lowercase();
    let bytes = lower.as_bytes();
    let needle = "secondary rate";
    let is_word = |b: u8| b.is_ascii_alphanumeric() || b == b'_';
    lower.match_indices(needle).any(|(i, _)| {
        let before = i.checked_sub(1).map(|j| bytes[j]);
        let after = bytes.get(i + needle.len()).copied();
        !before.is_some_and(is_word) && !after.is_some_and(is_word)
    })
}

/// Sends `request` through `http` under `policy`, reading the whole body.
///
/// The request body must be in memory, so every attempt can resend it.
pub(crate) async fn send(
    http: &reqwest::Client,
    request: Request,
    policy: &RetryPolicy,
) -> Result<RawResponse> {
    let method = request.method().clone();
    let url = request.url().to_string();
    let start = Instant::now();
    let deadline = policy.max_elapsed.map(|budget| start + budget);
    let mut rate_limit_retries = 0u32;

    for attempt in 0u32.. {
        let mut attempt_req = request
            .try_clone()
            .expect("SDK request bodies are buffered and can be cloned");
        if let Some(timeout) = policy.attempt_timeout {
            *attempt_req.timeout_mut() = Some(timeout);
        }

        let fut = execute(http, attempt_req);
        let outcome = match deadline {
            Some(deadline) => match tokio::time::timeout_at(deadline, fut).await {
                Ok(outcome) => outcome,
                Err(_) => {
                    return Err(Error::MaxElapsedExceeded {
                        method,
                        url,
                        budget: policy.max_elapsed.unwrap_or_default(),
                    });
                }
            },
            None => fut.await,
        };

        let can_retry = attempt < policy.max_retries;
        let mut rate_limited = false;

        let wait = match &outcome {
            Ok((status, headers, body)) => {
                let rl = if *status == StatusCode::FORBIDDEN
                    || *status == StatusCode::TOO_MANY_REQUESTS
                {
                    let (_, message) = parse_error_body(headers, body);
                    classify_rate_limit(*status, headers, &message, SystemTime::now()).map(
                        |(kind, retry_after, reset)| RateLimitError {
                            kind,
                            status: *status,
                            retry_after,
                            reset,
                            retries: rate_limit_retries,
                            response: ApiError {
                                method: method.clone(),
                                url: url.clone(),
                                status: *status,
                                kind: ApiErrorKind::Other,
                                activity_id: None,
                                github_request_id: None,
                                exception: None,
                                message,
                            },
                        },
                    )
                } else {
                    None
                };

                let backoff = || {
                    policy.wait(&BackoffContext {
                        attempt,
                        min_wait: policy.min_wait,
                        max_wait: policy.max_wait,
                        status: Some(*status),
                        headers: Some(headers),
                    })
                };

                if let Some(rl) = rl {
                    rate_limited = true;
                    if !can_retry {
                        None
                    } else {
                        match policy.rate_limit_decision(&rl) {
                            RateLimitDecision::GiveUp => {
                                tracing::debug!(
                                    %method, %url, attempt = attempt + 1, kind = %rl.kind,
                                    retry_after = ?rl.retry_after, rate_limit_retries,
                                    "not retrying: rate limit policy gave up"
                                );
                                None
                            }
                            RateLimitDecision::Retry => Some(backoff()),
                            RateLimitDecision::RetryAfter(d) if d.is_zero() => Some(backoff()),
                            RateLimitDecision::RetryAfter(d) => Some(d),
                        }
                    }
                } else if can_retry
                    && policy.wants_retry(&AttemptOutcome::Response {
                        status: *status,
                        headers,
                    })
                {
                    Some(backoff())
                } else {
                    None
                }
            }
            Err(err) => {
                if can_retry && policy.wants_retry(&AttemptOutcome::Error(err)) {
                    Some(policy.wait(&BackoffContext {
                        attempt,
                        min_wait: policy.min_wait,
                        max_wait: policy.max_wait,
                        status: None,
                        headers: None,
                    }))
                } else {
                    None
                }
            }
        };

        // A wait that reaches the budget would only end in a timeout and lose
        // the response that explains the failure. Return that response now.
        let wait = wait.filter(|wait| match deadline {
            Some(deadline) if Instant::now() + *wait >= deadline => {
                tracing::debug!(%method, %url, attempt = attempt + 1, ?wait,
                    "not retrying: wait reaches the max_elapsed budget");
                false
            }
            _ => true,
        });

        let Some(wait) = wait else {
            return match outcome {
                Ok((status, headers, body)) => Ok(RawResponse {
                    method,
                    url,
                    status,
                    headers,
                    body,
                    rate_limit_retries,
                }),
                Err(source) => Err(Error::Transport {
                    method,
                    url,
                    source,
                }),
            };
        };

        tracing::debug!(%method, %url, attempt = attempt + 1, ?wait, "retrying request");
        drop(outcome);
        tokio::time::sleep(wait).await;
        if rate_limited {
            rate_limit_retries += 1;
        }
    }

    unreachable!("the retry loop always returns")
}

/// One attempt: send the request and read the whole body, both under the
/// request's timeout.
async fn execute(
    http: &reqwest::Client,
    request: Request,
) -> std::result::Result<(StatusCode, HeaderMap, Bytes), reqwest::Error> {
    let response = http.execute(request).await?;
    let status = response.status();
    let headers = response.headers().clone();
    let body = response.bytes().await?;
    Ok((status, headers, trim_bom(body)))
}

/// Drops a UTF-8 byte order mark, which the Actions service sometimes sends.
fn trim_bom(body: Bytes) -> Bytes {
    if body.starts_with(b"\xEF\xBB\xBF") {
        body.slice(3..)
    } else {
        body
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use reqwest::header::HeaderValue;

    fn headers(pairs: &[(&'static str, &'static str)]) -> HeaderMap {
        let mut h = HeaderMap::new();
        for (k, v) in pairs {
            h.insert(*k, HeaderValue::from_static(v));
        }
        h
    }

    #[test]
    fn classifies_rate_limits() {
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1_000);

        // Plain 403 is not a rate limit.
        assert!(classify_rate_limit(StatusCode::FORBIDDEN, &headers(&[]), "nope", now).is_none());

        // 429 alone is secondary.
        let (kind, wait, _) =
            classify_rate_limit(StatusCode::TOO_MANY_REQUESTS, &headers(&[]), "", now).unwrap();
        assert_eq!(kind, RateLimitKind::Secondary);
        assert_eq!(wait, None);

        // Spent quota is primary and waits for the reset.
        let (kind, wait, reset) = classify_rate_limit(
            StatusCode::FORBIDDEN,
            &headers(&[
                ("x-ratelimit-remaining", "0"),
                ("x-ratelimit-reset", "1060"),
            ]),
            "API rate limit exceeded",
            now,
        )
        .unwrap();
        assert_eq!(kind, RateLimitKind::Primary);
        assert_eq!(wait, Some(Duration::from_secs(60)));
        assert_eq!(
            reset,
            Some(SystemTime::UNIX_EPOCH + Duration::from_secs(1060))
        );

        // A secondary-limit message wins over a spent quota.
        let (kind, _, _) = classify_rate_limit(
            StatusCode::FORBIDDEN,
            &headers(&[("x-ratelimit-remaining", "0")]),
            "You have exceeded a secondary rate limit.",
            now,
        )
        .unwrap();
        assert_eq!(kind, RateLimitKind::Secondary);

        // Retry-After takes precedence over the reset.
        let (_, wait, _) = classify_rate_limit(
            StatusCode::FORBIDDEN,
            &headers(&[
                ("retry-after", "7"),
                ("x-ratelimit-remaining", "0"),
                ("x-ratelimit-reset", "1060"),
            ]),
            "",
            now,
        )
        .unwrap();
        assert_eq!(wait, Some(Duration::from_secs(7)));
    }

    #[test]
    fn secondary_phrase_is_word_bounded() {
        assert!(mentions_secondary_rate("a Secondary Rate limit"));
        assert!(mentions_secondary_rate("secondary rate"));
        assert!(!mentions_secondary_rate("nonsecondary rate"));
        assert!(!mentions_secondary_rate("secondary rates"));
    }

    #[test]
    fn error_bodies() {
        let json = headers(&[("content-type", "application/json")]);
        let (e, msg) = parse_error_body(
            &json,
            br#"{"typeName":"GitHub.DistributedTask.WebApi.AgentNotFoundException","message":"gone"}"#,
        );
        assert_eq!(msg, "gone");
        assert_eq!(
            exception_kind(&e.unwrap().type_name),
            Some(ApiErrorKind::RunnerNotFound)
        );

        let (e, msg) = parse_error_body(&json, b"not json");
        assert!(e.is_none());
        assert_eq!(msg, "not json");

        let plain = headers(&[("content-type", "text/plain; charset=utf-8")]);
        let (e, msg) = parse_error_body(&plain, br#"{"message":"x"}"#);
        assert!(e.is_none());
        assert_eq!(msg, r#"{"message":"x"}"#);
    }

    #[test]
    fn bom_is_trimmed() {
        assert_eq!(trim_bom(Bytes::from_static(b"\xEF\xBB\xBF{}")), &b"{}"[..]);
        assert_eq!(trim_bom(Bytes::from_static(b"{}")), &b"{}"[..]);
    }
}
