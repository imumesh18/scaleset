//! A fake GitHub + Actions service for integration tests.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use scaleset::{Client, Credentials};
use wiremock::matchers::{header, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

use super::fast_retries;

pub const PAT: &str = "ghp_test";
pub const REGISTRATION_TOKEN: &str = "reg-token";

/// An unsigned JWT that expires `ttl` from now.
pub fn admin_jwt(ttl: Duration) -> String {
    let exp = (SystemTime::now() + ttl)
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let header = URL_SAFE_NO_PAD.encode(br#"{"alg":"none"}"#);
    let payload = URL_SAFE_NO_PAD.encode(format!(r#"{{"exp":{exp}}}"#));
    format!("{header}.{payload}.sig")
}

pub struct Fake {
    pub server: MockServer,
    pub admin_token: String,
}

impl Fake {
    /// A server that answers the token exchange for org `org`, issuing an
    /// admin token valid for an hour.
    pub async fn start() -> Self {
        Self::start_with_ttl(Duration::from_secs(3600)).await
    }

    pub async fn start_with_ttl(ttl: Duration) -> Self {
        let server = MockServer::start().await;
        let admin_token = admin_jwt(ttl);

        Mock::given(method("POST"))
            .and(path("/api/v3/orgs/org/actions/runners/registration-token"))
            .respond_with(ResponseTemplate::new(201).set_body_json(serde_json::json!({
                "token": REGISTRATION_TOKEN,
                "expires_at": "2099-01-01T00:00:00Z",
            })))
            .named("registration token")
            .mount(&server)
            .await;

        Mock::given(method("POST"))
            .and(path("/api/v3/actions/runner-registration"))
            .and(header(
                "authorization",
                format!("RemoteAuth {REGISTRATION_TOKEN}"),
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "url": format!("{}/tenant/", server.uri()),
                "token": admin_token,
            })))
            .named("admin connection")
            .mount(&server)
            .await;

        Self {
            server,
            admin_token,
        }
    }

    pub fn config_url(&self) -> String {
        format!("{}/org", self.server.uri())
    }

    pub fn admin_auth(&self) -> String {
        format!("Bearer {}", self.admin_token)
    }

    /// A PAT client with fast retries.
    pub fn client(&self) -> Client {
        Client::builder(
            self.config_url(),
            Credentials::personal_access_token(PAT).unwrap(),
        )
        .retry_policy(fast_retries())
        .allow_http(true)
        .build()
        .unwrap()
    }
}
