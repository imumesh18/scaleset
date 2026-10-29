//! Credentials and GitHub App JWTs.

use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use aws_lc_rs::rsa::KeyPair;
use aws_lc_rs::signature::RSA_PKCS1_SHA256;
use base64::Engine;
use base64::engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD};
use secrecy::SecretString;
use serde::{Deserialize, Serialize};

use crate::error::{BoxError, Error};

/// Creates short-lived JWTs for GitHub App authentication.
///
/// The JWT must be RS256-signed with `iss` (the app's client ID), `iat` and
/// `exp` claims. See [GitHub's documentation][docs].
///
/// Implement this to fetch JWTs from an external service. To sign with a key
/// held in a KMS or HSM, implement [`Rs256Signer`] and use
/// [`SignerJwtProvider`] instead.
///
/// [docs]: https://docs.github.com/en/apps/creating-github-apps/authenticating-with-a-github-app/generating-a-json-web-token-jwt-for-a-github-app
pub trait JwtProvider: Send + Sync + 'static {
    /// Returns a signed JWT.
    fn token(&self) -> impl Future<Output = Result<String, BoxError>> + Send;
}

/// Signs data with RSASSA-PKCS1-v1_5 and SHA-256 (the JWT `RS256`
/// algorithm), for keys that never leave a KMS or HSM.
///
/// `message` is the JWT signing input, not a digest. Services that sign a
/// precomputed digest (for example AWS KMS with `MessageType: DIGEST`) need
/// the SHA-256 of `message`.
pub trait Rs256Signer: Send + Sync + 'static {
    /// Returns the signature over `message`.
    fn sign(&self, message: &[u8]) -> impl Future<Output = Result<Vec<u8>, BoxError>> + Send;
}

/// Object-safe form of [`JwtProvider`], so credentials can hold any provider.
pub(crate) trait DynJwtProvider: Send + Sync {
    fn token(&self) -> Pin<Box<dyn Future<Output = Result<String, BoxError>> + Send + '_>>;
}

impl<P: JwtProvider> DynJwtProvider for P {
    fn token(&self) -> Pin<Box<dyn Future<Output = Result<String, BoxError>> + Send + '_>> {
        Box::pin(JwtProvider::token(self))
    }
}

/// Credentials for the scale set APIs.
///
/// Prefer a GitHub App: it is more narrowly scoped and its tokens rotate.
#[derive(Clone)]
pub struct Credentials {
    pub(crate) kind: CredentialsKind,
}

#[derive(Clone)]
pub(crate) enum CredentialsKind {
    Token(SecretString),
    App {
        installation_id: i64,
        provider: Arc<dyn DynJwtProvider>,
    },
}

impl Credentials {
    /// A personal access token.
    pub fn personal_access_token(token: impl Into<String>) -> Result<Self, Error> {
        let token = token.into();
        if token.trim().is_empty() {
            return Err(Error::InvalidCredentials(
                "personal access token is empty".into(),
            ));
        }
        Ok(Self {
            kind: CredentialsKind::Token(token.into()),
        })
    }

    /// A GitHub App with its private key in PEM form (PKCS#1 `RSA PRIVATE
    /// KEY` as downloaded from GitHub, or PKCS#8 `PRIVATE KEY`).
    ///
    /// `client_id` is the app's client ID; the numeric app ID also works.
    pub fn github_app(
        client_id: impl Into<String>,
        installation_id: i64,
        private_key_pem: &str,
    ) -> Result<Self, Error> {
        let provider = RsaKeyJwtProvider::from_pem(client_id, private_key_pem)?;
        Self::github_app_with_provider(installation_id, provider)
    }

    /// A GitHub App whose JWTs come from `provider`, for example a
    /// [`SignerJwtProvider`] backed by a KMS.
    pub fn github_app_with_provider(
        installation_id: i64,
        provider: impl JwtProvider,
    ) -> Result<Self, Error> {
        if installation_id <= 0 {
            return Err(Error::InvalidCredentials(
                "app installation ID is required".into(),
            ));
        }
        Ok(Self {
            kind: CredentialsKind::App {
                installation_id,
                provider: Arc::new(provider),
            },
        })
    }
}

impl fmt::Debug for Credentials {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.kind {
            CredentialsKind::Token(_) => f
                .debug_struct("Credentials::PersonalAccessToken")
                .finish_non_exhaustive(),
            CredentialsKind::App {
                installation_id, ..
            } => f
                .debug_struct("Credentials::GitHubApp")
                .field("installation_id", installation_id)
                .finish_non_exhaustive(),
        }
    }
}

/// Signs GitHub App JWTs with an RSA key held in memory.
pub(crate) struct RsaKeyJwtProvider {
    client_id: String,
    key: KeyPair,
}

impl RsaKeyJwtProvider {
    /// Parses a PEM-encoded RSA private key (PKCS#1 or PKCS#8).
    pub(crate) fn from_pem(client_id: impl Into<String>, pem: &str) -> Result<Self, Error> {
        let client_id = client_id.into();
        if client_id.trim().is_empty() {
            return Err(Error::InvalidCredentials("client ID is required".into()));
        }
        let (label, der) = decode_pem(pem)
            .ok_or_else(|| Error::InvalidCredentials("app private key is not valid PEM".into()))?;
        let key = match label.as_str() {
            "RSA PRIVATE KEY" => KeyPair::from_der(&der),
            "PRIVATE KEY" => KeyPair::from_pkcs8(&der),
            other => {
                return Err(Error::InvalidCredentials(format!(
                    "unsupported private key type {other:?}, expected an RSA private key"
                )));
            }
        }
        .map_err(|e| Error::InvalidCredentials(format!("invalid app private key: {e}")))?;
        Ok(Self { client_id, key })
    }

    fn sign_now(&self) -> Result<String, BoxError> {
        let signing_input = signing_input(&self.client_id, SystemTime::now())?;
        let rng = aws_lc_rs::rand::SystemRandom::new();
        let mut sig = vec![0; self.key.public_modulus_len()];
        self.key
            .sign(&RSA_PKCS1_SHA256, &rng, signing_input.as_bytes(), &mut sig)
            .map_err(|e| format!("failed to sign app JWT: {e}"))?;
        Ok(finish_jwt(signing_input, &sig))
    }
}

impl JwtProvider for RsaKeyJwtProvider {
    fn token(&self) -> impl Future<Output = Result<String, BoxError>> + Send {
        std::future::ready(self.sign_now())
    }
}

impl fmt::Debug for RsaKeyJwtProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RsaKeyJwtProvider")
            .field("client_id", &self.client_id)
            .finish_non_exhaustive()
    }
}

/// Builds GitHub App JWTs and has an [`Rs256Signer`] sign them, so the
/// private key can stay in a KMS or HSM.
///
/// ```no_run
/// use scaleset::{BoxError, Credentials, Rs256Signer, SignerJwtProvider};
///
/// struct Kms;
///
/// impl Rs256Signer for Kms {
///     async fn sign(&self, message: &[u8]) -> Result<Vec<u8>, BoxError> {
///         // call your KMS here
///         # let _ = message; unimplemented!()
///     }
/// }
///
/// let credentials =
///     Credentials::github_app_with_provider(12345, SignerJwtProvider::new("Iv1.abc", Kms))?;
/// # Ok::<(), scaleset::Error>(())
/// ```
#[derive(Debug, Clone)]
pub struct SignerJwtProvider<S> {
    client_id: String,
    signer: S,
}

impl<S: Rs256Signer> SignerJwtProvider<S> {
    /// A provider for the app with `client_id`.
    pub fn new(client_id: impl Into<String>, signer: S) -> Self {
        Self {
            client_id: client_id.into(),
            signer,
        }
    }
}

impl<S: Rs256Signer> JwtProvider for SignerJwtProvider<S> {
    async fn token(&self) -> Result<String, BoxError> {
        if self.client_id.trim().is_empty() {
            return Err("client ID is required".into());
        }
        let signing_input = signing_input(&self.client_id, SystemTime::now())?;
        let sig = self.signer.sign(signing_input.as_bytes()).await?;
        if sig.is_empty() {
            return Err("signer returned an empty signature".into());
        }
        Ok(finish_jwt(signing_input, &sig))
    }
}

#[derive(Serialize, Deserialize)]
struct Claims {
    #[serde(default)]
    iat: i64,
    exp: i64,
    #[serde(default)]
    iss: String,
}

/// Issued a minute in the past to allow for clock drift; valid for nine
/// minutes, under GitHub's ten-minute maximum.
fn signing_input(client_id: &str, now: SystemTime) -> Result<String, BoxError> {
    let issued_at = now - Duration::from_secs(60);
    let iat = i64::try_from(issued_at.duration_since(UNIX_EPOCH)?.as_secs())?;
    let claims = Claims {
        iat,
        exp: iat + 9 * 60,
        iss: client_id.to_owned(),
    };
    let header = URL_SAFE_NO_PAD.encode(br#"{"alg":"RS256","typ":"JWT"}"#);
    let payload = URL_SAFE_NO_PAD.encode(serde_json::to_vec(&claims)?);
    Ok(format!("{header}.{payload}"))
}

fn finish_jwt(signing_input: String, signature: &[u8]) -> String {
    let mut jwt = signing_input;
    jwt.push('.');
    URL_SAFE_NO_PAD.encode_string(signature, &mut jwt);
    jwt
}

/// Reads the `exp` claim of a JWT without verifying it.
pub(crate) fn unverified_expiry(jwt: &str) -> Option<SystemTime> {
    let payload = jwt.split('.').nth(1)?;
    let bytes = URL_SAFE_NO_PAD.decode(payload.trim_end_matches('=')).ok()?;
    let claims: Claims = serde_json::from_slice(&bytes).ok()?;
    let exp = u64::try_from(claims.exp).ok()?;
    Some(UNIX_EPOCH + Duration::from_secs(exp))
}

/// Decodes the first PEM block, returning its label and contents.
fn decode_pem(pem: &str) -> Option<(String, Vec<u8>)> {
    let pem = pem.trim();
    let begin = pem.find("-----BEGIN ")?;
    let rest = &pem[begin + "-----BEGIN ".len()..];
    let label_end = rest.find("-----")?;
    let label = rest[..label_end].to_owned();
    let body_start = label_end + "-----".len();
    let end_marker = format!("-----END {label}-----");
    let body_end = rest.find(&end_marker)?;
    let body: String = rest[body_start..body_end]
        .lines()
        .filter(|l| !l.contains(':')) // skip RFC 1421 headers, if any
        .flat_map(|l| l.chars())
        .filter(|c| !c.is_whitespace())
        .collect();
    let der = STANDARD.decode(body).ok()?;
    Some((label, der))
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use aws_lc_rs::encoding::AsDer;
    use aws_lc_rs::rsa::KeySize;
    use aws_lc_rs::signature::{RSA_PKCS1_2048_8192_SHA256, UnparsedPublicKey};

    /// A freshly generated PKCS#8 PEM key, for tests.
    pub(crate) fn test_key_pem() -> (String, KeyPair) {
        let key = KeyPair::generate(KeySize::Rsa2048).unwrap();
        let der = key.as_der().unwrap();
        let b64 = STANDARD.encode(der.as_ref());
        let mut pem = String::from("-----BEGIN PRIVATE KEY-----\n");
        for chunk in b64.as_bytes().chunks(64) {
            pem.push_str(std::str::from_utf8(chunk).unwrap());
            pem.push('\n');
        }
        pem.push_str("-----END PRIVATE KEY-----\n");
        (pem, key)
    }

    #[tokio::test]
    async fn signs_verifiable_jwt() {
        let (pem, key) = test_key_pem();
        let provider = RsaKeyJwtProvider::from_pem("Iv1.test", &pem).unwrap();
        let jwt = JwtProvider::token(&provider).await.unwrap();

        let parts: Vec<_> = jwt.split('.').collect();
        assert_eq!(parts.len(), 3);
        let header: serde_json::Value =
            serde_json::from_slice(&URL_SAFE_NO_PAD.decode(parts[0]).unwrap()).unwrap();
        assert_eq!(header["alg"], "RS256");
        let claims: Claims =
            serde_json::from_slice(&URL_SAFE_NO_PAD.decode(parts[1]).unwrap()).unwrap();
        assert_eq!(claims.iss, "Iv1.test");
        assert_eq!(claims.exp - claims.iat, 540);

        let verifier = UnparsedPublicKey::new(
            &RSA_PKCS1_2048_8192_SHA256,
            aws_lc_rs::signature::KeyPair::public_key(&key).as_ref(),
        );
        let signing_input = format!("{}.{}", parts[0], parts[1]);
        let sig = URL_SAFE_NO_PAD.decode(parts[2]).unwrap();
        verifier.verify(signing_input.as_bytes(), &sig).unwrap();

        assert!(unverified_expiry(&jwt).is_some());
    }

    #[tokio::test]
    async fn signer_provider() {
        struct Fixed;
        impl Rs256Signer for Fixed {
            async fn sign(&self, message: &[u8]) -> Result<Vec<u8>, BoxError> {
                assert!(message.starts_with(b"eyJ"));
                Ok(vec![1, 2, 3])
            }
        }
        let jwt = JwtProvider::token(&SignerJwtProvider::new("id", Fixed))
            .await
            .unwrap();
        assert!(jwt.ends_with(".AQID"));

        assert!(
            JwtProvider::token(&SignerJwtProvider::new("", Fixed))
                .await
                .is_err()
        );
    }

    #[test]
    fn rejects_bad_keys() {
        assert!(RsaKeyJwtProvider::from_pem("id", "garbage").is_err());
        assert!(
            RsaKeyJwtProvider::from_pem(
                "id",
                "-----BEGIN EC PRIVATE KEY-----\nAAAA\n-----END EC PRIVATE KEY-----"
            )
            .is_err()
        );
        let (pem, _) = test_key_pem();
        assert!(RsaKeyJwtProvider::from_pem("", &pem).is_err());
        assert!(Credentials::github_app("id", 0, &pem).is_err());
        assert!(Credentials::personal_access_token(" ").is_err());
    }

    #[test]
    fn credentials_debug_hides_secrets() {
        let creds = Credentials::personal_access_token("ghp_secret").unwrap();
        assert!(!format!("{creds:?}").contains("ghp_secret"));
    }

    #[test]
    fn expiry_of_unsigned_token() {
        let payload = URL_SAFE_NO_PAD.encode(br#"{"exp":2000000000}"#);
        let jwt = format!("e30.{payload}.sig");
        assert_eq!(
            unverified_expiry(&jwt),
            Some(UNIX_EPOCH + Duration::from_secs(2_000_000_000))
        );
        assert_eq!(unverified_expiry("nope"), None);
    }
}
