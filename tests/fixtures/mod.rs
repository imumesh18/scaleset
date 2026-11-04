//! Shared helpers for the integration tests.

#![allow(dead_code, reason = "each test crate uses only some of these")]

pub(crate) mod server;

use std::time::Duration;

use scaleset::RetryPolicy;

/// Retries that wait milliseconds instead of seconds.
pub(crate) fn fast_retries() -> RetryPolicy {
    RetryPolicy::default()
        .min_wait(Duration::from_millis(1))
        .max_wait(Duration::from_millis(5))
}

/// A fresh RSA private key in PKCS#8 PEM, for GitHub App credentials.
pub(crate) fn test_key_pem() -> String {
    use aws_lc_rs::encoding::AsDer;
    use base64::Engine;
    let key = aws_lc_rs::rsa::KeyPair::generate(aws_lc_rs::rsa::KeySize::Rsa2048)
        .expect("RSA key generation");
    let der = key.as_der().expect("RSA key encodes to DER");
    let b64 = base64::engine::general_purpose::STANDARD.encode(der.as_ref());
    let mut pem = String::from("-----BEGIN PRIVATE KEY-----\n");
    for chunk in b64.as_bytes().chunks(64) {
        pem.push_str(std::str::from_utf8(chunk).expect("base64 is ASCII"));
        pem.push('\n');
    }
    pem.push_str("-----END PRIVATE KEY-----\n");
    pem
}
