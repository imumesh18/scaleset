//! Shared helpers for the integration tests.

// Each test crate uses only some of these.
#![allow(dead_code)]

pub mod server;

use std::time::Duration;

use scaleset::RetryPolicy;

/// Retries that wait milliseconds instead of seconds.
pub fn fast_retries() -> RetryPolicy {
    RetryPolicy::default()
        .min_wait(Duration::from_millis(1))
        .max_wait(Duration::from_millis(5))
}

/// A fresh RSA private key in PKCS#8 PEM, for GitHub App credentials.
pub fn test_key_pem() -> String {
    use aws_lc_rs::encoding::AsDer;
    use base64::Engine;
    let key = aws_lc_rs::rsa::KeyPair::generate(aws_lc_rs::rsa::KeySize::Rsa2048).unwrap();
    let der = key.as_der().unwrap();
    let b64 = base64::engine::general_purpose::STANDARD.encode(der.as_ref());
    let mut pem = String::from("-----BEGIN PRIVATE KEY-----\n");
    for chunk in b64.as_bytes().chunks(64) {
        pem.push_str(std::str::from_utf8(chunk).unwrap());
        pem.push('\n');
    }
    pem + "-----END PRIVATE KEY-----\n"
}
