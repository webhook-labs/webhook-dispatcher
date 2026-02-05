use std::time::{SystemTime, UNIX_EPOCH};

use hmac::{Hmac, Mac};
use sha2::Sha256;

use crate::types::{Endpoint, Event};

#[allow(dead_code)]
pub struct SignatureHeaders {
    pub signature_header: Option<(String, String)>,
    pub timestamp_header: Option<(String, String)>,
}

pub fn build_signature_headers(endpoint: &Endpoint, event: &Event) -> SignatureHeaders {
    let Some(secret) = endpoint.secret.as_ref() else {
        return SignatureHeaders {
            signature_header: None,
            timestamp_header: None,
        };
    };

    let timestamp = if endpoint.include_timestamp {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs()
            .to_string()
    } else {
        String::new()
    };

    let signature = compute_signature(
        secret,
        &event.payload,
        if endpoint.include_timestamp {
            Some(&timestamp)
        } else {
            None
        },
    );

    SignatureHeaders {
        signature_header: Some((endpoint.signature_header.clone(), signature)),
        timestamp_header: if endpoint.include_timestamp {
            Some((endpoint.timestamp_header.clone(), timestamp))
        } else {
            None
        },
    }
}

/// Compute HMAC signature used for webhook delivery.
pub fn compute_signature(secret: &[u8], payload: &[u8], timestamp: Option<&str>) -> String {
    let data = if let Some(ts) = timestamp {
        [ts.as_bytes(), payload].concat()
    } else {
        payload.to_vec()
    };

    let mut mac = Hmac::<Sha256>::new_from_slice(secret)
        .unwrap_or_else(|_| Hmac::<Sha256>::new_from_slice(b"default").expect("hmac"));
    mac.update(&data);
    hex::encode(mac.finalize().into_bytes())
}

/// Verify a received signature with optional timestamp.
pub fn verify_signature(secret: &[u8], payload: &[u8], timestamp: Option<&str>, signature_hex: &str) -> bool {
    let data = if let Some(ts) = timestamp {
        [ts.as_bytes(), payload].concat()
    } else {
        payload.to_vec()
    };

    let Ok(signature) = hex::decode(signature_hex) else {
        return false;
    };

    let mut mac = Hmac::<Sha256>::new_from_slice(secret)
        .unwrap_or_else(|_| Hmac::<Sha256>::new_from_slice(b"default").expect("hmac"));
    mac.update(&data);

    mac.verify_slice(&signature).is_ok()
}

/// Basic timestamp freshness check for receivers.
pub fn is_timestamp_fresh(timestamp_secs: u64, now_secs: u64, max_age_secs: u64) -> bool {
    if now_secs >= timestamp_secs {
        now_secs - timestamp_secs <= max_age_secs
    } else {
        false
    }
}

#[derive(Debug, Clone)]
pub struct ParsedSignature {
    pub signature: Option<String>,
    pub timestamp: Option<String>,
}

/// Parse signature and timestamp headers from a list of headers.
pub fn parse_signature_headers<'a, I>(
    headers: I,
    signature_header: &str,
    timestamp_header: &str,
) -> ParsedSignature
where
    I: IntoIterator<Item = (&'a str, &'a str)>,
{
    let sig_key = signature_header.to_ascii_lowercase();
    let ts_key = timestamp_header.to_ascii_lowercase();

    let mut signature = None;
    let mut timestamp = None;

    for (name, value) in headers {
        let key = name.to_ascii_lowercase();
        if key == sig_key {
            signature = Some(value.to_string());
        } else if key == ts_key {
            timestamp = Some(value.to_string());
        }
    }

    ParsedSignature { signature, timestamp }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VerificationError {
    MissingSignature,
    MissingTimestamp,
    InvalidTimestamp,
    StaleTimestamp,
    InvalidSignature,
}

/// Verify an incoming webhook request in one call.
pub fn verify_webhook_request<'a, I>(
    headers: I,
    payload: &[u8],
    secret: &[u8],
    signature_header: &str,
    timestamp_header: &str,
    max_age_secs: u64,
    now_secs: u64,
) -> Result<(), VerificationError>
where
    I: IntoIterator<Item = (&'a str, &'a str)>,
{
    let parsed = parse_signature_headers(headers, signature_header, timestamp_header);
    let signature = parsed.signature.ok_or(VerificationError::MissingSignature)?;
    let timestamp_str = parsed.timestamp.ok_or(VerificationError::MissingTimestamp)?;
    let timestamp = timestamp_str.parse::<u64>().map_err(|_| VerificationError::InvalidTimestamp)?;

    if !is_timestamp_fresh(timestamp, now_secs, max_age_secs) {
        return Err(VerificationError::StaleTimestamp);
    }

    let ok = verify_signature(secret, payload, Some(&timestamp_str), &signature);
    if ok {
        Ok(())
    } else {
        Err(VerificationError::InvalidSignature)
    }
}
