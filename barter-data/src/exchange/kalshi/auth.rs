//! Kalshi WebSocket authentication.
//!
//! Kalshi requires RSA-PSS signed authentication headers for WebSocket connections.
//! The signature is computed over: `{timestamp}{method}{path}`
//!
//! Headers required:
//! - `KALSHI-ACCESS-KEY`: API key ID
//! - `KALSHI-ACCESS-SIGNATURE`: Base64-encoded RSA-PSS signature
//! - `KALSHI-ACCESS-TIMESTAMP`: Unix timestamp in milliseconds

use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use rsa::{
    RsaPrivateKey,
    pkcs1::DecodeRsaPrivateKey,
    pss::SigningKey,
    pkcs8::DecodePrivateKey,
    signature::{RandomizedSigner, SignatureEncoding},
};
use sha2::Sha256;
use std::time::{SystemTime, UNIX_EPOCH};
use thiserror::Error;
use tracing::debug;

/// Errors that can occur during Kalshi authentication.
#[derive(Debug, Error)]
pub enum KalshiAuthError {
    #[error("Failed to read private key file: {0}")]
    KeyFileRead(#[from] std::io::Error),

    #[error("Failed to parse RSA private key: {0}")]
    KeyParse(String),

    #[error("Failed to create signing key: {0}")]
    SigningKey(String),

    #[error("Failed to generate signature: {0}")]
    Signature(String),
}

/// Kalshi API credentials for WebSocket authentication.
#[derive(Clone)]
pub struct KalshiCredentials {
    /// API key ID
    pub api_key: String,
    /// RSA private key for signing
    private_key: RsaPrivateKey,
}

impl std::fmt::Debug for KalshiCredentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KalshiCredentials")
            .field("api_key", &self.api_key)
            .field("private_key", &"[REDACTED]")
            .finish()
    }
}

impl KalshiCredentials {
    /// Create credentials from API key and PEM-encoded private key string.
    pub fn from_pem(api_key: impl Into<String>, pem: &str) -> Result<Self, KalshiAuthError> {
        let private_key = RsaPrivateKey::from_pkcs8_pem(pem)
            .or_else(|_| RsaPrivateKey::from_pkcs1_pem(pem))
            .map_err(|e| KalshiAuthError::KeyParse(e.to_string()))?;

        Ok(Self {
            api_key: api_key.into(),
            private_key,
        })
    }

    /// Create credentials from API key and private key file path.
    ///
    /// Handles relative paths by searching:
    /// 1. The path as given (relative to current working directory)
    /// 2. Parent directory (useful when running from subdirectories)
    /// 3. Two levels up (for nested project structures)
    pub fn from_file(
        api_key: impl Into<String>,
        key_path: impl AsRef<std::path::Path>,
    ) -> Result<Self, KalshiAuthError> {
        let path = key_path.as_ref();

        // Try the path as-is first
        if path.exists() {
            let pem = std::fs::read_to_string(path)?;
            return Self::from_pem(api_key, &pem);
        }

        // If relative path, try parent directories
        if path.is_relative() {
            // Try parent directory
            let parent_path = std::path::Path::new("..").join(path);
            if parent_path.exists() {
                let pem = std::fs::read_to_string(&parent_path)?;
                return Self::from_pem(api_key, &pem);
            }

            // Try two levels up
            let grandparent_path = std::path::Path::new("../..").join(path);
            if grandparent_path.exists() {
                let pem = std::fs::read_to_string(&grandparent_path)?;
                return Self::from_pem(api_key, &pem);
            }
        }

        // Fall back to original path for the error message
        let pem = std::fs::read_to_string(path)?;
        Self::from_pem(api_key, &pem)
    }

    /// Create credentials from environment variables.
    ///
    /// Reads:
    /// - `KALSHI_API_KEY`: API key ID
    /// - `KALSHI_PRIVATE_KEY_PATH`: Path to PEM file (preferred)
    /// - `KALSHI_PRIVATE_KEY_PEM`: PEM string (fallback)
    pub fn from_env() -> Result<Self, KalshiAuthError> {
        let api_key = std::env::var("KALSHI_API_KEY")
            .map_err(|_| KalshiAuthError::KeyParse("KALSHI_API_KEY not set".into()))?;

        // Try file path first, then PEM string
        if let Ok(path) = std::env::var("KALSHI_PRIVATE_KEY_PATH") {
            Self::from_file(api_key, path)
        } else if let Ok(pem) = std::env::var("KALSHI_PRIVATE_KEY_PEM") {
            Self::from_pem(api_key, &pem)
        } else {
            Err(KalshiAuthError::KeyParse(
                "Neither KALSHI_PRIVATE_KEY_PATH nor KALSHI_PRIVATE_KEY_PEM is set".into(),
            ))
        }
    }

    /// Generate authentication headers for a WebSocket connection.
    ///
    /// Returns (api_key, signature, timestamp) tuple.
    pub fn generate_ws_auth(&self) -> Result<KalshiAuthHeaders, KalshiAuthError> {
        // Get current timestamp in milliseconds
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards")
            .as_millis() as u64;

        // WebSocket auth uses GET method and /trade-api/ws/v2 path
        let method = "GET";
        let path = "/trade-api/ws/v2";

        // Create message to sign: timestamp + method + path
        let message = format!("{}{}{}", timestamp, method, path);

        debug!("Signing Kalshi WebSocket auth message: {}", message);

        // Sign with RSA-PSS (SHA-256 + MGF1-SHA256)
        let signing_key = SigningKey::<Sha256>::new(self.private_key.clone());
        let mut rng = rsa::rand_core::OsRng;
        let signature = signing_key
            .sign_with_rng(&mut rng, message.as_bytes());

        // Base64 encode the signature
        let signature_b64 = BASE64.encode(signature.to_bytes());

        Ok(KalshiAuthHeaders {
            api_key: self.api_key.clone(),
            signature: signature_b64,
            timestamp: timestamp.to_string(),
        })
    }
}

/// Authentication headers for Kalshi WebSocket connection.
#[derive(Debug, Clone)]
pub struct KalshiAuthHeaders {
    /// API key ID
    pub api_key: String,
    /// Base64-encoded signature
    pub signature: String,
    /// Timestamp in milliseconds as string
    pub timestamp: String,
}

impl KalshiAuthHeaders {
    /// Header name for API key.
    pub const KEY_HEADER: &'static str = "KALSHI-ACCESS-KEY";
    /// Header name for signature.
    pub const SIGNATURE_HEADER: &'static str = "KALSHI-ACCESS-SIGNATURE";
    /// Header name for timestamp.
    pub const TIMESTAMP_HEADER: &'static str = "KALSHI-ACCESS-TIMESTAMP";
}

#[cfg(test)]
mod tests {
    use super::*;

    // Note: Real tests would require valid RSA keys
    // This test just verifies the structure compiles

    #[test]
    fn test_auth_headers_constants() {
        assert_eq!(KalshiAuthHeaders::KEY_HEADER, "KALSHI-ACCESS-KEY");
        assert_eq!(
            KalshiAuthHeaders::SIGNATURE_HEADER,
            "KALSHI-ACCESS-SIGNATURE"
        );
        assert_eq!(
            KalshiAuthHeaders::TIMESTAMP_HEADER,
            "KALSHI-ACCESS-TIMESTAMP"
        );
    }
}
