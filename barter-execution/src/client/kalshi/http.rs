//! Kalshi REST HTTP client with RSA signature authentication.
//!
//! Ported from barter-data/src/exchange/kalshi/auth.rs for Trade API v2.

use super::model::*;
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use reqwest::Client;
use rsa::{RsaPrivateKey, pkcs1::DecodeRsaPrivateKey, pss::SigningKey, pkcs8::DecodePrivateKey, signature::{RandomizedSigner, SignatureEncoding}};
use sha2::Sha256;
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::{debug, error};

const KALSHI_API_BASE: &str = "https://api.elections.kalshi.com/trade-api/v2";
const KALSHI_DEMO_API_BASE: &str = "https://demo-api.kalshi.co/trade-api/v2";

/// Kalshi REST client with RSA-signed authentication.
#[derive(Clone)]
pub struct KalshiHttpClient {
    client: Client,
    api_key: String,
    private_key: RsaPrivateKey,
    base_url: String,
}

impl std::fmt::Debug for KalshiHttpClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KalshiHttpClient")
            .field("api_key", &self.api_key)
            .field("base_url", &self.base_url)
            .finish()
    }
}

/// Configuration for the Kalshi HTTP client.
#[derive(Debug, Clone)]
pub struct KalshiHttpConfig {
    pub api_key: String,
    pub private_key_pem: String,
    pub demo: bool,
}

impl KalshiHttpClient {
    /// Create a new Kalshi HTTP client.
    pub fn new(config: KalshiHttpConfig) -> Result<Self, KalshiHttpError> {
        let private_key = RsaPrivateKey::from_pkcs8_pem(&config.private_key_pem)
            .or_else(|_| RsaPrivateKey::from_pkcs1_pem(&config.private_key_pem))
            .map_err(|e| KalshiHttpError::Auth(format!("Failed to parse RSA key: {}", e)))?;

        let base_url = if config.demo {
            KALSHI_DEMO_API_BASE
        } else {
            KALSHI_API_BASE
        };

        Ok(Self {
            client: Client::new(),
            api_key: config.api_key,
            private_key,
            base_url: base_url.to_string(),
        })
    }

    /// Generate auth headers for a request.
    fn sign_request(&self, method: &str, path: &str) -> (String, String, String) {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards")
            .as_millis() as u64;

        let message = format!("{}{}{}", timestamp, method, path);
        debug!(message = %message, "Signing Kalshi request");

        let signing_key = SigningKey::<Sha256>::new(self.private_key.clone());
        let mut rng = rsa::rand_core::OsRng;
        let signature = signing_key.sign_with_rng(&mut rng, message.as_bytes());
        let signature_b64 = BASE64.encode(signature.to_bytes());

        (self.api_key.clone(), signature_b64, timestamp.to_string())
    }

    /// Add authentication headers to a request builder.
    fn authenticated_request(
        &self,
        method: &str,
        path: &str,
    ) -> reqwest::RequestBuilder {
        // Signature must include full path (e.g. /trade-api/v2/portfolio/balance)
        let full_path = format!("/trade-api/v2{}", path);
        let (api_key, signature, timestamp) = self.sign_request(method, &full_path);
        let url = format!("{}{}", self.base_url, path);

        let builder = match method {
            "GET" => self.client.get(&url),
            "POST" => self.client.post(&url),
            "DELETE" => self.client.delete(&url),
            _ => self.client.get(&url),
        };

        builder
            .header("KALSHI-ACCESS-KEY", api_key)
            .header("KALSHI-ACCESS-SIGNATURE", signature)
            .header("KALSHI-ACCESS-TIMESTAMP", timestamp)
            .header("Content-Type", "application/json")
    }

    /// Create an order.
    pub async fn create_order(
        &self,
        order: &KalshiCreateOrder,
    ) -> Result<KalshiOrderResponse, KalshiHttpError> {
        let path = "/portfolio/orders";
        let resp = self
            .authenticated_request("POST", path)
            .json(order)
            .send()
            .await
            .map_err(|e| KalshiHttpError::Request(e.to_string()))?;

        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            error!(status = %status, body = %body, "Kalshi create order failed");
            return Err(KalshiHttpError::Api(format!(
                "Status {}: {}",
                status, body
            )));
        }

        resp.json()
            .await
            .map_err(|e| KalshiHttpError::Parse(e.to_string()))
    }

    /// Cancel an order by ID.
    pub async fn cancel_order(
        &self,
        order_id: &str,
    ) -> Result<KalshiCancelResponse, KalshiHttpError> {
        let path = format!("/portfolio/orders/{}", order_id);
        let resp = self
            .authenticated_request("DELETE", &path)
            .send()
            .await
            .map_err(|e| KalshiHttpError::Request(e.to_string()))?;

        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(KalshiHttpError::Api(format!(
                "Status {}: {}",
                status, body
            )));
        }

        resp.json()
            .await
            .map_err(|e| KalshiHttpError::Parse(e.to_string()))
    }

    /// Fetch open orders.
    pub async fn fetch_open_orders(&self) -> Result<Vec<KalshiOrder>, KalshiHttpError> {
        let path = "/portfolio/orders?status=resting";
        let resp = self
            .authenticated_request("GET", path)
            .send()
            .await
            .map_err(|e| KalshiHttpError::Request(e.to_string()))?;

        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(KalshiHttpError::Api(format!(
                "Status {}: {}",
                status, body
            )));
        }

        let response: KalshiOrdersResponse = resp
            .json()
            .await
            .map_err(|e| KalshiHttpError::Parse(e.to_string()))?;

        Ok(response.orders)
    }

    /// Fetch account balance.
    pub async fn fetch_balance(&self) -> Result<KalshiBalanceResponse, KalshiHttpError> {
        let path = "/portfolio/balance";
        let resp = self
            .authenticated_request("GET", path)
            .send()
            .await
            .map_err(|e| KalshiHttpError::Request(e.to_string()))?;

        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(KalshiHttpError::Api(format!(
                "Status {}: {}",
                status, body
            )));
        }

        resp.json()
            .await
            .map_err(|e| KalshiHttpError::Parse(e.to_string()))
    }

    /// Fetch fills (trades) since a given timestamp.
    pub async fn fetch_fills(
        &self,
        since: Option<&str>,
    ) -> Result<Vec<KalshiFill>, KalshiHttpError> {
        let path = match since {
            Some(ts) => format!("/portfolio/fills?min_ts={}", ts),
            None => "/portfolio/fills".to_string(),
        };

        let resp = self
            .authenticated_request("GET", &path)
            .send()
            .await
            .map_err(|e| KalshiHttpError::Request(e.to_string()))?;

        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(KalshiHttpError::Api(format!(
                "Status {}: {}",
                status, body
            )));
        }

        let response: KalshiFillsResponse = resp
            .json()
            .await
            .map_err(|e| KalshiHttpError::Parse(e.to_string()))?;

        Ok(response.fills)
    }
}

/// Errors from Kalshi HTTP operations.
#[derive(Debug, Clone, thiserror::Error)]
pub enum KalshiHttpError {
    #[error("Authentication error: {0}")]
    Auth(String),
    #[error("Request error: {0}")]
    Request(String),
    #[error("API error: {0}")]
    Api(String),
    #[error("Parse error: {0}")]
    Parse(String),
}
