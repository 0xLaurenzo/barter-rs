//! Polymarket CLOB REST HTTP client with HMAC authentication.

use super::model::*;
use super::signing::{PolymarketApiCredentials, build_auth_headers, build_l1_auth_headers};
use reqwest::Client;
use tracing::{debug, error, info};

const POLYMARKET_CLOB_BASE: &str = "https://clob.polymarket.com";

/// Polymarket CLOB REST client.
#[derive(Debug, Clone)]
pub struct PolymarketHttpClient {
    client: Client,
    credentials: PolymarketApiCredentials,
    base_url: String,
}

impl PolymarketHttpClient {
    /// Access the API credentials.
    pub fn credentials(&self) -> &PolymarketApiCredentials {
        &self.credentials
    }

    /// Create a new Polymarket HTTP client.
    pub fn new(credentials: PolymarketApiCredentials) -> Self {
        Self {
            client: Client::new(),
            credentials,
            base_url: POLYMARKET_CLOB_BASE.to_string(),
        }
    }

    /// Send an authenticated request.
    ///
    /// `sign_path` is the base path used for HMAC (no query params).
    /// `url_path` is the full path including query params.
    fn authenticated_request(
        &self,
        method: &str,
        sign_path: &str,
        url_path: &str,
        body: &str,
    ) -> reqwest::RequestBuilder {
        let url = format!("{}{}", self.base_url, url_path);
        let headers = build_auth_headers(&self.credentials, method, sign_path, body);

        let mut builder = match method {
            "GET" => self.client.get(&url),
            "POST" => self.client.post(&url),
            "DELETE" => self.client.delete(&url),
            _ => self.client.get(&url),
        };

        for (key, value) in headers {
            builder = builder.header(&key, &value);
        }

        if method == "POST" {
            builder = builder
                .header("Content-Type", "application/json")
                .body(body.to_string());
        }

        builder
    }

    /// Submit a signed order.
    pub async fn submit_order(
        &self,
        payload: &PolymarketOrderPayload,
    ) -> Result<PolymarketOrderResponse, PolymarketHttpError> {
        let body = serde_json::to_string(payload)
            .map_err(|e| PolymarketHttpError::Parse(e.to_string()))?;
        let path = "/order";

        debug!(path = %path, "Submitting Polymarket order");

        let resp = self
            .authenticated_request("POST", path, path, &body)
            .send()
            .await
            .map_err(|e| PolymarketHttpError::Request(e.to_string()))?;

        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            error!(status = %status, body = %body, "Polymarket order submission failed");
            return Err(PolymarketHttpError::Api(format!(
                "Status {}: {}",
                status, body
            )));
        }

        resp.json()
            .await
            .map_err(|e| PolymarketHttpError::Parse(e.to_string()))
    }

    /// Cancel an order by ID.
    pub async fn cancel_order(
        &self,
        order_id: &str,
    ) -> Result<(), PolymarketHttpError> {
        let path = format!("/order/{}", order_id);

        let resp = self
            .authenticated_request("DELETE", &path, &path, "")
            .send()
            .await
            .map_err(|e| PolymarketHttpError::Request(e.to_string()))?;

        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(PolymarketHttpError::Api(format!(
                "Status {}: {}",
                status, body
            )));
        }

        Ok(())
    }

    /// Fetch open orders.
    pub async fn fetch_open_orders(&self) -> Result<Vec<PolymarketOrder>, PolymarketHttpError> {
        let sign_path = "/orders";
        let url_path = "/orders?status=live";

        let resp = self
            .authenticated_request("GET", sign_path, url_path, "")
            .send()
            .await
            .map_err(|e| PolymarketHttpError::Request(e.to_string()))?;

        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(PolymarketHttpError::Api(format!(
                "Status {}: {}",
                status, body
            )));
        }

        let orders: Vec<PolymarketOrder> = resp
            .json()
            .await
            .map_err(|e| PolymarketHttpError::Parse(e.to_string()))?;

        Ok(orders)
    }

    /// Fetch USDC balance and allowance.
    pub async fn fetch_balance(&self) -> Result<PolymarketBalanceResponse, PolymarketHttpError> {
        let sign_path = "/balance-allowance";
        let url_path = "/balance-allowance?asset_type=COLLATERAL&signature_type=0";

        let resp = self
            .authenticated_request("GET", sign_path, url_path, "")
            .send()
            .await
            .map_err(|e| PolymarketHttpError::Request(e.to_string()))?;

        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(PolymarketHttpError::Api(format!(
                "Status {}: {}",
                status, body
            )));
        }

        resp.json()
            .await
            .map_err(|e| PolymarketHttpError::Parse(e.to_string()))
    }

    /// Derive or create API credentials from a private key.
    ///
    /// First attempts POST /auth/api-key (create). If that fails (key already
    /// exists), falls back to GET /auth/derive-api-key.
    pub async fn derive_api_credentials(
        private_key_hex: &str,
    ) -> Result<PolymarketApiCredentials, PolymarketHttpError> {
        let (address, signature, timestamp, nonce) = build_l1_auth_headers(private_key_hex)
            .map_err(|e| PolymarketHttpError::Auth(format!("L1 auth signing failed: {e}")))?;

        let client = Client::new();
        let base = POLYMARKET_CLOB_BASE;

        debug!(
            address = %address,
            signature_len = signature.len(),
            timestamp = %timestamp,
            nonce = %nonce,
            "Polymarket L1 auth headers"
        );

        // Try create first
        let create_resp = client
            .post(format!("{}/auth/api-key", base))
            .header("POLY_ADDRESS", &address)
            .header("POLY_SIGNATURE", &signature)
            .header("POLY_TIMESTAMP", &timestamp)
            .header("POLY_NONCE", &nonce)
            .send()
            .await
            .map_err(|e| PolymarketHttpError::Request(e.to_string()))?;

        let create_status = create_resp.status();
        let create_body: PolymarketApiKeyResponse = create_resp
            .json()
            .await
            .unwrap_or(PolymarketApiKeyResponse {
                api_key: None,
                secret: None,
                passphrase: None,
            });

        if create_status.is_success() && create_body.api_key.is_some() {
            info!("Created new Polymarket API credentials");
            return Ok(PolymarketApiCredentials {
                api_key: create_body.api_key.unwrap(),
                api_secret: create_body.secret.unwrap_or_default(),
                api_passphrase: create_body.passphrase.unwrap_or_default(),
                wallet_address: address.clone(),
            });
        }

        // Fall back to derive (key already exists)
        debug!("Create returned {:?}, trying derive", create_status);

        // Need fresh signature for derive request (timestamp may differ)
        let (address, signature, timestamp, nonce) = build_l1_auth_headers(private_key_hex)
            .map_err(|e| PolymarketHttpError::Auth(format!("L1 auth signing failed: {e}")))?;

        let derive_resp = client
            .get(format!("{}/auth/derive-api-key", base))
            .header("POLY_ADDRESS", &address)
            .header("POLY_SIGNATURE", &signature)
            .header("POLY_TIMESTAMP", &timestamp)
            .header("POLY_NONCE", &nonce)
            .send()
            .await
            .map_err(|e| PolymarketHttpError::Request(e.to_string()))?;

        let derive_status = derive_resp.status();
        if !derive_status.is_success() {
            let body = derive_resp.text().await.unwrap_or_default();
            return Err(PolymarketHttpError::Api(format!(
                "Derive API key failed ({}): {}",
                derive_status, body
            )));
        }

        let derive_body: PolymarketApiKeyResponse = derive_resp
            .json()
            .await
            .map_err(|e| PolymarketHttpError::Parse(format!("Derive response parse: {e}")))?;

        match derive_body.api_key {
            Some(key) => {
                info!("Derived existing Polymarket API credentials");
                Ok(PolymarketApiCredentials {
                    api_key: key,
                    api_secret: derive_body.secret.unwrap_or_default(),
                    api_passphrase: derive_body.passphrase.unwrap_or_default(),
                    wallet_address: address,
                })
            }
            None => Err(PolymarketHttpError::Api(
                "Derive response missing api_key".to_string(),
            )),
        }
    }
}

/// Errors from Polymarket HTTP operations.
#[derive(Debug, Clone, thiserror::Error)]
pub enum PolymarketHttpError {
    #[error("Authentication error: {0}")]
    Auth(String),
    #[error("Request error: {0}")]
    Request(String),
    #[error("API error: {0}")]
    Api(String),
    #[error("Parse error: {0}")]
    Parse(String),
}
