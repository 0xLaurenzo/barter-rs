//! Polymarket CLOB API request/response models.

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

/// Order payload for POST /order.
#[derive(Debug, Clone, Serialize)]
pub struct PolymarketOrderPayload {
    pub order: SignedOrderPayload,
    #[serde(rename = "orderType")]
    pub order_type: String, // "GTC", "FOK", "GTD"
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tick_size: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub neg_risk: Option<bool>,
}

/// The signed order structure sent to the CLOB.
#[derive(Debug, Clone, Serialize)]
pub struct SignedOrderPayload {
    /// Token ID (condition_id + outcome)
    pub token_id: String,
    /// Maker address
    pub maker: String,
    /// Taker address (usually 0x0 for CLOB)
    pub taker: String,
    /// Maker amount in raw units
    pub maker_amount: String,
    /// Taker amount in raw units
    pub taker_amount: String,
    /// Side: 0 = Buy, 1 = Sell
    pub side: u8,
    /// Fee rate bps
    pub fee_rate_bps: String,
    /// Salt for order uniqueness
    pub salt: String,
    /// Nonce for onchain cancellations
    pub nonce: String,
    /// Signature expiration timestamp
    pub expiration: String,
    /// EIP-712 signature
    pub signature: String,
    /// Signature type (0 = EOA)
    pub signature_type: u8,
}

/// Response from POST /order.
#[derive(Debug, Clone, Deserialize)]
pub struct PolymarketOrderResponse {
    #[serde(rename = "orderID")]
    pub order_id: Option<String>,
    pub success: Option<bool>,
    #[serde(rename = "errorMsg")]
    pub error_msg: Option<String>,
    pub status: Option<String>,
}

/// Response from GET /orders.
#[derive(Debug, Clone, Deserialize)]
pub struct PolymarketOrdersResponse(pub Vec<PolymarketOrder>);

/// A Polymarket order.
#[derive(Debug, Clone, Deserialize)]
pub struct PolymarketOrder {
    pub id: String,
    pub status: String,
    pub side: String, // "BUY" or "SELL"
    pub price: String,
    pub original_size: String,
    pub size_matched: String,
    pub outcome: Option<String>,
    pub asset_id: Option<String>,
    pub created_at: Option<String>,
}

/// Response from GET /balance-allowance.
#[derive(Debug, Clone, Deserialize)]
pub struct PolymarketBalanceResponse {
    #[serde(default)]
    pub balance: String,
    #[serde(default)]
    pub allowances: std::collections::HashMap<String, String>,
}

/// Response from POST /auth/api-key or GET /auth/derive-api-key.
#[derive(Debug, Clone, Deserialize)]
pub struct PolymarketApiKeyResponse {
    #[serde(alias = "apiKey")]
    pub api_key: Option<String>,
    pub secret: Option<String>,
    pub passphrase: Option<String>,
}

impl PolymarketOrder {
    pub fn price_decimal(&self) -> Decimal {
        self.price.parse::<Decimal>().unwrap_or(Decimal::ZERO)
    }

    pub fn original_size_decimal(&self) -> Decimal {
        self.original_size.parse::<Decimal>().unwrap_or(Decimal::ZERO)
    }

    pub fn size_matched_decimal(&self) -> Decimal {
        self.size_matched.parse::<Decimal>().unwrap_or(Decimal::ZERO)
    }

    pub fn remaining_size(&self) -> Decimal {
        self.original_size_decimal() - self.size_matched_decimal()
    }

    pub fn is_open(&self) -> bool {
        self.status == "live" || self.status == "open"
    }
}
