//! Kalshi API request/response models for Trade API v2.

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

/// Request body for POST /portfolio/orders.
#[derive(Debug, Clone, Serialize)]
pub struct KalshiCreateOrder {
    pub ticker: String,
    pub action: String,   // "buy" or "sell"
    pub side: String,      // "yes" or "no"
    #[serde(rename = "type")]
    pub order_type: String, // "limit" or "market"
    pub count: u32,
    /// Price in cents (1-99)
    pub yes_price: Option<u32>,
    pub no_price: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expiration_ts: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sell_position_floor: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub buy_max_cost: Option<u32>,
}

/// Response from POST /portfolio/orders.
#[derive(Debug, Clone, Deserialize)]
pub struct KalshiOrderResponse {
    pub order: KalshiOrder,
}

/// A Kalshi order.
#[derive(Debug, Clone, Deserialize)]
pub struct KalshiOrder {
    pub order_id: String,
    pub ticker: String,
    pub status: String,
    pub action: String,
    pub side: String,
    #[serde(rename = "type")]
    pub order_type: String,
    pub yes_price: Option<u32>,
    pub no_price: Option<u32>,
    pub count: Option<u32>,
    pub remaining_count: Option<u32>,
    pub created_time: Option<String>,
    pub expiration_time: Option<String>,
}

/// Response from GET /portfolio/orders.
#[derive(Debug, Clone, Deserialize)]
pub struct KalshiOrdersResponse {
    pub orders: Vec<KalshiOrder>,
    pub cursor: Option<String>,
}

/// Response from GET /portfolio/balance.
#[derive(Debug, Clone, Deserialize)]
pub struct KalshiBalanceResponse {
    pub balance: i64, // cents
}

/// Response from GET /portfolio/fills.
#[derive(Debug, Clone, Deserialize)]
pub struct KalshiFillsResponse {
    pub fills: Vec<KalshiFill>,
    pub cursor: Option<String>,
}

/// A Kalshi fill (trade execution).
#[derive(Debug, Clone, Deserialize)]
pub struct KalshiFill {
    pub trade_id: String,
    pub order_id: String,
    pub ticker: String,
    pub side: String,
    pub action: String,
    pub count: u32,
    pub yes_price: u32,
    pub no_price: u32,
    pub created_time: String,
}

/// Response from DELETE /portfolio/orders/{id}.
#[derive(Debug, Clone, Deserialize)]
pub struct KalshiCancelResponse {
    pub order: KalshiOrder,
    pub reduced_by: Option<u32>,
}

impl KalshiOrder {
    /// Price in decimal (0-1) from the yes_price cents field.
    pub fn price_decimal(&self) -> Option<Decimal> {
        self.yes_price.map(|p| Decimal::from(p) / Decimal::from(100))
    }

    /// Filled count = original count - remaining count.
    pub fn filled_count(&self) -> u32 {
        let total = self.count.unwrap_or(0);
        let remaining = self.remaining_count.unwrap_or(0);
        total.saturating_sub(remaining)
    }

    pub fn is_open(&self) -> bool {
        self.status == "resting"
    }
}
