//! Kalshi WebSocket fill monitoring.
//!
//! Connects to the authenticated Kalshi WS endpoint and subscribes to the
//! `fill` channel for real-time trade fill notifications.

use crate::{
    AccountEvent, AccountEventKind, UnindexedAccountEvent,
    order::id::{OrderId, StrategyId},
    trade::{AssetFees, Trade, TradeId},
};
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use barter_instrument::{
    Side,
    asset::QuoteAsset,
    exchange::ExchangeId,
    instrument::name::InstrumentNameExchange,
};
use barter_integration::protocol::websocket::{WebSocket, WsMessage, connect_with_headers};
use chrono::Utc;
use futures::{SinkExt, StreamExt, stream::BoxStream};
use rsa::{RsaPrivateKey, pss::SigningKey, signature::{RandomizedSigner, SignatureEncoding}};
use rust_decimal::Decimal;
use serde::Deserialize;
use sha2::Sha256;
use smol_str::SmolStr;
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::{debug, info, warn};

const KALSHI_WS_URL: &str = "wss://api.elections.kalshi.com/trade-api/ws/v2";
const KALSHI_DEMO_WS_URL: &str = "wss://demo-api.kalshi.co/trade-api/ws/v2";
const KALSHI_WS_SIGN_PATH: &str = "/trade-api/ws/v2";

// ---------------------------------------------------------------------------
// WS message types
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum KalshiWsMessage {
    Fill(KalshiWsFill),
    Subscribed {
        #[allow(dead_code)]
        id: u64,
        #[allow(dead_code)]
        msg: serde_json::Value,
    },
    Error {
        #[allow(dead_code)]
        id: u64,
        #[allow(dead_code)]
        msg: serde_json::Value,
    },
    #[serde(other)]
    Unknown,
}

#[derive(Debug, Clone, Deserialize)]
pub struct KalshiWsFill {
    #[allow(dead_code)]
    pub sid: u64,
    #[allow(dead_code)]
    pub seq: u64,
    pub msg: KalshiWsFillData,
}

#[derive(Debug, Clone, Deserialize)]
pub struct KalshiWsFillData {
    pub trade_id: String,
    pub order_id: String,
    pub ticker: String,
    pub side: String,   // "yes" or "no"
    pub action: String, // "buy" or "sell"
    pub count: u32,
    pub yes_price: u32, // cents
    #[allow(dead_code)]
    pub no_price: u32,  // cents
    #[allow(dead_code)]
    pub created_time: Option<String>,
}

// ---------------------------------------------------------------------------
// WS auth
// ---------------------------------------------------------------------------

/// Generate authentication headers for the Kalshi WebSocket handshake.
///
/// Signs `{timestamp_ms}GET/trade-api/ws/v2` with RSA-PSS (same scheme as HTTP).
pub fn generate_ws_auth_headers(
    api_key: &str,
    private_key: &RsaPrivateKey,
) -> Vec<(&'static str, String)> {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards")
        .as_millis() as u64;

    let message = format!("{}GET{}", timestamp, KALSHI_WS_SIGN_PATH);
    debug!(message = %message, "Signing Kalshi WS handshake");

    let signing_key = SigningKey::<Sha256>::new(private_key.clone());
    let mut rng = rsa::rand_core::OsRng;
    let signature = signing_key.sign_with_rng(&mut rng, message.as_bytes());
    let signature_b64 = BASE64.encode(signature.to_bytes());

    vec![
        ("KALSHI-ACCESS-KEY", api_key.to_string()),
        ("KALSHI-ACCESS-SIGNATURE", signature_b64),
        ("KALSHI-ACCESS-TIMESTAMP", timestamp.to_string()),
    ]
}

// ---------------------------------------------------------------------------
// Connection
// ---------------------------------------------------------------------------

/// Connect to the Kalshi WebSocket and subscribe to the fill channel.
pub async fn connect_kalshi_fills(
    api_key: &str,
    private_key: &RsaPrivateKey,
    demo: bool,
    tickers: &[String],
) -> Result<WebSocket, barter_integration::error::SocketError> {
    let ws_url_str = if demo { KALSHI_DEMO_WS_URL } else { KALSHI_WS_URL };
    let url: url::Url = ws_url_str
        .parse()
        .map_err(|e: url::ParseError| barter_integration::error::SocketError::UrlParse(e))?;

    let headers = generate_ws_auth_headers(api_key, private_key);
    let mut ws = connect_with_headers(url, headers).await?;
    info!("Connected to Kalshi fill WebSocket at {}", ws_url_str);

    // Subscribe to fill channel
    let sub_msg = serde_json::json!({
        "id": 1,
        "cmd": "subscribe",
        "params": {
            "channels": ["fill"],
            "market_tickers": tickers,
        }
    });
    ws.send(WsMessage::text(sub_msg.to_string()))
        .await
        .map_err(|e| barter_integration::error::SocketError::WebSocket(Box::new(e)))?;
    debug!(tickers = ?tickers, "Subscribed to Kalshi fill channel");

    Ok(ws)
}

// ---------------------------------------------------------------------------
// Stream conversion
// ---------------------------------------------------------------------------

/// Convert a Kalshi WebSocket into a stream of `AccountEvent::Trade` events.
pub fn kalshi_fill_stream(ws: WebSocket) -> BoxStream<'static, UnindexedAccountEvent> {
    let (_sink, stream) = ws.split();

    let mapped = stream.filter_map(|result| async move {
        let msg = match result {
            Ok(m) => m,
            Err(e) => {
                warn!(error = %e, "Kalshi fill WS read error");
                return None;
            }
        };

        let text = match msg {
            WsMessage::Text(t) => t,
            WsMessage::Ping(_) | WsMessage::Pong(_) => return None,
            WsMessage::Close(_) => {
                warn!("Kalshi fill WS closed by server");
                return None;
            }
            _ => return None,
        };

        let parsed: KalshiWsMessage = match serde_json::from_str(&text) {
            Ok(m) => m,
            Err(e) => {
                debug!(error = %e, payload = %text, "Failed to parse Kalshi WS message");
                return None;
            }
        };

        match parsed {
            KalshiWsMessage::Fill(fill) => {
                let data = &fill.msg;
                let instrument_name = format!("{}_{}", data.ticker, data.side);
                let side = match data.action.as_str() {
                    "buy" => Side::Buy,
                    _ => Side::Sell,
                };
                let price = Decimal::from(data.yes_price) / Decimal::from(100);

                info!(
                    trade_id = %data.trade_id,
                    ticker = %data.ticker,
                    side = %data.side,
                    action = %data.action,
                    count = data.count,
                    price = %price,
                    "Kalshi trade fill received via WS"
                );

                Some(AccountEvent {
                    exchange: ExchangeId::Kalshi,
                    kind: AccountEventKind::Trade(Trade {
                        id: TradeId(SmolStr::new(&data.trade_id)),
                        order_id: OrderId(SmolStr::new(&data.order_id)),
                        instrument: InstrumentNameExchange::from(instrument_name.as_str()),
                        strategy: StrategyId::new("unknown"),
                        time_exchange: Utc::now(),
                        side,
                        price,
                        quantity: Decimal::from(data.count),
                        fees: AssetFees::new(QuoteAsset, Decimal::ZERO),
                    }),
                })
            }
            KalshiWsMessage::Subscribed { .. } => {
                debug!("Kalshi fill WS subscription confirmed");
                None
            }
            KalshiWsMessage::Error { msg, .. } => {
                warn!(error = ?msg, "Kalshi fill WS error message");
                None
            }
            KalshiWsMessage::Unknown => None,
        }
    });

    Box::pin(mapped)
}
