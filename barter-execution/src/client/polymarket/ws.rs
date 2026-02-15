//! Polymarket WebSocket fill monitoring.
//!
//! Connects to the authenticated Polymarket user WS channel for real-time
//! trade fill notifications. Requires periodic PING keepalive (10s).

use crate::{
    AccountEvent, AccountEventKind, UnindexedAccountEvent,
    order::id::{OrderId, StrategyId},
    trade::{AssetFees, Trade, TradeId},
};
use barter_instrument::{
    Side,
    asset::QuoteAsset,
    exchange::ExchangeId,
    instrument::name::InstrumentNameExchange,
};
use barter_integration::protocol::websocket::{WebSocket, WsMessage, connect};
use chrono::Utc;
use futures::{SinkExt, StreamExt, stream::BoxStream};
use rust_decimal::Decimal;
use serde::Deserialize;
use smol_str::SmolStr;
use std::str::FromStr;
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};

const POLYMARKET_USER_WS_URL: &str = "wss://ws-subscriptions-clob.polymarket.com/ws/user";
const PING_INTERVAL_SECS: u64 = 10;

// ---------------------------------------------------------------------------
// WS message types
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Deserialize)]
pub struct PolymarketUserEnvelope {
    #[serde(default)]
    pub event_type: Option<String>,
    #[serde(flatten)]
    pub data: serde_json::Value,
}

#[derive(Debug, Clone, Deserialize)]
pub struct PolymarketWsTrade {
    pub id: Option<String>,
    pub status: Option<String>,
    pub side: Option<String>,
    pub asset_id: Option<String>,
    #[allow(dead_code)]
    pub taker_order_id: Option<String>,
    pub maker_orders: Option<Vec<PolymarketWsMakerOrder>>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct PolymarketWsMakerOrder {
    #[allow(dead_code)]
    pub order_id: Option<String>,
    pub matched_amount: Option<String>,
    pub price: Option<String>,
}

// ---------------------------------------------------------------------------
// Connection + Auth
// ---------------------------------------------------------------------------

/// Connect to the Polymarket user WebSocket and authenticate.
///
/// Authentication is done by sending a JSON message after connecting (not via
/// headers). The `markets` slice contains condition IDs (hex `0x...` strings)
/// to subscribe to. The server requires at least one market to keep the
/// connection alive; pass an empty slice only for quick connectivity tests.
pub async fn connect_polymarket_user(
    api_key: &str,
    api_secret: &str,
    api_passphrase: &str,
    markets: &[String],
) -> Result<WebSocket, barter_integration::error::SocketError> {
    let url: url::Url = POLYMARKET_USER_WS_URL
        .parse()
        .map_err(|e: url::ParseError| barter_integration::error::SocketError::UrlParse(e))?;

    let mut ws = connect(url).await?;
    info!("Connected to Polymarket user WebSocket");

    // Send auth + subscribe message
    let auth_msg = serde_json::json!({
        "auth": {
            "apiKey": api_key,
            "secret": api_secret,
            "passphrase": api_passphrase,
        },
        "type": "user",
        "markets": markets,
    });
    ws.send(WsMessage::text(auth_msg.to_string()))
        .await
        .map_err(|e| barter_integration::error::SocketError::WebSocket(Box::new(e)))?;
    debug!(market_count = markets.len(), "Sent Polymarket user WS auth message");

    Ok(ws)
}

// ---------------------------------------------------------------------------
// Keepalive ping
// ---------------------------------------------------------------------------

/// Spawn a background task that sends PING keepalive messages on the WS sink.
///
/// Returns the `JoinHandle` so the caller can manage its lifetime.
pub fn spawn_ping_task(
    mut sink: futures::stream::SplitSink<WebSocket, WsMessage>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval =
            tokio::time::interval(std::time::Duration::from_secs(PING_INTERVAL_SECS));
        // Skip the immediate first tick so we don't PING before the server
        // has processed the auth message.
        interval.tick().await;
        loop {
            interval.tick().await;
            if let Err(e) = sink.send(WsMessage::text("PING".to_string())).await {
                warn!(error = %e, "Polymarket user WS ping failed, stopping keepalive");
                break;
            }
        }
    })
}

// ---------------------------------------------------------------------------
// Stream conversion
// ---------------------------------------------------------------------------

/// Convert a Polymarket user WebSocket into a stream of `AccountEvent::Trade`
/// events plus a ping keepalive handle.
///
/// Filters for trade events with `status == "MATCHED"` (fastest fill signal).
/// Sums `maker_orders[].matched_amount` for total quantity, computes weighted
/// average price.
pub fn polymarket_fill_stream(
    ws: WebSocket,
) -> (BoxStream<'static, UnindexedAccountEvent>, JoinHandle<()>) {
    let (sink, stream) = ws.split();

    let ping_handle = spawn_ping_task(sink);

    let mapped = stream.filter_map(|result| async move {
        let msg = match result {
            Ok(m) => m,
            Err(e) => {
                warn!(error = %e, "Polymarket user WS read error");
                return None;
            }
        };

        let text = match msg {
            WsMessage::Text(t) => t,
            WsMessage::Ping(_) | WsMessage::Pong(_) => return None,
            WsMessage::Close(_) => {
                warn!("Polymarket user WS closed by server");
                return None;
            }
            _ => return None,
        };

        let trimmed = text.trim();
        if trimmed.eq_ignore_ascii_case("PONG") || trimmed.eq_ignore_ascii_case("PING") {
            return None;
        }

        let envelope: PolymarketUserEnvelope = match serde_json::from_str(trimmed) {
            Ok(e) => e,
            Err(e) => {
                debug!(error = %e, payload = %trimmed, "Failed to parse Polymarket user WS message");
                return None;
            }
        };

        let event_type = envelope.event_type.as_deref().unwrap_or("");
        if event_type != "trade" {
            debug!(event_type = %event_type, "Ignoring non-trade Polymarket user WS event");
            return None;
        }

        let trade: PolymarketWsTrade = match serde_json::from_value(envelope.data) {
            Ok(t) => t,
            Err(e) => {
                debug!(error = %e, "Failed to parse Polymarket trade payload");
                return None;
            }
        };

        // Only process MATCHED fills (fastest signal)
        let status = trade.status.as_deref().unwrap_or("");
        if status != "MATCHED" {
            debug!(status = %status, "Skipping non-MATCHED Polymarket trade");
            return None;
        }

        let asset_id = trade.asset_id.as_deref().unwrap_or("unknown");
        let trade_id = trade.id.as_deref().unwrap_or("unknown");

        let side = match trade.side.as_deref() {
            Some("BUY") => Side::Buy,
            _ => Side::Sell,
        };

        // Sum maker_orders matched amounts and compute weighted average price
        let maker_orders = trade.maker_orders.as_deref().unwrap_or(&[]);
        let mut total_quantity = Decimal::ZERO;
        let mut total_cost = Decimal::ZERO;

        for mo in maker_orders {
            let qty = mo
                .matched_amount
                .as_deref()
                .and_then(|s| Decimal::from_str(s).ok())
                .unwrap_or(Decimal::ZERO);
            let px = mo
                .price
                .as_deref()
                .and_then(|s| Decimal::from_str(s).ok())
                .unwrap_or(Decimal::ZERO);
            total_quantity += qty;
            total_cost += qty * px;
        }

        let avg_price = if total_quantity > Decimal::ZERO {
            total_cost / total_quantity
        } else {
            Decimal::ZERO
        };

        info!(
            trade_id = %trade_id,
            asset_id = %asset_id,
            side = ?side,
            quantity = %total_quantity,
            avg_price = %avg_price,
            "Polymarket trade fill received via WS"
        );

        Some(AccountEvent {
            exchange: ExchangeId::Polymarket,
            kind: AccountEventKind::Trade(Trade {
                id: TradeId(SmolStr::new(trade_id)),
                order_id: OrderId(SmolStr::new(
                    trade.taker_order_id.as_deref().unwrap_or("unknown"),
                )),
                instrument: InstrumentNameExchange::from(asset_id),
                strategy: StrategyId::new("unknown"),
                time_exchange: Utc::now(),
                side,
                price: avg_price,
                quantity: total_quantity,
                fees: AssetFees::new(QuoteAsset, Decimal::ZERO),
            }),
        })
    });

    (Box::pin(mapped), ping_handle)
}
