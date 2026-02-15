//! Example: Subscribe to Kalshi orderbook via WebSocket.
//!
//! This example connects to Kalshi's WebSocket API and streams orderbook updates
//! for a prediction market.
//!
//! Usage:
//!   KALSHI_TICKER=KXBTC-25JAN31-T100000 cargo run --example kalshi_orderbook_stream
//!
//! Note: You need a valid Kalshi market ticker. Find active markets at:
//! https://kalshi.com/markets

use barter_data::{
    exchange::kalshi::Kalshi,
    streams::{reconnect::stream::ReconnectingStream, Streams},
    subscription::book::OrderBooksL2,
};
use barter_instrument::{
    exchange::ExchangeId,
    instrument::market_data::kind::{
        MarketDataInstrumentKind, MarketDataPredictionContract, Outcome,
    },
};
use chrono::{Duration, Utc};
use futures_util::StreamExt;
use tracing::{info, warn};

#[tokio::main]
async fn main() {
    init_logging();

    // Get the Kalshi ticker from environment or use a default
    let ticker = std::env::var("KALSHI_TICKER")
        .unwrap_or_else(|_| "KXBTC-25JAN31-T100000".to_string());

    info!("Subscribing to Kalshi orderbook for ticker: {}", ticker);

    // Build the stream using the 5-tuple format:
    // (Exchange, base (ticker), quote, InstrumentKind, SubKind)
    let streams_result = Streams::<OrderBooksL2>::builder()
        .subscribe([(
            Kalshi,
            ticker.as_str(),
            "USD",
            MarketDataInstrumentKind::Prediction(MarketDataPredictionContract {
                outcome: Outcome::Yes,
                expiry: Utc::now() + Duration::days(30),
            }),
            OrderBooksL2,
        )])
        .init()
        .await;

    let mut streams = match streams_result {
        Ok(s) => s,
        Err(e) => {
            warn!("Failed to initialize Kalshi stream: {:?}", e);
            return;
        }
    };

    // Select the Kalshi stream
    let mut stream = streams
        .select(ExchangeId::Kalshi)
        .expect("Kalshi stream should exist")
        .with_error_handler(|error| warn!(?error, "Kalshi stream error"));

    info!("Connected to Kalshi WebSocket, waiting for orderbook updates...");

    // Process events
    let mut count = 0;
    while let Some(event) = stream.next().await {
        info!("Received orderbook event #{}: {:?}", count + 1, event);
        count += 1;

        // For demo purposes, exit after receiving 5 events
        if count >= 5 {
            info!("Received {} events, exiting demo", count);
            break;
        }
    }
}

fn init_logging() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::filter::EnvFilter::builder()
                .with_default_directive(tracing_subscriber::filter::LevelFilter::INFO.into())
                .from_env_lossy(),
        )
        .with_ansi(cfg!(debug_assertions))
        .init()
}
