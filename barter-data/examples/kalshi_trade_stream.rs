//! Example: Subscribe to Kalshi public trade stream via WebSocket.
//!
//! Streams real-time trade executions for a prediction market ticker.
//!
//! Usage:
//!   KALSHI_TICKER=KXBTC-25JAN31-T100000 cargo run -p barter-data --example kalshi_trade_stream
//!
//! Note: Requires a valid Kalshi market ticker. Find active markets at:
//! https://kalshi.com/markets

use barter_data::{
    exchange::kalshi::Kalshi,
    streams::{reconnect::stream::ReconnectingStream, Streams},
    subscription::trade::PublicTrades,
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

fn load_dotenv() {
    for path in &[".env", "../.env", "../../.env"] {
        if std::path::Path::new(path).exists() {
            if let Ok(contents) = std::fs::read_to_string(path) {
                for line in contents.lines() {
                    let line = line.trim();
                    if line.is_empty() || line.starts_with('#') {
                        continue;
                    }
                    if let Some((key, value)) = line.split_once('=') {
                        let key = key.trim();
                        let value = value.trim().trim_matches('"').trim_matches('\'');
                        if std::env::var(key).is_err() {
                            unsafe {
                                std::env::set_var(key, value);
                            }
                        }
                    }
                }
                break;
            }
        }
    }
}

#[tokio::main]
async fn main() {
    init_logging();
    load_dotenv();

    let ticker = std::env::var("KALSHI_TICKER")
        .unwrap_or_else(|_| "KXBTC-25JAN31-T100000".to_string());

    info!("Subscribing to Kalshi trade stream for ticker: {}", ticker);

    let streams = Streams::<PublicTrades>::builder()
        .subscribe([(
            Kalshi,
            ticker.as_str(),
            "USD",
            MarketDataInstrumentKind::Prediction(MarketDataPredictionContract {
                outcome: Outcome::Yes,
                expiry: Utc::now() + Duration::days(30),
            }),
            PublicTrades,
        )])
        .init()
        .await;

    let mut streams = match streams {
        Ok(s) => s,
        Err(e) => {
            warn!("Failed to initialize Kalshi trade stream: {:?}", e);
            return;
        }
    };

    let mut stream = streams
        .select(ExchangeId::Kalshi)
        .expect("Kalshi stream should exist")
        .with_error_handler(|error| warn!(?error, "Kalshi trade stream error"));

    info!("Connected. Waiting for trades...");

    let mut count = 0u64;
    while let Some(event) = stream.next().await {
        count += 1;
        info!("[trade #{}] {:?}", count, event);
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
