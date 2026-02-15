//! Example: Query correlated markets from database and stream orderbooks.
//!
//! This example demonstrates the full workflow:
//! 1. Query correlated market pairs from Supabase
//! 2. Subscribe to Kalshi and Polymarket orderbook streams
//! 3. Monitor for arbitrage opportunities
//!
//! Usage:
//!   # Set environment variables
//!   export SUPABASE_URL=https://your-project.supabase.co
//!   export SUPABASE_SERVICE_KEY=your-service-key
//!
//!   # Run the example
//!   cargo run -p barter-arb-strategy --example stream_correlated_markets
//!
//! Optional environment variables:
//!   MIN_SIMILARITY=0.85    # Minimum similarity score (default: 0.85)
//!   MIN_CONFIDENCE=0.85    # Minimum confidence score (default: 0.85)
//!   MAX_PAIRS=5            # Maximum pairs to stream (default: 5)

use barter_arb_strategy::{DatabaseQuerier, MarketPairFilters};
use barter_data::{
    exchange::{kalshi::Kalshi, polymarket::Polymarket},
    streams::Streams,
    subscription::book::OrderBooksL2,
};
use barter_instrument::instrument::market_data::kind::{
    MarketDataInstrumentKind, MarketDataPredictionContract, Outcome,
};
use futures_util::StreamExt;
use rust_decimal::Decimal;
use tracing::{error, info, warn};

#[tokio::main]
async fn main() {
    init_logging();
    load_dotenv();

    // Step 1: Query correlated markets from database
    info!("Querying correlated markets from database...");

    let db = match DatabaseQuerier::from_env() {
        Ok(db) => db,
        Err(e) => {
            error!("Failed to initialize database querier: {}", e);
            error!("Make sure SUPABASE_URL and SUPABASE_SERVICE_KEY are set");
            return;
        }
    };

    // Parse filter settings from environment
    let min_similarity = std::env::var("MIN_SIMILARITY")
        .ok()
        .and_then(|s| s.parse::<Decimal>().ok())
        .unwrap_or_else(|| Decimal::new(85, 2));

    let min_confidence = std::env::var("MIN_CONFIDENCE")
        .ok()
        .and_then(|s| s.parse::<Decimal>().ok())
        .unwrap_or_else(|| Decimal::new(85, 2));

    let max_pairs: u32 = std::env::var("MAX_PAIRS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(5);

    let filters = MarketPairFilters {
        min_similarity: Some(min_similarity),
        min_confidence: Some(min_confidence),
        limit: Some(max_pairs),
        valid_only: Some(true),
        ..Default::default()
    };

    info!(
        "Fetching pairs with similarity >= {}, confidence >= {}, limit = {}",
        min_similarity, min_confidence, max_pairs
    );

    let pairs = match db.get_correlated_pairs(filters).await {
        Ok(pairs) => pairs,
        Err(e) => {
            error!("Failed to fetch market pairs: {}", e);
            return;
        }
    };

    if pairs.is_empty() {
        warn!("No correlated market pairs found matching criteria");
        return;
    }

    info!("Found {} correlated market pairs:", pairs.len());
    for (i, pair) in pairs.iter().enumerate() {
        info!(
            "  {}. {} <-> {} ({})",
            i + 1,
            pair.kalshi_ticker,
            &pair.polymarket_condition_id[..16.min(pair.polymarket_condition_id.len())],
            if pair.description.len() > 50 {
                format!("{}...", &pair.description[..50])
            } else {
                pair.description.clone()
            }
        );
    }

    // Step 2: Build subscriptions for both exchanges
    info!("\nBuilding orderbook subscriptions...");

    let kalshi_subs: Vec<_> = pairs
        .iter()
        .map(|pair| {
            (
                Kalshi,
                pair.kalshi_ticker.as_str(),
                "USD",
                MarketDataInstrumentKind::Prediction(MarketDataPredictionContract {
                    outcome: Outcome::Yes,
                    expiry: pair.expiry,
                }),
                OrderBooksL2,
            )
        })
        .collect();

    let polymarket_subs: Vec<_> = pairs
        .iter()
        .map(|pair| {
            (
                Polymarket,
                pair.polymarket_yes_token.as_str(),
                "USDC",
                MarketDataInstrumentKind::Prediction(MarketDataPredictionContract {
                    outcome: Outcome::Yes,
                    expiry: pair.expiry,
                }),
                OrderBooksL2,
            )
        })
        .collect();

    info!(
        "Subscribing to {} Kalshi markets and {} Polymarket tokens",
        kalshi_subs.len(),
        polymarket_subs.len()
    );

    // Step 3: Initialize streams
    let streams_result = Streams::<OrderBooksL2>::builder()
        .subscribe(kalshi_subs)
        .subscribe(polymarket_subs)
        .init()
        .await;

    let streams = match streams_result {
        Ok(s) => s,
        Err(e) => {
            error!("Failed to initialize streams: {:?}", e);
            return;
        }
    };

    // Step 4: Process orderbook events
    info!("\nConnected! Streaming orderbook updates...");
    info!("(Press Ctrl+C to stop)\n");

    // Merge all streams into one using select_all
    let mut combined = streams.select_all();
    let mut event_count = 0u64;

    while let Some(event) = combined.next().await {
        event_count += 1;
        info!("[Event #{}] {:?}", event_count, event);

        // Print summary every 10 events
        if event_count % 10 == 0 {
            info!("--- Total events: {} ---", event_count);
        }
    }

    info!("\nFinal event count: {}", event_count);
}

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
                            unsafe { std::env::set_var(key, value); }
                        }
                    }
                }
                break;
            }
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
