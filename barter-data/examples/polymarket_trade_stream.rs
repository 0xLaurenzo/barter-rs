//! Example: Subscribe to Polymarket public trade stream via WebSocket.
//!
//! Fetches active markets from the gamma API, subscribes to the live_activity
//! channel, and streams real-time trade events.
//!
//! Usage:
//!   cargo run -p barter-data --example polymarket_trade_stream
//!   POLY_LIMIT=5 cargo run -p barter-data --example polymarket_trade_stream

use barter_data::{
    exchange::polymarket::Polymarket,
    streams::Streams,
    subscription::trade::PublicTrades,
};
use barter_instrument::instrument::market_data::kind::{
    MarketDataInstrumentKind, MarketDataPredictionContract, Outcome,
};
use chrono::Utc;
use futures_util::StreamExt;
use tracing::{info, warn};

/// Fetch active token IDs from Polymarket gamma API, sorted by 24h volume.
async fn fetch_active_token_ids(limit: usize) -> Vec<(String, String)> {
    let url = format!(
        "https://gamma-api.polymarket.com/markets?closed=false&limit={}&order=volume24hr&ascending=false",
        limit * 2
    );
    let resp = reqwest::get(&url).await.expect("gamma API request failed");
    let markets: Vec<serde_json::Value> = resp.json().await.expect("gamma API parse failed");

    let mut result = Vec::new();
    for market in &markets {
        if let Some(tokens) = market.get("clobTokenIds") {
            let parsed: Vec<String> = if let Some(s) = tokens.as_str() {
                serde_json::from_str(s).unwrap_or_default()
            } else if let Some(arr) = tokens.as_array() {
                arr.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            } else {
                vec![]
            };
            if let Some(first) = parsed.into_iter().next() {
                let question = market
                    .get("question")
                    .and_then(|q| q.as_str())
                    .unwrap_or("?")
                    .to_string();
                result.push((first, question));
            }
        }
        if result.len() >= limit {
            break;
        }
    }
    result
}

#[tokio::main]
async fn main() {
    init_logging();

    info!("Fetching active Polymarket markets...");
    let limit: usize = std::env::var("POLY_LIMIT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(3);

    let markets = fetch_active_token_ids(limit).await;

    if markets.is_empty() {
        warn!("No active markets found!");
        return;
    }

    info!("Found {} active markets:", markets.len());
    for (i, (token_id, question)) in markets.iter().enumerate() {
        let q = if question.len() > 60 {
            format!("{}...", &question[..60])
        } else {
            question.clone()
        };
        info!(
            "  {}. {} -> {}",
            i + 1,
            &token_id[..token_id.len().min(30)],
            q
        );
    }

    let expiry = Utc::now() + chrono::Duration::days(30);

    let subs: Vec<_> = markets
        .iter()
        .map(|(token_id, _)| {
            (
                Polymarket,
                token_id.as_str(),
                "USDC",
                MarketDataInstrumentKind::Prediction(MarketDataPredictionContract {
                    outcome: Outcome::Yes,
                    expiry,
                }),
                PublicTrades,
            )
        })
        .collect();

    info!(
        "Subscribing to {} Polymarket trade streams...",
        subs.len()
    );

    let streams = Streams::<PublicTrades>::builder()
        .subscribe(subs)
        .init()
        .await
        .expect("Failed to initialize streams");

    info!("Connected! Streaming trades...");
    info!("(Press Ctrl+C to stop)\n");

    let mut combined = streams.select_all();
    let mut count = 0u64;
    let mut ok_count = 0u64;
    let mut err_count = 0u64;

    while let Some(event) = combined.next().await {
        count += 1;
        match event {
            barter_data::streams::reconnect::Event::Item(Ok(market_event)) => {
                ok_count += 1;
                if ok_count <= 20 {
                    info!(
                        "[trade #{}/{}] exchange={:?} instrument={:?} kind={:?}",
                        ok_count, count, market_event.exchange, market_event.instrument, market_event.kind,
                    );
                } else if ok_count % 50 == 0 {
                    info!(
                        "--- {} ok / {} err / {} total ---",
                        ok_count, err_count, count
                    );
                }
            }
            barter_data::streams::reconnect::Event::Item(Err(e)) => {
                err_count += 1;
                if err_count <= 5 {
                    warn!("[ERR #{}/{}] {}", err_count, count, e);
                }
            }
            other => {
                info!("[Event #{}/{}] {:?}", count, count, other);
            }
        }
    }

    info!(
        "\nFinal: {} ok / {} err / {} total",
        ok_count, err_count, count
    );
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
