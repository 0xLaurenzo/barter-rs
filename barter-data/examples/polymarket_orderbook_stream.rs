//! Test Polymarket orderbook streaming through the full barter-data pipeline.
//!
//! Fetches active markets from the gamma API, subscribes via barter-data,
//! and verifies events flow end-to-end.
//!
//! Usage: cargo run -p barter-data --example polymarket_orderbook_stream

use barter_data::{
    exchange::polymarket::Polymarket,
    streams::Streams,
    subscription::book::OrderBooksL2,
};
use barter_instrument::instrument::market_data::kind::{
    MarketDataInstrumentKind, MarketDataPredictionContract, Outcome,
};
use chrono::Utc;
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
                            unsafe { std::env::set_var(key, value); }
                        }
                    }
                }
                break;
            }
        }
    }
}

/// Fetch active token IDs from Polymarket gamma API.
async fn fetch_active_token_ids(limit: usize) -> Vec<(String, String)> {
    let url = format!(
        "https://gamma-api.polymarket.com/markets?closed=false&limit={}",
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
                arr.iter().filter_map(|v| v.as_str().map(String::from)).collect()
            } else {
                vec![]
            };
            if let Some(first) = parsed.into_iter().next() {
                let question = market.get("question")
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
    load_dotenv();

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
        let q = if question.len() > 60 { format!("{}...", &question[..60]) } else { question.clone() };
        info!("  {}. {} -> {}", i + 1, &token_id[..token_id.len().min(30)], q);
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
                OrderBooksL2,
            )
        })
        .collect();

    info!("Subscribing to {} Polymarket orderbook streams...", subs.len());

    let streams = Streams::<OrderBooksL2>::builder()
        .subscribe(subs)
        .init()
        .await
        .expect("Failed to initialize streams");

    info!("Connected! Streaming orderbook updates...");
    info!("(Press Ctrl+C to stop)\n");

    let mut combined = streams.select_all();
    let mut event_count = 0u64;
    let mut ok_count = 0u64;
    let mut err_count = 0u64;

    while let Some(event) = combined.next().await {
        event_count += 1;
        // Events are Event::Item(Result<MarketEvent, DataError>) or Event::Reconnecting
        match event {
            barter_data::streams::reconnect::Event::Item(Ok(market_event)) => {
                ok_count += 1;
                if ok_count <= 10 {
                    info!("[OK #{}/{}] exchange={:?} instrument={:?} kind={:?}",
                        ok_count, event_count,
                        market_event.exchange,
                        market_event.instrument,
                        market_event.kind,
                    );
                } else if ok_count % 50 == 0 {
                    info!("--- {} ok / {} err / {} total ---", ok_count, err_count, event_count);
                }
            }
            barter_data::streams::reconnect::Event::Item(Err(e)) => {
                err_count += 1;
                if err_count <= 3 {
                    warn!("[ERR #{}/{}] {}", err_count, event_count, e);
                }
            }
            other => {
                info!("[Event #{}/{}] {:?}", event_count, event_count, other);
            }
        }
    }

    info!("\nFinal: {} ok / {} err / {} total", ok_count, err_count, event_count);
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
