//! Subscribe to ALL active Kalshi market orderbooks over a single WebSocket.
//!
//! Demonstrates that barter-data batches all subscriptions onto one WS connection,
//! avoiding the "too many open files" problem that occurs when opening one
//! connection per market.
//!
//! The example:
//! 1. Fetches all active market tickers from Kalshi REST API (paginated)
//! 2. Subscribes to all orderbooks in a single `.subscribe()` call (= 1 WS connection)
//! 3. Streams events and reports stats every 5 seconds
//!
//! Environment variables:
//!   KALSHI_API_KEY=...
//!   KALSHI_PRIVATE_KEY_PATH=./kalshi-priv.pem   (or KALSHI_PRIVATE_KEY_PEM=...)
//!   KALSHI_DEMO=true                             (optional, use demo API)
//!   KALSHI_LIMIT=0                               (optional, 0 = all markets)
//!
//! Usage:
//!   cargo run -p barter-data --example kalshi_mass_orderbook_stream

use barter_data::{
    exchange::kalshi::{Kalshi, auth::KalshiCredentials},
    streams::{Streams, reconnect::stream::ReconnectingStream},
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
use rsa::pkcs1::DecodeRsaPrivateKey;
use rsa::pkcs8::DecodePrivateKey;
use rsa::signature::{RandomizedSigner, SignatureEncoding};
use serde::Deserialize;
use std::collections::HashSet;
use std::time::Instant;
use tracing::{info, warn, error};

/// Kalshi REST API response for market listing.
#[derive(Debug, Deserialize)]
struct MarketsResponse {
    markets: Vec<KalshiMarketInfo>,
    cursor: Option<String>,
}

/// Minimal market info from Kalshi REST API.
#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct KalshiMarketInfo {
    ticker: String,
    status: String,
    #[serde(default)]
    result: Option<String>,
}

/// Fetch all active Kalshi market tickers via the REST API with pagination.
async fn fetch_all_active_tickers(demo: bool, max: usize) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let creds = KalshiCredentials::from_env()?;

    let base = if demo {
        "https://demo-api.kalshi.co"
    } else {
        "https://api.elections.kalshi.com"
    };

    let client = reqwest::Client::new();
    let mut all_tickers = Vec::new();
    let mut cursor: Option<String> = None;
    let page_limit = 1000;

    loop {
        // Generate auth headers for this request
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_millis() as u64;
        let path = "/trade-api/v2/markets";
        let method = "GET";
        let message = format!("{}{}{}", timestamp, method, path);

        let pem_str = load_pem()?;
        let private_key = rsa::RsaPrivateKey::from_pkcs8_pem(&pem_str)
            .or_else(|_| rsa::RsaPrivateKey::from_pkcs1_pem(&pem_str))
            .map_err(|e| format!("Failed to parse private key: {}", e))?;
        let signing_key = rsa::pss::SigningKey::<sha2::Sha256>::new(private_key);
        let mut rng = rsa::rand_core::OsRng;
        let signature = signing_key.sign_with_rng(&mut rng, message.as_bytes());
        let signature_b64 = base64::Engine::encode(
            &base64::engine::general_purpose::STANDARD,
            signature.to_bytes(),
        );

        let mut url = format!("{}/trade-api/v2/markets?limit={}&status=open", base, page_limit);
        if let Some(ref c) = cursor {
            url.push_str(&format!("&cursor={}", c));
        }

        let resp = client
            .get(&url)
            .header("KALSHI-ACCESS-KEY", &creds.api_key)
            .header("KALSHI-ACCESS-SIGNATURE", &signature_b64)
            .header("KALSHI-ACCESS-TIMESTAMP", timestamp.to_string())
            .send()
            .await?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(format!("Kalshi API error {}: {}", status, body).into());
        }

        let page: MarketsResponse = resp.json().await?;
        let count = page.markets.len();

        for market in &page.markets {
            // status=open already filters; skip settled markets with a definitive result
            let settled = market.result.as_deref().is_some_and(|r| r == "yes" || r == "no");
            if !settled {
                all_tickers.push(market.ticker.clone());
            }
        }

        info!("Fetched page: {} markets ({} active so far)", count, all_tickers.len());

        // Stop early if we have enough
        if max > 0 && all_tickers.len() >= max {
            all_tickers.truncate(max);
            break;
        }

        match page.cursor {
            Some(c) if !c.is_empty() && count == page_limit => {
                cursor = Some(c);
            }
            _ => break,
        }
    }

    Ok(all_tickers)
}

fn load_pem() -> Result<String, Box<dyn std::error::Error>> {
    if let Ok(path) = std::env::var("KALSHI_PRIVATE_KEY_PATH") {
        // Try the path as-is, then ../path, then ../../path
        for prefix in &["", "../", "../../"] {
            let full = format!("{}{}", prefix, path);
            if std::path::Path::new(&full).exists() {
                return Ok(std::fs::read_to_string(&full)?);
            }
        }
        Ok(std::fs::read_to_string(&path)?)
    } else if let Ok(pem) = std::env::var("KALSHI_PRIVATE_KEY_PEM") {
        Ok(pem)
    } else {
        Err("Neither KALSHI_PRIVATE_KEY_PATH nor KALSHI_PRIVATE_KEY_PEM set".into())
    }
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
                            // Safety: this runs single-threaded at startup before tokio init
                            unsafe { std::env::set_var(key, value); }
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

    let demo = std::env::var("KALSHI_DEMO").unwrap_or_default() == "true";
    let max_markets: usize = std::env::var("KALSHI_LIMIT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(0); // 0 = all

    // Step 1: Fetch active tickers (respects KALSHI_LIMIT)
    info!("Fetching active Kalshi market tickers (limit={})...", if max_markets == 0 { "all".to_string() } else { max_markets.to_string() });
    let tickers = match fetch_all_active_tickers(demo, max_markets).await {
        Ok(t) => t,
        Err(e) => {
            error!("Failed to fetch tickers: {}", e);
            return;
        }
    };

    if tickers.is_empty() {
        error!("No active markets found");
        return;
    }

    info!("Subscribing to {} orderbooks on a SINGLE WebSocket connection", tickers.len());

    // Step 2: Build subscriptions - ALL in one .subscribe() call = 1 WS connection
    let expiry = Utc::now() + Duration::days(90);
    let subs: Vec<_> = tickers
        .iter()
        .map(|ticker| {
            (
                Kalshi,
                ticker.as_str(),
                "USD",
                MarketDataInstrumentKind::Prediction(MarketDataPredictionContract {
                    outcome: Outcome::Yes,
                    expiry,
                }),
                OrderBooksL2,
            )
        })
        .collect();

    let sub_count = subs.len();

    let streams_result = Streams::<OrderBooksL2>::builder()
        .subscribe(subs)
        .init()
        .await;

    let mut streams = match streams_result {
        Ok(s) => s,
        Err(e) => {
            error!("Failed to initialize stream: {:?}", e);
            return;
        }
    };

    info!("WebSocket connected and subscribed to {} markets", sub_count);

    // Step 3: Stream events and report stats
    let mut stream = streams
        .select(ExchangeId::Kalshi)
        .expect("Kalshi stream should exist")
        .with_error_handler(|error| warn!(?error, "Stream error"));

    let start = Instant::now();
    let mut total_events: u64 = 0;
    let mut snapshot_count: u64 = 0;
    let mut delta_count: u64 = 0;
    let mut markets_seen = HashSet::new();
    let mut last_report = Instant::now();
    let report_interval = std::time::Duration::from_secs(5);

    info!("Streaming orderbook events... (Ctrl+C to stop)");

    use barter_data::streams::reconnect::Event as ReconnectEvent;
    use barter_data::subscription::book::OrderBookEvent;

    while let Some(event) = stream.next().await {
        match event {
            ReconnectEvent::Reconnecting(exchange) => {
                warn!("Reconnecting to {:?}...", exchange);
                continue;
            }
            ReconnectEvent::Item(market_event) => {
                total_events += 1;

                // Track which market this event is for
                let instrument_base = format!("{}", market_event.instrument);
                markets_seen.insert(instrument_base);

                // Count snapshots vs deltas
                match &market_event.kind {
                    OrderBookEvent::Snapshot(_) => snapshot_count += 1,
                    OrderBookEvent::Update(_) => delta_count += 1,
                }
            }
        }

        // Report stats periodically
        if last_report.elapsed() >= report_interval {
            let elapsed = start.elapsed().as_secs_f64();
            let events_per_sec = total_events as f64 / elapsed;
            info!(
                "Stats: {} events ({} snapshots, {} deltas) | {:.1} events/sec | {}/{} markets seen | {:.1}s elapsed",
                total_events,
                snapshot_count,
                delta_count,
                events_per_sec,
                markets_seen.len(),
                sub_count,
                elapsed,
            );
            last_report = Instant::now();
        }
    }

    let elapsed = start.elapsed().as_secs_f64();
    info!(
        "Stream ended. Total: {} events over {:.1}s from {}/{} markets",
        total_events,
        elapsed,
        markets_seen.len(),
        sub_count,
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
