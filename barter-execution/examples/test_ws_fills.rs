//! Test WebSocket fill connections for Kalshi and Polymarket.
//!
//! Connects to both private WS endpoints, verifies auth handshake,
//! and logs any incoming messages for 60 seconds.
//!
//! Kalshi defaults to PRODUCTION (set KALSHI_DEMO=true for demo).
//! Polymarket connects to production (no sandbox available).
//!
//! Usage:
//!   cargo run -p barter-execution --example test_ws_fills
//!
//! Required env vars (in .env or exported):
//!   KALSHI_API_KEY, KALSHI_PRIVATE_KEY_PEM (or KALSHI_PRIVATE_KEY_PATH)
//!   POLYMARKET_PRIVATE_KEY  (hex private key or mnemonic phrase)

use barter_execution::client::kalshi::ws as kalshi_ws;
use barter_execution::client::polymarket::{http::PolymarketHttpClient, ws as poly_ws};
use barter_integration::protocol::websocket::WsMessage;
use futures::{SinkExt, StreamExt};
use rsa::{RsaPrivateKey, pkcs1::DecodeRsaPrivateKey, pkcs8::DecodePrivateKey};
use std::time::Duration;

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

fn load_kalshi_private_key() -> Option<RsaPrivateKey> {
    let pem = if let Ok(pem) = std::env::var("KALSHI_PRIVATE_KEY_PEM") {
        pem
    } else if let Ok(path) = std::env::var("KALSHI_PRIVATE_KEY_PATH") {
        match std::fs::read_to_string(&path) {
            Ok(contents) => contents,
            Err(e) => {
                eprintln!("[kalshi] Failed to read key file {}: {}", path, e);
                return None;
            }
        }
    } else {
        eprintln!("[kalshi] Neither KALSHI_PRIVATE_KEY_PEM nor KALSHI_PRIVATE_KEY_PATH set");
        return None;
    };

    RsaPrivateKey::from_pkcs8_pem(&pem)
        .or_else(|_| RsaPrivateKey::from_pkcs1_pem(&pem))
        .map_err(|e| eprintln!("[kalshi] Failed to parse RSA key: {}", e))
        .ok()
}

async fn test_kalshi(duration: Duration) {
    println!("\n=== Kalshi Fill WS Test ===\n");

    let api_key = match std::env::var("KALSHI_API_KEY") {
        Ok(k) => k,
        Err(_) => {
            eprintln!("[kalshi] KALSHI_API_KEY not set, skipping");
            return;
        }
    };

    let private_key = match load_kalshi_private_key() {
        Some(k) => k,
        None => return,
    };

    let demo = std::env::var("KALSHI_DEMO")
        .map(|v| v == "true")
        .unwrap_or(false);

    println!(
        "[kalshi] Connecting to {} environment...",
        if demo { "DEMO" } else { "PRODUCTION" }
    );

    // Empty tickers = subscribe to all fills for this account
    let tickers: Vec<String> = vec![];

    let ws = match kalshi_ws::connect_kalshi_fills(&api_key, &private_key, demo, &tickers).await {
        Ok(ws) => {
            println!("[kalshi] Connected and subscribed to fill channel");
            ws
        }
        Err(e) => {
            eprintln!("[kalshi] Connection failed: {}", e);
            return;
        }
    };

    let (_sink, mut stream) = ws.split();
    let deadline = tokio::time::Instant::now() + duration;
    let mut count = 0u32;

    println!(
        "[kalshi] Listening for messages ({} seconds)...\n",
        duration.as_secs()
    );

    loop {
        tokio::select! {
            _ = tokio::time::sleep_until(deadline) => {
                println!("\n[kalshi] Timeout reached, received {} messages", count);
                break;
            }
            msg = stream.next() => {
                match msg {
                    Some(Ok(WsMessage::Text(text))) => {
                        count += 1;
                        if text.len() > 500 {
                            println!("[kalshi #{count}] ({} bytes): {}...", text.len(), &text[..500]);
                        } else {
                            println!("[kalshi #{count}] {text}");
                        }
                    }
                    Some(Ok(WsMessage::Ping(_))) => println!("[kalshi] ping"),
                    Some(Ok(WsMessage::Pong(_))) => println!("[kalshi] pong"),
                    Some(Ok(WsMessage::Close(frame))) => {
                        println!("[kalshi] CLOSE: {:?}", frame);
                        break;
                    }
                    Some(Err(e)) => {
                        eprintln!("[kalshi] ERROR: {}", e);
                        break;
                    }
                    None => {
                        println!("[kalshi] Stream ended");
                        break;
                    }
                    _ => {}
                }
            }
        }
    }
}

/// Fetch a few active condition IDs from the Polymarket gamma API for testing.
async fn fetch_active_condition_ids(limit: usize) -> Vec<String> {
    let url = format!(
        "https://gamma-api.polymarket.com/markets?closed=false&limit={}",
        limit
    );
    let resp = match reqwest::get(&url).await {
        Ok(r) => r,
        Err(e) => {
            eprintln!("[polymarket] Failed to fetch markets from gamma API: {}", e);
            return vec![];
        }
    };
    let markets: Vec<serde_json::Value> = match resp.json().await {
        Ok(m) => m,
        Err(e) => {
            eprintln!("[polymarket] Failed to parse gamma API response: {}", e);
            return vec![];
        }
    };

    let mut condition_ids = Vec::new();
    for market in &markets {
        if let Some(cid) = market.get("conditionId").and_then(|v| v.as_str()) {
            if !cid.is_empty() {
                let question = market
                    .get("question")
                    .and_then(|q| q.as_str())
                    .unwrap_or("?");
                println!(
                    "  Market: {} -> condition: {}...",
                    question,
                    &cid[..cid.len().min(20)]
                );
                condition_ids.push(cid.to_string());
            }
        }
        if condition_ids.len() >= limit {
            break;
        }
    }
    condition_ids
}

async fn test_polymarket(duration: Duration) {
    println!("\n=== Polymarket User WS Test ===\n");

    let private_key = match std::env::var("POLYMARKET_PRIVATE_KEY") {
        Ok(k) => k,
        Err(_) => {
            eprintln!("[polymarket] POLYMARKET_PRIVATE_KEY not set, skipping");
            return;
        }
    };

    // Derive API credentials from Ethereum private key (same as run_engine)
    println!("[polymarket] Deriving API credentials from private key...");
    let creds = match PolymarketHttpClient::derive_api_credentials(&private_key).await {
        Ok(c) => {
            println!("[polymarket] Credentials derived for wallet: {}", c.wallet_address);
            c
        }
        Err(e) => {
            eprintln!("[polymarket] Failed to derive credentials: {}", e);
            return;
        }
    };

    // Fetch a few active markets to subscribe to
    println!("[polymarket] Fetching active markets from gamma API...");
    let markets = fetch_active_condition_ids(3).await;
    if markets.is_empty() {
        eprintln!("[polymarket] No active markets found, connecting without market filter");
    } else {
        println!("[polymarket] Subscribing to {} markets", markets.len());
    }

    println!("[polymarket] Connecting to production user WS...");

    let ws = match poly_ws::connect_polymarket_user(
        &creds.api_key,
        &creds.api_secret,
        &creds.api_passphrase,
        &markets,
    )
    .await
    {
        Ok(ws) => {
            println!("[polymarket] Connected and authenticated");
            ws
        }
        Err(e) => {
            eprintln!("[polymarket] Connection failed: {}", e);
            return;
        }
    };

    let (mut sink, mut stream) = ws.split();
    let deadline = tokio::time::Instant::now() + duration;
    let mut ping_interval = tokio::time::interval(Duration::from_secs(10));
    // Skip the immediate first tick
    ping_interval.tick().await;
    let mut count = 0u32;

    println!(
        "[polymarket] Listening for messages ({} seconds, ping every 10s)...\n",
        duration.as_secs()
    );

    loop {
        tokio::select! {
            _ = tokio::time::sleep_until(deadline) => {
                println!("\n[polymarket] Timeout reached, received {} messages", count);
                break;
            }
            _ = ping_interval.tick() => {
                match sink.send(WsMessage::text("PING".to_string())).await {
                    Ok(_) => println!("[polymarket] PING sent"),
                    Err(e) => {
                        eprintln!("[polymarket] PING failed: {}", e);
                        break;
                    }
                }
            }
            msg = stream.next() => {
                match msg {
                    Some(Ok(WsMessage::Text(text))) => {
                        count += 1;
                        let trimmed = text.trim();
                        if trimmed == "PONG" {
                            println!("[polymarket] PONG");
                        } else if text.len() > 500 {
                            println!("[polymarket #{count}] ({} bytes): {}...", text.len(), &text[..500]);
                        } else {
                            println!("[polymarket #{count}] {text}");
                        }
                    }
                    Some(Ok(WsMessage::Ping(_))) => println!("[polymarket] ws-ping"),
                    Some(Ok(WsMessage::Pong(_))) => println!("[polymarket] ws-pong"),
                    Some(Ok(WsMessage::Close(frame))) => {
                        println!("[polymarket] CLOSE: {:?}", frame);
                        break;
                    }
                    Some(Err(e)) => {
                        eprintln!("[polymarket] ERROR: {}", e);
                        break;
                    }
                    None => {
                        println!("[polymarket] Stream ended");
                        break;
                    }
                    _ => {}
                }
            }
        }
    }
}

#[tokio::main]
async fn main() {
    load_dotenv();

    let duration = std::env::var("WS_TEST_DURATION_SECS")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .map(Duration::from_secs)
        .unwrap_or(Duration::from_secs(60));

    println!("WebSocket Fill Connection Test");
    println!("Duration: {} seconds per platform", duration.as_secs());
    println!("Set WS_TEST_DURATION_SECS to override\n");

    // Run both concurrently
    tokio::join!(test_kalshi(duration), test_polymarket(duration));

    println!("\nDone.");
}
