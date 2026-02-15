//! Raw Polymarket WebSocket test — bypass barter-data's subscriber to see raw WS messages.
//!
//! Usage: cargo run -p barter-data --example polymarket_ws_raw

use barter_integration::protocol::websocket::WsMessage;
use futures_util::{SinkExt, StreamExt};
use serde_json::json;

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

/// Fetch a few active token IDs from the Polymarket gamma API.
async fn fetch_active_token_ids(limit: usize) -> Vec<String> {
    let url = format!(
        "https://gamma-api.polymarket.com/markets?closed=false&limit={}",
        limit
    );
    let resp = reqwest::get(&url).await.expect("gamma API request failed");
    let markets: Vec<serde_json::Value> = resp.json().await.expect("gamma API parse failed");

    let mut token_ids = Vec::new();
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
                println!("  Market: {} -> token: {}...",
                    market.get("question").and_then(|q| q.as_str()).unwrap_or("?"),
                    &first[..first.len().min(40)]
                );
                token_ids.push(first);
            }
        }
        if token_ids.len() >= limit {
            break;
        }
    }
    token_ids
}

#[tokio::main]
async fn main() {
    load_dotenv();

    println!("Fetching active Polymarket markets...");
    let token_ids = fetch_active_token_ids(3).await;

    if token_ids.is_empty() {
        println!("No active token IDs found!");
        return;
    }
    println!("\nUsing {} token IDs\n", token_ids.len());

    // CLOB WS endpoint (from Polymarket docs)
    let url = url::Url::parse("wss://ws-subscriptions-clob.polymarket.com/ws/market").unwrap();
    println!("Connecting to {}...", url);

    let empty_headers: [(&str, String); 0] = [];
    let mut ws = barter_integration::protocol::websocket::connect_with_headers(url, empty_headers)
        .await
        .expect("Failed to connect");

    println!("Connected!");

    // Subscribe: {"assets_ids": [...], "type": "market"}
    let sub_msg = json!({
        "assets_ids": token_ids,
        "type": "market"
    });
    println!("Sending: {}", serde_json::to_string(&sub_msg).unwrap());
    ws.send(WsMessage::text(sub_msg.to_string()))
        .await
        .expect("Failed to send");

    // Read messages for 30 seconds, with "PING" keepalive every 10s
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(30);
    let mut ping_interval = tokio::time::interval(std::time::Duration::from_secs(10));
    let mut count = 0;

    loop {
        tokio::select! {
            _ = tokio::time::sleep_until(deadline) => {
                println!("\n--- Timeout reached, received {} messages ---", count);
                break;
            }
            _ = ping_interval.tick() => {
                let _ = ws.send(WsMessage::text("PING".to_string())).await;
            }
            msg = ws.next() => {
                match msg {
                    Some(Ok(WsMessage::Text(text))) => {
                        count += 1;
                        if text == "[]" || text == "PONG" {
                            if count <= 5 { println!("[{}] {}", count, text); }
                        } else if text.len() > 800 {
                            println!("\n[{}] TEXT ({} bytes): {}...", count, text.len(), &text[..800]);
                        } else {
                            println!("\n[{}] TEXT: {}", count, text);
                        }
                    }
                    Some(Ok(WsMessage::Ping(_))) => { println!("[ws-ping]"); }
                    Some(Ok(WsMessage::Pong(_))) => { println!("[ws-pong]"); }
                    Some(Ok(WsMessage::Close(frame))) => {
                        println!("CLOSE: {:?}", frame);
                        break;
                    }
                    Some(Err(e)) => {
                        println!("ERROR: {}", e);
                        break;
                    }
                    _ => {}
                }
            }
        }
    }
}
