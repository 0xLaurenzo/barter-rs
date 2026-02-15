//! Raw Kalshi WebSocket test — bypass barter-data's subscriber to see raw WS messages.
//!
//! Usage: cargo run -p barter-data --example kalshi_ws_raw

use barter_data::exchange::kalshi::auth::{KalshiAuthHeaders, KalshiCredentials};
use barter_integration::protocol::websocket::{connect_with_headers, WsMessage};
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

#[tokio::main]
async fn main() {
    load_dotenv();

    let creds = KalshiCredentials::from_env().expect("Failed to load credentials");
    let auth = creds.generate_ws_auth().expect("Failed to generate auth");

    println!("API key: {}", auth.api_key);
    println!("Timestamp: {}", auth.timestamp);
    println!("Signature len: {}", auth.signature.len());

    let url = url::Url::parse("wss://api.elections.kalshi.com/trade-api/ws/v2").unwrap();

    let headers = [
        (KalshiAuthHeaders::KEY_HEADER, auth.api_key),
        (KalshiAuthHeaders::SIGNATURE_HEADER, auth.signature),
        (KalshiAuthHeaders::TIMESTAMP_HEADER, auth.timestamp),
    ];

    println!("Connecting to {}...", url);

    let mut ws = connect_with_headers(url, headers)
        .await
        .expect("Failed to connect");

    println!("Connected!");

    // Subscribe to 1 market orderbook
    let sub_msg = json!({
        "id": 1,
        "cmd": "subscribe",
        "params": {
            "channels": ["orderbook_delta"],
            "market_tickers": ["KXBTC-26FEB14-T100000", "KXINX-26FEB14-T6055", "KXBTC-26FEB21-T100000"]
        }
    });

    println!("Sending subscribe: {}", sub_msg);
    ws.send(WsMessage::text(sub_msg.to_string()))
        .await
        .expect("Failed to send");

    // Read messages for 15 seconds
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(15);
    let mut count = 0;

    loop {
        tokio::select! {
            _ = tokio::time::sleep_until(deadline) => {
                println!("\n--- Timeout reached, received {} messages ---", count);
                break;
            }
            msg = ws.next() => {
                match msg {
                    Some(Ok(WsMessage::Text(text))) => {
                        count += 1;
                        if text.len() > 500 {
                            println!("[{}] TEXT ({} bytes): {}...", count, text.len(), &text[..500]);
                        } else {
                            println!("[{}] TEXT: {}", count, text);
                        }
                    }
                    Some(Ok(WsMessage::Binary(data))) => {
                        count += 1;
                        println!("[{}] BINARY: {} bytes", count, data.len());
                    }
                    Some(Ok(WsMessage::Ping(_))) => {
                        println!("[ping]");
                    }
                    Some(Ok(WsMessage::Pong(_))) => {
                        println!("[pong]");
                    }
                    Some(Ok(WsMessage::Close(frame))) => {
                        println!("CLOSE: {:?}", frame);
                        break;
                    }
                    Some(Ok(other)) => {
                        println!("[{}] OTHER: {:?}", count, other);
                    }
                    Some(Err(e)) => {
                        println!("ERROR: {}", e);
                        break;
                    }
                    None => {
                        println!("Stream ended");
                        break;
                    }
                }
            }
        }
    }
}
