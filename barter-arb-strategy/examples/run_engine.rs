//! Engine runner for prediction market arbitrage.
//!
//! Wires together:
//! - Market data streams (Kalshi + Polymarket WebSocket orderbooks)
//! - Execution clients (Kalshi REST + Polymarket CLOB)
//! - Arbitrage strategy with order generation
//! - Risk manager with capital limits
//!
//! Usage:
//!   # Set environment variables in .env
//!   KALSHI_API_KEY=...
//!   KALSHI_PRIVATE_KEY_PATH=./kalshi-priv.pem
//!   POLYMARKET_PRIVATE_KEY=...  (hex private key)
//!   POLY_API_KEY=...
//!   POLY_API_SECRET=...
//!   POLY_API_PASSPHRASE=...
//!   POLY_MAKER_ADDRESS=0x...
//!   SUPABASE_URL=...
//!   SUPABASE_ANON_KEY=...
//!
//!   cargo run -p barter-arb-strategy --example run_engine

use barter::engine::{Engine, state::builder::EngineStateBuilder, state::trading::TradingState};
use barter::execution::builder::ExecutionBuilder;
use barter::system::builder::{AuditMode, EngineFeedMode, SystemBuild};
use barter_arb_strategy::{
    ArbitrageConfig, ArbitrageRiskManager, DatabaseQuerier, MarketPairFilters,
    PredictionArbitrageStrategy,
    recorder::OrderbookRecorder,
    state::{ArbitrageGlobalData, ArbitrageInstrumentData},
};
use barter_data::{
    event::{DataKind, MarketEvent},
    exchange::{kalshi::Kalshi, polymarket::Polymarket},
    streams::{Streams, reconnect},
    subscription::book::OrderBooksL2,
};
use barter_execution::client::{
    kalshi::KalshiExecutionConfig,
    polymarket::{PolymarketExecutionConfig, http::PolymarketHttpClient},
};
use barter_instrument::{
    Underlying,
    asset::Asset,
    exchange::ExchangeId,
    index::IndexedInstruments,
    instrument::{
        Instrument, InstrumentIndex,
        name::InstrumentNameInternal,
        spec::{
            InstrumentSpec, InstrumentSpecNotional, InstrumentSpecPrice, InstrumentSpecQuantity,
            OrderQuantityUnits,
        },
    },
};
use barter_instrument::instrument::market_data::kind::{
    MarketDataInstrumentKind, MarketDataPredictionContract, Outcome,
};
use futures::StreamExt;
use rust_decimal_macros::dec;
use std::collections::HashMap;
use std::time::Duration;
use tracing::{error, info, warn};

#[tokio::main]
async fn main() {
    init_logging();
    dotenv();

    // Step 1: Fetch correlated pairs from Supabase
    info!("Fetching correlated market pairs...");
    let db = DatabaseQuerier::from_env().expect("Database connection failed");
    let filters = MarketPairFilters {
        min_similarity: Some(dec!(0.85)),
        min_confidence: Some(dec!(0.85)),
        limit: Some(10),
        valid_only: Some(true),
        ..Default::default()
    };

    let pairs = db
        .get_correlated_pairs(filters)
        .await
        .expect("Failed to fetch pairs");

    if pairs.is_empty() {
        error!("No correlated pairs found");
        return;
    }
    info!("Found {} correlated pairs", pairs.len());

    // Step 2: Build IndexedInstruments from pairs
    // Each pair generates 4 instruments: Kalshi YES/NO, Polymarket YES/NO
    let spec = InstrumentSpec::new(
        InstrumentSpecPrice::new(dec!(0.01), dec!(0.01)),
        InstrumentSpecQuantity::new(OrderQuantityUnits::Contract, dec!(1), dec!(1)),
        InstrumentSpecNotional::new(dec!(0.01)),
    );

    let mut builder = IndexedInstruments::builder();
    for pair in &pairs {
        // Kalshi YES
        let kalshi_yes_name = format!("{}_yes", pair.kalshi_ticker);
        builder = builder.add_instrument(Instrument::spot(
            ExchangeId::Kalshi,
            format!("kalshi_{}", kalshi_yes_name),
            kalshi_yes_name.as_str(),
            Underlying::new(Asset::from(kalshi_yes_name.as_str()), Asset::from("usd")),
            Some(spec.clone()),
        ));

        // Kalshi NO
        let kalshi_no_name = format!("{}_no", pair.kalshi_ticker);
        builder = builder.add_instrument(Instrument::spot(
            ExchangeId::Kalshi,
            format!("kalshi_{}", kalshi_no_name),
            kalshi_no_name.as_str(),
            Underlying::new(Asset::from(kalshi_no_name.as_str()), Asset::from("usd")),
            Some(spec.clone()),
        ));

        // Polymarket YES (token_id as name_exchange)
        let poly_yes_prefix = &pair.polymarket_yes_token[..8.min(pair.polymarket_yes_token.len())];
        builder = builder.add_instrument(Instrument::spot(
            ExchangeId::Polymarket,
            format!("poly_{}", poly_yes_prefix),
            pair.polymarket_yes_token.as_str(),
            Underlying::new(
                Asset::from(pair.polymarket_yes_token.as_str()),
                Asset::from("usdc"),
            ),
            Some(spec.clone()),
        ));

        // Polymarket NO
        let poly_no_prefix = &pair.polymarket_no_token[..8.min(pair.polymarket_no_token.len())];
        builder = builder.add_instrument(Instrument::spot(
            ExchangeId::Polymarket,
            format!("poly_{}", poly_no_prefix),
            pair.polymarket_no_token.as_str(),
            Underlying::new(
                Asset::from(pair.polymarket_no_token.as_str()),
                Asset::from("usdc"),
            ),
            Some(spec.clone()),
        ));
    }

    let indexed = builder.build();
    info!(
        "Built indexed instruments: {} instruments across {} exchanges",
        indexed.instruments().len(),
        indexed.exchanges().len()
    );

    // Step 3: Build data streams
    // We subscribe to YES orderbooks only; NO prices are derived (1 - YES).
    info!("Building market data streams...");
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

    let streams = Streams::<OrderBooksL2>::builder()
        .subscribe(kalshi_subs)
        .subscribe(polymarket_subs)
        .init()
        .await
        .expect("Failed to init data streams");

    let raw_stream = streams.select_all();

    // Build lookup map: (ExchangeId, subscription_base_lowercase) -> InstrumentIndex
    // The subscription base is the kalshi ticker or polymarket token_id.
    // AssetNameInternal lowercases its input, so we lowercase the lookup keys too.
    let mut instrument_lookup: HashMap<(ExchangeId, String), InstrumentIndex> = HashMap::new();
    for pair in &pairs {
        // Kalshi YES: subscription base = kalshi_ticker, instrument name_internal = "kalshi_{ticker}_yes"
        let kalshi_name_internal =
            InstrumentNameInternal::new(format!("kalshi_{}_yes", pair.kalshi_ticker));
        if let Ok(idx) = indexed.find_instrument_index(ExchangeId::Kalshi, &kalshi_name_internal) {
            instrument_lookup.insert(
                (ExchangeId::Kalshi, pair.kalshi_ticker.to_lowercase()),
                idx,
            );
        }

        // Polymarket YES: subscription base = token_id, instrument name_internal = "poly_{prefix}"
        let poly_prefix = &pair.polymarket_yes_token[..8.min(pair.polymarket_yes_token.len())];
        let poly_name_internal = InstrumentNameInternal::new(format!("poly_{}", poly_prefix));
        if let Ok(idx) =
            indexed.find_instrument_index(ExchangeId::Polymarket, &poly_name_internal)
        {
            instrument_lookup.insert(
                (
                    ExchangeId::Polymarket,
                    pair.polymarket_yes_token.to_lowercase(),
                ),
                idx,
            );
        }
    }

    // Optional: Set up orderbook snapshot recording
    let recorder = OrderbookRecorder::from_env();
    if recorder.is_some() {
        info!("Orderbook snapshot recording enabled");
    }
    let recorder = std::sync::Arc::new(std::sync::Mutex::new(recorder));

    // Adapt raw stream: filter errors, map instrument keys to InstrumentIndex, wrap kind in DataKind
    let recorder_tap = recorder.clone();
    let market_stream = raw_stream.filter_map(move |event| {
        let result = match event {
            reconnect::Event::Reconnecting(exchange) => {
                Some(reconnect::Event::Reconnecting(exchange))
            }
            reconnect::Event::Item(Ok(market_event)) => {
                let base: &str = market_event.instrument.base.as_ref();
                let key = (market_event.exchange, base.to_owned());
                if let Some(&idx) = instrument_lookup.get(&key) {
                    // Tap: record snapshot if recorder is enabled
                    if let Ok(mut guard) = recorder_tap.lock() {
                        if let Some(rec) = guard.as_mut() {
                            if let barter_data::subscription::book::OrderBookEvent::Snapshot(ref book) = market_event.kind {
                                rec.on_orderbook_update(&key.1, book);
                            }
                        }
                    }
                    Some(reconnect::Event::Item(MarketEvent {
                        time_exchange: market_event.time_exchange,
                        time_received: market_event.time_received,
                        exchange: market_event.exchange,
                        instrument: idx,
                        kind: DataKind::OrderBook(market_event.kind),
                    }))
                } else {
                    warn!(?key, "No instrument index found for market event");
                    None
                }
            }
            reconnect::Event::Item(Err(error)) => {
                warn!(?error, "Market stream error");
                None
            }
        };
        std::future::ready(result)
    });

    // Step 4: Build execution clients
    info!("Building execution clients...");
    let kalshi_pem = load_kalshi_pem();
    let kalshi_config = KalshiExecutionConfig {
        api_key: env("KALSHI_API_KEY"),
        private_key_pem: kalshi_pem,
        demo: std::env::var("KALSHI_DEMO")
            .or_else(|_| std::env::var("KALSHI_USE_DEMO"))
            .unwrap_or_default() == "true",
        poll_interval_ms: 2000,
    };

    let poly_private_key = env("POLYMARKET_PRIVATE_KEY");

    // Always derive fresh API credentials from private key
    info!("Deriving Polymarket API credentials from private key...");
    let poly_creds = PolymarketHttpClient::derive_api_credentials(&poly_private_key)
        .await
        .expect("Failed to derive Polymarket API credentials");
    info!("Polymarket wallet address: {}", poly_creds.wallet_address);

    let poly_config = PolymarketExecutionConfig {
        api_key: poly_creds.api_key,
        api_secret: poly_creds.api_secret,
        api_passphrase: poly_creds.api_passphrase,
        private_key_hex: poly_private_key,
        maker_address: poly_creds.wallet_address,
        poll_interval_ms: 2000,
        neg_risk: std::env::var("POLY_NEG_RISK").unwrap_or_default() == "true",
    };

    let execution = ExecutionBuilder::new(&indexed)
        .add_live::<barter_execution::client::kalshi::KalshiExecution>(
            kalshi_config,
            Duration::from_secs(10),
        )
        .expect("Failed to add Kalshi execution")
        .add_live::<barter_execution::client::polymarket::PolymarketExecution>(
            poly_config,
            Duration::from_secs(10),
        )
        .expect("Failed to add Polymarket execution")
        .build();

    // Step 5: Build strategy
    let config = ArbitrageConfig {
        min_spread_threshold: dec!(0.02),
        max_position_per_market: 500,
        max_total_capital: dec!(5000),
        ..Default::default()
    };

    let strategy = PredictionArbitrageStrategy::with_instruments(
        barter_execution::order::id::StrategyId::new("pred-arb"),
        config,
        pairs,
        &indexed,
    );

    // Step 6: Build risk manager
    let risk = ArbitrageRiskManager {
        max_total_capital: dec!(5000),
        max_order_notional: dec!(500),
    };

    // Step 7: Build engine state
    let global_data = ArbitrageGlobalData::default();
    let state = EngineStateBuilder::new(&indexed, global_data, |_| {
        ArbitrageInstrumentData::default()
    })
    .trading_state(TradingState::Enabled)
    .build();

    // Step 8: Construct engine
    let clock = barter::engine::clock::LiveClock;
    let engine = Engine::new(
        clock,
        state,
        execution.execution_tx_map,
        strategy,
        risk,
    );

    // Step 9: Assemble and run system
    info!("Starting arbitrage engine...");
    let system_build = SystemBuild::new(
        engine,
        EngineFeedMode::default(),
        AuditMode::default(),
        market_stream,
        execution.account_channel,
        execution.futures,
    );

    match system_build.init().await {
        Ok(system) => {
            info!("Engine running. Press Ctrl+C to stop.");
            let _ = tokio::signal::ctrl_c().await;
            info!("Shutting down...");
            drop(system);
        }
        Err(e) => {
            error!("Failed to initialize system: {:?}", e);
        }
    }
}

fn env(key: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| panic!("{} not set", key))
}

fn load_kalshi_pem() -> String {
    if let Ok(path) = std::env::var("KALSHI_PRIVATE_KEY_PATH") {
        // Try the path as-is first, then resolve relative to parent dirs
        // (the .env may be loaded from a parent directory)
        let candidates = [
            std::path::PathBuf::from(&path),
            std::path::PathBuf::from(format!("../{}", path.trim_start_matches("./"))),
            std::path::PathBuf::from(format!("../../{}", path.trim_start_matches("./"))),
        ];
        for candidate in &candidates {
            if candidate.exists() {
                return std::fs::read_to_string(candidate).unwrap_or_else(|e| {
                    panic!("Failed to read Kalshi private key from {:?}: {}", candidate, e)
                });
            }
        }
        panic!(
            "Kalshi private key not found at {} (tried parent dirs too)",
            path
        );
    } else if let Ok(pem) = std::env::var("KALSHI_PRIVATE_KEY_PEM") {
        pem
    } else {
        panic!("Neither KALSHI_PRIVATE_KEY_PATH nor KALSHI_PRIVATE_KEY_PEM is set");
    }
}

fn dotenv() {
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
                            std::env::set_var(key, value);
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
