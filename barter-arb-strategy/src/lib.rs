//! Prediction Market Arbitrage Strategy for the Barter Trading Ecosystem
//!
//! This crate provides a prediction market arbitrage strategy that monitors
//! correlated market pairs between Kalshi and Polymarket, detects spread
//! opportunities, and generates simultaneous buy/sell orders.
//!
//! # Architecture
//!
//! The strategy implements barter's `AlgoStrategy` trait, allowing it to
//! integrate seamlessly with the barter trading engine.
//!
//! # Key Components
//!
//! - [`PredictionArbitrageStrategy`]: Core strategy implementing `AlgoStrategy`
//! - [`ArbitrageConfig`]: Configuration for spread thresholds and position limits
//! - [`CorrelatedPair`]: Represents a market pair across platforms
//! - [`ArbitrageOpportunity`]: A detected arbitrage opportunity with profit calculation
//! - [`FeeCalculator`]: Platform-specific fee calculations
//!
//! # Example
//!
//! ```rust,ignore
//! use barter_arb_strategy::{
//!     PredictionArbitrageStrategy, ArbitrageConfig, CorrelatedPair,
//! };
//! use rust_decimal_macros::dec;
//!
//! // Configure the strategy
//! let config = ArbitrageConfig {
//!     min_spread_threshold: dec!(0.02),  // 2% minimum spread after fees
//!     max_position_per_market: 1000,
//!     max_total_capital: dec!(10000),
//!     ..Default::default()
//! };
//!
//! // Define correlated market pairs
//! let pairs = vec![
//!     CorrelatedPair::new(
//!         "KXBTC-25JAN31-T100000",
//!         "0xcondition_id",
//!         "0xyes_token",
//!         "0xno_token",
//!         "BTC > $100k by Jan 31",
//!         chrono::Utc::now() + chrono::Duration::days(30),
//!         false, // not inverse
//!     ),
//! ];
//!
//! // Create the strategy
//! let strategy = PredictionArbitrageStrategy::new(
//!     "pred-arb",
//!     config,
//!     pairs,
//! );
//!
//! // The strategy can now be used with barter's Engine
//! ```
//!
//! # Fee Model
//!
//! The strategy accounts for platform-specific fees:
//!
//! - **Kalshi**: 7% of profit potential: `0.07 * contracts * price * (1 - price)`
//! - **Polymarket**: Configurable taker fee (default 50 basis points = 0.5%)
//!
//! # Market Model (Delta-Neutral)
//!
//! Each correlated pair uses 2 YES orderbooks; NO asks are derived from YES bids:
//!
//! ```text
//! Buy YES on Platform A + Buy NO on Platform B = guaranteed $1 payout
//! Profit = $1.00 - (YES_ask + NO_ask + fees)
//! ```
//!
//! Two directions per pair:
//! - Buy Polymarket YES + Buy Kalshi NO
//! - Buy Kalshi YES + Buy Polymarket NO
//!
//! With `inverse` flag, Kalshi YES/NO perspective is swapped before checking.

pub mod config;
pub mod correlation;
pub mod database;
pub mod fees;
pub mod opportunity;
pub mod recorder;
pub mod risk;
pub mod state;
pub mod strategy;

// Re-exports for convenience
pub use config::{ArbitrageConfig, MinOrderValues};
pub use correlation::{CorrelatedPair, Outcome, PredictionMarketKey};
pub use database::{DatabaseError, DatabaseQuerier, MarketPairFilters, MarketPairRecord};
pub use fees::FeeCalculator;
pub use opportunity::{ArbitrageDirection, ArbitrageOpportunity, OrderSide};
pub use state::{ArbitrageEngineState, ArbitrageGlobalData, ArbitrageInstrumentData, OrderbookLookup};
pub use risk::ArbitrageRiskManager;
pub use strategy::PredictionArbitrageStrategy;
