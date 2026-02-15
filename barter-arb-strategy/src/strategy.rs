//! Core arbitrage strategy implementation using barter's AlgoStrategy interface.

use crate::{
    config::ArbitrageConfig,
    correlation::{CorrelatedPair, Outcome, PredictionMarketKey},
    opportunity::{ArbitrageDirection, ArbitrageOpportunity, OrderSide},
    state::ArbitrageEngineState,
};
use barter::engine::Engine;
use barter::engine::state::instrument::filter::InstrumentFilter;
use barter::strategy::algo::AlgoStrategy;
use barter::strategy::close_positions::ClosePositionsStrategy;
use barter::strategy::on_disconnect::OnDisconnectStrategy;
use barter::strategy::on_trading_disabled::OnTradingDisabled;
use barter_data::books::OrderBook;
use barter_execution::order::{
    OrderKey,
    id::{ClientOrderId, StrategyId},
    request::{OrderRequestCancel, OrderRequestOpen, RequestOpen},
};
use barter_instrument::{
    Side,
    asset::AssetIndex,
    exchange::{ExchangeId, ExchangeIndex},
    index::IndexedInstruments,
    instrument::InstrumentIndex,
};
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;
use std::cell::Cell;
use std::collections::HashMap;
use tracing::{debug, info, warn};

/// Prediction market arbitrage strategy.
///
/// Monitors correlated market pairs between Kalshi and Polymarket,
/// detects spread opportunities, and generates simultaneous buy/sell orders.
#[derive(Debug, Clone)]
pub struct PredictionArbitrageStrategy {
    /// Unique identifier for this strategy
    pub id: StrategyId,
    /// Configuration for the strategy
    pub config: ArbitrageConfig,
    /// Correlated market pairs to monitor
    pub pairs: Vec<CorrelatedPair>,
    /// Polymarket fee in basis points (default 50 = 0.5%)
    pub poly_fee_bps: u32,
    /// Map from PredictionMarketKey to (ExchangeIndex, InstrumentIndex) for order generation
    instrument_index: HashMap<PredictionMarketKey, (ExchangeIndex, InstrumentIndex)>,
    /// Counter for generating unique client order IDs (Cell for &self access in trait method)
    order_counter: Cell<u64>,
}

impl PredictionArbitrageStrategy {
    /// Create a new arbitrage strategy (without engine instrument mapping).
    pub fn new(
        id: impl Into<StrategyId>,
        config: ArbitrageConfig,
        pairs: Vec<CorrelatedPair>,
    ) -> Self {
        Self {
            id: id.into(),
            config,
            pairs,
            poly_fee_bps: 50,
            instrument_index: HashMap::new(),
            order_counter: Cell::new(0),
        }
    }

    /// Create a strategy with instrument index mapping for engine integration.
    ///
    /// Populates `instrument_index` by matching each pair's market keys to the
    /// indexed instruments. Expects instruments named as:
    /// - Kalshi: `"{ticker}_yes"` / `"{ticker}_no"`
    /// - Polymarket: `"{token_id}"` (YES and NO tokens are separate instruments)
    pub fn with_instruments(
        id: impl Into<StrategyId>,
        config: ArbitrageConfig,
        pairs: Vec<CorrelatedPair>,
        indexed: &IndexedInstruments,
    ) -> Self {
        let mut instrument_index = HashMap::new();
        let id = id.into();

        // Build a lookup from exchange instrument name → (ExchangeIndex, InstrumentIndex)
        let mut name_to_index: HashMap<(barter_instrument::exchange::ExchangeId, String), (ExchangeIndex, InstrumentIndex)> = HashMap::new();
        for keyed_instrument in indexed.instruments() {
            let exchange_id = keyed_instrument.value.exchange.value;
            let name = keyed_instrument.value.name_exchange.to_string();
            name_to_index.insert(
                (exchange_id, name),
                (keyed_instrument.value.exchange.key, keyed_instrument.key),
            );
        }

        for pair in &pairs {
            // Kalshi YES
            let kalshi_yes_name = format!("{}_yes", pair.kalshi_ticker);
            if let Some(&indices) = name_to_index.get(&(barter_instrument::exchange::ExchangeId::Kalshi, kalshi_yes_name)) {
                instrument_index.insert(
                    PredictionMarketKey::kalshi_yes(pair.kalshi_ticker.clone()),
                    indices,
                );
            }

            // Kalshi NO
            let kalshi_no_name = format!("{}_no", pair.kalshi_ticker);
            if let Some(&indices) = name_to_index.get(&(barter_instrument::exchange::ExchangeId::Kalshi, kalshi_no_name)) {
                instrument_index.insert(
                    PredictionMarketKey::kalshi_no(pair.kalshi_ticker.clone()),
                    indices,
                );
            }

            // Polymarket YES (name_exchange = token_id)
            if let Some(&indices) = name_to_index.get(&(barter_instrument::exchange::ExchangeId::Polymarket, pair.polymarket_yes_token.to_string())) {
                instrument_index.insert(
                    PredictionMarketKey::polymarket_yes(pair.polymarket_yes_token.clone()),
                    indices,
                );
            }

            // Polymarket NO
            if let Some(&indices) = name_to_index.get(&(barter_instrument::exchange::ExchangeId::Polymarket, pair.polymarket_no_token.to_string())) {
                instrument_index.insert(
                    PredictionMarketKey::polymarket_no(pair.polymarket_no_token.clone()),
                    indices,
                );
            }
        }

        Self {
            id,
            config,
            pairs,
            poly_fee_bps: 50,
            instrument_index,
            order_counter: Cell::new(0),
        }
    }

    /// Build a map of orderbooks from engine state using instrument_index.
    fn build_book_map<'a>(
        &self,
        state: &'a ArbitrageEngineState,
    ) -> HashMap<PredictionMarketKey, &'a OrderBook> {
        let mut books = HashMap::new();
        for (key, (_, inst_idx)) in &self.instrument_index {
            if let Some(book) = &state.instruments.instrument_index(inst_idx).data.orderbook {
                books.insert(key.clone(), book);
            }
        }
        books
    }

    /// Generate a unique client order ID.
    fn next_order_id(&self) -> ClientOrderId {
        let id = self.order_counter.get() + 1;
        self.order_counter.set(id);
        ClientOrderId::new(format!("{}_{}", self.id.0.as_str(), id))
    }

    /// Detect arbitrage opportunities across all monitored pairs.
    pub fn detect_opportunities(
        &self,
        books: &HashMap<PredictionMarketKey, &OrderBook>,
    ) -> Vec<ArbitrageOpportunity> {
        self.pairs
            .iter()
            .filter(|pair| !pair.is_expired())
            .filter(|pair| {
                self.config.max_days_to_expiry
                    .map(|max| pair.days_to_expiry() <= max as i64)
                    .unwrap_or(true)
            })
            .flat_map(|pair| self.check_pair_for_arbitrage(pair, books))
            .collect()
    }

    /// Check a single correlated pair for arbitrage opportunities.
    fn check_pair_for_arbitrage(
        &self,
        pair: &CorrelatedPair,
        books: &HashMap<PredictionMarketKey, &OrderBook>,
    ) -> Vec<ArbitrageOpportunity> {
        let mut opportunities = Vec::new();

        // Get all 4 orderbooks for this pair
        let kalshi_yes_key = PredictionMarketKey::kalshi_yes(pair.kalshi_ticker.clone());
        let kalshi_no_key = PredictionMarketKey::kalshi_no(pair.kalshi_ticker.clone());
        let poly_yes_key = PredictionMarketKey::polymarket_yes(pair.polymarket_yes_token.clone());
        let poly_no_key = PredictionMarketKey::polymarket_no(pair.polymarket_no_token.clone());

        let kalshi_yes = books.get(&kalshi_yes_key).copied();
        let kalshi_no = books.get(&kalshi_no_key).copied();
        let poly_yes = books.get(&poly_yes_key).copied();
        let poly_no = books.get(&poly_no_key).copied();

        // Check YES arbitrage: buy Poly YES, sell Kalshi YES
        if let (Some(poly_book), Some(kalshi_book)) = (poly_yes, kalshi_yes) {
            if let (Some(poly_ask), Some(kalshi_bid)) = (
                poly_book.asks().best(),
                kalshi_book.bids().best(),
            ) {
                let spread = kalshi_bid.price - poly_ask.price;
                if spread > Decimal::ZERO {
                    let buy_side = OrderSide::poly_buy(
                        pair.polymarket_yes_token.clone(),
                        Outcome::Yes,
                        poly_ask.price,
                        poly_ask.amount.to_u32().unwrap_or(0),
                    );
                    let sell_side = OrderSide::kalshi_sell(
                        pair.kalshi_ticker.clone(),
                        Outcome::Yes,
                        kalshi_bid.price,
                        kalshi_bid.amount.to_u32().unwrap_or(0),
                    );

                    opportunities.push(ArbitrageOpportunity::new(
                        pair.clone(),
                        ArbitrageDirection::PolyToKalshi,
                        Outcome::Yes,
                        buy_side,
                        sell_side,
                        self.poly_fee_bps,
                    ));
                }
            }
        }

        // Check YES arbitrage: buy Kalshi YES, sell Poly YES
        if let (Some(kalshi_book), Some(poly_book)) = (kalshi_yes, poly_yes) {
            if let (Some(kalshi_ask), Some(poly_bid)) = (
                kalshi_book.asks().best(),
                poly_book.bids().best(),
            ) {
                let spread = poly_bid.price - kalshi_ask.price;
                if spread > Decimal::ZERO {
                    let buy_side = OrderSide::kalshi_buy(
                        pair.kalshi_ticker.clone(),
                        Outcome::Yes,
                        kalshi_ask.price,
                        kalshi_ask.amount.to_u32().unwrap_or(0),
                    );
                    let sell_side = OrderSide::poly_sell(
                        pair.polymarket_yes_token.clone(),
                        Outcome::Yes,
                        poly_bid.price,
                        poly_bid.amount.to_u32().unwrap_or(0),
                    );

                    opportunities.push(ArbitrageOpportunity::new(
                        pair.clone(),
                        ArbitrageDirection::KalshiToPoly,
                        Outcome::Yes,
                        buy_side,
                        sell_side,
                        self.poly_fee_bps,
                    ));
                }
            }
        }

        // Check NO arbitrage: buy Poly NO, sell Kalshi NO
        if let (Some(poly_book), Some(kalshi_book)) = (poly_no, kalshi_no) {
            if let (Some(poly_ask), Some(kalshi_bid)) = (
                poly_book.asks().best(),
                kalshi_book.bids().best(),
            ) {
                let spread = kalshi_bid.price - poly_ask.price;
                if spread > Decimal::ZERO {
                    let buy_side = OrderSide::poly_buy(
                        pair.polymarket_no_token.clone(),
                        Outcome::No,
                        poly_ask.price,
                        poly_ask.amount.to_u32().unwrap_or(0),
                    );
                    let sell_side = OrderSide::kalshi_sell(
                        pair.kalshi_ticker.clone(),
                        Outcome::No,
                        kalshi_bid.price,
                        kalshi_bid.amount.to_u32().unwrap_or(0),
                    );

                    opportunities.push(ArbitrageOpportunity::new(
                        pair.clone(),
                        ArbitrageDirection::PolyToKalshi,
                        Outcome::No,
                        buy_side,
                        sell_side,
                        self.poly_fee_bps,
                    ));
                }
            }
        }

        // Check NO arbitrage: buy Kalshi NO, sell Poly NO
        if let (Some(kalshi_book), Some(poly_book)) = (kalshi_no, poly_no) {
            if let (Some(kalshi_ask), Some(poly_bid)) = (
                kalshi_book.asks().best(),
                poly_book.bids().best(),
            ) {
                let spread = poly_bid.price - kalshi_ask.price;
                if spread > Decimal::ZERO {
                    let buy_side = OrderSide::kalshi_buy(
                        pair.kalshi_ticker.clone(),
                        Outcome::No,
                        kalshi_ask.price,
                        kalshi_ask.amount.to_u32().unwrap_or(0),
                    );
                    let sell_side = OrderSide::poly_sell(
                        pair.polymarket_no_token.clone(),
                        Outcome::No,
                        poly_bid.price,
                        poly_bid.amount.to_u32().unwrap_or(0),
                    );

                    opportunities.push(ArbitrageOpportunity::new(
                        pair.clone(),
                        ArbitrageDirection::KalshiToPoly,
                        Outcome::No,
                        buy_side,
                        sell_side,
                        self.poly_fee_bps,
                    ));
                }
            }
        }

        opportunities
    }

    /// Check if an opportunity passes minimum order value requirements.
    fn passes_min_order_values(&self, opp: &ArbitrageOpportunity) -> bool {
        let buy_value = opp.buy_side.order_value();
        let sell_value = opp.sell_side.order_value();

        let (buy_min, sell_min) = match opp.direction {
            ArbitrageDirection::PolyToKalshi => {
                (self.config.min_order_value.polymarket, self.config.min_order_value.kalshi)
            }
            ArbitrageDirection::KalshiToPoly => {
                (self.config.min_order_value.kalshi, self.config.min_order_value.polymarket)
            }
        };

        buy_value >= buy_min && sell_value >= sell_min
    }

    /// Check if an opportunity passes position limits.
    fn passes_position_limits(&self, opp: &ArbitrageOpportunity, _state: &ArbitrageEngineState) -> bool {
        opp.max_contracts <= self.config.max_position_per_market
    }

    /// Generate a pair of orders (buy + sell) for a valid opportunity.
    fn generate_order_pair(
        &self,
        opp: &ArbitrageOpportunity,
    ) -> Vec<OrderRequestOpen<ExchangeIndex, InstrumentIndex>> {
        let buy_indices = self.instrument_index.get(&opp.buy_side.instrument);
        let sell_indices = self.instrument_index.get(&opp.sell_side.instrument);

        let (buy_exchange, buy_instrument) = match buy_indices {
            Some(&indices) => indices,
            None => {
                warn!(
                    key = %opp.buy_side.instrument,
                    "Buy instrument not found in index"
                );
                return vec![];
            }
        };

        let (sell_exchange, sell_instrument) = match sell_indices {
            Some(&indices) => indices,
            None => {
                warn!(
                    key = %opp.sell_side.instrument,
                    "Sell instrument not found in index"
                );
                return vec![];
            }
        };

        let quantity = Decimal::from(opp.max_contracts);

        let buy_order = OrderRequestOpen {
            key: OrderKey {
                exchange: buy_exchange,
                instrument: buy_instrument,
                strategy: self.id.clone(),
                cid: self.next_order_id(),
            },
            state: RequestOpen {
                side: Side::Buy,
                price: opp.buy_side.price,
                quantity,
                kind: barter_execution::order::OrderKind::Limit,
                time_in_force: barter_execution::order::TimeInForce::ImmediateOrCancel,
            },
        };

        let sell_order = OrderRequestOpen {
            key: OrderKey {
                exchange: sell_exchange,
                instrument: sell_instrument,
                strategy: self.id.clone(),
                cid: self.next_order_id(),
            },
            state: RequestOpen {
                side: Side::Sell,
                price: opp.sell_side.price,
                quantity,
                kind: barter_execution::order::OrderKind::Limit,
                time_in_force: barter_execution::order::TimeInForce::ImmediateOrCancel,
            },
        };

        vec![buy_order, sell_order]
    }
}

impl AlgoStrategy<ExchangeIndex, InstrumentIndex> for PredictionArbitrageStrategy {
    type State = ArbitrageEngineState;

    fn generate_algo_orders(
        &self,
        state: &Self::State,
    ) -> (
        impl IntoIterator<Item = OrderRequestCancel<ExchangeIndex, InstrumentIndex>>,
        impl IntoIterator<Item = OrderRequestOpen<ExchangeIndex, InstrumentIndex>>,
    ) {
        let books = self.build_book_map(state);
        debug!(
            books = books.len(),
            instruments = self.instrument_index.len(),
            "Strategy scanning for opportunities"
        );
        let opportunities = self.detect_opportunities(&books);

        let valid_opps: Vec<_> = opportunities
            .into_iter()
            .filter(|opp| opp.meets_threshold(self.config.min_spread_threshold))
            .filter(|opp| opp.is_profitable())
            .filter(|opp| self.passes_position_limits(opp, state))
            .filter(|opp| self.passes_min_order_values(opp))
            .collect();

        // Log opportunities found
        for opp in &valid_opps {
            info!(
                pair = %opp.pair.kalshi_ticker,
                direction = ?opp.direction,
                outcome = ?opp.outcome,
                spread = %opp.spread_after_fees,
                contracts = opp.max_contracts,
                profit = %opp.expected_profit,
                "Arbitrage opportunity detected"
            );
        }

        let cancels: Vec<OrderRequestCancel<ExchangeIndex, InstrumentIndex>> = Vec::new();

        let opens: Vec<OrderRequestOpen<ExchangeIndex, InstrumentIndex>> = valid_opps
            .iter()
            .flat_map(|opp| self.generate_order_pair(opp))
            .collect();

        (cancels, opens)
    }
}

impl<Clock, State, ExecutionTxs, Risk> OnTradingDisabled<Clock, State, ExecutionTxs, Risk>
    for PredictionArbitrageStrategy
{
    type OnTradingDisabled = ();

    fn on_trading_disabled(
        _engine: &mut Engine<Clock, State, ExecutionTxs, Self, Risk>,
    ) -> Self::OnTradingDisabled {
        // Arbitrage strategy uses IOC orders so no resting orders to cancel
    }
}

impl<Clock, State, ExecutionTxs, Risk> OnDisconnectStrategy<Clock, State, ExecutionTxs, Risk>
    for PredictionArbitrageStrategy
{
    type OnDisconnect = ();

    fn on_disconnect(
        _engine: &mut Engine<Clock, State, ExecutionTxs, Self, Risk>,
        _exchange: ExchangeId,
    ) -> Self::OnDisconnect {
        // No resting orders to manage on disconnect
    }
}

impl ClosePositionsStrategy for PredictionArbitrageStrategy {
    type State = ArbitrageEngineState;

    fn close_positions_requests<'a>(
        &'a self,
        _state: &'a Self::State,
        _filter: &'a InstrumentFilter,
    ) -> (
        impl IntoIterator<Item = OrderRequestCancel<ExchangeIndex, InstrumentIndex>> + 'a,
        impl IntoIterator<Item = OrderRequestOpen<ExchangeIndex, InstrumentIndex>> + 'a,
    )
    where
        ExchangeIndex: 'a,
        AssetIndex: 'a,
        InstrumentIndex: 'a,
    {
        // Arbitrage positions are held to expiry, not actively closed
        (std::iter::empty(), std::iter::empty())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use barter_data::books::Level;
    use chrono::Utc;
    use rust_decimal_macros::dec;

    fn test_config() -> ArbitrageConfig {
        ArbitrageConfig {
            min_spread_threshold: dec!(0.02),
            max_position_per_market: 1000,
            max_total_capital: dec!(10000),
            min_order_value: crate::config::MinOrderValues {
                kalshi: Decimal::ZERO,
                polymarket: dec!(1),
            },
            max_days_to_expiry: Some(90),
        }
    }

    fn test_pair() -> CorrelatedPair {
        CorrelatedPair::new(
            "KXTEST",
            "0xcondition",
            "0xyes",
            "0xno",
            "Test market",
            Utc::now() + chrono::Duration::days(30),
        )
    }

    #[test]
    fn test_strategy_creation() {
        let strategy = PredictionArbitrageStrategy::new(
            StrategyId::new("test-arb"),
            test_config(),
            vec![test_pair()],
        );

        assert_eq!(strategy.pairs.len(), 1);
        assert_eq!(strategy.poly_fee_bps, 50);
    }

    #[test]
    fn test_opportunity_detection() {
        let strategy = PredictionArbitrageStrategy::new(
            StrategyId::new("test-arb"),
            test_config(),
            vec![test_pair()],
        );

        let pair = test_pair();

        // Set up orderbooks with an arbitrage spread
        // Poly YES ask at 40c, Kalshi YES bid at 46c = 6c spread
        let poly_yes_book = OrderBook::new(
            1, None,
            vec![Level::new(dec!(0.38), dec!(100))], // bids
            vec![Level::new(dec!(0.40), dec!(100))], // asks
        );
        let kalshi_yes_book = OrderBook::new(
            1, None,
            vec![Level::new(dec!(0.46), dec!(100))], // bids
            vec![Level::new(dec!(0.48), dec!(100))], // asks
        );

        let mut books: HashMap<PredictionMarketKey, &OrderBook> = HashMap::new();
        books.insert(
            PredictionMarketKey::polymarket_yes(pair.polymarket_yes_token.clone()),
            &poly_yes_book,
        );
        books.insert(
            PredictionMarketKey::kalshi_yes(pair.kalshi_ticker.clone()),
            &kalshi_yes_book,
        );

        let opportunities = strategy.detect_opportunities(&books);

        // Should find at least one opportunity (Poly -> Kalshi YES)
        assert!(!opportunities.is_empty());

        let opp = &opportunities[0];
        assert_eq!(opp.direction, ArbitrageDirection::PolyToKalshi);
        assert_eq!(opp.outcome, Outcome::Yes);
        assert_eq!(opp.spread_before_fees, dec!(0.06));
        assert!(opp.is_profitable());
    }

    #[test]
    fn test_min_order_value_filter() {
        let strategy = PredictionArbitrageStrategy::new(
            StrategyId::new("test-arb"),
            test_config(),
            vec![test_pair()],
        );

        let pair = test_pair();

        // Create opportunity with very small size (should fail Polymarket minimum)
        let buy_side = OrderSide::poly_buy("0xyes", Outcome::Yes, dec!(0.40), 1); // $0.40 value
        let sell_side = OrderSide::kalshi_sell("KXTEST", Outcome::Yes, dec!(0.46), 100);

        let opp = ArbitrageOpportunity::new(
            pair,
            ArbitrageDirection::PolyToKalshi,
            Outcome::Yes,
            buy_side,
            sell_side,
            50,
        );

        // Should fail because Polymarket order value ($0.40) < minimum ($1.00)
        assert!(!strategy.passes_min_order_values(&opp));
    }
}
