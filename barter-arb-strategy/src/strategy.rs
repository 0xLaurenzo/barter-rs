//! Delta-neutral prediction market arbitrage strategy.
//!
//! Buy YES on one platform + Buy NO on the other = guaranteed $1 payout.
//! Walks orderbook depth to find maximum profitable fill size.

use crate::{
    config::ArbitrageConfig,
    correlation::{CorrelatedPair, Outcome, PredictionMarketKey},
    fees::FeeCalculator,
    opportunity::{ArbitrageDirection, ArbitrageOpportunity, OrderSide},
    state::ArbitrageEngineState,
};
use barter::engine::Engine;
use barter::engine::state::instrument::filter::InstrumentFilter;
use barter::strategy::algo::AlgoStrategy;
use barter::strategy::close_positions::ClosePositionsStrategy;
use barter::strategy::on_disconnect::OnDisconnectStrategy;
use barter::strategy::on_trading_disabled::OnTradingDisabled;
use barter_data::books::{Level, OrderBook};
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
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;
use std::cell::Cell;
use std::collections::HashMap;
use tracing::{debug, info, warn};

/// Result of walking two orderbook sides simultaneously.
struct WalkResult {
    total_size: u32,
    total_profit: Decimal,
    avg_yes_price: Decimal,
    avg_no_price: Decimal,
    total_fees: Decimal,
    total_cost: Decimal,
}

/// Derive NO ask levels from a YES orderbook's bid side.
///
/// YES bids are sorted high-to-low: \[0.55x100, 0.52x200, 0.50x150\]
/// NO asks derived low-to-high: \[0.45x100, 0.48x200, 0.50x150\]
/// (Already ascending because 1 - descending = ascending.)
fn derive_no_asks(yes_book: &OrderBook) -> Vec<Level> {
    yes_book
        .bids()
        .levels()
        .iter()
        .map(|l| Level::new(Decimal::ONE - l.price, l.amount))
        .collect()
}

/// Walk two orderbook sides simultaneously, maintaining 1:1 contract ratio.
///
/// Accumulates cost per contract: yes_price + no_price + fees.
/// Stops when cost >= $1.00 (no longer profitable).
fn walk_orderbook_levels(
    yes_asks: &[Level],
    no_asks: &[Level],
    yes_platform: ExchangeId,
    no_platform: ExchangeId,
    poly_fee_bps: u32,
) -> WalkResult {
    let mut total_size: u32 = 0;
    let mut total_yes_cost = Decimal::ZERO;
    let mut total_no_cost = Decimal::ZERO;
    let mut total_fees = Decimal::ZERO;
    let mut total_profit = Decimal::ZERO;

    let mut yes_idx: usize = 0;
    let mut no_idx: usize = 0;
    let mut yes_remaining = yes_asks.first().map(|l| l.amount).unwrap_or(Decimal::ZERO);
    let mut no_remaining = no_asks.first().map(|l| l.amount).unwrap_or(Decimal::ZERO);

    while yes_idx < yes_asks.len() && no_idx < no_asks.len() {
        let yes_price = yes_asks[yes_idx].price;
        let no_price = no_asks[no_idx].price;

        let fill_amount = yes_remaining.min(no_remaining);
        let fill_size = fill_amount.to_u32().unwrap_or(0);
        if fill_size == 0 {
            break;
        }

        // Per-fill fees
        let yes_fee = match yes_platform {
            ExchangeId::Kalshi => FeeCalculator::kalshi_taker_fee(yes_price, fill_size),
            ExchangeId::Polymarket => {
                FeeCalculator::polymarket_taker_fee(yes_price, fill_size, poly_fee_bps)
            }
            _ => Decimal::ZERO,
        };
        let no_fee = match no_platform {
            ExchangeId::Kalshi => FeeCalculator::kalshi_taker_fee(no_price, fill_size),
            ExchangeId::Polymarket => {
                FeeCalculator::polymarket_taker_fee(no_price, fill_size, poly_fee_bps)
            }
            _ => Decimal::ZERO,
        };

        let fill_decimal = Decimal::from(fill_size);
        let cost_per_contract = yes_price + no_price + (yes_fee + no_fee) / fill_decimal;

        if cost_per_contract >= Decimal::ONE {
            break;
        }

        total_size += fill_size;
        total_yes_cost += yes_price * fill_decimal;
        total_no_cost += no_price * fill_decimal;
        total_fees += yes_fee + no_fee;
        total_profit += (Decimal::ONE - cost_per_contract) * fill_decimal;

        yes_remaining -= fill_amount;
        no_remaining -= fill_amount;

        if yes_remaining <= Decimal::ZERO {
            yes_idx += 1;
            if yes_idx < yes_asks.len() {
                yes_remaining = yes_asks[yes_idx].amount;
            }
        }
        if no_remaining <= Decimal::ZERO {
            no_idx += 1;
            if no_idx < no_asks.len() {
                no_remaining = no_asks[no_idx].amount;
            }
        }

        if yes_remaining <= Decimal::ZERO && no_remaining <= Decimal::ZERO {
            break;
        }
    }

    let total_decimal = Decimal::from(total_size);
    let (avg_yes_price, avg_no_price, total_cost) = if total_size > 0 {
        let avg_yes = total_yes_cost / total_decimal;
        let avg_no = total_no_cost / total_decimal;
        let avg_fee = total_fees / total_decimal;
        (avg_yes, avg_no, avg_yes + avg_no + avg_fee)
    } else {
        (Decimal::ZERO, Decimal::ZERO, Decimal::ZERO)
    };

    WalkResult {
        total_size,
        total_profit,
        avg_yes_price,
        avg_no_price,
        total_fees,
        total_cost,
    }
}

/// Prediction market arbitrage strategy.
///
/// Monitors correlated market pairs between Kalshi and Polymarket,
/// detects delta-neutral opportunities (buy YES + buy NO = $1 payout),
/// and generates simultaneous BUY orders on both platforms.
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
    /// Counter for generating unique client order IDs
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
    pub fn with_instruments(
        id: impl Into<StrategyId>,
        config: ArbitrageConfig,
        pairs: Vec<CorrelatedPair>,
        indexed: &IndexedInstruments,
    ) -> Self {
        let mut instrument_index = HashMap::new();
        let id = id.into();

        let mut name_to_index: HashMap<
            (ExchangeId, String),
            (ExchangeIndex, InstrumentIndex),
        > = HashMap::new();
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
            if let Some(&indices) = name_to_index.get(&(ExchangeId::Kalshi, kalshi_yes_name)) {
                instrument_index.insert(
                    PredictionMarketKey::kalshi_yes(pair.kalshi_ticker.clone()),
                    indices,
                );
            }

            // Kalshi NO
            let kalshi_no_name = format!("{}_no", pair.kalshi_ticker);
            if let Some(&indices) = name_to_index.get(&(ExchangeId::Kalshi, kalshi_no_name)) {
                instrument_index.insert(
                    PredictionMarketKey::kalshi_no(pair.kalshi_ticker.clone()),
                    indices,
                );
            }

            // Polymarket YES
            if let Some(&indices) = name_to_index
                .get(&(ExchangeId::Polymarket, pair.polymarket_yes_token.to_string()))
            {
                instrument_index.insert(
                    PredictionMarketKey::polymarket_yes(pair.polymarket_yes_token.clone()),
                    indices,
                );
            }

            // Polymarket NO
            if let Some(&indices) = name_to_index
                .get(&(ExchangeId::Polymarket, pair.polymarket_no_token.to_string()))
            {
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
                self.config
                    .max_days_to_expiry
                    .map(|max| pair.days_to_expiry() <= max as i64)
                    .unwrap_or(true)
            })
            .flat_map(|pair| self.check_pair_for_arbitrage(pair, books))
            .collect()
    }

    /// Check a single correlated pair for delta-neutral arbitrage.
    ///
    /// Uses only YES orderbooks; derives NO asks from YES bids.
    /// Checks two directions and applies inverse flag.
    fn check_pair_for_arbitrage(
        &self,
        pair: &CorrelatedPair,
        books: &HashMap<PredictionMarketKey, &OrderBook>,
    ) -> Vec<ArbitrageOpportunity> {
        let mut opportunities = Vec::new();

        let poly_yes_key =
            PredictionMarketKey::polymarket_yes(pair.polymarket_yes_token.clone());
        let kalshi_yes_key = PredictionMarketKey::kalshi_yes(pair.kalshi_ticker.clone());

        let poly_yes_book = match books.get(&poly_yes_key) {
            Some(book) => *book,
            None => return opportunities,
        };
        let kalshi_yes_book = match books.get(&kalshi_yes_key) {
            Some(book) => *book,
            None => return opportunities,
        };

        // Derive NO asks from YES bids
        let poly_no_asks = derive_no_asks(poly_yes_book);
        let kalshi_no_asks = derive_no_asks(kalshi_yes_book);

        if pair.inverse {
            // Inverse: Polymarket YES = Kalshi NO
            // The Kalshi YES book we receive actually represents the "NO" side
            // from Polymarket's perspective.

            // Direction 1: Buy Poly YES + Buy "Kalshi NO"
            // "Kalshi NO" in inverse = original Kalshi YES asks
            let result1 = walk_orderbook_levels(
                poly_yes_book.asks().levels(),
                kalshi_yes_book.asks().levels(),
                ExchangeId::Polymarket,
                ExchangeId::Kalshi,
                self.poly_fee_bps,
            );

            if result1.total_size > 0 && result1.total_profit > Decimal::ZERO {
                opportunities.push(ArbitrageOpportunity {
                    pair: pair.clone(),
                    direction: ArbitrageDirection::YesPolyNoKalshi,
                    yes_side: OrderSide::poly(
                        pair.polymarket_yes_token.clone(),
                        Outcome::Yes,
                        result1.avg_yes_price,
                        result1.total_size,
                    ),
                    no_side: OrderSide::kalshi(
                        pair.kalshi_ticker.clone(),
                        Outcome::Yes, // Buy Kalshi YES contract (= semantic NO in inverse)
                        result1.avg_no_price,
                        result1.total_size,
                    ),
                    total_cost: result1.total_cost,
                    avg_yes_price: result1.avg_yes_price,
                    avg_no_price: result1.avg_no_price,
                    max_contracts: result1.total_size,
                    expected_profit: result1.total_profit,
                    total_fees: result1.total_fees,
                });
            }

            // Direction 2: Buy "Kalshi YES" + Buy Poly NO
            // "Kalshi YES" in inverse = derived from Kalshi YES bids (semantic NO bids)
            let result2 = walk_orderbook_levels(
                &kalshi_no_asks,
                &poly_no_asks,
                ExchangeId::Kalshi,
                ExchangeId::Polymarket,
                self.poly_fee_bps,
            );

            if result2.total_size > 0 && result2.total_profit > Decimal::ZERO {
                opportunities.push(ArbitrageOpportunity {
                    pair: pair.clone(),
                    direction: ArbitrageDirection::YesKalshiNoPoly,
                    yes_side: OrderSide::kalshi(
                        pair.kalshi_ticker.clone(),
                        Outcome::No, // Buy Kalshi NO contract (= semantic YES in inverse)
                        result2.avg_yes_price,
                        result2.total_size,
                    ),
                    no_side: OrderSide::poly(
                        pair.polymarket_no_token.clone(),
                        Outcome::No,
                        result2.avg_no_price,
                        result2.total_size,
                    ),
                    total_cost: result2.total_cost,
                    avg_yes_price: result2.avg_yes_price,
                    avg_no_price: result2.avg_no_price,
                    max_contracts: result2.total_size,
                    expected_profit: result2.total_profit,
                    total_fees: result2.total_fees,
                });
            }
        } else {
            // Non-inverse: standard pairing

            // Direction 1: Buy Poly YES + Buy Kalshi NO
            let result1 = walk_orderbook_levels(
                poly_yes_book.asks().levels(),
                &kalshi_no_asks,
                ExchangeId::Polymarket,
                ExchangeId::Kalshi,
                self.poly_fee_bps,
            );

            if result1.total_size > 0 && result1.total_profit > Decimal::ZERO {
                opportunities.push(ArbitrageOpportunity {
                    pair: pair.clone(),
                    direction: ArbitrageDirection::YesPolyNoKalshi,
                    yes_side: OrderSide::poly(
                        pair.polymarket_yes_token.clone(),
                        Outcome::Yes,
                        result1.avg_yes_price,
                        result1.total_size,
                    ),
                    no_side: OrderSide::kalshi(
                        pair.kalshi_ticker.clone(),
                        Outcome::No,
                        result1.avg_no_price,
                        result1.total_size,
                    ),
                    total_cost: result1.total_cost,
                    avg_yes_price: result1.avg_yes_price,
                    avg_no_price: result1.avg_no_price,
                    max_contracts: result1.total_size,
                    expected_profit: result1.total_profit,
                    total_fees: result1.total_fees,
                });
            }

            // Direction 2: Buy Kalshi YES + Buy Poly NO
            let result2 = walk_orderbook_levels(
                kalshi_yes_book.asks().levels(),
                &poly_no_asks,
                ExchangeId::Kalshi,
                ExchangeId::Polymarket,
                self.poly_fee_bps,
            );

            if result2.total_size > 0 && result2.total_profit > Decimal::ZERO {
                opportunities.push(ArbitrageOpportunity {
                    pair: pair.clone(),
                    direction: ArbitrageDirection::YesKalshiNoPoly,
                    yes_side: OrderSide::kalshi(
                        pair.kalshi_ticker.clone(),
                        Outcome::Yes,
                        result2.avg_yes_price,
                        result2.total_size,
                    ),
                    no_side: OrderSide::poly(
                        pair.polymarket_no_token.clone(),
                        Outcome::No,
                        result2.avg_no_price,
                        result2.total_size,
                    ),
                    total_cost: result2.total_cost,
                    avg_yes_price: result2.avg_yes_price,
                    avg_no_price: result2.avg_no_price,
                    max_contracts: result2.total_size,
                    expected_profit: result2.total_profit,
                    total_fees: result2.total_fees,
                });
            }
        }

        opportunities
    }

    /// Check if an opportunity passes minimum order value requirements.
    fn passes_min_order_values(&self, opp: &ArbitrageOpportunity) -> bool {
        let yes_value = opp.yes_side.order_value();
        let no_value = opp.no_side.order_value();

        let yes_min = match opp.yes_side.exchange {
            ExchangeId::Polymarket => self.config.min_order_value.polymarket,
            ExchangeId::Kalshi => self.config.min_order_value.kalshi,
            _ => Decimal::ZERO,
        };
        let no_min = match opp.no_side.exchange {
            ExchangeId::Polymarket => self.config.min_order_value.polymarket,
            ExchangeId::Kalshi => self.config.min_order_value.kalshi,
            _ => Decimal::ZERO,
        };

        yes_value >= yes_min && no_value >= no_min
    }

    /// Check if an opportunity passes position limits.
    fn passes_position_limits(
        &self,
        opp: &ArbitrageOpportunity,
        _state: &ArbitrageEngineState,
    ) -> bool {
        opp.max_contracts <= self.config.max_position_per_market
    }

    /// Generate a pair of BUY orders for a valid opportunity.
    fn generate_order_pair(
        &self,
        opp: &ArbitrageOpportunity,
    ) -> Vec<OrderRequestOpen<ExchangeIndex, InstrumentIndex>> {
        let yes_indices = self.instrument_index.get(&opp.yes_side.instrument);
        let no_indices = self.instrument_index.get(&opp.no_side.instrument);

        let (yes_exchange, yes_instrument) = match yes_indices {
            Some(&indices) => indices,
            None => {
                warn!(
                    key = %opp.yes_side.instrument,
                    "YES instrument not found in index"
                );
                return vec![];
            }
        };

        let (no_exchange, no_instrument) = match no_indices {
            Some(&indices) => indices,
            None => {
                warn!(
                    key = %opp.no_side.instrument,
                    "NO instrument not found in index"
                );
                return vec![];
            }
        };

        let quantity = Decimal::from(opp.max_contracts);

        let yes_order = OrderRequestOpen {
            key: OrderKey {
                exchange: yes_exchange,
                instrument: yes_instrument,
                strategy: self.id.clone(),
                cid: self.next_order_id(),
            },
            state: RequestOpen {
                side: Side::Buy,
                price: opp.avg_yes_price,
                quantity,
                kind: barter_execution::order::OrderKind::Limit,
                time_in_force: barter_execution::order::TimeInForce::ImmediateOrCancel,
            },
        };

        let no_order = OrderRequestOpen {
            key: OrderKey {
                exchange: no_exchange,
                instrument: no_instrument,
                strategy: self.id.clone(),
                cid: self.next_order_id(),
            },
            state: RequestOpen {
                side: Side::Buy,
                price: opp.avg_no_price,
                quantity,
                kind: barter_execution::order::OrderKind::Limit,
                time_in_force: barter_execution::order::TimeInForce::ImmediateOrCancel,
            },
        };

        vec![yes_order, no_order]
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

        for opp in &valid_opps {
            info!(
                pair = %opp.pair.kalshi_ticker,
                direction = ?opp.direction,
                total_cost = %opp.total_cost,
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
            false,
        )
    }

    fn inverse_pair() -> CorrelatedPair {
        CorrelatedPair::new(
            "KXTEST",
            "0xcondition",
            "0xyes",
            "0xno",
            "Test market (inverse)",
            Utc::now() + chrono::Duration::days(30),
            true,
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
    fn test_derive_no_asks() {
        // YES bids: [0.55x100, 0.52x200, 0.50x150] (high to low)
        let book = OrderBook::new(
            1,
            None,
            vec![
                Level::new(dec!(0.55), dec!(100)),
                Level::new(dec!(0.52), dec!(200)),
                Level::new(dec!(0.50), dec!(150)),
            ],
            vec![Level::new(dec!(0.57), dec!(50))],
        );

        let no_asks = derive_no_asks(&book);
        assert_eq!(no_asks.len(), 3);
        // Should be ascending: 0.45, 0.48, 0.50
        assert_eq!(no_asks[0].price, dec!(0.45));
        assert_eq!(no_asks[0].amount, dec!(100));
        assert_eq!(no_asks[1].price, dec!(0.48));
        assert_eq!(no_asks[1].amount, dec!(200));
        assert_eq!(no_asks[2].price, dec!(0.50));
        assert_eq!(no_asks[2].amount, dec!(150));
    }

    #[test]
    fn test_walk_profitable_levels() {
        // YES asks: 40c x 100
        let yes_asks = vec![Level::new(dec!(0.40), dec!(100))];
        // NO asks: 54c x 100
        let no_asks = vec![Level::new(dec!(0.54), dec!(100))];

        // Cost = 0.40 + 0.54 = 0.94 before fees < 1.00
        let result = walk_orderbook_levels(
            &yes_asks,
            &no_asks,
            ExchangeId::Polymarket,
            ExchangeId::Kalshi,
            50,
        );

        assert!(result.total_size > 0);
        assert!(result.total_profit > Decimal::ZERO);
        assert!(result.total_cost < Decimal::ONE);
    }

    #[test]
    fn test_walk_unprofitable_levels() {
        // YES asks: 55c x 100
        let yes_asks = vec![Level::new(dec!(0.55), dec!(100))];
        // NO asks: 50c x 100
        let no_asks = vec![Level::new(dec!(0.50), dec!(100))];

        // Cost = 0.55 + 0.50 = 1.05 > 1.00 (not profitable even before fees)
        let result = walk_orderbook_levels(
            &yes_asks,
            &no_asks,
            ExchangeId::Polymarket,
            ExchangeId::Kalshi,
            50,
        );

        assert_eq!(result.total_size, 0);
        assert_eq!(result.total_profit, Decimal::ZERO);
    }

    #[test]
    fn test_walk_depth_stops_at_unprofitable() {
        // YES asks: 2 levels - 40c x 50, 48c x 100
        let yes_asks = vec![
            Level::new(dec!(0.40), dec!(50)),
            Level::new(dec!(0.48), dec!(100)),
        ];
        // NO asks: 54c x 200
        let no_asks = vec![Level::new(dec!(0.54), dec!(200))];

        // Level 1: 0.40 + 0.54 = 0.94 + fees < 1.00 → fill 50
        // Level 2: 0.48 + 0.54 = 1.02 + fees > 1.00 → stop
        let result = walk_orderbook_levels(
            &yes_asks,
            &no_asks,
            ExchangeId::Polymarket,
            ExchangeId::Kalshi,
            50,
        );

        assert_eq!(result.total_size, 50);
    }

    #[test]
    fn test_delta_neutral_detection() {
        let strategy = PredictionArbitrageStrategy::new(
            StrategyId::new("test-arb"),
            test_config(),
            vec![test_pair()],
        );

        let pair = test_pair();

        // Poly YES book: ask 40c, bid 38c
        // → Poly NO asks derived from bids: 1-0.38 = 62c
        // Kalshi YES book: ask 48c, bid 55c
        // → Kalshi NO asks derived from bids: 1-0.55 = 45c
        //
        // Direction 1 (Poly YES + Kalshi NO): 0.40 + 0.45 = 0.85 + fees < 1.00
        // Direction 2 (Kalshi YES + Poly NO): 0.48 + 0.62 = 1.10 > 1.00
        let poly_yes_book = OrderBook::new(
            1,
            None,
            vec![Level::new(dec!(0.38), dec!(100))],
            vec![Level::new(dec!(0.40), dec!(100))],
        );
        let kalshi_yes_book = OrderBook::new(
            1,
            None,
            vec![Level::new(dec!(0.55), dec!(100))],
            vec![Level::new(dec!(0.48), dec!(100))],
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

        let opps = strategy.detect_opportunities(&books);

        assert!(!opps.is_empty());
        let opp = &opps[0];
        assert_eq!(opp.direction, ArbitrageDirection::YesPolyNoKalshi);
        assert!(opp.is_profitable());
        assert!(opp.total_cost < Decimal::ONE);
    }

    #[test]
    fn test_inverse_flag_swaps_kalshi() {
        let strategy = PredictionArbitrageStrategy::new(
            StrategyId::new("test-arb"),
            test_config(),
            vec![inverse_pair()],
        );

        let pair = inverse_pair();

        // Kalshi YES book (= semantic NO in inverse): ask 45c, bid 55c
        // Poly YES book: ask 40c, bid 38c
        //
        // With inverse, Direction 1 (Poly YES + "Kalshi NO"):
        // poly_yes_asks(40c) + kalshi_yes_asks(45c) = 0.85 + fees < 1.00 → profitable
        let poly_yes_book = OrderBook::new(
            1,
            None,
            vec![Level::new(dec!(0.38), dec!(100))],
            vec![Level::new(dec!(0.40), dec!(100))],
        );
        let kalshi_yes_book = OrderBook::new(
            1,
            None,
            vec![Level::new(dec!(0.55), dec!(100))],
            vec![Level::new(dec!(0.45), dec!(100))],
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

        let opps = strategy.detect_opportunities(&books);
        assert!(!opps.is_empty());

        let opp = &opps[0];
        assert_eq!(opp.direction, ArbitrageDirection::YesPolyNoKalshi);
        // The NO side should target Kalshi YES contract (inverse)
        assert_eq!(opp.no_side.outcome, Outcome::Yes);
        assert_eq!(opp.no_side.exchange, ExchangeId::Kalshi);
    }

    #[test]
    fn test_both_buy_orders_generated() {
        let strategy = PredictionArbitrageStrategy::new(
            StrategyId::new("test-arb"),
            test_config(),
            vec![test_pair()],
        );

        let pair = test_pair();

        let poly_yes_book = OrderBook::new(
            1,
            None,
            vec![Level::new(dec!(0.55), dec!(100))],
            vec![Level::new(dec!(0.40), dec!(100))],
        );
        let kalshi_yes_book = OrderBook::new(
            1,
            None,
            vec![Level::new(dec!(0.55), dec!(100))],
            vec![Level::new(dec!(0.48), dec!(100))],
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

        let opps = strategy.detect_opportunities(&books);
        assert!(!opps.is_empty());

        let opp = &opps[0];
        // YES side is Polymarket, NO side is Kalshi
        assert_eq!(opp.yes_side.exchange, ExchangeId::Polymarket);
        assert_eq!(opp.yes_side.outcome, Outcome::Yes);
        assert_eq!(opp.no_side.exchange, ExchangeId::Kalshi);
        assert_eq!(opp.no_side.outcome, Outcome::No);
    }

    #[test]
    fn test_min_order_value_filter() {
        let strategy = PredictionArbitrageStrategy::new(
            StrategyId::new("test-arb"),
            test_config(),
            vec![test_pair()],
        );

        // YES side on Poly at 40c with 1 contract = $0.40 < $1 minimum
        let opp = ArbitrageOpportunity {
            pair: test_pair(),
            direction: ArbitrageDirection::YesPolyNoKalshi,
            yes_side: OrderSide::poly("0xyes", Outcome::Yes, dec!(0.40), 1),
            no_side: OrderSide::kalshi("KXTEST", Outcome::No, dec!(0.45), 1),
            total_cost: dec!(0.87),
            avg_yes_price: dec!(0.40),
            avg_no_price: dec!(0.45),
            max_contracts: 1,
            expected_profit: dec!(0.13),
            total_fees: dec!(0.02),
        };

        assert!(!strategy.passes_min_order_values(&opp));
    }
}
