//! Custom engine state for the arbitrage strategy.

use crate::correlation::PredictionMarketKey;
use barter::engine::{
    Processor,
    state::{EngineState, order::in_flight_recorder::InFlightRequestRecorder},
};
use barter_data::{
    books::OrderBook,
    event::{DataKind, MarketEvent},
    subscription::book::OrderBookEvent,
};
use barter_execution::{
    AccountEvent, AccountEventKind,
    order::request::{OrderRequestCancel, OrderRequestOpen},
};
use barter_instrument::Side;
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tracing::info;

/// Type alias for the arbitrage engine state.
pub type ArbitrageEngineState = EngineState<ArbitrageGlobalData, ArbitrageInstrumentData>;

/// Global data tracked across all instruments.
#[derive(Debug, Default, Clone, Deserialize, Serialize)]
pub struct ArbitrageGlobalData {
    /// Total capital deployed across all positions
    pub total_deployed: Decimal,
    /// Kalshi account balance
    pub kalshi_balance: Decimal,
    /// Polymarket account balance
    pub polymarket_balance: Decimal,
}

impl ArbitrageGlobalData {
    /// Get available capital (not yet deployed).
    pub fn available_capital(&self) -> Decimal {
        self.kalshi_balance + self.polymarket_balance - self.total_deployed
    }

    /// Reserve capital for a new position.
    pub fn reserve_capital(&mut self, amount: Decimal) {
        self.total_deployed += amount;
    }

    /// Release capital when a position is closed.
    pub fn release_capital(&mut self, amount: Decimal) {
        self.total_deployed -= amount;
        if self.total_deployed < Decimal::ZERO {
            self.total_deployed = Decimal::ZERO;
        }
    }
}

/// Per-instrument data for the arbitrage strategy.
#[derive(Debug, Default, Clone, Deserialize, Serialize)]
pub struct ArbitrageInstrumentData {
    /// Latest orderbook for this instrument
    pub orderbook: Option<OrderBook>,
    /// Current position (positive = long, negative = short)
    pub position: i32,
    /// Average entry price
    pub avg_entry: Option<Decimal>,
    /// Total cost basis for the position
    pub cost_basis: Decimal,
}

impl ArbitrageInstrumentData {
    /// Update the orderbook for this instrument.
    pub fn update_orderbook(&mut self, orderbook: OrderBook) {
        self.orderbook = Some(orderbook);
    }

    /// Get the best bid price.
    pub fn best_bid(&self) -> Option<Decimal> {
        self.orderbook
            .as_ref()
            .and_then(|b| b.bids().best())
            .map(|l| l.price)
    }

    /// Get the best ask price.
    pub fn best_ask(&self) -> Option<Decimal> {
        self.orderbook
            .as_ref()
            .and_then(|b| b.asks().best())
            .map(|l| l.price)
    }

    /// Get the mid price.
    pub fn mid_price(&self) -> Option<Decimal> {
        self.orderbook.as_ref().and_then(|b| b.mid_price())
    }

    /// Update position after a fill.
    pub fn update_position(&mut self, quantity: i32, price: Decimal) {
        if quantity == 0 {
            return;
        }

        let new_position = self.position + quantity;

        if self.position == 0 {
            // Opening new position
            self.avg_entry = Some(price);
            self.cost_basis = price * Decimal::from(quantity.abs());
        } else if (self.position > 0) == (quantity > 0) {
            // Adding to existing position
            let old_cost = self.cost_basis;
            let add_cost = price * Decimal::from(quantity.abs());
            self.cost_basis = old_cost + add_cost;
            self.avg_entry = Some(self.cost_basis / Decimal::from(new_position.abs()));
        } else {
            // Reducing or flipping position
            let reduce_by = quantity.abs().min(self.position.abs());
            let remaining = self.position.abs() - reduce_by;

            if remaining == 0 {
                // Position fully closed
                if new_position == 0 {
                    self.avg_entry = None;
                    self.cost_basis = Decimal::ZERO;
                } else {
                    // Position flipped
                    let flip_amount = (quantity.abs() - reduce_by) as u32;
                    self.avg_entry = Some(price);
                    self.cost_basis = price * Decimal::from(flip_amount);
                }
            } else {
                // Partial close
                self.cost_basis = self.avg_entry.unwrap_or(Decimal::ZERO)
                    * Decimal::from(remaining);
            }
        }

        self.position = new_position;
    }

    /// Calculate unrealized P&L.
    pub fn unrealized_pnl(&self) -> Option<Decimal> {
        let current_price = self.mid_price()?;
        let avg_entry = self.avg_entry?;

        if self.position > 0 {
            Some((current_price - avg_entry) * Decimal::from(self.position))
        } else if self.position < 0 {
            Some((avg_entry - current_price) * Decimal::from(-self.position))
        } else {
            Some(Decimal::ZERO)
        }
    }
}

// --- Engine integration trait implementations ---

use barter::engine::state::instrument::data::InstrumentDataState;

impl InstrumentDataState for ArbitrageInstrumentData {
    type MarketEventKind = DataKind;

    fn price(&self) -> Option<Decimal> {
        self.mid_price()
    }
}

impl<InstrumentKey> Processor<&MarketEvent<InstrumentKey, DataKind>>
    for ArbitrageInstrumentData
{
    type Audit = ();

    fn process(&mut self, event: &MarketEvent<InstrumentKey, DataKind>) -> Self::Audit {
        match &event.kind {
            DataKind::OrderBook(book_event) => {
                let book = match book_event {
                    OrderBookEvent::Snapshot(book) => book.clone(),
                    OrderBookEvent::Update(book) => book.clone(),
                };
                self.update_orderbook(book);
            }
            _ => {}
        }
    }
}

impl<ExchangeKey, AssetKey, InstrumentKey> Processor<&AccountEvent<ExchangeKey, AssetKey, InstrumentKey>>
    for ArbitrageInstrumentData
where
    ExchangeKey: std::fmt::Debug,
    InstrumentKey: std::fmt::Debug,
{
    type Audit = ();

    fn process(
        &mut self,
        event: &AccountEvent<ExchangeKey, AssetKey, InstrumentKey>,
    ) -> Self::Audit {
        match &event.kind {
            AccountEventKind::Trade(trade) => {
                let signed_qty = match trade.side {
                    Side::Buy => trade.quantity.to_i32().unwrap_or(0),
                    Side::Sell => -trade.quantity.to_i32().unwrap_or(0),
                };
                info!(
                    instrument = ?trade.instrument,
                    side = ?trade.side,
                    price = %trade.price,
                    quantity = %trade.quantity,
                    fees = %trade.fees.fees,
                    trade_id = %trade.id.0,
                    "Trade fill received"
                );
                self.update_position(signed_qty, trade.price);
            }
            _ => {}
        }
    }
}

impl<ExchangeKey, InstrumentKey> InFlightRequestRecorder<ExchangeKey, InstrumentKey>
    for ArbitrageInstrumentData
where
    ExchangeKey: std::fmt::Debug,
    InstrumentKey: std::fmt::Debug,
{
    fn record_in_flight_cancel(&mut self, req: &OrderRequestCancel<ExchangeKey, InstrumentKey>) {
        info!(
            cid = %req.key.cid,
            exchange = ?req.key.exchange,
            "Cancel request in-flight"
        );
    }

    fn record_in_flight_open(&mut self, req: &OrderRequestOpen<ExchangeKey, InstrumentKey>) {
        info!(
            cid = %req.key.cid,
            exchange = ?req.key.exchange,
            side = ?req.state.side,
            price = %req.state.price,
            quantity = %req.state.quantity,
            "Order request in-flight"
        );
    }
}

impl<InstrumentKey, Kind> Processor<&MarketEvent<InstrumentKey, Kind>>
    for ArbitrageGlobalData
{
    type Audit = ();
    fn process(&mut self, _: &MarketEvent<InstrumentKey, Kind>) -> Self::Audit {}
}

impl<ExchangeKey, AssetKey, InstrumentKey> Processor<&AccountEvent<ExchangeKey, AssetKey, InstrumentKey>>
    for ArbitrageGlobalData
{
    type Audit = ();

    fn process(
        &mut self,
        event: &AccountEvent<ExchangeKey, AssetKey, InstrumentKey>,
    ) -> Self::Audit {
        match &event.kind {
            AccountEventKind::Trade(trade) => {
                // Buy = deploying capital, Sell = releasing capital
                let trade_value = trade.price * trade.quantity.abs();
                match trade.side {
                    Side::Buy => self.reserve_capital(trade_value),
                    Side::Sell => self.release_capital(trade_value),
                }
            }
            AccountEventKind::BalanceSnapshot(balance) => {
                // Update platform-specific balance from polling.
                // We can't distinguish Kalshi vs Poly here since ExchangeKey is generic,
                // but the engine tracks balances in its own asset state anyway.
                let _ = balance;
            }
            _ => {}
        }
    }
}

/// Helper struct to look up orderbooks by prediction market key.
#[derive(Debug, Default)]
pub struct OrderbookLookup {
    /// Map from PredictionMarketKey to orderbook
    books: HashMap<PredictionMarketKey, OrderBook>,
}

impl OrderbookLookup {
    /// Insert or update an orderbook.
    pub fn upsert(&mut self, key: PredictionMarketKey, book: OrderBook) {
        self.books.insert(key, book);
    }

    /// Get an orderbook by key.
    pub fn get(&self, key: &PredictionMarketKey) -> Option<&OrderBook> {
        self.books.get(key)
    }

    /// Remove an orderbook.
    pub fn remove(&mut self, key: &PredictionMarketKey) -> Option<OrderBook> {
        self.books.remove(key)
    }

    /// Get all keys.
    pub fn keys(&self) -> impl Iterator<Item = &PredictionMarketKey> {
        self.books.keys()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    #[test]
    fn test_global_data_capital_management() {
        let mut global = ArbitrageGlobalData {
            total_deployed: Decimal::ZERO,
            kalshi_balance: dec!(5000),
            polymarket_balance: dec!(5000),
        };

        assert_eq!(global.available_capital(), dec!(10000));

        global.reserve_capital(dec!(2000));
        assert_eq!(global.total_deployed, dec!(2000));
        assert_eq!(global.available_capital(), dec!(8000));

        global.release_capital(dec!(1000));
        assert_eq!(global.total_deployed, dec!(1000));
        assert_eq!(global.available_capital(), dec!(9000));
    }

    #[test]
    fn test_instrument_data_position_tracking() {
        let mut data = ArbitrageInstrumentData::default();

        // Open long position
        data.update_position(100, dec!(0.40));
        assert_eq!(data.position, 100);
        assert_eq!(data.avg_entry, Some(dec!(0.40)));
        assert_eq!(data.cost_basis, dec!(40.0));

        // Add to position
        data.update_position(50, dec!(0.50));
        assert_eq!(data.position, 150);
        // New avg = (40 + 25) / 150 = 0.4333...
        assert_eq!(data.cost_basis, dec!(65.0));

        // Partial close
        data.update_position(-50, dec!(0.60));
        assert_eq!(data.position, 100);
    }

    #[test]
    fn test_orderbook_lookup() {
        use barter_data::books::Level;

        let mut lookup = OrderbookLookup::default();

        let key = PredictionMarketKey::kalshi_yes("TEST-MARKET");
        let book = OrderBook::new(
            1,
            None,
            vec![Level::new(dec!(0.45), dec!(100))],
            vec![Level::new(dec!(0.46), dec!(100))],
        );

        lookup.upsert(key.clone(), book);

        assert!(lookup.get(&key).is_some());
        assert!(lookup.get(&PredictionMarketKey::kalshi_no("TEST-MARKET")).is_none());
    }
}
