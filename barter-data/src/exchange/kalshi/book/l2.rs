use crate::{
    books::OrderBook,
    event::{MarketEvent, MarketIter},
    exchange::kalshi::{channel::KalshiChannel, message::{KalshiMarketLifecycle, KalshiOrderbookSnapshot, KalshiOrderbookDelta, KalshiLevel}},
    subscription::book::OrderBookEvent,
};
use barter_instrument::exchange::ExchangeId;
use barter_integration::subscription::SubscriptionId;
use chrono::Utc;
use derive_more::Constructor;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use smol_str::format_smolstr;

/// Metadata for managing a Kalshi OrderBook L2 stream.
#[derive(Debug, Constructor)]
pub struct KalshiOrderBookL2Meta<InstrumentKey> {
    pub key: InstrumentKey,
    /// Current sequence number for ordering deltas
    pub sequence: u64,
}

/// Internal representation of a Kalshi orderbook for a single market.
///
/// This tracks both YES and NO sides. For arbitrage, we typically treat
/// YES and NO as separate instruments.
#[derive(Clone, PartialEq, Eq, Debug, Default, Deserialize, Serialize)]
pub struct KalshiOrderBook {
    /// YES side levels (price_cents -> quantity)
    pub yes: std::collections::BTreeMap<u32, u32>,
    /// NO side levels (price_cents -> quantity)
    pub no: std::collections::BTreeMap<u32, u32>,
    /// Current sequence number
    pub seq: u64,
    /// Last update timestamp
    pub last_update: Option<String>,
}

impl KalshiOrderBook {
    /// Create a new orderbook from a snapshot.
    pub fn from_snapshot(snapshot: &KalshiOrderbookSnapshot) -> Self {
        let mut yes = std::collections::BTreeMap::new();
        let mut no = std::collections::BTreeMap::new();

        for (price, amount) in &snapshot.msg.yes {
            if *amount > 0 {
                yes.insert(*price, *amount);
            }
        }
        for (price, amount) in &snapshot.msg.no {
            if *amount > 0 {
                no.insert(*price, *amount);
            }
        }

        Self {
            yes,
            no,
            seq: snapshot.seq,
            last_update: None,
        }
    }

    /// Apply a delta update to the orderbook.
    pub fn apply_delta(&mut self, delta: &KalshiOrderbookDelta) {
        let side = match delta.msg.side.as_str() {
            "yes" => &mut self.yes,
            "no" => &mut self.no,
            _ => return,
        };

        let current_size = side.get(&delta.msg.price).copied().unwrap_or(0);
        let new_size = (current_size as i32 + delta.msg.delta).max(0) as u32;

        if new_size == 0 {
            // Remove price level when size reaches zero
            side.remove(&delta.msg.price);
        } else {
            side.insert(delta.msg.price, new_size);
        }

        self.seq = delta.seq;
    }

    /// Clear all levels from the orderbook (used on market lifecycle events).
    pub fn clear(&mut self) {
        self.yes.clear();
        self.no.clear();
    }

    /// Get the best YES bid (highest price with quantity).
    pub fn best_yes_bid(&self) -> Option<KalshiLevel> {
        self.yes.iter().next_back().map(|(&price, &amount)| KalshiLevel { price, amount })
    }

    /// Get the best YES ask (lowest price to buy YES = 100 - best NO bid).
    pub fn best_yes_ask(&self) -> Option<KalshiLevel> {
        // YES ask = 100 - NO bid (inverse relationship)
        self.no.iter().next_back().map(|(&no_bid_price, &amount)| {
            KalshiLevel { price: 100 - no_bid_price, amount }
        })
    }

    /// Get the best NO bid (highest NO price with quantity).
    pub fn best_no_bid(&self) -> Option<KalshiLevel> {
        self.no.iter().next_back().map(|(&price, &amount)| KalshiLevel { price, amount })
    }

    /// Get the best NO ask (lowest price to buy NO = 100 - best YES bid).
    pub fn best_no_ask(&self) -> Option<KalshiLevel> {
        // NO ask = 100 - YES bid (inverse relationship)
        self.yes.iter().next_back().map(|(&yes_bid_price, &amount)| {
            KalshiLevel { price: 100 - yes_bid_price, amount }
        })
    }

    /// Convert to barter OrderBook for the YES side.
    pub fn to_yes_orderbook(&self) -> OrderBook {
        let bids: Vec<_> = self.yes.iter()
            .map(|(&price, &amount)| {
                (Decimal::from(price) / Decimal::from(100), Decimal::from(amount))
            })
            .collect();

        // YES asks = inverse of NO bids
        let asks: Vec<_> = self.no.iter()
            .map(|(&no_bid_price, &amount)| {
                let yes_ask_price = 100 - no_bid_price;
                (Decimal::from(yes_ask_price) / Decimal::from(100), Decimal::from(amount))
            })
            .collect();

        OrderBook::new(self.seq, None, bids, asks)
    }

    /// Convert to barter OrderBook for the NO side.
    pub fn to_no_orderbook(&self) -> OrderBook {
        let bids: Vec<_> = self.no.iter()
            .map(|(&price, &amount)| {
                (Decimal::from(price) / Decimal::from(100), Decimal::from(amount))
            })
            .collect();

        // NO asks = inverse of YES bids
        let asks: Vec<_> = self.yes.iter()
            .map(|(&yes_bid_price, &amount)| {
                let no_ask_price = 100 - yes_bid_price;
                (Decimal::from(no_ask_price) / Decimal::from(100), Decimal::from(amount))
            })
            .collect();

        OrderBook::new(self.seq, None, bids, asks)
    }
}

impl<InstrumentKey> From<(ExchangeId, InstrumentKey, KalshiOrderbookSnapshot)>
    for MarketIter<InstrumentKey, OrderBookEvent>
{
    fn from(
        (exchange, instrument, snapshot): (ExchangeId, InstrumentKey, KalshiOrderbookSnapshot),
    ) -> Self {
        Self(vec![Ok(MarketEvent::from((exchange, instrument, snapshot)))])
    }
}

impl<InstrumentKey> From<(ExchangeId, InstrumentKey, KalshiOrderbookSnapshot)>
    for MarketEvent<InstrumentKey, OrderBookEvent>
{
    fn from(
        (exchange, instrument, snapshot): (ExchangeId, InstrumentKey, KalshiOrderbookSnapshot),
    ) -> Self {
        // Default to YES side orderbook - the subscription determines which outcome to track
        let kalshi_book = KalshiOrderBook::from_snapshot(&snapshot);
        let orderbook = kalshi_book.to_yes_orderbook();

        Self {
            time_exchange: Utc::now(),
            time_received: Utc::now(),
            exchange,
            instrument,
            kind: OrderBookEvent::Snapshot(orderbook),
        }
    }
}

impl<InstrumentKey> From<(ExchangeId, InstrumentKey, KalshiOrderbookSnapshot, bool)>
    for MarketEvent<InstrumentKey, OrderBookEvent>
{
    fn from(
        (exchange, instrument, snapshot, is_yes): (ExchangeId, InstrumentKey, KalshiOrderbookSnapshot, bool),
    ) -> Self {
        let kalshi_book = KalshiOrderBook::from_snapshot(&snapshot);
        let orderbook = if is_yes {
            kalshi_book.to_yes_orderbook()
        } else {
            kalshi_book.to_no_orderbook()
        };

        Self {
            time_exchange: Utc::now(),
            time_received: Utc::now(),
            exchange,
            instrument,
            kind: OrderBookEvent::Snapshot(orderbook),
        }
    }
}

impl<InstrumentKey> From<(ExchangeId, InstrumentKey, KalshiMarketLifecycle)>
    for MarketIter<InstrumentKey, OrderBookEvent>
{
    fn from(
        (exchange, instrument, lifecycle): (ExchangeId, InstrumentKey, KalshiMarketLifecycle),
    ) -> Self {
        Self(vec![Ok(MarketEvent::from((exchange, instrument, lifecycle)))])
    }
}

impl<InstrumentKey> From<(ExchangeId, InstrumentKey, KalshiMarketLifecycle)>
    for MarketEvent<InstrumentKey, OrderBookEvent>
{
    fn from(
        (exchange, instrument, lifecycle): (ExchangeId, InstrumentKey, KalshiMarketLifecycle),
    ) -> Self {
        // Emit an empty orderbook snapshot to clear the book for this market.
        // The strategy skips pairs without orderbook data, so this effectively
        // removes the market from arbitrage monitoring.
        tracing::warn!(
            ticker = %lifecycle.msg.market_ticker,
            event_type = %lifecycle.msg.event_type,
            "Market lifecycle event — clearing orderbook"
        );
        let empty_book = OrderBook::new(lifecycle.seq, None, Vec::<(Decimal, Decimal)>::new(), Vec::<(Decimal, Decimal)>::new());
        Self {
            time_exchange: Utc::now(),
            time_received: Utc::now(),
            exchange,
            instrument,
            kind: OrderBookEvent::Snapshot(empty_book),
        }
    }
}

/// Deserialize a Kalshi market ticker as the associated [`SubscriptionId`].
pub fn de_ob_l2_subscription_id<'de, D>(deserializer: D) -> Result<SubscriptionId, D::Error>
where
    D: serde::de::Deserializer<'de>,
{
    <&str as Deserialize>::deserialize(deserializer)
        .map(|market| SubscriptionId(format_smolstr!("{}|{}", KalshiChannel::ORDER_BOOK_DELTA.as_ref(), market)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exchange::kalshi::message::{KalshiOrderbookSnapshotData, KalshiOrderbookDeltaData};

    fn test_snapshot(yes: Vec<(u32, u32)>, no: Vec<(u32, u32)>, seq: u64) -> KalshiOrderbookSnapshot {
        KalshiOrderbookSnapshot {
            sid: 1,
            seq,
            msg: KalshiOrderbookSnapshotData {
                market_ticker: "TEST".to_string(),
                yes,
                no,
            },
        }
    }

    fn test_delta(price: u32, delta: i32, side: &str, seq: u64) -> KalshiOrderbookDelta {
        KalshiOrderbookDelta {
            sid: 1,
            seq,
            msg: KalshiOrderbookDeltaData {
                market_ticker: "TEST".to_string(),
                price,
                delta,
                side: side.to_string(),
            },
        }
    }

    #[test]
    fn test_kalshi_orderbook_from_snapshot() {
        let snapshot = test_snapshot(
            vec![(40, 100), (39, 200)],
            vec![(60, 150), (61, 250)],
            1,
        );

        let book = KalshiOrderBook::from_snapshot(&snapshot);

        assert_eq!(book.yes.get(&40), Some(&100));
        assert_eq!(book.yes.get(&39), Some(&200));
        assert_eq!(book.no.get(&60), Some(&150));
        assert_eq!(book.no.get(&61), Some(&250));
        assert_eq!(book.seq, 1);
    }

    #[test]
    fn test_kalshi_orderbook_apply_delta() {
        let snapshot = test_snapshot(vec![(40, 100)], vec![(60, 150)], 1);
        let mut book = KalshiOrderBook::from_snapshot(&snapshot);

        // Add to existing level
        book.apply_delta(&test_delta(40, 50, "yes", 2));
        assert_eq!(book.yes.get(&40), Some(&150));

        // Remove from existing level
        book.apply_delta(&test_delta(40, -150, "yes", 3));
        assert_eq!(book.yes.get(&40), None);

        // Add new level
        book.apply_delta(&test_delta(45, 75, "yes", 4));
        assert_eq!(book.yes.get(&45), Some(&75));
    }

    #[test]
    fn test_kalshi_orderbook_best_prices() {
        let snapshot = test_snapshot(
            vec![(40, 100), (39, 200)],  // YES bids at 40c and 39c
            vec![(60, 150), (58, 250)],  // NO bids at 60c and 58c
            1,
        );

        let book = KalshiOrderBook::from_snapshot(&snapshot);

        // Best YES bid = 40c
        let yes_bid = book.best_yes_bid().unwrap();
        assert_eq!(yes_bid.price, 40);

        // Best YES ask = 100 - 60 = 40c (inverse of best NO bid)
        let yes_ask = book.best_yes_ask().unwrap();
        assert_eq!(yes_ask.price, 40);

        // Best NO bid = 60c
        let no_bid = book.best_no_bid().unwrap();
        assert_eq!(no_bid.price, 60);

        // Best NO ask = 100 - 40 = 60c (inverse of best YES bid)
        let no_ask = book.best_no_ask().unwrap();
        assert_eq!(no_ask.price, 60);
    }

    #[test]
    fn test_kalshi_orderbook_clear() {
        let snapshot = test_snapshot(vec![(40, 100)], vec![(60, 150)], 1);
        let mut book = KalshiOrderBook::from_snapshot(&snapshot);
        assert!(!book.yes.is_empty());
        assert!(!book.no.is_empty());

        book.clear();
        assert!(book.yes.is_empty());
        assert!(book.no.is_empty());
        assert!(book.best_yes_bid().is_none());
        assert!(book.best_no_bid().is_none());
    }

    #[test]
    fn test_lifecycle_json_deserializes_as_empty_snapshot() {
        // Lifecycle messages on the same WS connection deserialize as
        // KalshiOrderbookSnapshot with empty yes/no (via #[serde(default)]).
        // This effectively clears the orderbook for that market.
        let lifecycle_json = r#"{
            "type": "market_lifecycle_v2",
            "sid": 1,
            "seq": 4,
            "msg": {
                "market_ticker": "KXBTC-25JAN31-T100000",
                "event_type": "settled"
            }
        }"#;

        let snapshot: KalshiOrderbookSnapshot = serde_json::from_str(lifecycle_json).unwrap();
        assert_eq!(snapshot.msg.market_ticker, "KXBTC-25JAN31-T100000");
        assert!(snapshot.msg.yes.is_empty());
        assert!(snapshot.msg.no.is_empty());

        // When converted to an orderbook, it should be empty
        let book = KalshiOrderBook::from_snapshot(&snapshot);
        assert!(book.yes.is_empty());
        assert!(book.no.is_empty());

        // The YES/NO orderbooks should have no levels
        let yes_book = book.to_yes_orderbook();
        assert!(yes_book.bids().best().is_none());
        assert!(yes_book.asks().best().is_none());
    }
}
