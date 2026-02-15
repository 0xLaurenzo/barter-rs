use crate::Identifier;
use barter_integration::subscription::SubscriptionId;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use smol_str::format_smolstr;

/// [`Kalshi`](super::Kalshi) message variants that can be received over WebSocket.
///
/// All Kalshi WS messages use a wrapper format:
/// ```json
/// { "type": "<message_type>", "sid": <sub_id>, "seq": <seq_num>, "msg": { ... } }
/// ```
///
/// See docs: <https://docs.kalshi.com/getting_started/quick_start_websockets>
#[derive(Clone, PartialEq, Debug, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum KalshiMessage<T> {
    /// Orderbook snapshot message
    OrderbookSnapshot(KalshiOrderbookSnapshot),
    /// Orderbook delta (incremental update) message
    OrderbookDelta(KalshiOrderbookDelta),
    /// Trade execution message
    Trade(KalshiTrade),
    /// Market lifecycle event
    #[serde(rename = "market_lifecycle_v2")]
    MarketLifecycle(KalshiMarketLifecycle),
    /// Generic data message
    #[serde(untagged)]
    Data(T),
}

impl<T> Identifier<Option<SubscriptionId>> for KalshiMessage<T>
where
    T: Identifier<Option<SubscriptionId>>,
{
    fn id(&self) -> Option<SubscriptionId> {
        match self {
            Self::OrderbookSnapshot(snapshot) => snapshot.id(),
            Self::OrderbookDelta(delta) => delta.id(),
            Self::Trade(trade) => trade.id(),
            Self::MarketLifecycle(lifecycle) => lifecycle.id(),
            Self::Data(data) => data.id(),
        }
    }
}

impl Identifier<Option<SubscriptionId>> for KalshiOrderbookSnapshot {
    fn id(&self) -> Option<SubscriptionId> {
        // Lowercase to match AssetNameInternal normalization used in subscription map
        Some(SubscriptionId(format_smolstr!(
            "orderbook_delta|{}",
            self.msg.market_ticker.to_lowercase()
        )))
    }
}

impl Identifier<Option<SubscriptionId>> for KalshiOrderbookDelta {
    fn id(&self) -> Option<SubscriptionId> {
        Some(SubscriptionId(format_smolstr!(
            "orderbook_delta|{}",
            self.msg.market_ticker.to_lowercase()
        )))
    }
}

impl Identifier<Option<SubscriptionId>> for KalshiTrade {
    fn id(&self) -> Option<SubscriptionId> {
        Some(SubscriptionId(format_smolstr!(
            "trade|{}",
            self.msg.market_ticker.to_lowercase()
        )))
    }
}

impl Identifier<Option<SubscriptionId>> for KalshiMarketLifecycle {
    fn id(&self) -> Option<SubscriptionId> {
        // Route lifecycle events to the orderbook_delta subscription ID so the
        // transformer can find the instrument and emit an empty-book snapshot,
        // effectively clearing the orderbook when a market closes/settles.
        Some(SubscriptionId(format_smolstr!(
            "orderbook_delta|{}",
            self.msg.market_ticker.to_lowercase()
        )))
    }
}

/// Kalshi orderbook snapshot wrapper.
///
/// ### Raw Payload
/// ```json
/// {
///   "type": "orderbook_snapshot",
///   "sid": 1,
///   "seq": 1,
///   "msg": {
///     "market_ticker": "KXBTC-25JAN31-T100000",
///     "market_id": "",
///     "yes": [[40, 100], [39, 200]],
///     "no": [[60, 150], [61, 250]]
///   }
/// }
/// ```
#[derive(Clone, PartialEq, Eq, Debug, Deserialize, Serialize)]
pub struct KalshiOrderbookSnapshot {
    pub sid: u64,
    pub seq: u64,
    pub msg: KalshiOrderbookSnapshotData,
}

/// Inner data for orderbook snapshot.
#[derive(Clone, PartialEq, Eq, Debug, Deserialize, Serialize)]
pub struct KalshiOrderbookSnapshotData {
    /// Market ticker identifier
    pub market_ticker: String,
    /// YES side orderbook levels: (price_cents, quantity)
    #[serde(default)]
    pub yes: Vec<(u32, u32)>,
    /// NO side orderbook levels: (price_cents, quantity)
    #[serde(default)]
    pub no: Vec<(u32, u32)>,
}

impl KalshiOrderbookSnapshot {
    /// Convenience accessor for market_ticker.
    pub fn market_ticker(&self) -> &str {
        &self.msg.market_ticker
    }
}

/// Kalshi orderbook delta wrapper.
///
/// ### Raw Payload
/// ```json
/// {
///   "type": "orderbook_delta",
///   "sid": 1,
///   "seq": 2,
///   "msg": {
///     "market_ticker": "KXBTC-25JAN31-T100000",
///     "price": 40,
///     "delta": -50,
///     "side": "yes"
///   }
/// }
/// ```
#[derive(Clone, PartialEq, Eq, Debug, Deserialize, Serialize)]
pub struct KalshiOrderbookDelta {
    pub sid: u64,
    pub seq: u64,
    pub msg: KalshiOrderbookDeltaData,
}

/// Inner data for orderbook delta.
#[derive(Clone, PartialEq, Eq, Debug, Deserialize, Serialize)]
pub struct KalshiOrderbookDeltaData {
    /// Market ticker identifier
    pub market_ticker: String,
    /// Price level in cents (1-99)
    pub price: u32,
    /// Quantity change (positive = add, negative = remove)
    pub delta: i32,
    /// Side: "yes" or "no"
    pub side: String,
}

impl KalshiOrderbookDelta {
    /// Convenience accessor for market_ticker.
    pub fn market_ticker(&self) -> &str {
        &self.msg.market_ticker
    }
}

/// Kalshi trade wrapper.
///
/// ### Raw Payload
/// ```json
/// {
///   "type": "trade",
///   "sid": 1,
///   "seq": 3,
///   "msg": {
///     "market_ticker": "KXBTC-25JAN31-T100000",
///     "yes_price": 40,
///     "no_price": 60,
///     "count": 100,
///     "taker_side": "yes"
///   }
/// }
/// ```
#[derive(Clone, PartialEq, Debug, Deserialize, Serialize)]
pub struct KalshiTrade {
    pub sid: u64,
    pub seq: u64,
    pub msg: KalshiTradeData,
}

/// Inner data for trade message.
#[derive(Clone, PartialEq, Debug, Deserialize, Serialize)]
pub struct KalshiTradeData {
    /// Market ticker identifier
    pub market_ticker: String,
    /// YES side price in cents
    pub yes_price: u32,
    /// NO side price in cents
    pub no_price: u32,
    /// Number of contracts traded
    pub count: u32,
    /// Side that took liquidity: "yes" or "no"
    pub taker_side: String,
}

/// Kalshi market lifecycle event wrapper.
///
/// ### Raw Payload
/// ```json
/// {
///   "type": "market_lifecycle_v2",
///   "sid": 1,
///   "seq": 4,
///   "msg": {
///     "market_ticker": "KXBTC-25JAN31-T100000",
///     "event_type": "settled"
///   }
/// }
/// ```
#[derive(Clone, PartialEq, Eq, Debug, Deserialize, Serialize)]
pub struct KalshiMarketLifecycle {
    pub sid: u64,
    pub seq: u64,
    pub msg: KalshiMarketLifecycleData,
}

/// Inner data for market lifecycle event.
#[derive(Clone, PartialEq, Eq, Debug, Deserialize, Serialize)]
pub struct KalshiMarketLifecycleData {
    /// Market ticker identifier
    pub market_ticker: String,
    /// Event type: "closed", "settled", "determined", "deactivated"
    pub event_type: String,
}

/// Kalshi generic error message.
#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Deserialize, Serialize)]
pub struct KalshiError {
    #[serde(alias = "error_message")]
    pub message: String,
}

/// Level in a Kalshi orderbook (price in cents, quantity in contracts).
#[derive(Debug, Copy, Clone, PartialEq, Eq, Ord, PartialOrd, Hash, Default, Deserialize, Serialize)]
pub struct KalshiLevel {
    /// Price in cents (1-99)
    pub price: u32,
    /// Quantity in contracts
    pub amount: u32,
}

impl KalshiLevel {
    /// Convert price from cents (1-99) to decimal (0.01-0.99).
    pub fn price_decimal(&self) -> Decimal {
        Decimal::from(self.price) / Decimal::from(100)
    }

    /// Convert amount to decimal.
    pub fn amount_decimal(&self) -> Decimal {
        Decimal::from(self.amount)
    }
}

impl From<(u32, u32)> for KalshiLevel {
    fn from((price, amount): (u32, u32)) -> Self {
        Self { price, amount }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod de {
        use super::*;

        #[test]
        fn test_kalshi_orderbook_snapshot() {
            let input = r#"
            {
                "type": "orderbook_snapshot",
                "sid": 1,
                "seq": 1,
                "msg": {
                    "market_ticker": "KXBTC-25JAN31-T100000",
                    "market_id": "",
                    "yes": [[40, 100], [39, 200]],
                    "no": [[60, 150], [61, 250]]
                }
            }
            "#;

            let msg: KalshiMessage<()> = serde_json::from_str(input).unwrap();
            match msg {
                KalshiMessage::OrderbookSnapshot(snapshot) => {
                    assert_eq!(snapshot.msg.market_ticker, "KXBTC-25JAN31-T100000");
                    assert_eq!(snapshot.msg.yes, vec![(40, 100), (39, 200)]);
                    assert_eq!(snapshot.msg.no, vec![(60, 150), (61, 250)]);
                    assert_eq!(snapshot.seq, 1);
                    assert_eq!(snapshot.sid, 1);
                }
                _ => panic!("Expected OrderbookSnapshot"),
            }
        }

        #[test]
        fn test_kalshi_orderbook_snapshot_empty() {
            let input = r#"
            {
                "type": "orderbook_snapshot",
                "sid": 1,
                "seq": 1,
                "msg": {
                    "market_ticker": "KXINX-26FEB14-T6055",
                    "market_id": ""
                }
            }
            "#;

            let msg: KalshiMessage<()> = serde_json::from_str(input).unwrap();
            match msg {
                KalshiMessage::OrderbookSnapshot(snapshot) => {
                    assert_eq!(snapshot.msg.market_ticker, "KXINX-26FEB14-T6055");
                    assert!(snapshot.msg.yes.is_empty());
                    assert!(snapshot.msg.no.is_empty());
                }
                _ => panic!("Expected OrderbookSnapshot"),
            }
        }

        #[test]
        fn test_kalshi_orderbook_delta() {
            let input = r#"
            {
                "type": "orderbook_delta",
                "sid": 1,
                "seq": 2,
                "msg": {
                    "market_ticker": "KXBTC-25JAN31-T100000",
                    "price": 40,
                    "delta": -50,
                    "side": "yes"
                }
            }
            "#;

            let msg: KalshiMessage<()> = serde_json::from_str(input).unwrap();
            match msg {
                KalshiMessage::OrderbookDelta(delta) => {
                    assert_eq!(delta.msg.market_ticker, "KXBTC-25JAN31-T100000");
                    assert_eq!(delta.msg.price, 40);
                    assert_eq!(delta.msg.delta, -50);
                    assert_eq!(delta.msg.side, "yes");
                    assert_eq!(delta.seq, 2);
                }
                _ => panic!("Expected OrderbookDelta"),
            }
        }

        #[test]
        fn test_kalshi_trade() {
            let input = r#"
            {
                "type": "trade",
                "sid": 1,
                "seq": 3,
                "msg": {
                    "market_ticker": "KXBTC-25JAN31-T100000",
                    "yes_price": 40,
                    "no_price": 60,
                    "count": 100,
                    "taker_side": "yes"
                }
            }
            "#;

            let msg: KalshiMessage<()> = serde_json::from_str(input).unwrap();
            match msg {
                KalshiMessage::Trade(trade) => {
                    assert_eq!(trade.msg.market_ticker, "KXBTC-25JAN31-T100000");
                    assert_eq!(trade.msg.yes_price, 40);
                    assert_eq!(trade.msg.no_price, 60);
                    assert_eq!(trade.msg.count, 100);
                    assert_eq!(trade.msg.taker_side, "yes");
                }
                _ => panic!("Expected Trade"),
            }
        }

        #[test]
        fn test_kalshi_market_lifecycle() {
            let input = r#"
            {
                "type": "market_lifecycle_v2",
                "sid": 1,
                "seq": 4,
                "msg": {
                    "market_ticker": "KXBTC-25JAN31-T100000",
                    "event_type": "settled"
                }
            }
            "#;

            let msg: KalshiMessage<()> = serde_json::from_str(input).unwrap();
            match msg {
                KalshiMessage::MarketLifecycle(lifecycle) => {
                    assert_eq!(lifecycle.msg.market_ticker, "KXBTC-25JAN31-T100000");
                    assert_eq!(lifecycle.msg.event_type, "settled");
                }
                _ => panic!("Expected MarketLifecycle"),
            }
        }
    }
}
