use crate::Identifier;
use barter_integration::subscription::SubscriptionId;
use rust_decimal::Decimal;
use serde::{Deserialize, Deserializer, Serialize};
use smol_str::format_smolstr;

/// [`Polymarket`](super::Polymarket) message variants that can be received over WebSocket.
///
/// See docs: <https://docs.polymarket.com/#websocket-api>
#[derive(Clone, PartialEq, Debug, Deserialize, Serialize)]
#[serde(untagged)]
pub enum PolymarketMessage<T> {
    /// Price book snapshot/update message
    PriceBook(PolymarketPriceBook),
    /// Live activity (trades) message
    LiveActivity(PolymarketLiveActivity),
    /// Generic data message
    Data(T),
    /// Empty array heartbeat or other unknown messages (must be last for untagged)
    Other(serde_json::Value),
}

impl<T> Identifier<Option<SubscriptionId>> for PolymarketMessage<T>
where
    T: Identifier<Option<SubscriptionId>>,
{
    fn id(&self) -> Option<SubscriptionId> {
        match self {
            Self::PriceBook(book) => book.id(),
            Self::LiveActivity(activity) => activity.id(),
            Self::Data(data) => data.id(),
            Self::Other(_) => None, // Heartbeats/unknown messages have no subscription ID
        }
    }
}

impl Identifier<Option<SubscriptionId>> for PolymarketPriceBook {
    fn id(&self) -> Option<SubscriptionId> {
        Some(SubscriptionId(format_smolstr!(
            "market|{}",
            self.asset_id
        )))
    }
}

impl Identifier<Option<SubscriptionId>> for PolymarketLiveActivity {
    fn id(&self) -> Option<SubscriptionId> {
        Some(SubscriptionId(format_smolstr!(
            "live_activity|{}",
            self.asset_id
        )))
    }
}

/// Polymarket price book message containing orderbook state.
///
/// ### Payload Example
/// ```json
/// {
///   "event_type": "price_change",
///   "asset_id": "0x1234...abcd",
///   "market": "0x5678...efgh",
///   "timestamp": 1706313600000,
///   "bids": [{"price": "0.45", "size": "100.5"}],
///   "asks": [{"price": "0.46", "size": "200.0"}]
/// }
/// ```
#[derive(Clone, PartialEq, Debug, Deserialize, Serialize)]
pub struct PolymarketPriceBook {
    /// Event type: "price_change", "book"
    #[serde(default)]
    pub event_type: Option<String>,
    /// Token ID (specific YES or NO token)
    pub asset_id: String,
    /// Condition ID (the overall market)
    #[serde(default)]
    pub market: Option<String>,
    /// Timestamp in milliseconds (Polymarket sends as string or number)
    #[serde(default, deserialize_with = "de_string_or_u64")]
    pub timestamp: Option<u64>,
    /// Bid levels
    pub bids: Vec<PolymarketLevel>,
    /// Ask levels
    pub asks: Vec<PolymarketLevel>,
}

/// Polymarket live activity message for trades.
///
/// ### Payload Example
/// ```json
/// {
///   "event_type": "trade",
///   "asset_id": "0x1234...abcd",
///   "market": "0x5678...efgh",
///   "price": "0.45",
///   "size": "50.0",
///   "side": "BUY",
///   "timestamp": 1706313600000
/// }
/// ```
#[derive(Clone, PartialEq, Debug, Deserialize, Serialize)]
pub struct PolymarketLiveActivity {
    /// Event type: "trade", "new_order", "canceled", etc.
    pub event_type: String,
    /// Token ID
    pub asset_id: String,
    /// Condition ID (the overall market)
    #[serde(default)]
    pub market: Option<String>,
    /// Trade price (decimal string)
    #[serde(default)]
    pub price: Option<String>,
    /// Trade size (decimal string)
    #[serde(default)]
    pub size: Option<String>,
    /// Trade side: "BUY" or "SELL"
    #[serde(default)]
    pub side: Option<String>,
    /// Timestamp in milliseconds (Polymarket sends as string or number)
    #[serde(default, deserialize_with = "de_string_or_u64")]
    pub timestamp: Option<u64>,
}

/// Polymarket orderbook level.
#[derive(Clone, PartialEq, Debug, Deserialize, Serialize)]
pub struct PolymarketLevel {
    /// Price as decimal string (e.g., "0.45")
    pub price: String,
    /// Size as decimal string (e.g., "100.5")
    pub size: String,
}

impl PolymarketLevel {
    /// Parse price as Decimal.
    pub fn price_decimal(&self) -> Option<Decimal> {
        self.price.parse().ok()
    }

    /// Parse size as Decimal.
    pub fn size_decimal(&self) -> Option<Decimal> {
        self.size.parse().ok()
    }
}

/// Polymarket price_change event wrapping multiple per-asset level changes.
///
/// ### Payload Example
/// ```json
/// {
///   "event_type": "price_change",
///   "market": "0x5f65...",
///   "price_changes": [
///     {
///       "asset_id": "71321...",
///       "price": "0.5",
///       "size": "200",
///       "side": "BUY"
///     }
///   ],
///   "timestamp": "1757908892351"
/// }
/// ```
#[derive(Clone, PartialEq, Debug, Deserialize, Serialize)]
pub struct PolymarketPriceChangeEvent {
    pub event_type: String,
    #[serde(default)]
    pub market: Option<String>,
    pub price_changes: Vec<PolymarketPriceChangeEntry>,
    #[serde(default, deserialize_with = "de_string_or_u64")]
    pub timestamp: Option<u64>,
}

/// A single price level change within a price_change event.
#[derive(Clone, PartialEq, Debug, Deserialize, Serialize)]
pub struct PolymarketPriceChangeEntry {
    pub asset_id: String,
    pub price: String,
    pub size: String,
    pub side: String,
    #[serde(default)]
    pub best_bid: Option<String>,
    #[serde(default)]
    pub best_ask: Option<String>,
}

/// Deserialize a timestamp that may be either a number or a string.
fn de_string_or_u64<'de, D>(deserializer: D) -> Result<Option<u64>, D::Error>
where
    D: Deserializer<'de>,
{
    use serde::de;

    struct StringOrU64Visitor;

    impl<'de> de::Visitor<'de> for StringOrU64Visitor {
        type Value = Option<u64>;

        fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("a u64, a string containing a u64, or null")
        }

        fn visit_u64<E: de::Error>(self, v: u64) -> Result<Self::Value, E> {
            Ok(Some(v))
        }

        fn visit_str<E: de::Error>(self, v: &str) -> Result<Self::Value, E> {
            v.parse::<u64>().map(Some).map_err(de::Error::custom)
        }

        fn visit_none<E: de::Error>(self) -> Result<Self::Value, E> {
            Ok(None)
        }

        fn visit_unit<E: de::Error>(self) -> Result<Self::Value, E> {
            Ok(None)
        }
    }

    deserializer.deserialize_any(StringOrU64Visitor)
}

/// Polymarket generic error message.
#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Deserialize, Serialize)]
pub struct PolymarketError {
    pub message: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    mod de {
        use super::*;

        #[test]
        fn test_polymarket_price_book() {
            let input = r#"
            {
                "event_type": "price_change",
                "asset_id": "0x1234abcd",
                "market": "0x5678efgh",
                "timestamp": 1706313600000,
                "bids": [{"price": "0.45", "size": "100.5"}],
                "asks": [{"price": "0.46", "size": "200.0"}]
            }
            "#;

            let msg: PolymarketMessage<()> = serde_json::from_str(input).unwrap();
            match msg {
                PolymarketMessage::PriceBook(book) => {
                    assert_eq!(book.asset_id, "0x1234abcd");
                    assert_eq!(book.market, Some("0x5678efgh".to_string()));
                    assert_eq!(book.bids.len(), 1);
                    assert_eq!(book.bids[0].price, "0.45");
                    assert_eq!(book.bids[0].size, "100.5");
                    assert_eq!(book.asks.len(), 1);
                    assert_eq!(book.asks[0].price, "0.46");
                    assert_eq!(book.asks[0].size, "200.0");
                }
                _ => panic!("Expected PriceBook"),
            }
        }

        #[test]
        fn test_polymarket_live_activity() {
            let input = r#"
            {
                "event_type": "trade",
                "asset_id": "0x1234abcd",
                "market": "0x5678efgh",
                "price": "0.45",
                "size": "50.0",
                "side": "BUY",
                "timestamp": 1706313600000
            }
            "#;

            let msg: PolymarketMessage<()> = serde_json::from_str(input).unwrap();
            match msg {
                PolymarketMessage::LiveActivity(activity) => {
                    assert_eq!(activity.event_type, "trade");
                    assert_eq!(activity.asset_id, "0x1234abcd");
                    assert_eq!(activity.price, Some("0.45".to_string()));
                    assert_eq!(activity.size, Some("50.0".to_string()));
                    assert_eq!(activity.side, Some("BUY".to_string()));
                }
                _ => panic!("Expected LiveActivity"),
            }
        }

        #[test]
        fn test_polymarket_level_parsing() {
            let level = PolymarketLevel {
                price: "0.456".to_string(),
                size: "123.789".to_string(),
            };

            assert_eq!(
                level.price_decimal(),
                Some(Decimal::from_str_exact("0.456").unwrap())
            );
            assert_eq!(
                level.size_decimal(),
                Some(Decimal::from_str_exact("123.789").unwrap())
            );
        }
    }
}
