use crate::{
    Identifier,
    event::{MarketEvent, MarketIter},
    subscription::trade::PublicTrade,
};
use barter_instrument::{Side, exchange::ExchangeId};
use barter_integration::subscription::SubscriptionId;
use chrono::{DateTime, Utc};
use serde::Deserialize;

use super::message::{PolymarketLiveActivity, PolymarketMessage};

impl<InstrumentKey> From<(ExchangeId, InstrumentKey, PolymarketLiveActivity)>
    for MarketIter<InstrumentKey, PublicTrade>
{
    fn from(
        (exchange, instrument, activity): (ExchangeId, InstrumentKey, PolymarketLiveActivity),
    ) -> Self {
        // Only emit for trade events; skip new_order, canceled, etc.
        if activity.event_type != "trade" {
            return Self(vec![]);
        }

        let price = activity
            .price
            .as_deref()
            .and_then(|s| s.parse::<f64>().ok())
            .unwrap_or(0.0);
        let amount = activity
            .size
            .as_deref()
            .and_then(|s| s.parse::<f64>().ok())
            .unwrap_or(0.0);
        let side = match activity.side.as_deref() {
            Some("BUY") => Side::Buy,
            _ => Side::Sell,
        };
        let time = activity
            .timestamp
            .and_then(|ms| DateTime::from_timestamp_millis(ms as i64))
            .unwrap_or_else(Utc::now);
        let id = format!(
            "{}-{}",
            activity.asset_id,
            activity.timestamp.unwrap_or(0)
        );

        Self(vec![Ok(MarketEvent {
            time_exchange: time,
            time_received: Utc::now(),
            exchange,
            instrument,
            kind: PublicTrade {
                id,
                price,
                amount,
                side,
            },
        })])
    }
}

/// Wrapper around `PolymarketMessage` that only routes `LiveActivity` variants
/// to the subscription map. All other message types (book snapshots, price_change
/// deltas, etc.) that Polymarket sends on the same WS connection return `None`
/// from `id()`, causing the `StatelessTransformer` to silently drop them.
#[derive(Clone, PartialEq, Debug, Deserialize)]
#[serde(transparent)]
pub struct PolymarketTradeMessage(pub PolymarketMessage<PolymarketLiveActivity>);

impl Identifier<Option<SubscriptionId>> for PolymarketTradeMessage {
    fn id(&self) -> Option<SubscriptionId> {
        match &self.0 {
            PolymarketMessage::LiveActivity(activity) => activity.id(),
            _ => None,
        }
    }
}

impl<InstrumentKey> From<(ExchangeId, InstrumentKey, PolymarketTradeMessage)>
    for MarketIter<InstrumentKey, PublicTrade>
{
    fn from(
        (exchange, instrument, msg): (ExchangeId, InstrumentKey, PolymarketTradeMessage),
    ) -> Self {
        match msg.0 {
            PolymarketMessage::LiveActivity(activity)
            | PolymarketMessage::Data(activity) => {
                Self::from((exchange, instrument, activity))
            }
            _ => Self(vec![]),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_activity(event_type: &str) -> PolymarketLiveActivity {
        PolymarketLiveActivity {
            event_type: event_type.to_string(),
            asset_id: "0xabc123".to_string(),
            market: Some("0xmarket".to_string()),
            price: Some("0.45".to_string()),
            size: Some("50.0".to_string()),
            side: Some("BUY".to_string()),
            timestamp: Some(1706313600000),
        }
    }

    #[test]
    fn test_trade_event_converts() {
        let activity = make_activity("trade");
        let iter =
            MarketIter::<&str, PublicTrade>::from((ExchangeId::Polymarket, "test", activity));
        let events: Vec<_> = iter.0.into_iter().collect();
        assert_eq!(events.len(), 1);

        let event = events[0].as_ref().unwrap();
        assert_eq!(event.kind.id, "0xabc123-1706313600000");
        assert!((event.kind.price - 0.45).abs() < f64::EPSILON);
        assert!((event.kind.amount - 50.0).abs() < f64::EPSILON);
        assert_eq!(event.kind.side, Side::Buy);
    }

    #[test]
    fn test_non_trade_event_skipped() {
        for event_type in &["new_order", "canceled", "order_fill"] {
            let activity = make_activity(event_type);
            let iter =
                MarketIter::<&str, PublicTrade>::from((ExchangeId::Polymarket, "test", activity));
            assert!(iter.0.is_empty(), "expected empty for {}", event_type);
        }
    }

    #[test]
    fn test_sell_side() {
        let mut activity = make_activity("trade");
        activity.side = Some("SELL".to_string());
        let iter =
            MarketIter::<&str, PublicTrade>::from((ExchangeId::Polymarket, "test", activity));
        let event = iter.0[0].as_ref().unwrap();
        assert_eq!(event.kind.side, Side::Sell);
    }

    #[test]
    fn test_missing_fields_use_defaults() {
        let activity = PolymarketLiveActivity {
            event_type: "trade".to_string(),
            asset_id: "0xtoken".to_string(),
            market: None,
            price: None,
            size: None,
            side: None,
            timestamp: None,
        };
        let iter =
            MarketIter::<&str, PublicTrade>::from((ExchangeId::Polymarket, "test", activity));
        let event = iter.0[0].as_ref().unwrap();
        assert!((event.kind.price - 0.0).abs() < f64::EPSILON);
        assert!((event.kind.amount - 0.0).abs() < f64::EPSILON);
        assert_eq!(event.kind.side, Side::Sell);
        assert_eq!(event.kind.id, "0xtoken-0");
    }

    #[test]
    fn test_trade_message_routes_live_activity() {
        let msg = PolymarketTradeMessage(PolymarketMessage::LiveActivity(make_activity("trade")));
        assert!(msg.id().is_some());
        let iter = MarketIter::<&str, PublicTrade>::from((ExchangeId::Polymarket, "test", msg));
        assert_eq!(iter.0.len(), 1);
        assert!(iter.0[0].is_ok());
    }

    #[test]
    fn test_trade_message_drops_price_book() {
        // PriceBook messages should return None from id() and empty from From
        let msg = PolymarketTradeMessage(PolymarketMessage::Other(
            serde_json::json!({"event_type": "book", "asset_id": "0xabc", "bids": [], "asks": []}),
        ));
        assert!(msg.id().is_none());
        let iter = MarketIter::<&str, PublicTrade>::from((ExchangeId::Polymarket, "test", msg));
        assert!(iter.0.is_empty());
    }

    #[test]
    fn test_trade_message_drops_price_change() {
        let msg = PolymarketTradeMessage(PolymarketMessage::Other(
            serde_json::json!({"event_type": "price_change", "price_changes": []}),
        ));
        assert!(msg.id().is_none());
        let iter = MarketIter::<&str, PublicTrade>::from((ExchangeId::Polymarket, "test", msg));
        assert!(iter.0.is_empty());
    }
}
