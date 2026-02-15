use crate::{
    books::OrderBook,
    event::{MarketEvent, MarketIter},
    exchange::polymarket::{channel::PolymarketChannel, message::{PolymarketMessage, PolymarketPriceBook}},
    subscription::book::OrderBookEvent,
};
use barter_instrument::exchange::ExchangeId;
use barter_integration::subscription::SubscriptionId;
use chrono::{DateTime, Utc};
use derive_more::Constructor;
use serde::Deserialize;
use smol_str::format_smolstr;

/// Metadata for managing a Polymarket OrderBook L2 stream.
#[derive(Debug, Constructor)]
pub struct PolymarketOrderBookL2Meta<InstrumentKey> {
    pub key: InstrumentKey,
}

impl<InstrumentKey> From<(ExchangeId, InstrumentKey, PolymarketPriceBook)>
    for MarketIter<InstrumentKey, OrderBookEvent>
{
    fn from(
        (exchange, instrument, price_book): (ExchangeId, InstrumentKey, PolymarketPriceBook),
    ) -> Self {
        Self(vec![Ok(MarketEvent::from((exchange, instrument, price_book)))])
    }
}

impl<InstrumentKey> From<(ExchangeId, InstrumentKey, PolymarketMessage<PolymarketPriceBook>)>
    for MarketIter<InstrumentKey, OrderBookEvent>
{
    fn from(
        (exchange, instrument, message): (ExchangeId, InstrumentKey, PolymarketMessage<PolymarketPriceBook>),
    ) -> Self {
        match message {
            PolymarketMessage::PriceBook(price_book) => {
                Self(vec![Ok(MarketEvent::from((exchange, instrument, price_book)))])
            }
            // Heartbeats, live activity, and other messages are ignored for orderbook streams
            _ => Self(vec![]),
        }
    }
}

impl<InstrumentKey> From<(ExchangeId, InstrumentKey, PolymarketPriceBook)>
    for MarketEvent<InstrumentKey, OrderBookEvent>
{
    fn from(
        (exchange, instrument, price_book): (ExchangeId, InstrumentKey, PolymarketPriceBook),
    ) -> Self {
        let bids: Vec<_> = price_book.bids.iter()
            .filter_map(|level| {
                let price = level.price_decimal()?;
                let size = level.size_decimal()?;
                Some((price, size))
            })
            .collect();

        let asks: Vec<_> = price_book.asks.iter()
            .filter_map(|level| {
                let price = level.price_decimal()?;
                let size = level.size_decimal()?;
                Some((price, size))
            })
            .collect();

        let timestamp = price_book.timestamp
            .and_then(|ts| DateTime::from_timestamp_millis(ts as i64));
        let time_received = Utc::now();

        let orderbook = OrderBook::new(
            price_book.timestamp.unwrap_or(0),
            timestamp,
            bids,
            asks,
        );

        Self {
            time_exchange: timestamp.unwrap_or(time_received),
            time_received,
            exchange,
            instrument,
            kind: OrderBookEvent::Snapshot(orderbook),
        }
    }
}

/// Deserialize a Polymarket asset_id as the associated [`SubscriptionId`].
pub fn de_ob_l2_subscription_id<'de, D>(deserializer: D) -> Result<SubscriptionId, D::Error>
where
    D: serde::de::Deserializer<'de>,
{
    <&str as Deserialize>::deserialize(deserializer)
        .map(|asset_id| SubscriptionId(format_smolstr!("{}|{}", PolymarketChannel::MARKET.as_ref(), asset_id)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exchange::polymarket::message::PolymarketLevel;
    use rust_decimal_macros::dec;

    #[test]
    fn test_price_book_to_market_event() {
        let price_book = PolymarketPriceBook {
            event_type: Some("price_change".to_string()),
            asset_id: "0x1234abcd".to_string(),
            market: Some("0x5678efgh".to_string()),
            timestamp: Some(1706313600000),
            bids: vec![
                PolymarketLevel { price: "0.45".to_string(), size: "100.5".to_string() },
                PolymarketLevel { price: "0.44".to_string(), size: "200.0".to_string() },
            ],
            asks: vec![
                PolymarketLevel { price: "0.46".to_string(), size: "150.0".to_string() },
                PolymarketLevel { price: "0.47".to_string(), size: "250.0".to_string() },
            ],
        };

        let event: MarketEvent<String, OrderBookEvent> =
            (ExchangeId::Polymarket, "test_instrument".to_string(), price_book).into();

        assert_eq!(event.exchange, ExchangeId::Polymarket);
        assert_eq!(event.instrument, "test_instrument");

        match event.kind {
            OrderBookEvent::Snapshot(book) => {
                let bids = book.bids().levels();
                let asks = book.asks().levels();

                // Best bid should be 0.45 (highest)
                assert_eq!(bids.first().unwrap().price, dec!(0.45));
                assert_eq!(bids.first().unwrap().amount, dec!(100.5));

                // Best ask should be 0.46 (lowest)
                assert_eq!(asks.first().unwrap().price, dec!(0.46));
                assert_eq!(asks.first().unwrap().amount, dec!(150.0));
            }
            _ => panic!("Expected Snapshot"),
        }
    }
}
