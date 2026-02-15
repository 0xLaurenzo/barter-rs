use super::Kalshi;
use crate::{
    Identifier,
    subscription::{Subscription, book::OrderBooksL2, trade::PublicTrades},
};
use serde::Serialize;

/// Type that defines how to translate a Barter [`Subscription`] into a
/// [`Kalshi`] channel to be subscribed to.
///
/// See docs: <https://trading-api.readme.io/reference/websocket-public-channels>
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Serialize)]
pub struct KalshiChannel(pub &'static str);

impl KalshiChannel {
    /// [`Kalshi`] orderbook delta channel - provides snapshots and incremental updates.
    ///
    /// See docs: <https://trading-api.readme.io/reference/websocket-public-channels>
    pub const ORDER_BOOK_DELTA: Self = Self("orderbook_delta");

    /// [`Kalshi`] real-time trades channel.
    ///
    /// See docs: <https://trading-api.readme.io/reference/websocket-public-channels>
    pub const TRADES: Self = Self("trade");

    /// [`Kalshi`] market lifecycle channel - detect when markets close/settle/determine.
    ///
    /// See docs: <https://trading-api.readme.io/reference/websocket-public-channels>
    pub const MARKET_LIFECYCLE: Self = Self("market_lifecycle_v2");
}

impl<Instrument> Identifier<KalshiChannel> for Subscription<Kalshi, Instrument, PublicTrades> {
    fn id(&self) -> KalshiChannel {
        KalshiChannel::TRADES
    }
}

impl<Instrument> Identifier<KalshiChannel> for Subscription<Kalshi, Instrument, OrderBooksL2> {
    fn id(&self) -> KalshiChannel {
        KalshiChannel::ORDER_BOOK_DELTA
    }
}

impl AsRef<str> for KalshiChannel {
    fn as_ref(&self) -> &str {
        self.0
    }
}
