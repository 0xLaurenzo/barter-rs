use super::Polymarket;
use crate::{
    Identifier,
    subscription::{Subscription, book::OrderBooksL2, trade::PublicTrades},
};
use serde::Serialize;

/// Type that defines how to translate a Barter [`Subscription`] into a
/// [`Polymarket`] channel to be subscribed to.
///
/// See docs: <https://docs.polymarket.com/#websocket-api>
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Serialize)]
pub struct PolymarketChannel(pub &'static str);

impl PolymarketChannel {
    /// [`Polymarket`] market channel for orderbook price level updates.
    ///
    /// The "market" type provides book snapshots and price changes.
    /// See docs: <https://docs.polymarket.com/#websocket-api>
    pub const MARKET: Self = Self("market");

    /// [`Polymarket`] live activity (trades, orders).
    ///
    /// See docs: <https://docs.polymarket.com/#websocket-api>
    pub const LIVE_ACTIVITY: Self = Self("live_activity");

    /// Alias for MARKET channel (legacy name).
    #[deprecated(note = "Use MARKET instead")]
    pub const PRICE_BOOK: Self = Self("market");

    /// [`Polymarket`] user channel for authenticated order updates.
    ///
    /// See docs: <https://docs.polymarket.com/#websocket-api>
    pub const USER: Self = Self("user");
}

impl<Instrument> Identifier<PolymarketChannel> for Subscription<Polymarket, Instrument, PublicTrades> {
    fn id(&self) -> PolymarketChannel {
        PolymarketChannel::LIVE_ACTIVITY
    }
}

impl<Instrument> Identifier<PolymarketChannel> for Subscription<Polymarket, Instrument, OrderBooksL2> {
    fn id(&self) -> PolymarketChannel {
        PolymarketChannel::MARKET
    }
}

impl AsRef<str> for PolymarketChannel {
    fn as_ref(&self) -> &str {
        self.0
    }
}
