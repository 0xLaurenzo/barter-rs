use super::Polymarket;
use crate::{Identifier, instrument::MarketInstrumentData, subscription::Subscription};
use barter_instrument::{
    Keyed, instrument::market_data::MarketDataInstrument,
};
use serde::{Deserialize, Serialize};
use smol_str::SmolStr;

/// Type that defines how to translate a Barter [`Subscription`] into a
/// [`Polymarket`] market that can be subscribed to.
///
/// For Polymarket, this is the token_id (the specific YES or NO token).
/// Note: Polymarket has separate token_ids for YES and NO outcomes.
///
/// See docs: <https://docs.polymarket.com/#get-markets>
#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Deserialize, Serialize)]
pub struct PolymarketMarket(pub SmolStr);

impl PolymarketMarket {
    /// Create a new [`PolymarketMarket`] from a token_id string.
    pub fn new<S: Into<SmolStr>>(token_id: S) -> Self {
        Self(token_id.into())
    }

    /// Get the underlying token_id.
    pub fn token_id(&self) -> &str {
        &self.0
    }
}

impl<Kind> Identifier<PolymarketMarket> for Subscription<Polymarket, MarketDataInstrument, Kind> {
    fn id(&self) -> PolymarketMarket {
        // For prediction markets, the "base" field holds the token_id
        PolymarketMarket(self.instrument.base.name().clone())
    }
}

impl<InstrumentKey, Kind> Identifier<PolymarketMarket>
    for Subscription<Polymarket, Keyed<InstrumentKey, MarketDataInstrument>, Kind>
{
    fn id(&self) -> PolymarketMarket {
        // For prediction markets, the "base" field holds the token_id
        PolymarketMarket(self.instrument.value.base.name().clone())
    }
}

impl<InstrumentKey, Kind> Identifier<PolymarketMarket>
    for Subscription<Polymarket, MarketInstrumentData<InstrumentKey>, Kind>
{
    fn id(&self) -> PolymarketMarket {
        PolymarketMarket(self.instrument.name_exchange.name().clone())
    }
}

impl AsRef<str> for PolymarketMarket {
    fn as_ref(&self) -> &str {
        &self.0
    }
}
