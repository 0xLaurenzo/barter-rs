use super::Kalshi;
use crate::{Identifier, instrument::MarketInstrumentData, subscription::Subscription};
use barter_instrument::{
    Keyed, instrument::market_data::MarketDataInstrument,
};
use serde::{Deserialize, Serialize};
use smol_str::SmolStr;

/// Type that defines how to translate a Barter [`Subscription`] into a
/// [`Kalshi`] market that can be subscribed to.
///
/// For Kalshi, this is the market ticker (e.g., "KXBTC-25JAN31-T100000").
///
/// See docs: <https://trading-api.readme.io/reference/getmarket>
#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Deserialize, Serialize)]
pub struct KalshiMarket(pub SmolStr);

impl KalshiMarket {
    /// Create a new [`KalshiMarket`] from a ticker string.
    pub fn new<S: Into<SmolStr>>(ticker: S) -> Self {
        Self(ticker.into())
    }
}

impl<Kind> Identifier<KalshiMarket> for Subscription<Kalshi, MarketDataInstrument, Kind> {
    fn id(&self) -> KalshiMarket {
        // For prediction markets, the "base" field holds the market ticker
        KalshiMarket(self.instrument.base.name().clone())
    }
}

impl<InstrumentKey, Kind> Identifier<KalshiMarket>
    for Subscription<Kalshi, Keyed<InstrumentKey, MarketDataInstrument>, Kind>
{
    fn id(&self) -> KalshiMarket {
        // For prediction markets, the "base" field holds the market ticker
        KalshiMarket(self.instrument.value.base.name().clone())
    }
}

impl<InstrumentKey, Kind> Identifier<KalshiMarket>
    for Subscription<Kalshi, MarketInstrumentData<InstrumentKey>, Kind>
{
    fn id(&self) -> KalshiMarket {
        KalshiMarket(self.instrument.name_exchange.name().clone())
    }
}

impl AsRef<str> for KalshiMarket {
    fn as_ref(&self) -> &str {
        &self.0
    }
}
