use self::{
    channel::PolymarketChannel,
    market::PolymarketMarket,
    subscription::PolymarketSubResponse,
    trade::PolymarketTradeMessage,
    transformer::PolymarketOrderBookTransformer,
};
use crate::{
    ExchangeWsStream, NoInitialSnapshots,
    exchange::{Connector, ExchangeSub, PingInterval, StreamSelector},
    instrument::InstrumentData,
    subscriber::{WebSocketSubscriber, validator::WebSocketSubValidator},
    subscription::{book::OrderBooksL2, trade::PublicTrades},
    transformer::stateless::StatelessTransformer,
};
use barter_instrument::exchange::ExchangeId;
use barter_integration::{error::SocketError, protocol::websocket::WsMessage};
use barter_macro::{DeExchange, SerExchange};
use derive_more::Display;
use serde_json::json;
use url::Url;

/// OrderBook types for [`Polymarket`].
pub mod book;

/// Public trade types for [`Polymarket`].
pub mod trade;

/// Defines the type that translates a Barter [`Subscription`](crate::subscription::Subscription)
/// into an exchange [`Connector`] specific channel used for generating [`Connector::requests`].
pub mod channel;

/// Defines the type that translates a Barter [`Subscription`](crate::subscription::Subscription)
/// into an exchange [`Connector`] specific market used for generating [`Connector::requests`].
pub mod market;

/// [`PolymarketMessage`] types for [`Polymarket`].
pub mod message;

/// [`Subscription`](crate::subscription::Subscription) response type and response
/// [`Validator`](barter_integration) for [`Polymarket`].
pub mod subscription;

/// Custom [`ExchangeTransformer`](crate::transformer::ExchangeTransformer) for Polymarket
/// that handles JSON array snapshots.
pub mod transformer;

/// [`Polymarket`] WebSocket base URL.
///
/// See docs: <https://docs.polymarket.com/#websocket-api>
pub const BASE_URL_POLYMARKET: &str = "wss://ws-subscriptions-clob.polymarket.com/ws/market";

/// [`Polymarket`] user channel WebSocket base URL (requires authentication).
pub const BASE_URL_POLYMARKET_USER: &str = "wss://ws-subscriptions-clob.polymarket.com/ws/user";

/// [`Polymarket`] prediction market exchange.
///
/// Polymarket is a decentralized prediction market built on Polygon.
///
/// See docs: <https://docs.polymarket.com/>
#[derive(
    Copy,
    Clone,
    Eq,
    PartialEq,
    Ord,
    PartialOrd,
    Hash,
    Debug,
    Default,
    Display,
    DeExchange,
    SerExchange,
)]
pub struct Polymarket;

impl Connector for Polymarket {
    const ID: ExchangeId = ExchangeId::Polymarket;
    type Channel = PolymarketChannel;
    type Market = PolymarketMarket;
    type Subscriber = WebSocketSubscriber;
    type SubValidator = WebSocketSubValidator;
    type SubResponse = PolymarketSubResponse;

    fn url() -> Result<Url, SocketError> {
        Url::parse(BASE_URL_POLYMARKET).map_err(SocketError::UrlParse)
    }

    fn ping_interval() -> Option<PingInterval> {
        Some(PingInterval {
            interval: tokio::time::interval(std::time::Duration::from_secs(10)),
            ping: || WsMessage::text("PING"),
        })
    }

    fn requests(exchange_subs: Vec<ExchangeSub<Self::Channel, Self::Market>>) -> Vec<WsMessage> {
        // Group subscriptions by channel type
        let mut channels_to_assets: std::collections::HashMap<&str, Vec<String>> =
            std::collections::HashMap::new();

        for ExchangeSub { channel, market } in &exchange_subs {
            channels_to_assets
                .entry(channel.as_ref())
                .or_default()
                .push(market.as_ref().to_string());
        }

        // Create subscription requests
        // Polymarket format: {"assets_ids": ["token1", "token2"], "type": "market"}
        channels_to_assets
            .into_iter()
            .map(|(channel_type, assets)| {
                WsMessage::text(
                    json!({
                        "assets_ids": assets,
                        "type": channel_type
                    })
                    .to_string(),
                )
            })
            .collect()
    }

    fn expected_responses<InstrumentKey>(_map: &crate::subscription::Map<InstrumentKey>) -> usize {
        // Polymarket doesn't send subscription confirmations - it starts sending
        // book data immediately after subscription request
        0
    }
}

impl<Instrument> StreamSelector<Instrument, OrderBooksL2> for Polymarket
where
    Instrument: InstrumentData,
{
    type SnapFetcher = NoInitialSnapshots;
    type Stream = ExchangeWsStream<PolymarketOrderBookTransformer<Instrument::Key>>;
}

impl<Instrument> StreamSelector<Instrument, PublicTrades> for Polymarket
where
    Instrument: InstrumentData,
{
    type SnapFetcher = NoInitialSnapshots;
    type Stream = ExchangeWsStream<
        StatelessTransformer<Self, Instrument::Key, PublicTrades, PolymarketTradeMessage>,
    >;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_polymarket_url() {
        let url = Polymarket::url().unwrap();
        assert_eq!(url.as_str(), BASE_URL_POLYMARKET);
    }

    #[test]
    fn test_polymarket_requests() {
        let subs = vec![
            ExchangeSub {
                channel: PolymarketChannel::MARKET,
                market: PolymarketMarket::new("0x1234abcd"),
            },
            ExchangeSub {
                channel: PolymarketChannel::MARKET,
                market: PolymarketMarket::new("0x5678efgh"),
            },
            ExchangeSub {
                channel: PolymarketChannel::LIVE_ACTIVITY,
                market: PolymarketMarket::new("0x1234abcd"),
            },
        ];

        let requests = Polymarket::requests(subs);

        // Should have 2 requests: one for market, one for live_activity
        assert_eq!(requests.len(), 2);
    }
}
