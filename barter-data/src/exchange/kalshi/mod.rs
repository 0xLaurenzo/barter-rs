use self::{
    channel::KalshiChannel,
    market::KalshiMarket,
    message::KalshiOrderbookSnapshot,
    subscriber::KalshiAuthenticatedSubscriber,
    subscription::KalshiSubResponse,
};
use crate::{
    ExchangeWsStream, NoInitialSnapshots,
    exchange::{Connector, ExchangeSub, StreamSelector},
    instrument::InstrumentData,
    subscriber::validator::WebSocketSubValidator,
    subscription::book::OrderBooksL2,
    transformer::stateless::StatelessTransformer,
};
use barter_instrument::exchange::ExchangeId;
use barter_integration::{error::SocketError, protocol::websocket::WsMessage};
use barter_macro::{DeExchange, SerExchange};
use derive_more::Display;
use serde_json::json;
use url::Url;

/// Authentication for Kalshi WebSocket connections.
pub mod auth;

/// OrderBook types for [`Kalshi`].
pub mod book;

/// Defines the type that translates a Barter [`Subscription`](crate::subscription::Subscription)
/// into an exchange [`Connector`] specific channel used for generating [`Connector::requests`].
pub mod channel;

/// Defines the type that translates a Barter [`Subscription`](crate::subscription::Subscription)
/// into an exchange [`Connector`] specific market used for generating [`Connector::requests`].
pub mod market;

/// [`KalshiMessage`] types for [`Kalshi`].
pub mod message;

/// Authenticated WebSocket subscriber for Kalshi.
pub mod subscriber;

/// [`Subscription`](crate::subscription::Subscription) response type and response
/// [`Validator`](barter_integration) for [`Kalshi`].
pub mod subscription;

/// [`Kalshi`] WebSocket base URL.
///
/// See docs: <https://trading-api.readme.io/reference/websocket-overview>
pub const BASE_URL_KALSHI: &str = "wss://api.elections.kalshi.com/trade-api/ws/v2";

/// [`Kalshi`] demo/sandbox WebSocket base URL.
pub const BASE_URL_KALSHI_DEMO: &str = "wss://demo-api.kalshi.co/trade-api/ws/v2";

/// [`Kalshi`] prediction market exchange.
///
/// Kalshi is a CFTC-regulated prediction market offering binary event contracts.
///
/// See docs: <https://trading-api.readme.io/reference/getting-started-with-your-api>
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
pub struct Kalshi;

impl Connector for Kalshi {
    const ID: ExchangeId = ExchangeId::Kalshi;
    type Channel = KalshiChannel;
    type Market = KalshiMarket;
    type Subscriber = KalshiAuthenticatedSubscriber;
    type SubValidator = WebSocketSubValidator;
    type SubResponse = KalshiSubResponse;

    fn url() -> Result<Url, SocketError> {
        Url::parse(BASE_URL_KALSHI).map_err(SocketError::UrlParse)
    }

    fn requests(exchange_subs: Vec<ExchangeSub<Self::Channel, Self::Market>>) -> Vec<WsMessage> {
        // Group subscriptions by channel
        let mut channels_to_markets: std::collections::HashMap<&str, Vec<String>> =
            std::collections::HashMap::new();

        for ExchangeSub { channel, market } in &exchange_subs {
            // Kalshi requires uppercase tickers in subscription requests,
            // but AssetNameInternal lowercases everything internally
            channels_to_markets
                .entry(channel.as_ref())
                .or_default()
                .push(market.as_ref().to_uppercase());
        }

        // Auto-subscribe to market_lifecycle_v2 for tickers on orderbook_delta,
        // so we receive settlement/close events and can clear stale orderbooks.
        if let Some(ob_tickers) = channels_to_markets.get(KalshiChannel::ORDER_BOOK_DELTA.as_ref())
        {
            let lifecycle_tickers = ob_tickers.clone();
            channels_to_markets
                .entry(KalshiChannel::MARKET_LIFECYCLE.as_ref())
                .or_default()
                .extend(lifecycle_tickers);
        }

        // Create one subscription request per channel with all markets
        channels_to_markets
            .into_iter()
            .enumerate()
            .map(|(id, (channel, market_tickers))| {
                WsMessage::text(
                    json!({
                        "id": id + 1,
                        "cmd": "subscribe",
                        "params": {
                            "channels": [channel],
                            "market_tickers": market_tickers
                        }
                    })
                    .to_string(),
                )
            })
            .collect()
    }

    fn expected_responses<InstrumentKey>(_map: &crate::subscription::Map<InstrumentKey>) -> usize {
        // Kalshi returns one response per channel subscription, not per market
        // For simplicity, we expect one response per unique channel
        1
    }

    fn subscription_timeout() -> std::time::Duration {
        // Kalshi may send many snapshot messages before the subscription confirmation
        // when subscribing to a large number of markets at once
        std::time::Duration::from_secs(60)
    }
}

impl<Instrument> StreamSelector<Instrument, OrderBooksL2> for Kalshi
where
    Instrument: InstrumentData,
{
    type SnapFetcher = NoInitialSnapshots;
    type Stream = ExchangeWsStream<
        StatelessTransformer<Self, Instrument::Key, OrderBooksL2, KalshiOrderbookSnapshot>,
    >;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_kalshi_url() {
        let url = Kalshi::url().unwrap();
        assert_eq!(url.as_str(), BASE_URL_KALSHI);
    }

    #[test]
    fn test_kalshi_requests() {
        let subs = vec![
            ExchangeSub {
                channel: KalshiChannel::ORDER_BOOK_DELTA,
                market: KalshiMarket::new("KXBTC-25JAN31-T100000"),
            },
            ExchangeSub {
                channel: KalshiChannel::ORDER_BOOK_DELTA,
                market: KalshiMarket::new("KXETH-25JAN31-T5000"),
            },
            ExchangeSub {
                channel: KalshiChannel::TRADES,
                market: KalshiMarket::new("KXBTC-25JAN31-T100000"),
            },
        ];

        let requests = Kalshi::requests(subs);

        // Should have 3 requests: orderbook_delta, trade, market_lifecycle_v2
        // (lifecycle auto-subscribed for orderbook_delta tickers)
        assert_eq!(requests.len(), 3);
    }
}
