//! Kalshi-specific authenticated WebSocket subscriber.
//!
//! Kalshi requires authentication headers on WebSocket connections.
//! This subscriber handles generating and including those headers.

use super::auth::{KalshiCredentials, KalshiAuthHeaders};
use crate::{
    Identifier,
    exchange::Connector,
    instrument::InstrumentData,
    subscriber::{
        Subscribed,
        mapper::{SubscriptionMapper, WebSocketSubMapper},
        validator::SubscriptionValidator,
    },
    subscription::{Subscription, SubscriptionKind, SubscriptionMeta},
};
use async_trait::async_trait;
use barter_integration::{
    error::SocketError,
    protocol::websocket::connect_with_headers,
};
use futures::SinkExt;
use std::sync::OnceLock;
use tracing::debug;

/// Global storage for Kalshi credentials.
/// Initialized on first use from environment variables.
static KALSHI_CREDENTIALS: OnceLock<Result<KalshiCredentials, String>> = OnceLock::new();

/// Initialize Kalshi credentials from environment.
fn get_credentials() -> Result<&'static KalshiCredentials, SocketError> {
    let result = KALSHI_CREDENTIALS.get_or_init(|| {
        KalshiCredentials::from_env()
            .map_err(|e| e.to_string())
    });

    match result {
        Ok(creds) => Ok(creds),
        Err(e) => Err(SocketError::Subscribe(format!(
            "Failed to load Kalshi credentials: {}",
            e
        ))),
    }
}

/// Authenticated WebSocket subscriber for Kalshi.
///
/// This subscriber generates RSA-signed authentication headers
/// before connecting to Kalshi's WebSocket API.
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
pub struct KalshiAuthenticatedSubscriber;

#[async_trait]
impl crate::subscriber::Subscriber for KalshiAuthenticatedSubscriber {
    type SubMapper = WebSocketSubMapper;

    async fn subscribe<Exchange, Instrument, Kind>(
        subscriptions: &[Subscription<Exchange, Instrument, Kind>],
    ) -> Result<Subscribed<Instrument::Key>, SocketError>
    where
        Exchange: Connector + Send + Sync,
        Kind: SubscriptionKind + Send + Sync,
        Instrument: InstrumentData,
        Subscription<Exchange, Instrument, Kind>:
            Identifier<Exchange::Channel> + Identifier<Exchange::Market>,
    {
        // Define variables for logging ergonomics
        let exchange = Exchange::ID;
        let url = Exchange::url()?;
        debug!(%exchange, %url, ?subscriptions, "subscribing to Kalshi WebSocket with authentication");

        // Get credentials and generate auth headers
        let credentials = get_credentials()?;
        let auth_headers = credentials
            .generate_ws_auth()
            .map_err(|e| SocketError::Subscribe(format!("Failed to generate auth: {}", e)))?;

        debug!(
            %exchange,
            api_key = %auth_headers.api_key,
            timestamp = %auth_headers.timestamp,
            "Generated Kalshi authentication headers"
        );

        // Build headers iterator
        let headers = [
            (KalshiAuthHeaders::KEY_HEADER, auth_headers.api_key),
            (KalshiAuthHeaders::SIGNATURE_HEADER, auth_headers.signature),
            (KalshiAuthHeaders::TIMESTAMP_HEADER, auth_headers.timestamp),
        ];

        // Connect with authentication headers
        let mut websocket = connect_with_headers(url.clone(), headers).await?;
        debug!(%exchange, ?subscriptions, "connected to Kalshi WebSocket");

        // Map &[Subscription<Exchange, Kind>] to SubscriptionMeta
        let SubscriptionMeta {
            instrument_map,
            ws_subscriptions,
        } = Self::SubMapper::map::<Exchange, Instrument, Kind>(subscriptions);

        // Send Subscriptions over WebSocket
        for subscription in ws_subscriptions {
            debug!(%exchange, payload = ?subscription, "sending exchange subscription");
            websocket
                .send(subscription)
                .await
                .map_err(|error| SocketError::WebSocket(Box::new(error)))?;
        }

        // Validate Subscription responses
        let (map, buffered_websocket_events) = Exchange::SubValidator::validate::<
            Exchange,
            Instrument::Key,
            Kind,
        >(instrument_map, &mut websocket)
        .await?;

        debug!(%exchange, "successfully initialised authenticated Kalshi WebSocket stream");
        Ok(Subscribed {
            websocket,
            map,
            buffered_websocket_events,
        })
    }
}
