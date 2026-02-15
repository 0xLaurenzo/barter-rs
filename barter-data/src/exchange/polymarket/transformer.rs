use crate::{
    books::OrderBook,
    error::DataError,
    event::{MarketEvent, MarketIter},
    exchange::polymarket::message::{
        PolymarketMessage, PolymarketPriceBook, PolymarketPriceChangeEvent,
    },
    subscription::{Map, book::OrderBooksL2},
    transformer::ExchangeTransformer,
};
use async_trait::async_trait;
use barter_instrument::exchange::ExchangeId;
use barter_integration::{
    Transformer, protocol::websocket::WsMessage, subscription::SubscriptionId,
};
use crate::Identifier;
use crate::subscription::book::OrderBookEvent;
use chrono::Utc;
use rust_decimal::Decimal;
use smol_str::format_smolstr;
use tokio::sync::mpsc;
use tracing::debug;

/// Custom transformer for Polymarket OrderBook L2 streams.
///
/// Handles Polymarket's message formats:
/// - Initial snapshots as JSON arrays `[{...}, {...}]`
/// - Individual book updates as JSON objects with `asset_id`, `bids`, `asks`
/// - Price change deltas: `{"event_type": "price_change", "price_changes": [...]}`
/// - PONG text replies (skipped upstream as parse errors)
#[derive(Debug)]
pub struct PolymarketOrderBookTransformer<InstrumentKey> {
    instrument_map: Map<InstrumentKey>,
}

#[async_trait]
impl<InstrumentKey> ExchangeTransformer<super::Polymarket, InstrumentKey, OrderBooksL2>
    for PolymarketOrderBookTransformer<InstrumentKey>
where
    InstrumentKey: Clone + Send,
{
    async fn init(
        instrument_map: Map<InstrumentKey>,
        _initial_snapshots: &[MarketEvent<InstrumentKey, OrderBookEvent>],
        _ws_sink_tx: mpsc::UnboundedSender<WsMessage>,
    ) -> Result<Self, DataError> {
        Ok(Self { instrument_map })
    }
}

impl<InstrumentKey> Transformer for PolymarketOrderBookTransformer<InstrumentKey>
where
    InstrumentKey: Clone,
{
    type Error = DataError;
    type Input = serde_json::Value;
    type Output = MarketEvent<InstrumentKey, OrderBookEvent>;
    type OutputIter = Vec<Result<Self::Output, Self::Error>>;

    fn transform(&mut self, input: Self::Input) -> Self::OutputIter {
        // Handle JSON arrays (initial snapshots come as arrays of orderbooks)
        if let Some(arr) = input.as_array() {
            debug!(array_len = arr.len(), "Polymarket received snapshot array");
            return arr
                .iter()
                .filter_map(|v| {
                    let book: PolymarketPriceBook = serde_json::from_value(v.clone())
                        .inspect_err(|e| {
                            debug!(error = %e, "failed to parse snapshot array element")
                        })
                        .ok()?;
                    self.transform_price_book(book)
                })
                .flatten()
                .collect();
        }

        // Skip non-object messages
        if !input.is_object() {
            return vec![];
        }

        // Try price_change event first (most common ongoing message)
        if input.get("event_type").and_then(|v| v.as_str()) == Some("price_change") {
            return self.transform_price_change(input);
        }

        // Try to parse as a PolymarketMessage (individual book updates, live activity, etc.)
        match serde_json::from_value::<PolymarketMessage<PolymarketPriceBook>>(input) {
            Ok(msg) => {
                let sub_id = match msg.id() {
                    Some(id) => id,
                    None => return vec![],
                };
                match self.instrument_map.find(&sub_id) {
                    Ok(instrument) => {
                        MarketIter::<InstrumentKey, OrderBookEvent>::from((
                            ExchangeId::Polymarket,
                            instrument.clone(),
                            msg,
                        ))
                        .0
                    }
                    Err(unidentifiable) => vec![Err(DataError::from(unidentifiable))],
                }
            }
            Err(_) => vec![],
        }
    }
}

impl<InstrumentKey> PolymarketOrderBookTransformer<InstrumentKey>
where
    InstrumentKey: Clone,
{
    fn transform_price_book(
        &self,
        book: PolymarketPriceBook,
    ) -> Option<Vec<Result<MarketEvent<InstrumentKey, OrderBookEvent>, DataError>>> {
        let sub_id = SubscriptionId(format_smolstr!("market|{}", book.asset_id));
        let instrument = self.instrument_map.find(&sub_id).ok()?;
        Some(
            MarketIter::<InstrumentKey, OrderBookEvent>::from((
                ExchangeId::Polymarket,
                instrument.clone(),
                book,
            ))
            .0,
        )
    }

    /// Convert a price_change event into OrderBookEvent::Update events.
    ///
    /// Each entry in `price_changes` contains a single level change for a specific
    /// asset_id. We group by asset_id and emit one Update per asset.
    fn transform_price_change(
        &self,
        input: serde_json::Value,
    ) -> Vec<Result<MarketEvent<InstrumentKey, OrderBookEvent>, DataError>> {
        let event: PolymarketPriceChangeEvent = match serde_json::from_value(input) {
            Ok(e) => e,
            Err(e) => {
                debug!(error = %e, "failed to parse price_change event");
                return vec![];
            }
        };

        let now = Utc::now();
        let mut results = Vec::new();

        // Group changes by asset_id
        let mut grouped: std::collections::HashMap<&str, (Vec<(Decimal, Decimal)>, Vec<(Decimal, Decimal)>)> =
            std::collections::HashMap::new();

        for change in &event.price_changes {
            let price: Decimal = match change.price.parse() {
                Ok(p) => p,
                Err(_) => continue,
            };
            let size: Decimal = match change.size.parse() {
                Ok(s) => s,
                Err(_) => continue,
            };

            let (bids, asks) = grouped.entry(&change.asset_id).or_default();
            match change.side.as_str() {
                "BUY" => bids.push((price, size)),
                "SELL" => asks.push((price, size)),
                _ => {}
            }
        }

        // Emit one Update event per asset_id
        for (asset_id, (bids, asks)) in grouped {
            let sub_id = SubscriptionId(format_smolstr!("market|{}", asset_id));
            let instrument = match self.instrument_map.find(&sub_id) {
                Ok(i) => i.clone(),
                Err(_) => continue,
            };

            let seq = event.timestamp.unwrap_or(0);
            let orderbook = OrderBook::new(seq, Some(now), bids, asks);

            results.push(Ok(MarketEvent {
                time_exchange: now,
                time_received: now,
                exchange: ExchangeId::Polymarket,
                instrument,
                kind: OrderBookEvent::Update(orderbook),
            }));
        }

        results
    }
}
