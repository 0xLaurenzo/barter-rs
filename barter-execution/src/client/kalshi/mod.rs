//! Kalshi execution client for the barter trading engine.
//!
//! Instrument naming convention: `"{ticker}_{yes|no}"`.
//! Parse to extract ticker and side for API calls.
//! Prices are decimal 0-1 internally, converted to cents (1-99) for Kalshi API.

pub mod http;
pub mod model;

use self::http::{KalshiHttpClient, KalshiHttpConfig, KalshiHttpError};
use self::model::KalshiCreateOrder;
use crate::{
    AccountEvent, AccountEventKind, UnindexedAccountEvent, UnindexedAccountSnapshot,
    balance::{AssetBalance, Balance},
    error::{ConnectivityError, UnindexedClientError, UnindexedOrderError},
    order::{
        Order, OrderKey, OrderKind, TimeInForce,
        id::{ClientOrderId, OrderId, StrategyId},
        request::{OrderRequestCancel, OrderRequestOpen, UnindexedOrderResponseCancel},
        state::Open,
    },
    trade::Trade,
};
use barter_integration::snapshot::Snapshot;
use super::ExecutionClient;
use barter_instrument::{
    Side,
    asset::{QuoteAsset, name::AssetNameExchange},
    exchange::ExchangeId,
    instrument::name::InstrumentNameExchange,
};
use chrono::{DateTime, Utc};
use crate::order::state::Cancelled;
use futures::{stream::BoxStream, StreamExt};
use rust_decimal::Decimal;
use smol_str::SmolStr;
use tokio_stream::wrappers::IntervalStream;
use tracing::{error, warn};

/// Configuration for the Kalshi execution client.
#[derive(Debug, Clone)]
pub struct KalshiExecutionConfig {
    pub api_key: String,
    pub private_key_pem: String,
    pub demo: bool,
    /// Polling interval for account stream in milliseconds.
    pub poll_interval_ms: u64,
}

/// Kalshi execution client implementing the barter ExecutionClient trait.
#[derive(Debug, Clone)]
pub struct KalshiExecution {
    http: KalshiHttpClient,
    poll_interval_ms: u64,
}

impl KalshiExecution {
    /// Parse an instrument name of form "{ticker}_{yes|no}" into (ticker, side).
    fn parse_instrument(name: &InstrumentNameExchange) -> Option<(String, String)> {
        let s = name.to_string();
        let parts: Vec<&str> = s.rsplitn(2, '_').collect();
        if parts.len() == 2 {
            let side = parts[0].to_string();
            let ticker = parts[1].to_string();
            if side == "yes" || side == "no" {
                return Some((ticker, side));
            }
        }
        None
    }

    /// Convert a decimal price (0-1) to Kalshi cents (1-99).
    fn price_to_cents(price: Decimal) -> u32 {
        let cents = (price * Decimal::from(100))
            .round()
            .to_string()
            .parse::<u32>()
            .unwrap_or(50);
        cents.clamp(1, 99)
    }

    fn map_http_error(e: KalshiHttpError) -> UnindexedClientError {
        UnindexedClientError::Connectivity(ConnectivityError::Socket(e.to_string()))
    }
}

impl ExecutionClient for KalshiExecution {
    const EXCHANGE: ExchangeId = ExchangeId::Kalshi;

    type Config = KalshiExecutionConfig;
    type AccountStream = BoxStream<'static, UnindexedAccountEvent>;

    fn new(config: Self::Config) -> Self {
        let http = KalshiHttpClient::new(KalshiHttpConfig {
            api_key: config.api_key,
            private_key_pem: config.private_key_pem,
            demo: config.demo,
        })
        .expect("Failed to create Kalshi HTTP client");

        Self {
            http,
            poll_interval_ms: config.poll_interval_ms,
        }
    }

    async fn account_snapshot(
        &self,
        _assets: &[AssetNameExchange],
        _instruments: &[InstrumentNameExchange],
    ) -> Result<UnindexedAccountSnapshot, UnindexedClientError> {
        let balance_decimal = match self.http.fetch_balance().await {
            Ok(resp) => Decimal::from(resp.balance) / Decimal::from(100),
            Err(e) => {
                warn!(error = %e, "Kalshi balance fetch failed, using zero balance");
                Decimal::ZERO
            }
        };

        let balances = vec![AssetBalance {
            asset: AssetNameExchange::from("usd"),
            balance: Balance {
                total: balance_decimal,
                free: balance_decimal,
            },
            time_exchange: Utc::now(),
        }];

        Ok(UnindexedAccountSnapshot {
            exchange: ExchangeId::Kalshi,
            balances,
            instruments: vec![],
        })
    }

    async fn account_stream(
        &self,
        _assets: &[AssetNameExchange],
        _instruments: &[InstrumentNameExchange],
    ) -> Result<Self::AccountStream, UnindexedClientError> {
        // Kalshi has no user WebSocket. Poll for order updates.
        let interval = tokio::time::interval(
            std::time::Duration::from_millis(self.poll_interval_ms),
        );
        let http = self.http.clone();

        let stream = IntervalStream::new(interval).filter_map(move |_| {
            let http = http.clone();
            async move {
                match http.fetch_balance().await {
                    Ok(resp) => {
                        let balance_decimal =
                            Decimal::from(resp.balance) / Decimal::from(100);
                        Some(AccountEvent {
                            exchange: ExchangeId::Kalshi,
                            kind: AccountEventKind::BalanceSnapshot(
                                Snapshot(AssetBalance {
                                    asset: AssetNameExchange::from("usd"),
                                    balance: Balance {
                                        total: balance_decimal,
                                        free: balance_decimal,
                                    },
                                    time_exchange: Utc::now(),
                                }),
                            ),
                        })
                    }
                    Err(e) => {
                        warn!(error = %e, "Kalshi balance poll failed");
                        None
                    }
                }
            }
        });

        Ok(Box::pin(stream))
    }

    async fn cancel_order(
        &self,
        request: OrderRequestCancel<ExchangeId, &InstrumentNameExchange>,
    ) -> Option<UnindexedOrderResponseCancel> {
        let order_id = match &request.state.id {
            Some(id) => id.0.to_string(),
            None => {
                return Some(UnindexedOrderResponseCancel {
                    key: OrderKey {
                        exchange: request.key.exchange,
                        instrument: request.key.instrument.clone(),
                        strategy: request.key.strategy.clone(),
                        cid: request.key.cid.clone(),
                    },
                    state: Err(UnindexedOrderError::Connectivity(
                        ConnectivityError::Socket("No order ID for cancel".into()),
                    )),
                });
            }
        };

        let result = self.http.cancel_order(&order_id).await;
        let key = OrderKey {
            exchange: request.key.exchange,
            instrument: request.key.instrument.clone(),
            strategy: request.key.strategy.clone(),
            cid: request.key.cid.clone(),
        };

        Some(match result {
            Ok(_) => UnindexedOrderResponseCancel {
                key,
                state: Ok(Cancelled {
                    id: OrderId(SmolStr::new(&order_id)),
                    time_exchange: Utc::now(),
                }),
            },
            Err(e) => {
                error!(error = %e, "Kalshi cancel order failed");
                UnindexedOrderResponseCancel {
                    key,
                    state: Err(UnindexedOrderError::Connectivity(
                        ConnectivityError::Socket(e.to_string()),
                    )),
                }
            }
        })
    }

    async fn open_order(
        &self,
        request: OrderRequestOpen<ExchangeId, &InstrumentNameExchange>,
    ) -> Option<Order<ExchangeId, InstrumentNameExchange, Result<Open, UnindexedOrderError>>> {
        let (ticker, side_str) = match Self::parse_instrument(request.key.instrument) {
            Some(parsed) => parsed,
            None => {
                error!(
                    instrument = %request.key.instrument,
                    "Failed to parse Kalshi instrument name"
                );
                return Some(Order {
                    key: OrderKey {
                        exchange: request.key.exchange,
                        instrument: request.key.instrument.clone(),
                        strategy: request.key.strategy.clone(),
                        cid: request.key.cid.clone(),
                    },
                    side: request.state.side,
                    price: request.state.price,
                    quantity: request.state.quantity,
                    kind: request.state.kind,
                    time_in_force: request.state.time_in_force,
                    state: Err(UnindexedOrderError::Connectivity(
                        ConnectivityError::Socket("Invalid instrument name format".into()),
                    )),
                });
            }
        };

        let action = match request.state.side {
            Side::Buy => "buy",
            Side::Sell => "sell",
        };

        let price_cents = Self::price_to_cents(request.state.price);
        let count = request
            .state
            .quantity
            .round()
            .to_string()
            .parse::<u32>()
            .unwrap_or(1);

        let (yes_price, no_price) = if side_str == "yes" {
            (Some(price_cents), None)
        } else {
            (None, Some(price_cents))
        };

        let create_order = KalshiCreateOrder {
            ticker,
            action: action.to_string(),
            side: side_str,
            order_type: "limit".to_string(),
            count,
            yes_price,
            no_price,
            expiration_ts: None,
            sell_position_floor: None,
            buy_max_cost: None,
        };

        let result = self.http.create_order(&create_order).await;
        let key = OrderKey {
            exchange: request.key.exchange,
            instrument: request.key.instrument.clone(),
            strategy: request.key.strategy.clone(),
            cid: request.key.cid.clone(),
        };

        Some(match result {
            Ok(resp) => Order {
                key,
                side: request.state.side,
                price: request.state.price,
                quantity: request.state.quantity,
                kind: request.state.kind,
                time_in_force: request.state.time_in_force,
                state: Ok(Open {
                    id: OrderId(SmolStr::new(&resp.order.order_id)),
                    time_exchange: Utc::now(),
                    filled_quantity: Decimal::from(resp.order.filled_count()),
                }),
            },
            Err(e) => {
                error!(error = %e, "Kalshi open order failed");
                Order {
                    key,
                    side: request.state.side,
                    price: request.state.price,
                    quantity: request.state.quantity,
                    kind: request.state.kind,
                    time_in_force: request.state.time_in_force,
                    state: Err(UnindexedOrderError::Connectivity(
                        ConnectivityError::Socket(e.to_string()),
                    )),
                }
            }
        })
    }

    async fn fetch_balances(
        &self,
    ) -> Result<Vec<AssetBalance<AssetNameExchange>>, UnindexedClientError> {
        let resp = self
            .http
            .fetch_balance()
            .await
            .map_err(Self::map_http_error)?;

        let balance_decimal =
            Decimal::from(resp.balance) / Decimal::from(100);

        Ok(vec![AssetBalance {
            asset: AssetNameExchange::from("usd"),
            balance: Balance {
                total: balance_decimal,
                free: balance_decimal,
            },
            time_exchange: Utc::now(),
        }])
    }

    async fn fetch_open_orders(
        &self,
    ) -> Result<Vec<Order<ExchangeId, InstrumentNameExchange, Open>>, UnindexedClientError> {
        let orders = self
            .http
            .fetch_open_orders()
            .await
            .map_err(Self::map_http_error)?;

        let mapped: Vec<_> = orders
            .into_iter()
            .filter_map(|o| {
                let side_str = &o.side;
                let instrument_name = format!("{}_{}", o.ticker, side_str);
                let side = match o.action.as_str() {
                    "buy" => Side::Buy,
                    _ => Side::Sell,
                };

                Some(Order {
                    key: OrderKey {
                        exchange: ExchangeId::Kalshi,
                        instrument: InstrumentNameExchange::from(instrument_name.as_str()),
                        strategy: StrategyId::new("unknown"),
                        cid: ClientOrderId::new(&o.order_id),
                    },
                    side,
                    price: o.price_decimal().unwrap_or(Decimal::ZERO),
                    quantity: Decimal::from(o.remaining_count.unwrap_or(0)),
                    kind: OrderKind::Limit,
                    time_in_force: TimeInForce::GoodUntilCancelled { post_only: false },
                    state: Open {
                        id: OrderId(SmolStr::new(&o.order_id)),
                        time_exchange: Utc::now(),
                        filled_quantity: Decimal::from(o.filled_count()),
                    },
                })
            })
            .collect();

        Ok(mapped)
    }

    async fn fetch_trades(
        &self,
        time_since: DateTime<Utc>,
    ) -> Result<Vec<Trade<QuoteAsset, InstrumentNameExchange>>, UnindexedClientError> {
        let since_str = time_since.to_rfc3339();
        let fills = self
            .http
            .fetch_fills(Some(&since_str))
            .await
            .map_err(Self::map_http_error)?;

        let trades: Vec<_> = fills
            .into_iter()
            .map(|f| {
                let instrument_name = format!("{}_{}", f.ticker, f.side);
                let side = match f.action.as_str() {
                    "buy" => Side::Buy,
                    _ => Side::Sell,
                };
                let price = Decimal::from(f.yes_price) / Decimal::from(100);

                Trade {
                    id: crate::trade::TradeId(SmolStr::new(&f.trade_id)),
                    order_id: OrderId(SmolStr::new(&f.order_id)),
                    instrument: InstrumentNameExchange::from(instrument_name.as_str()),
                    strategy: StrategyId::new("unknown"),
                    time_exchange: Utc::now(),
                    side,
                    price,
                    quantity: Decimal::from(f.count),
                    fees: crate::trade::AssetFees::new(QuoteAsset, Decimal::ZERO),
                }
            })
            .collect();

        Ok(trades)
    }
}
