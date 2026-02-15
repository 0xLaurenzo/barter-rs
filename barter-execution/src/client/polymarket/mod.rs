//! Polymarket execution client for the barter trading engine.
//!
//! Instrument naming convention: `InstrumentNameExchange` = token_id directly.
//! Prices are already in decimal 0-1.
//!
//! Two-layer auth:
//! 1. API auth (all requests): HMAC-SHA256 headers
//! 2. Order signing (order submission): EIP-712 typed data signature

pub mod http;
pub mod model;
pub mod signing;
pub mod ws;

use self::http::{PolymarketHttpClient, PolymarketHttpError};
use self::model::*;
use self::signing::{OrderParams, PolymarketApiCredentials, sign_order};
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
use super::ExecutionClient;
use alloy_primitives::{Address, U256};
use barter_instrument::{
    Side,
    asset::{QuoteAsset, name::AssetNameExchange},
    exchange::ExchangeId,
    instrument::name::InstrumentNameExchange,
};
use barter_integration::snapshot::Snapshot;
use chrono::{DateTime, Utc};
use crate::order::state::Cancelled;
use futures::{stream::BoxStream, StreamExt};
use rust_decimal::Decimal;
use smol_str::SmolStr;
use tokio_stream::wrappers::IntervalStream;
use tracing::{error, info, warn};

/// Configuration for the Polymarket execution client.
#[derive(Debug, Clone)]
pub struct PolymarketExecutionConfig {
    /// API credentials (derived from private key via CLOB).
    pub api_key: String,
    pub api_secret: String,
    pub api_passphrase: String,
    /// Ethereum private key hex for EIP-712 signing.
    pub private_key_hex: String,
    /// Maker (wallet) address.
    pub maker_address: String,
    /// Polling interval for account stream in milliseconds.
    pub poll_interval_ms: u64,
    /// Whether markets are neg risk (uses different exchange contract).
    /// Default: false.
    pub neg_risk: bool,
}

/// Polymarket execution client implementing the barter ExecutionClient trait.
#[derive(Debug, Clone)]
pub struct PolymarketExecution {
    http: PolymarketHttpClient,
    private_key_hex: String,
    maker_address: String,
    poll_interval_ms: u64,
    neg_risk: bool,
}

impl PolymarketExecution {
    fn map_http_error(e: PolymarketHttpError) -> UnindexedClientError {
        UnindexedClientError::Connectivity(ConnectivityError::Socket(e.to_string()))
    }

    fn order_error(
        request: &OrderRequestOpen<ExchangeId, &InstrumentNameExchange>,
        msg: String,
    ) -> Order<ExchangeId, InstrumentNameExchange, Result<Open, UnindexedOrderError>> {
        Order {
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
                ConnectivityError::Socket(msg),
            )),
        }
    }
}

impl ExecutionClient for PolymarketExecution {
    const EXCHANGE: ExchangeId = ExchangeId::Polymarket;

    type Config = PolymarketExecutionConfig;
    type AccountStream = BoxStream<'static, UnindexedAccountEvent>;

    fn new(config: Self::Config) -> Self {
        let credentials = PolymarketApiCredentials {
            api_key: config.api_key,
            api_secret: config.api_secret,
            api_passphrase: config.api_passphrase,
            wallet_address: config.maker_address.clone(),
        };

        let http = PolymarketHttpClient::new(credentials);

        Self {
            http,
            private_key_hex: config.private_key_hex,
            maker_address: config.maker_address,
            poll_interval_ms: config.poll_interval_ms,
            neg_risk: config.neg_risk,
        }
    }

    async fn account_snapshot(
        &self,
        _assets: &[AssetNameExchange],
        _instruments: &[InstrumentNameExchange],
    ) -> Result<UnindexedAccountSnapshot, UnindexedClientError> {
        let balance_decimal = match self.http.fetch_balance().await {
            Ok(resp) => resp.balance.parse::<Decimal>().unwrap_or(Decimal::ZERO),
            Err(e) => {
                warn!(error = %e, "Polymarket balance fetch failed, using zero balance");
                Decimal::ZERO
            }
        };

        let balances = vec![AssetBalance {
            asset: AssetNameExchange::from("usdc"),
            balance: Balance {
                total: balance_decimal,
                free: balance_decimal,
            },
            time_exchange: Utc::now(),
        }];

        Ok(UnindexedAccountSnapshot {
            exchange: ExchangeId::Polymarket,
            balances,
            instruments: vec![],
        })
    }

    async fn account_stream(
        &self,
        _assets: &[AssetNameExchange],
        _instruments: &[InstrumentNameExchange],
    ) -> Result<Self::AccountStream, UnindexedClientError> {
        // Balance polling stream (existing behavior)
        let interval = tokio::time::interval(
            std::time::Duration::from_millis(self.poll_interval_ms),
        );
        let http = self.http.clone();

        let balance_stream = IntervalStream::new(interval).filter_map(move |_| {
            let http = http.clone();
            async move {
                match http.fetch_balance().await {
                    Ok(resp) => {
                        let balance_decimal = resp
                            .balance
                            .parse::<Decimal>()
                            .unwrap_or(Decimal::ZERO);
                        Some(AccountEvent {
                            exchange: ExchangeId::Polymarket,
                            kind: AccountEventKind::BalanceSnapshot(
                                Snapshot(AssetBalance {
                                    asset: AssetNameExchange::from("usdc"),
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
                        warn!(error = %e, "Polymarket balance poll failed");
                        None
                    }
                }
            }
        });

        // Polymarket instrument names are token IDs (asset_ids).
        // The user WS subscribes by condition ID (market), but passing
        // asset_ids also works as the server resolves them.
        let markets: Vec<String> = _instruments
            .iter()
            .map(|name| name.to_string())
            .collect();

        // Attempt WS fill connection; fall back to balance-only on failure
        let creds = self.http.credentials();
        match ws::connect_polymarket_user(
            &creds.api_key,
            &creds.api_secret,
            &creds.api_passphrase,
            &markets,
        )
        .await
        {
            Ok(websocket) => {
                info!("Polymarket user WS connected, merging with balance polling");
                let (fill_stream, _ping_handle) = ws::polymarket_fill_stream(websocket);
                let merged = tokio_stream::StreamExt::merge(
                    tokio_stream::StreamExt::fuse(balance_stream),
                    tokio_stream::StreamExt::fuse(fill_stream),
                );
                Ok(Box::pin(merged))
            }
            Err(e) => {
                warn!(error = %e, "Polymarket user WS connection failed, using balance-only polling");
                Ok(Box::pin(balance_stream))
            }
        }
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
            Ok(()) => UnindexedOrderResponseCancel {
                key,
                state: Ok(Cancelled {
                    id: OrderId(SmolStr::new(&order_id)),
                    time_exchange: Utc::now(),
                }),
            },
            Err(e) => {
                error!(error = %e, "Polymarket cancel order failed");
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
        let token_id = request.key.instrument.to_string();

        let side_num: u8 = match request.state.side {
            Side::Buy => 0,
            Side::Sell => 1,
        };

        // Polymarket amounts are in USDC base units (6 decimals).
        // BUY:  maker provides USDC (price * qty), receives outcome tokens (qty)
        // SELL: maker provides outcome tokens (qty), receives USDC (price * qty)
        let quantity_raw = (request.state.quantity * Decimal::from(1_000_000))
            .round()
            .to_string();
        let cost_raw = (request.state.price * request.state.quantity * Decimal::from(1_000_000))
            .round()
            .to_string();

        let (maker_amount_str, taker_amount_str) = match request.state.side {
            Side::Buy => (cost_raw.clone(), quantity_raw.clone()),
            Side::Sell => (quantity_raw.clone(), cost_raw.clone()),
        };

        let salt = U256::from(rand::random::<u64>());
        let maker_addr: Address = match self.maker_address.parse() {
            Ok(a) => a,
            Err(e) => {
                error!(error = %e, "Invalid maker address");
                return Some(Self::order_error(
                    &request,
                    format!("Invalid maker address: {e}"),
                ));
            }
        };

        let token_id_u256 = match U256::from_str_radix(
            token_id.strip_prefix("0x").unwrap_or(&token_id),
            if token_id.starts_with("0x") { 16 } else { 10 },
        ) {
            Ok(v) => v,
            Err(e) => {
                error!(error = %e, token_id = %token_id, "Invalid token ID");
                return Some(Self::order_error(
                    &request,
                    format!("Invalid token ID: {e}"),
                ));
            }
        };

        let maker_amount_u256 = U256::from_str_radix(&maker_amount_str, 10).unwrap_or(U256::ZERO);
        let taker_amount_u256 = U256::from_str_radix(&taker_amount_str, 10).unwrap_or(U256::ZERO);

        let expiration_ts = (chrono::Utc::now() + chrono::Duration::minutes(5)).timestamp();

        let params = OrderParams {
            salt,
            maker: maker_addr,
            signer: maker_addr,
            taker: Address::ZERO,
            token_id: token_id_u256,
            maker_amount: maker_amount_u256,
            taker_amount: taker_amount_u256,
            expiration: U256::from(expiration_ts as u64),
            nonce: U256::ZERO,
            fee_rate_bps: U256::ZERO,
            side: side_num,
            signature_type: 0, // EOA
            neg_risk: self.neg_risk,
        };

        let signature = match sign_order(&self.private_key_hex, &params) {
            Ok(sig) => sig,
            Err(e) => {
                error!(error = %e, "EIP-712 signing failed");
                return Some(Self::order_error(
                    &request,
                    format!("EIP-712 signing failed: {e}"),
                ));
            }
        };

        let order_payload = PolymarketOrderPayload {
            order: SignedOrderPayload {
                token_id: token_id.clone(),
                maker: self.maker_address.clone(),
                taker: "0x0000000000000000000000000000000000000000".to_string(),
                maker_amount: maker_amount_str,
                taker_amount: taker_amount_str,
                side: side_num,
                fee_rate_bps: "0".to_string(),
                nonce: "0".to_string(),
                expiration: expiration_ts.to_string(),
                salt: salt.to_string(),
                signature,
                signature_type: 0,
            },
            order_type: "FOK".to_string(),
            tick_size: Some("0.01".to_string()),
            neg_risk: if self.neg_risk { Some(true) } else { None },
        };

        let result = self.http.submit_order(&order_payload).await;
        let key = OrderKey {
            exchange: request.key.exchange,
            instrument: request.key.instrument.clone(),
            strategy: request.key.strategy.clone(),
            cid: request.key.cid.clone(),
        };

        Some(match result {
            Ok(resp) => {
                let order_id = resp
                    .order_id
                    .unwrap_or_else(|| "unknown".to_string());

                if resp.success.unwrap_or(false) {
                    Order {
                        key,
                        side: request.state.side,
                        price: request.state.price,
                        quantity: request.state.quantity,
                        kind: request.state.kind,
                        time_in_force: request.state.time_in_force,
                        state: Ok(Open {
                            id: OrderId(SmolStr::new(&order_id)),
                            time_exchange: Utc::now(),
                            filled_quantity: Decimal::ZERO,
                        }),
                    }
                } else {
                    let err_msg = resp
                        .error_msg
                        .unwrap_or_else(|| "Order rejected".to_string());
                    Order {
                        key,
                        side: request.state.side,
                        price: request.state.price,
                        quantity: request.state.quantity,
                        kind: request.state.kind,
                        time_in_force: request.state.time_in_force,
                        state: Err(UnindexedOrderError::Connectivity(
                            ConnectivityError::Socket(err_msg),
                        )),
                    }
                }
            }
            Err(e) => {
                error!(error = %e, "Polymarket open order failed");
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

        let balance_decimal = resp
            .balance
            .parse::<Decimal>()
            .unwrap_or(Decimal::ZERO);

        Ok(vec![AssetBalance {
            asset: AssetNameExchange::from("usdc"),
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
            .filter(|o| o.is_open())
            .map(|o| {
                let side = match o.side.as_str() {
                    "BUY" => Side::Buy,
                    _ => Side::Sell,
                };

                let asset_id = o.asset_id.clone().unwrap_or_default();

                Order {
                    key: OrderKey {
                        exchange: ExchangeId::Polymarket,
                        instrument: InstrumentNameExchange::from(asset_id.as_str()),
                        strategy: StrategyId::new("unknown"),
                        cid: ClientOrderId::new(&o.id),
                    },
                    side,
                    price: o.price_decimal(),
                    quantity: o.remaining_size(),
                    kind: OrderKind::Limit,
                    time_in_force: TimeInForce::GoodUntilCancelled { post_only: false },
                    state: Open {
                        id: OrderId(SmolStr::new(&o.id)),
                        time_exchange: Utc::now(),
                        filled_quantity: o.size_matched_decimal(),
                    },
                }
            })
            .collect();

        Ok(mapped)
    }

    async fn fetch_trades(
        &self,
        _time_since: DateTime<Utc>,
    ) -> Result<Vec<Trade<QuoteAsset, InstrumentNameExchange>>, UnindexedClientError> {
        // Polymarket CLOB doesn't have a direct fills endpoint with time filtering.
        // Trades are tracked via order updates. Return empty for now.
        Ok(vec![])
    }
}
