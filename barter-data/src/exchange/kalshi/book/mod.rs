//! OrderBook types for [`Kalshi`](super::Kalshi).

/// L2 OrderBook implementation for Kalshi prediction markets.
pub mod l2;

pub use l2::{KalshiOrderBook, KalshiOrderBookL2Meta};
