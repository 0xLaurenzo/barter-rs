//! OrderBook types for [`Polymarket`](super::Polymarket).

/// L2 OrderBook implementation for Polymarket prediction markets.
pub mod l2;

pub use l2::PolymarketOrderBookL2Meta;
