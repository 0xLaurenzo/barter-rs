//! Market pair correlation management for arbitrage detection.

use barter_instrument::exchange::ExchangeId;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use smol_str::SmolStr;

/// A pair of markets that ask the same question on different platforms.
///
/// This represents a correlated pair between Kalshi and Polymarket that
/// can be arbitraged.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct CorrelatedPair {
    /// Kalshi market ticker (e.g., "KXBTC-25JAN31-T100000")
    pub kalshi_ticker: SmolStr,
    /// Polymarket condition ID
    pub polymarket_condition_id: SmolStr,
    /// Polymarket YES token ID
    pub polymarket_yes_token: SmolStr,
    /// Polymarket NO token ID
    pub polymarket_no_token: SmolStr,
    /// Human-readable description of the market
    pub description: String,
    /// When the market resolves/expires
    pub expiry: DateTime<Utc>,
    /// Whether Kalshi YES/NO are inverted relative to Polymarket
    /// When true: Polymarket YES = Kalshi NO
    pub inverse: bool,
}

impl CorrelatedPair {
    /// Create a new correlated pair.
    pub fn new(
        kalshi_ticker: impl Into<SmolStr>,
        polymarket_condition_id: impl Into<SmolStr>,
        polymarket_yes_token: impl Into<SmolStr>,
        polymarket_no_token: impl Into<SmolStr>,
        description: impl Into<String>,
        expiry: DateTime<Utc>,
        inverse: bool,
    ) -> Self {
        Self {
            kalshi_ticker: kalshi_ticker.into(),
            polymarket_condition_id: polymarket_condition_id.into(),
            polymarket_yes_token: polymarket_yes_token.into(),
            polymarket_no_token: polymarket_no_token.into(),
            description: description.into(),
            expiry,
            inverse,
        }
    }

    /// Get the number of days until this market expires.
    pub fn days_to_expiry(&self) -> i64 {
        (self.expiry - Utc::now()).num_days()
    }

    /// Check if the market has expired.
    pub fn is_expired(&self) -> bool {
        self.expiry <= Utc::now()
    }
}

/// Unique identifier for a prediction market instrument.
///
/// Each prediction market instrument is uniquely identified by exchange,
/// market identifier, and outcome side.
#[derive(Clone, Hash, Eq, PartialEq, Ord, PartialOrd, Debug, Deserialize, Serialize)]
pub struct PredictionMarketKey {
    /// Exchange (Kalshi, Polymarket)
    pub exchange: ExchangeId,
    /// Market identifier (ticker or token_id)
    pub market_id: SmolStr,
    /// Outcome side
    pub outcome: Outcome,
}

impl PredictionMarketKey {
    /// Create a new prediction market key.
    pub fn new(exchange: ExchangeId, market_id: impl Into<SmolStr>, outcome: Outcome) -> Self {
        Self {
            exchange,
            market_id: market_id.into(),
            outcome,
        }
    }

    /// Create key for Kalshi YES side.
    pub fn kalshi_yes(ticker: impl Into<SmolStr>) -> Self {
        Self::new(ExchangeId::Kalshi, ticker, Outcome::Yes)
    }

    /// Create key for Kalshi NO side.
    pub fn kalshi_no(ticker: impl Into<SmolStr>) -> Self {
        Self::new(ExchangeId::Kalshi, ticker, Outcome::No)
    }

    /// Create key for Polymarket YES side.
    pub fn polymarket_yes(token_id: impl Into<SmolStr>) -> Self {
        Self::new(ExchangeId::Polymarket, token_id, Outcome::Yes)
    }

    /// Create key for Polymarket NO side.
    pub fn polymarket_no(token_id: impl Into<SmolStr>) -> Self {
        Self::new(ExchangeId::Polymarket, token_id, Outcome::No)
    }
}

impl std::fmt::Display for PredictionMarketKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}|{}|{}", self.exchange, self.market_id, self.outcome)
    }
}

/// Outcome side for a prediction market (YES or NO).
#[derive(Copy, Clone, Hash, Eq, PartialEq, Ord, PartialOrd, Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Outcome {
    Yes,
    No,
}

impl std::fmt::Display for Outcome {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Outcome::Yes => write!(f, "yes"),
            Outcome::No => write!(f, "no"),
        }
    }
}

impl Outcome {
    /// Get the inverse outcome.
    pub fn inverse(&self) -> Self {
        match self {
            Outcome::Yes => Outcome::No,
            Outcome::No => Outcome::Yes,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_correlated_pair_creation() {
        let pair = CorrelatedPair::new(
            "KXBTC-25JAN31-T100000",
            "0xcondition",
            "0xyes_token",
            "0xno_token",
            "Will BTC be above $100k on Jan 31?",
            DateTime::from_timestamp(1738368000, 0).unwrap(), // 2025-02-01
            false,
        );

        assert_eq!(pair.kalshi_ticker.as_str(), "KXBTC-25JAN31-T100000");
        assert_eq!(pair.polymarket_yes_token.as_str(), "0xyes_token");
        assert_eq!(pair.polymarket_no_token.as_str(), "0xno_token");
    }

    #[test]
    fn test_prediction_market_key() {
        let key = PredictionMarketKey::kalshi_yes("KXBTC-25JAN31-T100000");
        assert_eq!(key.exchange, ExchangeId::Kalshi);
        assert_eq!(key.outcome, Outcome::Yes);
        assert_eq!(key.to_string(), "Kalshi|KXBTC-25JAN31-T100000|yes");
    }

    #[test]
    fn test_outcome_inverse() {
        assert_eq!(Outcome::Yes.inverse(), Outcome::No);
        assert_eq!(Outcome::No.inverse(), Outcome::Yes);
    }
}
