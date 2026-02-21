//! Delta-neutral arbitrage opportunity detection and representation.
//!
//! Strategy: Buy YES on one platform + Buy NO on the other = guaranteed $1 payout.
//! Profit = $1.00 - (YES_ask + NO_ask + fees).

use crate::correlation::{CorrelatedPair, Outcome, PredictionMarketKey};
use barter_instrument::exchange::ExchangeId;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

/// Direction of the delta-neutral arbitrage trade.
///
/// Both sides are always BUY orders on different platforms.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
pub enum ArbitrageDirection {
    /// Buy YES on Polymarket + Buy NO on Kalshi
    YesPolyNoKalshi,
    /// Buy YES on Kalshi + Buy NO on Polymarket
    YesKalshiNoPoly,
}

impl ArbitrageDirection {
    /// Get the exchange where YES is bought.
    pub fn yes_exchange(&self) -> ExchangeId {
        match self {
            ArbitrageDirection::YesPolyNoKalshi => ExchangeId::Polymarket,
            ArbitrageDirection::YesKalshiNoPoly => ExchangeId::Kalshi,
        }
    }

    /// Get the exchange where NO is bought.
    pub fn no_exchange(&self) -> ExchangeId {
        match self {
            ArbitrageDirection::YesPolyNoKalshi => ExchangeId::Kalshi,
            ArbitrageDirection::YesKalshiNoPoly => ExchangeId::Polymarket,
        }
    }
}

/// One side of a delta-neutral arbitrage order.
///
/// Both sides are always BUY orders.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct OrderSide {
    /// Exchange for this side
    pub exchange: ExchangeId,
    /// Instrument key (includes exchange, market_id, outcome)
    pub instrument: PredictionMarketKey,
    /// Outcome being bought (YES or NO)
    pub outcome: Outcome,
    /// Average price from depth walk
    pub price: Decimal,
    /// Number of contracts
    pub available_size: u32,
}

impl OrderSide {
    /// Create a BUY order side for Polymarket.
    pub fn poly(
        token_id: impl Into<smol_str::SmolStr>,
        outcome: Outcome,
        price: Decimal,
        size: u32,
    ) -> Self {
        Self {
            exchange: ExchangeId::Polymarket,
            instrument: PredictionMarketKey::new(ExchangeId::Polymarket, token_id, outcome),
            outcome,
            price,
            available_size: size,
        }
    }

    /// Create a BUY order side for Kalshi.
    pub fn kalshi(
        ticker: impl Into<smol_str::SmolStr>,
        outcome: Outcome,
        price: Decimal,
        size: u32,
    ) -> Self {
        Self {
            exchange: ExchangeId::Kalshi,
            instrument: PredictionMarketKey::new(ExchangeId::Kalshi, ticker, outcome),
            outcome,
            price,
            available_size: size,
        }
    }

    /// Calculate the order value (price * size).
    pub fn order_value(&self) -> Decimal {
        self.price * Decimal::from(self.available_size)
    }
}

/// A detected delta-neutral arbitrage opportunity.
///
/// Profit condition: total_cost < $1.00 (guaranteed $1 payout at expiry).
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ArbitrageOpportunity {
    /// The correlated market pair
    pub pair: CorrelatedPair,
    /// Direction: which platform gets YES vs NO
    pub direction: ArbitrageDirection,
    /// YES side of the trade (always BUY)
    pub yes_side: OrderSide,
    /// NO side of the trade (always BUY)
    pub no_side: OrderSide,
    /// Total cost per contract: avg_yes + avg_no + fees/contract
    pub total_cost: Decimal,
    /// Weighted average YES price from depth walk
    pub avg_yes_price: Decimal,
    /// Weighted average NO price from depth walk
    pub avg_no_price: Decimal,
    /// Maximum contracts fillable at profitable levels
    pub max_contracts: u32,
    /// Expected profit in dollars (sum across all filled levels)
    pub expected_profit: Decimal,
    /// Total fees across both sides
    pub total_fees: Decimal,
}

impl ArbitrageOpportunity {
    /// Profit per contract (average across depth-walked levels).
    pub fn profit_per_contract(&self) -> Decimal {
        if self.max_contracts > 0 {
            self.expected_profit / Decimal::from(self.max_contracts)
        } else {
            Decimal::ZERO
        }
    }

    /// Check if this opportunity is profitable after fees.
    pub fn is_profitable(&self) -> bool {
        self.expected_profit > Decimal::ZERO
    }

    /// Check if profit per contract meets a minimum threshold.
    pub fn meets_threshold(&self, min_profit_per_contract: Decimal) -> bool {
        self.profit_per_contract() >= min_profit_per_contract
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use rust_decimal_macros::dec;

    fn test_pair() -> CorrelatedPair {
        CorrelatedPair::new(
            "KXBTC-25JAN31-T100000",
            "0xcondition",
            "0xyes_token",
            "0xno_token",
            "Test market",
            Utc::now() + chrono::Duration::days(30),
            false,
        )
    }

    #[test]
    fn test_arbitrage_direction() {
        let d1 = ArbitrageDirection::YesPolyNoKalshi;
        assert_eq!(d1.yes_exchange(), ExchangeId::Polymarket);
        assert_eq!(d1.no_exchange(), ExchangeId::Kalshi);

        let d2 = ArbitrageDirection::YesKalshiNoPoly;
        assert_eq!(d2.yes_exchange(), ExchangeId::Kalshi);
        assert_eq!(d2.no_exchange(), ExchangeId::Polymarket);
    }

    #[test]
    fn test_order_side_value() {
        let side = OrderSide::poly("0xtoken", Outcome::Yes, dec!(0.40), 100);
        assert_eq!(side.order_value(), dec!(40.0));
        assert_eq!(side.outcome, Outcome::Yes);
    }

    #[test]
    fn test_profitable_opportunity() {
        let opp = ArbitrageOpportunity {
            pair: test_pair(),
            direction: ArbitrageDirection::YesPolyNoKalshi,
            yes_side: OrderSide::poly("0xyes_token", Outcome::Yes, dec!(0.40), 100),
            no_side: OrderSide::kalshi("KXBTC-25JAN31-T100000", Outcome::No, dec!(0.54), 100),
            total_cost: dec!(0.96),
            avg_yes_price: dec!(0.40),
            avg_no_price: dec!(0.54),
            max_contracts: 100,
            expected_profit: dec!(4.00),
            total_fees: dec!(2.00),
        };

        assert!(opp.is_profitable());
        assert_eq!(opp.profit_per_contract(), dec!(0.04));
        assert!(opp.meets_threshold(dec!(0.02)));
        assert!(!opp.meets_threshold(dec!(0.05)));
    }

    #[test]
    fn test_unprofitable_opportunity() {
        let opp = ArbitrageOpportunity {
            pair: test_pair(),
            direction: ArbitrageDirection::YesPolyNoKalshi,
            yes_side: OrderSide::poly("0xyes_token", Outcome::Yes, dec!(0.50), 0),
            no_side: OrderSide::kalshi("KXBTC-25JAN31-T100000", Outcome::No, dec!(0.55), 0),
            total_cost: dec!(1.05),
            avg_yes_price: dec!(0.50),
            avg_no_price: dec!(0.55),
            max_contracts: 0,
            expected_profit: Decimal::ZERO,
            total_fees: Decimal::ZERO,
        };

        assert!(!opp.is_profitable());
        assert_eq!(opp.profit_per_contract(), Decimal::ZERO);
    }
}
