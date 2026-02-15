//! Arbitrage opportunity detection and representation.

use crate::{
    correlation::{CorrelatedPair, Outcome, PredictionMarketKey},
    fees::FeeCalculator,
};
use barter_instrument::exchange::ExchangeId;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

/// Direction of the arbitrage trade.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
pub enum ArbitrageDirection {
    /// Buy on Polymarket, sell on Kalshi
    PolyToKalshi,
    /// Buy on Kalshi, sell on Polymarket
    KalshiToPoly,
}

impl ArbitrageDirection {
    /// Get the exchange we're buying from.
    pub fn buy_exchange(&self) -> ExchangeId {
        match self {
            ArbitrageDirection::PolyToKalshi => ExchangeId::Polymarket,
            ArbitrageDirection::KalshiToPoly => ExchangeId::Kalshi,
        }
    }

    /// Get the exchange we're selling to.
    pub fn sell_exchange(&self) -> ExchangeId {
        match self {
            ArbitrageDirection::PolyToKalshi => ExchangeId::Kalshi,
            ArbitrageDirection::KalshiToPoly => ExchangeId::Polymarket,
        }
    }
}

/// One side of an arbitrage order.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct OrderSide {
    /// Exchange for this side of the trade
    pub exchange: ExchangeId,
    /// Instrument key for this side
    pub instrument: PredictionMarketKey,
    /// Price for this side
    pub price: Decimal,
    /// Available size at this price
    pub available_size: u32,
    /// Action: true = buy, false = sell
    pub is_buy: bool,
}

impl OrderSide {
    /// Create a buy order side for Polymarket.
    pub fn poly_buy(token_id: impl Into<smol_str::SmolStr>, outcome: Outcome, price: Decimal, size: u32) -> Self {
        Self {
            exchange: ExchangeId::Polymarket,
            instrument: PredictionMarketKey::new(ExchangeId::Polymarket, token_id, outcome),
            price,
            available_size: size,
            is_buy: true,
        }
    }

    /// Create a sell order side for Polymarket.
    pub fn poly_sell(token_id: impl Into<smol_str::SmolStr>, outcome: Outcome, price: Decimal, size: u32) -> Self {
        Self {
            exchange: ExchangeId::Polymarket,
            instrument: PredictionMarketKey::new(ExchangeId::Polymarket, token_id, outcome),
            price,
            available_size: size,
            is_buy: false,
        }
    }

    /// Create a buy order side for Kalshi.
    pub fn kalshi_buy(ticker: impl Into<smol_str::SmolStr>, outcome: Outcome, price: Decimal, size: u32) -> Self {
        Self {
            exchange: ExchangeId::Kalshi,
            instrument: PredictionMarketKey::new(ExchangeId::Kalshi, ticker, outcome),
            price,
            available_size: size,
            is_buy: true,
        }
    }

    /// Create a sell order side for Kalshi.
    pub fn kalshi_sell(ticker: impl Into<smol_str::SmolStr>, outcome: Outcome, price: Decimal, size: u32) -> Self {
        Self {
            exchange: ExchangeId::Kalshi,
            instrument: PredictionMarketKey::new(ExchangeId::Kalshi, ticker, outcome),
            price,
            available_size: size,
            is_buy: false,
        }
    }

    /// Calculate the order value (price * size).
    pub fn order_value(&self) -> Decimal {
        self.price * Decimal::from(self.available_size)
    }
}

/// A detected arbitrage opportunity between correlated markets.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ArbitrageOpportunity {
    /// The correlated market pair this opportunity is for
    pub pair: CorrelatedPair,
    /// Direction of the arbitrage
    pub direction: ArbitrageDirection,
    /// The outcome being arbitraged (YES or NO)
    pub outcome: Outcome,
    /// Buy side of the trade
    pub buy_side: OrderSide,
    /// Sell side of the trade
    pub sell_side: OrderSide,
    /// Spread before fees (sell price - buy price)
    pub spread_before_fees: Decimal,
    /// Spread after fees
    pub spread_after_fees: Decimal,
    /// Maximum contracts we can trade (limited by liquidity)
    pub max_contracts: u32,
    /// Expected profit in dollars
    pub expected_profit: Decimal,
}

impl ArbitrageOpportunity {
    /// Create a new arbitrage opportunity.
    pub fn new(
        pair: CorrelatedPair,
        direction: ArbitrageDirection,
        outcome: Outcome,
        buy_side: OrderSide,
        sell_side: OrderSide,
        poly_fee_bps: u32,
    ) -> Self {
        let spread_before_fees = sell_side.price - buy_side.price;
        let max_contracts = buy_side.available_size.min(sell_side.available_size);

        let buy_is_kalshi = matches!(direction, ArbitrageDirection::KalshiToPoly);
        let expected_profit = FeeCalculator::calculate_net_profit(
            buy_side.price,
            sell_side.price,
            max_contracts,
            buy_is_kalshi,
            poly_fee_bps,
        );

        let spread_after_fees = if max_contracts > 0 {
            expected_profit / Decimal::from(max_contracts)
        } else {
            Decimal::ZERO
        };

        Self {
            pair,
            direction,
            outcome,
            buy_side,
            sell_side,
            spread_before_fees,
            spread_after_fees,
            max_contracts,
            expected_profit,
        }
    }

    /// Check if this opportunity is profitable after fees.
    pub fn is_profitable(&self) -> bool {
        self.expected_profit > Decimal::ZERO
    }

    /// Check if this opportunity meets a minimum spread threshold.
    pub fn meets_threshold(&self, min_spread: Decimal) -> bool {
        self.spread_after_fees >= min_spread
    }

    /// Calculate profit for a specific number of contracts.
    pub fn profit_for_contracts(&self, contracts: u32, poly_fee_bps: u32) -> Decimal {
        let buy_is_kalshi = matches!(self.direction, ArbitrageDirection::KalshiToPoly);
        FeeCalculator::calculate_net_profit(
            self.buy_side.price,
            self.sell_side.price,
            contracts,
            buy_is_kalshi,
            poly_fee_bps,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::DateTime;
    use rust_decimal_macros::dec;

    fn test_pair() -> CorrelatedPair {
        CorrelatedPair::new(
            "KXBTC-25JAN31-T100000",
            "0xcondition",
            "0xyes_token",
            "0xno_token",
            "Test market",
            DateTime::from_timestamp(1738368000, 0).unwrap(),
        )
    }

    #[test]
    fn test_arbitrage_direction() {
        let poly_to_kalshi = ArbitrageDirection::PolyToKalshi;
        assert_eq!(poly_to_kalshi.buy_exchange(), ExchangeId::Polymarket);
        assert_eq!(poly_to_kalshi.sell_exchange(), ExchangeId::Kalshi);

        let kalshi_to_poly = ArbitrageDirection::KalshiToPoly;
        assert_eq!(kalshi_to_poly.buy_exchange(), ExchangeId::Kalshi);
        assert_eq!(kalshi_to_poly.sell_exchange(), ExchangeId::Polymarket);
    }

    #[test]
    fn test_order_side_value() {
        let side = OrderSide::poly_buy("0xtoken", Outcome::Yes, dec!(0.40), 100);
        assert_eq!(side.order_value(), dec!(40.0));
    }

    #[test]
    fn test_arbitrage_opportunity_creation() {
        let pair = test_pair();
        let buy_side = OrderSide::poly_buy("0xyes_token", Outcome::Yes, dec!(0.40), 100);
        let sell_side = OrderSide::kalshi_sell("KXBTC-25JAN31-T100000", Outcome::Yes, dec!(0.45), 150);

        let opp = ArbitrageOpportunity::new(
            pair,
            ArbitrageDirection::PolyToKalshi,
            Outcome::Yes,
            buy_side,
            sell_side,
            50, // 50 bps Polymarket fee
        );

        assert_eq!(opp.spread_before_fees, dec!(0.05));
        assert_eq!(opp.max_contracts, 100);
        assert!(opp.is_profitable());
    }

    #[test]
    fn test_opportunity_threshold_check() {
        let pair = test_pair();
        let buy_side = OrderSide::poly_buy("0xyes_token", Outcome::Yes, dec!(0.40), 100);
        let sell_side = OrderSide::kalshi_sell("KXBTC-25JAN31-T100000", Outcome::Yes, dec!(0.45), 150);

        let opp = ArbitrageOpportunity::new(
            pair,
            ArbitrageDirection::PolyToKalshi,
            Outcome::Yes,
            buy_side,
            sell_side,
            50,
        );

        // Spread after fees should be around 3c, so 2% threshold should pass
        assert!(opp.meets_threshold(dec!(0.02)));
        // 5% threshold should fail
        assert!(!opp.meets_threshold(dec!(0.05)));
    }
}
