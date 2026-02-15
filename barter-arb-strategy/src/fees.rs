//! Platform fee calculations for Kalshi and Polymarket.
//!
//! Uses rust_decimal for exact precision in financial calculations.

use rust_decimal::Decimal;

/// Fee calculator for prediction market platforms.
pub struct FeeCalculator;

impl FeeCalculator {
    /// Kalshi taker fee: 7% of profit potential.
    ///
    /// Formula: 0.07 * contracts * price * (1 - price)
    ///
    /// This is based on the maximum profit potential of a binary contract,
    /// where buying at price P means max profit is (1 - P) if YES wins.
    ///
    /// # Arguments
    /// * `price` - Price per contract (0.00 - 1.00)
    /// * `contracts` - Number of contracts
    ///
    /// # Returns
    /// Fee amount in dollars
    pub fn kalshi_taker_fee(price: Decimal, contracts: u32) -> Decimal {
        let c = Decimal::from(contracts);
        let fee_rate = Decimal::new(7, 2); // 0.07
        fee_rate * c * price * (Decimal::ONE - price)
    }

    /// Kalshi maker fee: 0% (makers pay no fees).
    pub fn kalshi_maker_fee(_price: Decimal, _contracts: u32) -> Decimal {
        Decimal::ZERO
    }

    /// Polymarket taker fee based on basis points.
    ///
    /// Formula: contracts * price * (fee_bps / 10000)
    ///
    /// # Arguments
    /// * `price` - Price per contract (0.00 - 1.00)
    /// * `contracts` - Number of contracts
    /// * `fee_bps` - Fee in basis points (e.g., 50 = 0.5%)
    ///
    /// # Returns
    /// Fee amount in dollars
    pub fn polymarket_taker_fee(price: Decimal, contracts: u32, fee_bps: u32) -> Decimal {
        let c = Decimal::from(contracts);
        let bps = Decimal::new(fee_bps as i64, 4); // Convert bps to decimal (50 bps = 0.0050)
        c * price * bps
    }

    /// Polymarket maker rebate (negative fee = credit).
    ///
    /// # Arguments
    /// * `price` - Price per contract (0.00 - 1.00)
    /// * `contracts` - Number of contracts
    /// * `rebate_bps` - Rebate in basis points (e.g., 10 = 0.1%)
    ///
    /// # Returns
    /// Rebate amount in dollars (positive value = money received)
    pub fn polymarket_maker_rebate(price: Decimal, contracts: u32, rebate_bps: u32) -> Decimal {
        let c = Decimal::from(contracts);
        let bps = Decimal::new(rebate_bps as i64, 4);
        c * price * bps
    }

    /// Calculate total cost to enter an arbitrage position.
    ///
    /// For arbitrage, we buy on one platform and sell on another.
    /// This calculates the total cost including fees.
    ///
    /// # Arguments
    /// * `buy_price` - Price we're buying at
    /// * `sell_price` - Price we're selling at
    /// * `contracts` - Number of contracts
    /// * `buy_is_kalshi` - True if buying on Kalshi, false if buying on Polymarket
    /// * `poly_fee_bps` - Polymarket fee in basis points
    ///
    /// # Returns
    /// Net profit after fees (positive = profit, negative = loss)
    pub fn calculate_net_profit(
        buy_price: Decimal,
        sell_price: Decimal,
        contracts: u32,
        buy_is_kalshi: bool,
        poly_fee_bps: u32,
    ) -> Decimal {
        let c = Decimal::from(contracts);

        // Gross spread
        let gross_profit = (sell_price - buy_price) * c;

        // Calculate fees based on which side is which platform
        let (buy_fee, sell_fee) = if buy_is_kalshi {
            // Buying on Kalshi (taker), selling on Polymarket (taker)
            let kalshi_fee = Self::kalshi_taker_fee(buy_price, contracts);
            let poly_fee = Self::polymarket_taker_fee(sell_price, contracts, poly_fee_bps);
            (kalshi_fee, poly_fee)
        } else {
            // Buying on Polymarket (taker), selling on Kalshi (taker)
            let poly_fee = Self::polymarket_taker_fee(buy_price, contracts, poly_fee_bps);
            let kalshi_fee = Self::kalshi_taker_fee(sell_price, contracts);
            (poly_fee, kalshi_fee)
        };

        gross_profit - buy_fee - sell_fee
    }

    /// Calculate the minimum spread required to break even after fees.
    ///
    /// # Arguments
    /// * `price` - Approximate price point (used for Kalshi fee calculation)
    /// * `poly_fee_bps` - Polymarket fee in basis points
    ///
    /// # Returns
    /// Minimum spread as a decimal (e.g., 0.02 = 2%)
    pub fn minimum_breakeven_spread(price: Decimal, poly_fee_bps: u32) -> Decimal {
        // Kalshi fee = 0.07 * price * (1 - price) per contract
        // For 1 contract, this equals the fee percentage of the contract value
        let kalshi_fee_per_dollar = Decimal::new(7, 2) * price * (Decimal::ONE - price);

        // Polymarket fee
        let poly_fee_per_dollar = Decimal::new(poly_fee_bps as i64, 4);

        // Total fees as percentage of trade value
        kalshi_fee_per_dollar + poly_fee_per_dollar
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    #[test]
    fn test_kalshi_taker_fee() {
        // At 50c price, 100 contracts
        // Fee = 0.07 * 100 * 0.50 * 0.50 = 1.75
        let fee = FeeCalculator::kalshi_taker_fee(dec!(0.50), 100);
        assert_eq!(fee, dec!(1.75));

        // At 40c price, 100 contracts
        // Fee = 0.07 * 100 * 0.40 * 0.60 = 1.68
        let fee = FeeCalculator::kalshi_taker_fee(dec!(0.40), 100);
        assert_eq!(fee, dec!(1.68));

        // At 90c price, 100 contracts
        // Fee = 0.07 * 100 * 0.90 * 0.10 = 0.63
        let fee = FeeCalculator::kalshi_taker_fee(dec!(0.90), 100);
        assert_eq!(fee, dec!(0.63));
    }

    #[test]
    fn test_kalshi_maker_fee() {
        let fee = FeeCalculator::kalshi_maker_fee(dec!(0.50), 100);
        assert_eq!(fee, Decimal::ZERO);
    }

    #[test]
    fn test_polymarket_taker_fee() {
        // 100 contracts at 50c with 50bps fee
        // Fee = 100 * 0.50 * 0.0050 = 0.25
        let fee = FeeCalculator::polymarket_taker_fee(dec!(0.50), 100, 50);
        assert_eq!(fee, dec!(0.25));
    }

    #[test]
    fn test_calculate_net_profit_poly_to_kalshi() {
        // Buy on Polymarket at 40c, sell on Kalshi at 45c, 100 contracts
        // Gross profit = (0.45 - 0.40) * 100 = $5.00
        // Polymarket fee = 100 * 0.40 * 0.0050 = $0.20 (50 bps)
        // Kalshi fee = 0.07 * 100 * 0.45 * 0.55 = $1.7325
        // Net = 5.00 - 0.20 - 1.7325 = $3.0675
        let profit = FeeCalculator::calculate_net_profit(
            dec!(0.40), // buy price (Polymarket)
            dec!(0.45), // sell price (Kalshi)
            100,
            false, // buying on Polymarket
            50,    // Polymarket fee bps
        );

        assert_eq!(profit, dec!(3.0675));
    }

    #[test]
    fn test_calculate_net_profit_kalshi_to_poly() {
        // Buy on Kalshi at 40c, sell on Polymarket at 45c, 100 contracts
        // Gross profit = (0.45 - 0.40) * 100 = $5.00
        // Kalshi fee = 0.07 * 100 * 0.40 * 0.60 = $1.68
        // Polymarket fee = 100 * 0.45 * 0.0050 = $0.225
        // Net = 5.00 - 1.68 - 0.225 = $3.095
        let profit = FeeCalculator::calculate_net_profit(
            dec!(0.40), // buy price (Kalshi)
            dec!(0.45), // sell price (Polymarket)
            100,
            true, // buying on Kalshi
            50,   // Polymarket fee bps
        );

        assert_eq!(profit, dec!(3.095));
    }

    #[test]
    fn test_minimum_breakeven_spread() {
        // At 50c price with 50bps Polymarket fee
        // Kalshi: 0.07 * 0.50 * 0.50 = 0.0175
        // Poly: 0.0050
        // Total = 0.0225 = 2.25%
        let spread = FeeCalculator::minimum_breakeven_spread(dec!(0.50), 50);
        assert_eq!(spread, dec!(0.0225));
    }
}
