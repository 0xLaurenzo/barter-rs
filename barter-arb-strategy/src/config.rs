//! Configuration types for the prediction arbitrage strategy.

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

/// Configuration for the prediction market arbitrage strategy.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ArbitrageConfig {
    /// Minimum spread after fees to trigger trade (e.g., 0.02 = 2%)
    pub min_spread_threshold: Decimal,
    /// Maximum position size per market (in contracts)
    pub max_position_per_market: u32,
    /// Maximum total capital deployed across all positions
    pub max_total_capital: Decimal,
    /// Minimum order values per platform
    pub min_order_value: MinOrderValues,
    /// Maximum days until market expiry to consider
    pub max_days_to_expiry: Option<u32>,
}

impl Default for ArbitrageConfig {
    fn default() -> Self {
        Self {
            min_spread_threshold: Decimal::new(2, 2), // 2%
            max_position_per_market: 1000,
            max_total_capital: Decimal::new(10000, 0), // $10,000
            min_order_value: MinOrderValues::default(),
            max_days_to_expiry: Some(90),
        }
    }
}

/// Minimum order values per platform.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct MinOrderValues {
    /// Kalshi minimum order value (typically $0)
    pub kalshi: Decimal,
    /// Polymarket minimum order value (typically $1)
    pub polymarket: Decimal,
}

impl Default for MinOrderValues {
    fn default() -> Self {
        Self {
            kalshi: Decimal::ZERO,
            polymarket: Decimal::ONE,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = ArbitrageConfig::default();
        assert_eq!(config.min_spread_threshold, Decimal::new(2, 2));
        assert_eq!(config.max_position_per_market, 1000);
        assert_eq!(config.max_total_capital, Decimal::new(10000, 0));
    }

    #[test]
    fn test_min_order_values_default() {
        let min_values = MinOrderValues::default();
        assert_eq!(min_values.kalshi, Decimal::ZERO);
        assert_eq!(min_values.polymarket, Decimal::ONE);
    }
}
