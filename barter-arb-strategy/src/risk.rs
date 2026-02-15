//! Risk manager for prediction market arbitrage.
//!
//! Validates orders against maximum capital constraints before execution.

use crate::state::ArbitrageEngineState;
use barter::risk::{RiskApproved, RiskManager, RiskRefused};
use barter_execution::order::request::{OrderRequestCancel, OrderRequestOpen};
use barter_instrument::{exchange::ExchangeIndex, instrument::InstrumentIndex};
use rust_decimal::Decimal;

/// Risk manager that enforces max capital and per-order limits.
#[derive(Debug, Clone)]
pub struct ArbitrageRiskManager {
    /// Maximum total capital that can be deployed across all positions.
    pub max_total_capital: Decimal,
    /// Maximum notional value per single order.
    pub max_order_notional: Decimal,
}

impl Default for ArbitrageRiskManager {
    fn default() -> Self {
        Self {
            max_total_capital: Decimal::from(10_000),
            max_order_notional: Decimal::from(1_000),
        }
    }
}

impl RiskManager<ExchangeIndex, InstrumentIndex> for ArbitrageRiskManager {
    type State = ArbitrageEngineState;

    fn check(
        &self,
        state: &Self::State,
        cancels: impl IntoIterator<Item = OrderRequestCancel<ExchangeIndex, InstrumentIndex>>,
        opens: impl IntoIterator<Item = OrderRequestOpen<ExchangeIndex, InstrumentIndex>>,
    ) -> (
        impl IntoIterator<Item = RiskApproved<OrderRequestCancel<ExchangeIndex, InstrumentIndex>>>,
        impl IntoIterator<Item = RiskApproved<OrderRequestOpen<ExchangeIndex, InstrumentIndex>>>,
        impl IntoIterator<Item = RiskRefused<OrderRequestCancel<ExchangeIndex, InstrumentIndex>>>,
        impl IntoIterator<Item = RiskRefused<OrderRequestOpen<ExchangeIndex, InstrumentIndex>>>,
    ) {
        // Cancels always approved
        let approved_cancels: Vec<_> = cancels.into_iter().map(RiskApproved::new).collect();

        let mut approved_opens = Vec::new();
        let mut refused_opens = Vec::new();

        let deployed = state.global.total_deployed;

        for open in opens {
            let notional = open.state.price * open.state.quantity;

            if deployed + notional > self.max_total_capital {
                refused_opens.push(RiskRefused::new(
                    open,
                    format!(
                        "Would exceed max capital: deployed={} + order={} > max={}",
                        deployed, notional, self.max_total_capital
                    ),
                ));
                continue;
            }

            if notional > self.max_order_notional {
                refused_opens.push(RiskRefused::new(
                    open,
                    format!(
                        "Order notional {} exceeds max {}",
                        notional, self.max_order_notional
                    ),
                ));
                continue;
            }

            approved_opens.push(RiskApproved::new(open));
        }

        (approved_cancels, approved_opens, std::iter::empty(), refused_opens)
    }
}
