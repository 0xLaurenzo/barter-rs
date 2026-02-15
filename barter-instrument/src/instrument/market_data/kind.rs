use crate::instrument::kind::option::{OptionExercise, OptionKind};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};

/// Defines the type of [`MarketDataInstrument`](super::MarketDataInstrument) which is being
/// traded on a given `base_quote` market.
/// Outcome side for prediction market instruments (YES or NO)
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Outcome {
    Yes,
    No,
}

impl std::fmt::Display for Outcome {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Outcome::Yes => write!(f, "yes"),
            Outcome::No => write!(f, "no"),
        }
    }
}

#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MarketDataInstrumentKind {
    Spot,
    Perpetual,
    Future(MarketDataFutureContract),
    Option(MarketDataOptionContract),
    /// Prediction market instrument with YES/NO outcomes
    Prediction(MarketDataPredictionContract),
}

impl Default for MarketDataInstrumentKind {
    fn default() -> Self {
        Self::Spot
    }
}

impl Display for MarketDataInstrumentKind {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            match self {
                MarketDataInstrumentKind::Spot => "spot".to_string(),
                MarketDataInstrumentKind::Perpetual => "perpetual".to_string(),
                MarketDataInstrumentKind::Future(contract) =>
                    format!("future_{}-UTC", contract.expiry.date_naive()),
                MarketDataInstrumentKind::Option(contract) => format!(
                    "option_{}_{}_{}-UTC_{}",
                    contract.kind,
                    contract.exercise,
                    contract.expiry.date_naive(),
                    contract.strike,
                ),
                MarketDataInstrumentKind::Prediction(contract) => format!(
                    "prediction_{}_{}-UTC",
                    contract.outcome,
                    contract.expiry.date_naive(),
                ),
            }
        )
    }
}

#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Deserialize, Serialize)]
pub struct MarketDataFutureContract {
    #[serde(with = "chrono::serde::ts_milliseconds")]
    pub expiry: DateTime<Utc>,
}

#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Deserialize, Serialize)]
pub struct MarketDataOptionContract {
    pub kind: OptionKind,
    pub exercise: OptionExercise,
    #[serde(with = "chrono::serde::ts_milliseconds")]
    pub expiry: DateTime<Utc>,
    pub strike: Decimal,
}

/// Prediction market contract representing a binary outcome (YES/NO) for a specific question
#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Deserialize, Serialize)]
pub struct MarketDataPredictionContract {
    /// The outcome side this contract represents (YES or NO)
    pub outcome: Outcome,
    /// When the prediction market resolves/settles
    #[serde(with = "chrono::serde::ts_milliseconds")]
    pub expiry: DateTime<Utc>,
}
