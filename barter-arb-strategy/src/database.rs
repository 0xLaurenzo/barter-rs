//! Database querier for fetching correlated market pairs from Supabase.
//!
//! This module provides a Rust implementation of the same database queries
//! used by the TypeScript arbitrage bot, enabling the barter strategy to
//! fetch market pairs directly.

use crate::correlation::CorrelatedPair;
use chrono::{DateTime, Utc};
use reqwest::Client;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use smol_str::SmolStr;
use thiserror::Error;
use tracing::{debug, warn};

/// Errors that can occur when querying the database.
#[derive(Debug, Error)]
pub enum DatabaseError {
    #[error("HTTP request failed: {0}")]
    Request(#[from] reqwest::Error),

    #[error("Failed to parse response: {0}")]
    Parse(String),

    #[error("Database error: {message}")]
    Database { message: String, code: Option<String> },

    #[error("Missing configuration: {0}")]
    Config(String),
}

/// Filters for querying market pairs.
#[derive(Debug, Clone, Default)]
pub struct MarketPairFilters {
    /// Minimum similarity score (0.0-1.0)
    pub min_similarity: Option<Decimal>,
    /// Minimum confidence score (0.0-1.0)
    pub min_confidence: Option<Decimal>,
    /// Maximum number of results
    pub limit: Option<u32>,
    /// Offset for pagination
    pub offset: Option<u32>,
    /// Only return validated pairs
    pub valid_only: Option<bool>,
}

impl MarketPairFilters {
    /// Create filters with default values matching the TypeScript implementation.
    pub fn default_filters() -> Self {
        Self {
            min_similarity: Some(Decimal::new(70, 2)), // 0.70
            min_confidence: Some(Decimal::new(70, 2)), // 0.70
            limit: Some(100),
            offset: Some(0),
            valid_only: Some(true),
        }
    }

    /// Create filters for high-quality pairs.
    pub fn high_quality() -> Self {
        Self {
            min_similarity: Some(Decimal::new(85, 2)), // 0.85
            min_confidence: Some(Decimal::new(85, 2)), // 0.85
            limit: Some(20),
            offset: Some(0),
            valid_only: Some(true),
        }
    }
}

/// Raw market pair record from the database.
///
/// This matches the schema returned by `get_market_pairs_with_volume` RPC.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct MarketPairRecord {
    pub id: i64,
    pub polymarket_id: String,
    #[serde(default)]
    pub polymarket_condition_id: Option<String>,
    /// Token IDs stored as JSON array: ["yes_token", "no_token"]
    #[serde(default)]
    pub polymarket_yes_token_id: Option<String>,
    pub kalshi_ticker: String,
    pub similarity_score: Decimal,
    pub confidence_score: Decimal,
    pub evaluation_id: i64,
    #[serde(default)]
    pub llm_notes: Option<String>,
    pub discovered_at: DateTime<Utc>,
    #[serde(default)]
    pub last_verified_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub verified_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub valid: Option<bool>,
    #[serde(default)]
    pub validation_result: Option<String>,
    #[serde(default)]
    pub inverse: Option<bool>,
    #[serde(default)]
    pub polymarket_volume: Option<Decimal>,
    #[serde(default)]
    pub kalshi_volume: Option<Decimal>,
    #[serde(default)]
    pub polymarket_question: Option<String>,
    #[serde(default)]
    pub kalshi_expiry: Option<DateTime<Utc>>,
}

impl MarketPairRecord {
    /// Parse YES token ID from the JSON array format.
    pub fn yes_token_id(&self) -> Option<SmolStr> {
        parse_token_id(&self.polymarket_yes_token_id, 0)
    }

    /// Parse NO token ID from the JSON array format.
    pub fn no_token_id(&self) -> Option<SmolStr> {
        parse_token_id(&self.polymarket_yes_token_id, 1)
    }

    /// Convert to a CorrelatedPair.
    ///
    /// Returns None if required fields are missing (token IDs, condition ID).
    pub fn to_correlated_pair(&self) -> Option<CorrelatedPair> {
        let yes_token = self.yes_token_id()?;
        let no_token = self.no_token_id()?;
        let condition_id = self.polymarket_condition_id.as_ref()?;

        // Use Kalshi expiry if available, otherwise default to 30 days from now
        let expiry = self.kalshi_expiry.unwrap_or_else(|| {
            Utc::now() + chrono::Duration::days(30)
        });

        Some(CorrelatedPair::new(
            self.kalshi_ticker.as_str(),
            condition_id.as_str(),
            yes_token,
            no_token,
            self.polymarket_question.as_deref().unwrap_or(""),
            expiry,
            self.inverse.unwrap_or(false),
        ))
    }

    /// Check if this is a valid, usable market pair.
    pub fn is_valid(&self) -> bool {
        self.valid == Some(true)
            && self.yes_token_id().is_some()
            && self.no_token_id().is_some()
            && self.polymarket_condition_id.is_some()
    }
}

/// Parse token ID from database value.
///
/// Database may return:
/// - A JSON array string like '["token1", "token2"]'
/// - A raw token ID string
/// - null
fn parse_token_id(value: &Option<String>, index: usize) -> Option<SmolStr> {
    let value = value.as_ref()?;
    let trimmed = value.trim();

    // Check if it's a JSON array
    if trimmed.starts_with('[') {
        if let Ok(arr) = serde_json::from_str::<Vec<String>>(trimmed) {
            return arr.get(index).map(|s| SmolStr::new(s));
        }
    }

    // Return as-is if it looks like a valid token ID (long numeric string)
    // and this is the first index (YES token)
    if index == 0 && trimmed.len() >= 70 && trimmed.chars().all(|c| c.is_ascii_digit()) {
        return Some(SmolStr::new(trimmed));
    }

    None
}

/// RPC request parameters for get_market_pairs_with_volume.
#[derive(Debug, Serialize)]
struct RpcParams {
    min_similarity: Decimal,
    min_confidence: Decimal,
    row_limit: u32,
    row_offset: u32,
    valid_only: bool,
}

/// Supabase error response.
#[derive(Debug, Deserialize)]
struct SupabaseError {
    message: String,
    #[serde(default)]
    code: Option<String>,
}

/// Database querier for Supabase.
///
/// Fetches correlated market pairs using the same RPC function
/// as the TypeScript implementation.
#[derive(Debug, Clone)]
pub struct DatabaseQuerier {
    client: Client,
    base_url: String,
    api_key: String,
}

impl DatabaseQuerier {
    /// Create a new database querier.
    ///
    /// # Arguments
    /// * `supabase_url` - Supabase project URL (e.g., "https://xxx.supabase.co")
    /// * `api_key` - Supabase API key (service key or anon key)
    pub fn new(supabase_url: impl Into<String>, api_key: impl Into<String>) -> Self {
        Self {
            client: Client::new(),
            base_url: supabase_url.into(),
            api_key: api_key.into(),
        }
    }

    /// Create from environment variables.
    ///
    /// Reads `SUPABASE_URL` and `SUPABASE_SERVICE_KEY` (or `SUPABASE_ANON_KEY`).
    pub fn from_env() -> Result<Self, DatabaseError> {
        let url = std::env::var("SUPABASE_URL")
            .map_err(|_| DatabaseError::Config("SUPABASE_URL not set".into()))?;

        let key = std::env::var("SUPABASE_SERVICE_KEY")
            .or_else(|_| std::env::var("SUPABASE_ANON_KEY"))
            .map_err(|_| {
                DatabaseError::Config("SUPABASE_SERVICE_KEY or SUPABASE_ANON_KEY not set".into())
            })?;

        Ok(Self::new(url, key))
    }

    /// Query correlated market pairs from the database.
    ///
    /// Uses the `get_market_pairs_with_volume` RPC function.
    pub async fn get_market_pairs(
        &self,
        filters: MarketPairFilters,
    ) -> Result<Vec<MarketPairRecord>, DatabaseError> {
        let filters = MarketPairFilters {
            min_similarity: filters.min_similarity.or(Some(Decimal::new(70, 2))),
            min_confidence: filters.min_confidence.or(Some(Decimal::new(70, 2))),
            limit: filters.limit.or(Some(100)),
            offset: filters.offset.or(Some(0)),
            valid_only: filters.valid_only.or(Some(true)),
        };

        let params = RpcParams {
            min_similarity: filters.min_similarity.unwrap(),
            min_confidence: filters.min_confidence.unwrap(),
            row_limit: filters.limit.unwrap(),
            row_offset: filters.offset.unwrap(),
            valid_only: filters.valid_only.unwrap(),
        };

        let url = format!("{}/rest/v1/rpc/get_market_pairs_with_volume", self.base_url);

        debug!("Fetching market pairs from {}", url);

        let response = self
            .client
            .post(&url)
            .header("apikey", &self.api_key)
            .header("Authorization", format!("Bearer {}", self.api_key))
            .header("Content-Type", "application/json")
            .json(&params)
            .send()
            .await?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();

            // Try to parse as Supabase error
            if let Ok(err) = serde_json::from_str::<SupabaseError>(&body) {
                return Err(DatabaseError::Database {
                    message: err.message,
                    code: err.code,
                });
            }

            return Err(DatabaseError::Database {
                message: format!("HTTP {}: {}", status, body),
                code: None,
            });
        }

        let records: Vec<MarketPairRecord> = response.json().await.map_err(|e| {
            DatabaseError::Parse(format!("Failed to parse market pairs: {}", e))
        })?;

        debug!("Fetched {} market pairs", records.len());

        Ok(records)
    }

    /// Get high-quality market pairs.
    pub async fn get_high_quality_pairs(&self) -> Result<Vec<MarketPairRecord>, DatabaseError> {
        self.get_market_pairs(MarketPairFilters::high_quality()).await
    }

    /// Get market pairs and convert to CorrelatedPair.
    ///
    /// Filters out pairs that can't be converted (missing token IDs, etc.).
    pub async fn get_correlated_pairs(
        &self,
        filters: MarketPairFilters,
    ) -> Result<Vec<CorrelatedPair>, DatabaseError> {
        let records = self.get_market_pairs(filters).await?;

        let pairs: Vec<CorrelatedPair> = records
            .into_iter()
            .filter_map(|record| {
                if !record.is_valid() {
                    warn!(
                        "Skipping invalid market pair: kalshi={}, valid={:?}",
                        record.kalshi_ticker, record.valid
                    );
                    return None;
                }
                record.to_correlated_pair()
            })
            .collect();

        debug!("Converted {} records to correlated pairs", pairs.len());

        Ok(pairs)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_token_id_json_array() {
        let value = Some(r#"["111222333444555666777888999000", "999888777666555444333222111000"]"#.to_string());

        let yes = parse_token_id(&value, 0);
        let no = parse_token_id(&value, 1);

        assert_eq!(yes, Some(SmolStr::new("111222333444555666777888999000")));
        assert_eq!(no, Some(SmolStr::new("999888777666555444333222111000")));
    }

    #[test]
    fn test_parse_token_id_raw_string() {
        // 70+ digit numeric string
        let value = Some("1234567890123456789012345678901234567890123456789012345678901234567890".to_string());

        let yes = parse_token_id(&value, 0);
        let no = parse_token_id(&value, 1);

        assert!(yes.is_some());
        assert!(no.is_none()); // Can't extract NO from raw string
    }

    #[test]
    fn test_parse_token_id_none() {
        let value: Option<String> = None;
        assert!(parse_token_id(&value, 0).is_none());
    }

    #[test]
    fn test_market_pair_filters_default() {
        let filters = MarketPairFilters::default_filters();
        assert_eq!(filters.min_similarity, Some(Decimal::new(70, 2)));
        assert_eq!(filters.min_confidence, Some(Decimal::new(70, 2)));
        assert_eq!(filters.limit, Some(100));
        assert_eq!(filters.valid_only, Some(true));
    }

    #[test]
    fn test_market_pair_record_to_correlated_pair() {
        let record = MarketPairRecord {
            id: 1,
            polymarket_id: "poly123".to_string(),
            polymarket_condition_id: Some("0xcondition".to_string()),
            polymarket_yes_token_id: Some(r#"["111222333444555666777888999000111222333444555666777888999000111222333444", "999888777666555444333222111000999888777666555444333222111000999888777666"]"#.to_string()),
            kalshi_ticker: "KXTEST-25JAN31".to_string(),
            similarity_score: Decimal::new(95, 2),
            confidence_score: Decimal::new(90, 2),
            evaluation_id: 1,
            llm_notes: None,
            discovered_at: Utc::now(),
            last_verified_at: None,
            verified_at: None,
            valid: Some(true),
            validation_result: None,
            inverse: None,
            polymarket_volume: None,
            kalshi_volume: None,
            polymarket_question: Some("Will X happen?".to_string()),
            kalshi_expiry: None,
        };

        assert!(record.is_valid());

        let pair = record.to_correlated_pair().unwrap();
        assert_eq!(pair.kalshi_ticker.as_str(), "KXTEST-25JAN31");
        assert_eq!(pair.polymarket_condition_id.as_str(), "0xcondition");
        assert_eq!(pair.description, "Will X happen?");
    }
}
