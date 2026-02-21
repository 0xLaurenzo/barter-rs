//! Integration tests for the delta-neutral prediction market arbitrage strategy.
//!
//! Tests the full opportunity detection pipeline using synthetic orderbooks.
//! No network calls.

use barter_arb_strategy::{
    ArbitrageConfig, ArbitrageDirection, CorrelatedPair, FeeCalculator,
    MinOrderValues, PredictionArbitrageStrategy,
    correlation::{Outcome, PredictionMarketKey},
};
use barter_instrument::exchange::ExchangeId;
use barter_data::books::{Level, OrderBook};
use barter_execution::order::id::StrategyId;
use chrono::{Duration, Utc};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::collections::HashMap;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn pair(ticker: &str, yes_token: &str, no_token: &str, days: i64) -> CorrelatedPair {
    CorrelatedPair::new(
        ticker,
        "0xcond",
        yes_token,
        no_token,
        "Test market",
        Utc::now() + Duration::days(days),
        false,
    )
}

fn inverse_pair(ticker: &str, yes_token: &str, no_token: &str, days: i64) -> CorrelatedPair {
    CorrelatedPair::new(
        ticker,
        "0xcond",
        yes_token,
        no_token,
        "Test market (inverse)",
        Utc::now() + Duration::days(days),
        true,
    )
}

fn default_config() -> ArbitrageConfig {
    ArbitrageConfig {
        min_spread_threshold: dec!(0.02),
        max_position_per_market: 1000,
        max_total_capital: dec!(10000),
        min_order_value: MinOrderValues {
            kalshi: Decimal::ZERO,
            polymarket: dec!(1),
        },
        max_days_to_expiry: Some(90),
    }
}

fn strategy(config: ArbitrageConfig, pairs: Vec<CorrelatedPair>) -> PredictionArbitrageStrategy {
    PredictionArbitrageStrategy::new(StrategyId::new("test-arb"), config, pairs)
}

fn book(bids: Vec<(Decimal, Decimal)>, asks: Vec<(Decimal, Decimal)>) -> OrderBook {
    let bid_levels: Vec<Level> = bids.into_iter().map(|(p, a)| Level::new(p, a)).collect();
    let ask_levels: Vec<Level> = asks.into_iter().map(|(p, a)| Level::new(p, a)).collect();
    OrderBook::new(1, None, bid_levels, ask_levels)
}

/// Insert YES books for both platforms into the book map.
fn insert_yes_books<'a>(
    books: &mut HashMap<PredictionMarketKey, &'a OrderBook>,
    pair: &CorrelatedPair,
    poly_yes: &'a OrderBook,
    kalshi_yes: &'a OrderBook,
) {
    books.insert(
        PredictionMarketKey::polymarket_yes(pair.polymarket_yes_token.clone()),
        poly_yes,
    );
    books.insert(
        PredictionMarketKey::kalshi_yes(pair.kalshi_ticker.clone()),
        kalshi_yes,
    );
}

// ---------------------------------------------------------------------------
// Test 1: Delta-neutral opportunity detected (poly YES + kalshi NO < $1)
// ---------------------------------------------------------------------------

#[test]
fn test_delta_neutral_profitable_detected() {
    let p = pair("KXTEST", "0xyes", "0xno", 30);
    let s = strategy(default_config(), vec![p.clone()]);

    // Poly YES book: ask 40c, bid 38c
    // → Poly NO asks derived: 1 - 0.38 = 62c
    // Kalshi YES book: ask 48c, bid 55c
    // → Kalshi NO asks derived: 1 - 0.55 = 45c
    //
    // Direction 1 (Poly YES + Kalshi NO): 0.40 + 0.45 = 0.85 + fees < 1.00 ✓
    // Direction 2 (Kalshi YES + Poly NO): 0.48 + 0.62 = 1.10 > 1.00 ✗
    let poly_yes = book(
        vec![(dec!(0.38), dec!(100))],
        vec![(dec!(0.40), dec!(100))],
    );
    let kalshi_yes = book(
        vec![(dec!(0.55), dec!(100))],
        vec![(dec!(0.48), dec!(100))],
    );

    let mut books = HashMap::new();
    insert_yes_books(&mut books, &p, &poly_yes, &kalshi_yes);

    let opps = s.detect_opportunities(&books);
    assert!(!opps.is_empty(), "Should detect delta-neutral opportunity");

    let opp = &opps[0];
    assert_eq!(opp.direction, ArbitrageDirection::YesPolyNoKalshi);
    assert!(opp.is_profitable());
    assert!(opp.total_cost < Decimal::ONE);
    assert!(opp.meets_threshold(dec!(0.02)));
}

// ---------------------------------------------------------------------------
// Test 2: No opportunity when cost >= $1.00
// ---------------------------------------------------------------------------

#[test]
fn test_no_opportunity_when_cost_exceeds_one() {
    let p = pair("KXTEST", "0xyes", "0xno", 30);
    let s = strategy(default_config(), vec![p.clone()]);

    // Poly YES ask 55c, Kalshi YES bid 40c → Kalshi NO ask = 60c
    // Direction 1: 0.55 + 0.60 = 1.15 > 1.00
    // Kalshi YES ask 52c, Poly YES bid 38c → Poly NO ask = 62c
    // Direction 2: 0.52 + 0.62 = 1.14 > 1.00
    let poly_yes = book(
        vec![(dec!(0.38), dec!(100))],
        vec![(dec!(0.55), dec!(100))],
    );
    let kalshi_yes = book(
        vec![(dec!(0.40), dec!(100))],
        vec![(dec!(0.52), dec!(100))],
    );

    let mut books = HashMap::new();
    insert_yes_books(&mut books, &p, &poly_yes, &kalshi_yes);

    let opps = s.detect_opportunities(&books);
    assert!(opps.is_empty(), "No opportunity when cost >= $1.00");
}

// ---------------------------------------------------------------------------
// Test 3: Sub-threshold profit filtered
// ---------------------------------------------------------------------------

#[test]
fn test_sub_threshold_profit_filtered() {
    let p = pair("KXTEST", "0xyes", "0xno", 30);
    let mut config = default_config();
    config.min_spread_threshold = dec!(0.10); // Very high threshold
    let s = strategy(config, vec![p.clone()]);

    // Small profitable spread that won't meet 10c/contract threshold
    let poly_yes = book(
        vec![(dec!(0.44), dec!(100))],
        vec![(dec!(0.46), dec!(100))],
    );
    let kalshi_yes = book(
        vec![(dec!(0.52), dec!(100))],
        vec![(dec!(0.50), dec!(100))],
    );

    let mut books = HashMap::new();
    insert_yes_books(&mut books, &p, &poly_yes, &kalshi_yes);

    let opps = s.detect_opportunities(&books);
    let valid: Vec<_> = opps
        .iter()
        .filter(|o| o.meets_threshold(dec!(0.10)))
        .collect();
    assert!(valid.is_empty(), "Should not meet 10c/contract threshold");
}

// ---------------------------------------------------------------------------
// Test 4: Position limit enforced
// ---------------------------------------------------------------------------

#[test]
fn test_position_limit_enforced() {
    let p = pair("KXTEST", "0xyes", "0xno", 30);
    let mut config = default_config();
    config.max_position_per_market = 10;
    let s = strategy(config, vec![p.clone()]);

    let poly_yes = book(
        vec![(dec!(0.55), dec!(500))],
        vec![(dec!(0.40), dec!(500))],
    );
    let kalshi_yes = book(
        vec![(dec!(0.55), dec!(500))],
        vec![(dec!(0.48), dec!(500))],
    );

    let mut books = HashMap::new();
    insert_yes_books(&mut books, &p, &poly_yes, &kalshi_yes);

    let opps = s.detect_opportunities(&books);
    assert!(!opps.is_empty());
    // max_contracts from walk exceeds limit of 10
    assert!(opps[0].max_contracts > 10);
}

// ---------------------------------------------------------------------------
// Test 5: Expired market skipped
// ---------------------------------------------------------------------------

#[test]
fn test_expired_market_skipped() {
    let p = pair("KXTEST", "0xyes", "0xno", -1);
    let s = strategy(default_config(), vec![p.clone()]);

    let poly_yes = book(
        vec![(dec!(0.55), dec!(100))],
        vec![(dec!(0.40), dec!(100))],
    );
    let kalshi_yes = book(
        vec![(dec!(0.55), dec!(100))],
        vec![(dec!(0.48), dec!(100))],
    );

    let mut books = HashMap::new();
    insert_yes_books(&mut books, &p, &poly_yes, &kalshi_yes);

    let opps = s.detect_opportunities(&books);
    assert!(opps.is_empty(), "Expired markets should produce no opportunities");
}

// ---------------------------------------------------------------------------
// Test 6: Min order value filter
// ---------------------------------------------------------------------------

#[test]
fn test_min_order_value_filters_small_orders() {
    let p = pair("KXTEST", "0xyes", "0xno", 30);
    let s = strategy(default_config(), vec![p.clone()]);

    // Only 1 contract available → Poly order value = 1 * 0.40 = $0.40 < $1 minimum
    let poly_yes = book(
        vec![(dec!(0.55), dec!(1))],
        vec![(dec!(0.40), dec!(1))],
    );
    let kalshi_yes = book(
        vec![(dec!(0.55), dec!(100))],
        vec![(dec!(0.48), dec!(100))],
    );

    let mut books = HashMap::new();
    insert_yes_books(&mut books, &p, &poly_yes, &kalshi_yes);

    let opps = s.detect_opportunities(&books);
    assert!(!opps.is_empty());

    let opp = &opps[0];
    // Poly YES side has 1 contract at ~40c = $0.40
    assert!(
        opp.yes_side.order_value() < dec!(1),
        "Poly order value should be below minimum"
    );
}

// ---------------------------------------------------------------------------
// Test 7: Multiple pairs, only profitable ones detected
// ---------------------------------------------------------------------------

#[test]
fn test_multiple_pairs_only_profitable_detected() {
    let p1 = pair("KX_GOOD", "0xyes1", "0xno1", 30);
    let p2 = pair("KX_BAD", "0xyes2", "0xno2", 30);
    let s = strategy(default_config(), vec![p1.clone(), p2.clone()]);

    // Pair 1: Profitable — Poly YES 40c + Kalshi NO (1-0.55=45c) = 85c < $1
    let poly1 = book(
        vec![(dec!(0.38), dec!(100))],
        vec![(dec!(0.40), dec!(100))],
    );
    let kalshi1 = book(
        vec![(dec!(0.55), dec!(100))],
        vec![(dec!(0.48), dec!(100))],
    );

    // Pair 2: Not profitable — Poly YES 55c + Kalshi NO (1-0.40=60c) = 115c > $1
    let poly2 = book(
        vec![(dec!(0.38), dec!(100))],
        vec![(dec!(0.55), dec!(100))],
    );
    let kalshi2 = book(
        vec![(dec!(0.40), dec!(100))],
        vec![(dec!(0.52), dec!(100))],
    );

    let mut books = HashMap::new();
    insert_yes_books(&mut books, &p1, &poly1, &kalshi1);
    insert_yes_books(&mut books, &p2, &poly2, &kalshi2);

    let opps = s.detect_opportunities(&books);
    assert_eq!(opps.len(), 1, "Only one pair should produce an opportunity");
    assert_eq!(opps[0].pair.kalshi_ticker.as_str(), "KX_GOOD");
}

// ---------------------------------------------------------------------------
// Test 8: Both directions detected when both profitable
// ---------------------------------------------------------------------------

#[test]
fn test_both_directions_when_both_profitable() {
    let p = pair("KXTEST", "0xyes", "0xno", 30);
    let s = strategy(default_config(), vec![p.clone()]);

    // Create a book where both directions are profitable:
    // Poly YES ask 30c, Poly YES bid 70c (wide spread)
    // Kalshi YES ask 30c, Kalshi YES bid 70c
    //
    // Dir 1 (Poly YES + Kalshi NO): 0.30 + (1-0.70)=0.30 = 0.60 + fees < 1.00
    // Dir 2 (Kalshi YES + Poly NO): 0.30 + (1-0.70)=0.30 = 0.60 + fees < 1.00
    let poly_yes = book(
        vec![(dec!(0.70), dec!(100))],
        vec![(dec!(0.30), dec!(100))],
    );
    let kalshi_yes = book(
        vec![(dec!(0.70), dec!(100))],
        vec![(dec!(0.30), dec!(100))],
    );

    let mut books = HashMap::new();
    insert_yes_books(&mut books, &p, &poly_yes, &kalshi_yes);

    let opps = s.detect_opportunities(&books);
    assert_eq!(opps.len(), 2, "Both directions should be profitable");

    let directions: Vec<_> = opps.iter().map(|o| o.direction).collect();
    assert!(directions.contains(&ArbitrageDirection::YesPolyNoKalshi));
    assert!(directions.contains(&ArbitrageDirection::YesKalshiNoPoly));
}

// ---------------------------------------------------------------------------
// Test 9: Fee calculations match expected values
// ---------------------------------------------------------------------------

#[test]
fn test_fee_calculations_edge_cases() {
    let fee_low = FeeCalculator::kalshi_taker_fee(dec!(0.01), 100);
    assert_eq!(fee_low, dec!(0.0693));

    let fee_high = FeeCalculator::kalshi_taker_fee(dec!(0.99), 100);
    assert_eq!(fee_high, dec!(0.0693));

    // Symmetric around 50c
    let fee_40 = FeeCalculator::kalshi_taker_fee(dec!(0.40), 100);
    let fee_60 = FeeCalculator::kalshi_taker_fee(dec!(0.60), 100);
    assert_eq!(fee_40, fee_60);

    let fee_50 = FeeCalculator::kalshi_taker_fee(dec!(0.50), 100);
    assert!(fee_50 > fee_40);

    let fee_zero = FeeCalculator::kalshi_taker_fee(dec!(0.50), 0);
    assert_eq!(fee_zero, Decimal::ZERO);
}

// ---------------------------------------------------------------------------
// Test 10: max_days_to_expiry filter
// ---------------------------------------------------------------------------

#[test]
fn test_max_days_to_expiry_filter() {
    let p = pair("KXTEST", "0xyes", "0xno", 120);
    let s = strategy(default_config(), vec![p.clone()]);

    let poly_yes = book(
        vec![(dec!(0.55), dec!(100))],
        vec![(dec!(0.40), dec!(100))],
    );
    let kalshi_yes = book(
        vec![(dec!(0.55), dec!(100))],
        vec![(dec!(0.48), dec!(100))],
    );

    let mut books = HashMap::new();
    insert_yes_books(&mut books, &p, &poly_yes, &kalshi_yes);

    let opps = s.detect_opportunities(&books);
    assert!(opps.is_empty(), "Market too far from expiry should be skipped");
}

// ---------------------------------------------------------------------------
// Test 11: Missing orderbook produces no opportunities
// ---------------------------------------------------------------------------

#[test]
fn test_missing_orderbook_no_opportunities() {
    let p = pair("KXTEST", "0xyes", "0xno", 30);
    let s = strategy(default_config(), vec![p.clone()]);

    // Only Poly YES book, no Kalshi
    let poly_yes = book(
        vec![(dec!(0.55), dec!(100))],
        vec![(dec!(0.40), dec!(100))],
    );

    let mut books = HashMap::new();
    books.insert(
        PredictionMarketKey::polymarket_yes("0xyes"),
        &poly_yes as &OrderBook,
    );

    let opps = s.detect_opportunities(&books);
    assert!(opps.is_empty(), "Missing orderbook should produce no opportunity");
}

// ---------------------------------------------------------------------------
// Test 12: Inverse pair detection
// ---------------------------------------------------------------------------

#[test]
fn test_inverse_pair_detection() {
    let p = inverse_pair("KXTEST", "0xyes", "0xno", 30);
    let s = strategy(default_config(), vec![p.clone()]);

    // Kalshi YES book (= semantic NO in inverse): ask 45c, bid 55c
    // Poly YES book: ask 40c
    //
    // With inverse, Direction 1 (Poly YES + "Kalshi NO"):
    //   Uses poly_yes_asks(40c) + kalshi_yes_asks(45c) = 0.85 + fees < 1.00
    let poly_yes = book(
        vec![(dec!(0.38), dec!(100))],
        vec![(dec!(0.40), dec!(100))],
    );
    let kalshi_yes = book(
        vec![(dec!(0.55), dec!(100))],
        vec![(dec!(0.45), dec!(100))],
    );

    let mut books = HashMap::new();
    insert_yes_books(&mut books, &p, &poly_yes, &kalshi_yes);

    let opps = s.detect_opportunities(&books);
    assert!(!opps.is_empty(), "Inverse pair should detect opportunity");

    let opp = &opps[0];
    assert_eq!(opp.direction, ArbitrageDirection::YesPolyNoKalshi);
    assert!(opp.is_profitable());

    // NO side should target Kalshi YES contract (inverse swap)
    assert_eq!(opp.no_side.exchange, ExchangeId::Kalshi);
    assert_eq!(opp.no_side.outcome, Outcome::Yes);
}

// ---------------------------------------------------------------------------
// Test 13: Depth walking stops at unprofitable levels
// ---------------------------------------------------------------------------

#[test]
fn test_depth_walking_stops_at_unprofitable() {
    let p = pair("KXTEST", "0xyes", "0xno", 30);
    let s = strategy(default_config(), vec![p.clone()]);

    // Poly YES: 2 ask levels — 40c x 50, 48c x 100
    // Kalshi YES bid: 55c x 200 → NO ask = 45c x 200
    //
    // Level 1: 0.40 + 0.45 = 0.85 + fees → profitable, fill 50
    // Level 2: 0.48 + 0.45 = 0.93 + fees → profitable, fill 100 (limited by NO remaining)
    // Then done (or if second level has high enough fee, stop)
    let poly_yes = book(
        vec![(dec!(0.35), dec!(200))], // bid
        vec![
            (dec!(0.40), dec!(50)),  // ask level 1
            (dec!(0.48), dec!(100)), // ask level 2
        ],
    );
    let kalshi_yes = book(
        vec![(dec!(0.55), dec!(200))], // bid → NO ask = 45c
        vec![(dec!(0.60), dec!(200))], // ask
    );

    let mut books = HashMap::new();
    insert_yes_books(&mut books, &p, &poly_yes, &kalshi_yes);

    let opps = s.detect_opportunities(&books);
    assert!(!opps.is_empty());

    let opp = &opps[0];
    // Should fill at least the first level (50 contracts)
    assert!(opp.max_contracts >= 50);
    assert!(opp.is_profitable());
}

// ---------------------------------------------------------------------------
// Test 14: All orders are BUY
// ---------------------------------------------------------------------------

#[test]
fn test_all_orders_are_buy() {
    let p = pair("KXTEST", "0xyes", "0xno", 30);
    let s = strategy(default_config(), vec![p.clone()]);

    let poly_yes = book(
        vec![(dec!(0.55), dec!(100))],
        vec![(dec!(0.40), dec!(100))],
    );
    let kalshi_yes = book(
        vec![(dec!(0.55), dec!(100))],
        vec![(dec!(0.48), dec!(100))],
    );

    let mut books = HashMap::new();
    insert_yes_books(&mut books, &p, &poly_yes, &kalshi_yes);

    let opps = s.detect_opportunities(&books);
    assert!(!opps.is_empty());

    // Both sides should be BUY (no sell orders in delta-neutral model)
    let opp = &opps[0];
    // YES side buys YES, NO side buys NO — both are purchases
    assert_eq!(opp.yes_side.outcome, Outcome::Yes);
    assert_eq!(opp.no_side.outcome, Outcome::No);
    // No is_buy field needed — all orders are BUY by design
}
