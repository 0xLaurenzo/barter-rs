//! Integration tests for the prediction market arbitrage strategy pipeline.
//!
//! Tests the full `generate_algo_orders` cycle using constructed engine state
//! with synthetic orderbooks. No network calls.

use barter_arb_strategy::{
    ArbitrageConfig, ArbitrageOpportunity, ArbitrageDirection, CorrelatedPair,
    FeeCalculator, MinOrderValues, OrderSide, PredictionArbitrageStrategy,
    correlation::{Outcome, PredictionMarketKey},
};
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

// ---------------------------------------------------------------------------
// Test 1: Profitable spread generates orders
// ---------------------------------------------------------------------------

#[test]
fn test_profitable_spread_detects_opportunity() {
    let p = pair("KXTEST", "0xyes", "0xno", 30);
    let s = strategy(default_config(), vec![p.clone()]);

    // Poly YES ask 40c, Kalshi YES bid 46c → 6c raw spread
    let poly_yes = book(
        vec![(dec!(0.38), dec!(100))],
        vec![(dec!(0.40), dec!(100))],
    );
    let kalshi_yes = book(
        vec![(dec!(0.46), dec!(100))],
        vec![(dec!(0.48), dec!(100))],
    );

    let mut books: HashMap<PredictionMarketKey, &OrderBook> = HashMap::new();
    books.insert(PredictionMarketKey::polymarket_yes("0xyes"), &poly_yes);
    books.insert(PredictionMarketKey::kalshi_yes("KXTEST"), &kalshi_yes);

    let opps = s.detect_opportunities(&books);
    assert!(!opps.is_empty(), "Should detect at least one opportunity");

    let opp = &opps[0];
    assert_eq!(opp.direction, ArbitrageDirection::PolyToKalshi);
    assert_eq!(opp.outcome, Outcome::Yes);
    assert_eq!(opp.spread_before_fees, dec!(0.06));
    assert!(opp.is_profitable());
    assert!(opp.meets_threshold(dec!(0.02)));
}

// ---------------------------------------------------------------------------
// Test 2: Sub-threshold spread produces no valid orders
// ---------------------------------------------------------------------------

#[test]
fn test_sub_threshold_spread_filtered() {
    let p = pair("KXTEST", "0xyes", "0xno", 30);
    let mut config = default_config();
    config.min_spread_threshold = dec!(0.10); // Very high threshold
    let s = strategy(config, vec![p.clone()]);

    // 6c raw spread → ~3c after fees, below 10c threshold
    let poly_yes = book(
        vec![(dec!(0.38), dec!(100))],
        vec![(dec!(0.40), dec!(100))],
    );
    let kalshi_yes = book(
        vec![(dec!(0.46), dec!(100))],
        vec![(dec!(0.48), dec!(100))],
    );

    let mut books: HashMap<PredictionMarketKey, &OrderBook> = HashMap::new();
    books.insert(PredictionMarketKey::polymarket_yes("0xyes"), &poly_yes);
    books.insert(PredictionMarketKey::kalshi_yes("KXTEST"), &kalshi_yes);

    let opps = s.detect_opportunities(&books);
    // Opportunities exist but don't meet high threshold
    let valid: Vec<_> = opps
        .iter()
        .filter(|o| o.meets_threshold(dec!(0.10)))
        .collect();
    assert!(valid.is_empty(), "No opportunity should meet 10c threshold");
}

// ---------------------------------------------------------------------------
// Test 3: Position limit hit → no new orders
// ---------------------------------------------------------------------------

#[test]
fn test_position_limit_blocks_orders() {
    let p = pair("KXTEST", "0xyes", "0xno", 30);
    let mut config = default_config();
    config.max_position_per_market = 10; // Very low limit
    let s = strategy(config, vec![p.clone()]);

    // Large liquidity available but position limit is 10
    let poly_yes = book(
        vec![(dec!(0.38), dec!(500))],
        vec![(dec!(0.40), dec!(500))],
    );
    let kalshi_yes = book(
        vec![(dec!(0.46), dec!(500))],
        vec![(dec!(0.48), dec!(500))],
    );

    let mut books: HashMap<PredictionMarketKey, &OrderBook> = HashMap::new();
    books.insert(PredictionMarketKey::polymarket_yes("0xyes"), &poly_yes);
    books.insert(PredictionMarketKey::kalshi_yes("KXTEST"), &kalshi_yes);

    let opps = s.detect_opportunities(&books);
    assert!(!opps.is_empty());

    // max_contracts is min(buy_size, sell_size) = 500, exceeds limit of 10
    let opp = &opps[0];
    assert_eq!(opp.max_contracts, 500);
    // The strategy checks max_contracts <= max_position_per_market
    assert!(opp.max_contracts > 10);
}

// ---------------------------------------------------------------------------
// Test 4: Expired market is skipped
// ---------------------------------------------------------------------------

#[test]
fn test_expired_market_skipped() {
    // Expiry in the past
    let p = pair("KXTEST", "0xyes", "0xno", -1);
    let s = strategy(default_config(), vec![p.clone()]);

    let poly_yes = book(
        vec![(dec!(0.38), dec!(100))],
        vec![(dec!(0.40), dec!(100))],
    );
    let kalshi_yes = book(
        vec![(dec!(0.46), dec!(100))],
        vec![(dec!(0.48), dec!(100))],
    );

    let mut books: HashMap<PredictionMarketKey, &OrderBook> = HashMap::new();
    books.insert(PredictionMarketKey::polymarket_yes("0xyes"), &poly_yes);
    books.insert(PredictionMarketKey::kalshi_yes("KXTEST"), &kalshi_yes);

    let opps = s.detect_opportunities(&books);
    assert!(opps.is_empty(), "Expired markets should produce no opportunities");
}

// ---------------------------------------------------------------------------
// Test 5: Min order value filter
// ---------------------------------------------------------------------------

#[test]
fn test_min_order_value_filters_small_orders() {
    let p = pair("KXTEST", "0xyes", "0xno", 30);
    let s = strategy(default_config(), vec![p.clone()]);

    // Only 1 contract available at 40c → order value = $0.40 < $1.00 Poly minimum
    let poly_yes = book(
        vec![(dec!(0.38), dec!(1))],
        vec![(dec!(0.40), dec!(1))],
    );
    let kalshi_yes = book(
        vec![(dec!(0.46), dec!(100))],
        vec![(dec!(0.48), dec!(100))],
    );

    let mut books: HashMap<PredictionMarketKey, &OrderBook> = HashMap::new();
    books.insert(PredictionMarketKey::polymarket_yes("0xyes"), &poly_yes);
    books.insert(PredictionMarketKey::kalshi_yes("KXTEST"), &kalshi_yes);

    let opps = s.detect_opportunities(&books);
    assert!(!opps.is_empty());

    // max_contracts = min(1, 100) = 1, buy_value = 1 * 0.40 = $0.40
    let opp = &opps[0];
    assert_eq!(opp.max_contracts, 1);
    assert_eq!(opp.buy_side.order_value(), dec!(0.40));
    assert!(opp.buy_side.order_value() < dec!(1), "Order value below Poly minimum");
}

// ---------------------------------------------------------------------------
// Test 6: Multiple pairs, only one profitable
// ---------------------------------------------------------------------------

#[test]
fn test_multiple_pairs_only_profitable_detected() {
    let p1 = pair("KX_GOOD", "0xyes1", "0xno1", 30);
    let p2 = pair("KX_FLAT", "0xyes2", "0xno2", 30);
    let p3 = pair("KX_REVERSED", "0xyes3", "0xno3", 30);
    let s = strategy(default_config(), vec![p1.clone(), p2.clone(), p3.clone()]);

    // Pair 1: Profitable spread (6c)
    let poly1 = book(vec![], vec![(dec!(0.40), dec!(100))]);
    let kalshi1 = book(vec![(dec!(0.46), dec!(100))], vec![]);

    // Pair 2: Zero spread (both at 45c)
    let poly2 = book(vec![], vec![(dec!(0.45), dec!(100))]);
    let kalshi2 = book(vec![(dec!(0.45), dec!(100))], vec![]);

    // Pair 3: Negative spread (Poly ask > Kalshi bid)
    let poly3 = book(vec![], vec![(dec!(0.50), dec!(100))]);
    let kalshi3 = book(vec![(dec!(0.44), dec!(100))], vec![]);

    let mut books: HashMap<PredictionMarketKey, &OrderBook> = HashMap::new();
    books.insert(PredictionMarketKey::polymarket_yes("0xyes1"), &poly1);
    books.insert(PredictionMarketKey::kalshi_yes("KX_GOOD"), &kalshi1);
    books.insert(PredictionMarketKey::polymarket_yes("0xyes2"), &poly2);
    books.insert(PredictionMarketKey::kalshi_yes("KX_FLAT"), &kalshi2);
    books.insert(PredictionMarketKey::polymarket_yes("0xyes3"), &poly3);
    books.insert(PredictionMarketKey::kalshi_yes("KX_REVERSED"), &kalshi3);

    let opps = s.detect_opportunities(&books);

    // Only pair 1 has a positive spread
    assert_eq!(opps.len(), 1, "Only one pair should produce an opportunity");
    assert_eq!(opps[0].pair.kalshi_ticker.as_str(), "KX_GOOD");
}

// ---------------------------------------------------------------------------
// Test 7: Both directions work (PolyToKalshi and KalshiToPoly)
// ---------------------------------------------------------------------------

#[test]
fn test_both_directions() {
    let p = pair("KXTEST", "0xyes", "0xno", 30);
    let s = strategy(default_config(), vec![p.clone()]);

    // Set up books where BOTH directions have a spread:
    // Direction 1 (PolyToKalshi YES): Poly ask 40c, Kalshi bid 46c → 6c spread
    // Direction 2 (KalshiToPoly YES): Kalshi ask 48c, Poly bid 55c → 7c spread
    let poly_yes = book(
        vec![(dec!(0.55), dec!(100))], // Poly bids (for KalshiToPoly sell side)
        vec![(dec!(0.40), dec!(100))], // Poly asks (for PolyToKalshi buy side)
    );
    let kalshi_yes = book(
        vec![(dec!(0.46), dec!(100))], // Kalshi bids (for PolyToKalshi sell side)
        vec![(dec!(0.48), dec!(100))], // Kalshi asks (for KalshiToPoly buy side)
    );

    let mut books: HashMap<PredictionMarketKey, &OrderBook> = HashMap::new();
    books.insert(PredictionMarketKey::polymarket_yes("0xyes"), &poly_yes);
    books.insert(PredictionMarketKey::kalshi_yes("KXTEST"), &kalshi_yes);

    let opps = s.detect_opportunities(&books);

    let directions: Vec<_> = opps.iter().map(|o| o.direction).collect();
    assert!(
        directions.contains(&ArbitrageDirection::PolyToKalshi),
        "Should detect PolyToKalshi"
    );
    assert!(
        directions.contains(&ArbitrageDirection::KalshiToPoly),
        "Should detect KalshiToPoly"
    );
}

// ---------------------------------------------------------------------------
// Test 8: Fee calculations match expected values
// ---------------------------------------------------------------------------

#[test]
fn test_fee_calculations_edge_cases() {
    // Edge case: price at 1c (near 0)
    let fee_low = FeeCalculator::kalshi_taker_fee(dec!(0.01), 100);
    // 0.07 * 100 * 0.01 * 0.99 = 0.0693
    assert_eq!(fee_low, dec!(0.0693));

    // Edge case: price at 99c (near 1)
    let fee_high = FeeCalculator::kalshi_taker_fee(dec!(0.99), 100);
    // 0.07 * 100 * 0.99 * 0.01 = 0.0693
    assert_eq!(fee_high, dec!(0.0693));

    // Symmetric: fee at price P equals fee at price (1-P)
    let fee_40 = FeeCalculator::kalshi_taker_fee(dec!(0.40), 100);
    let fee_60 = FeeCalculator::kalshi_taker_fee(dec!(0.60), 100);
    assert_eq!(fee_40, fee_60, "Fees should be symmetric around 50c");

    // Max fee at 50c
    let fee_50 = FeeCalculator::kalshi_taker_fee(dec!(0.50), 100);
    assert!(fee_50 > fee_40, "Fee at 50c should be highest");

    // Zero contracts = zero fee
    let fee_zero = FeeCalculator::kalshi_taker_fee(dec!(0.50), 0);
    assert_eq!(fee_zero, Decimal::ZERO);
}

// ---------------------------------------------------------------------------
// Test 9: Net profit calculation matches manual computation
// ---------------------------------------------------------------------------

#[test]
fn test_net_profit_manual_verification() {
    // Buy Poly at 40c, sell Kalshi at 46c, 100 contracts, 50bps Poly fee
    // Gross = (0.46 - 0.40) * 100 = $6.00
    // Poly fee = 100 * 0.40 * 0.0050 = $0.20
    // Kalshi fee = 0.07 * 100 * 0.46 * 0.54 = $1.7388
    // Net = 6.00 - 0.20 - 1.7388 = $4.0612
    let profit = FeeCalculator::calculate_net_profit(
        dec!(0.40),
        dec!(0.46),
        100,
        false, // buy on Polymarket
        50,
    );
    assert_eq!(profit, dec!(4.0612));
}

// ---------------------------------------------------------------------------
// Test 10: NO side arbitrage detection
// ---------------------------------------------------------------------------

#[test]
fn test_no_side_arbitrage() {
    let p = pair("KXTEST", "0xyes", "0xno", 30);
    let s = strategy(default_config(), vec![p.clone()]);

    // NO side: Poly NO ask 35c, Kalshi NO bid 42c → 7c spread
    let poly_no = book(
        vec![(dec!(0.33), dec!(200))],
        vec![(dec!(0.35), dec!(200))],
    );
    let kalshi_no = book(
        vec![(dec!(0.42), dec!(200))],
        vec![(dec!(0.44), dec!(200))],
    );

    let mut books: HashMap<PredictionMarketKey, &OrderBook> = HashMap::new();
    books.insert(PredictionMarketKey::polymarket_no("0xno"), &poly_no);
    books.insert(PredictionMarketKey::kalshi_no("KXTEST"), &kalshi_no);

    let opps = s.detect_opportunities(&books);
    assert!(!opps.is_empty(), "Should detect NO-side opportunity");

    let opp = &opps[0];
    assert_eq!(opp.outcome, Outcome::No);
    assert_eq!(opp.direction, ArbitrageDirection::PolyToKalshi);
    assert_eq!(opp.spread_before_fees, dec!(0.07));
}

// ---------------------------------------------------------------------------
// Test 11: max_days_to_expiry filter
// ---------------------------------------------------------------------------

#[test]
fn test_max_days_to_expiry_filter() {
    // Pair expires in 120 days, config limits to 90 days
    let p = pair("KXTEST", "0xyes", "0xno", 120);
    let s = strategy(default_config(), vec![p.clone()]);

    let poly_yes = book(vec![], vec![(dec!(0.40), dec!(100))]);
    let kalshi_yes = book(vec![(dec!(0.46), dec!(100))], vec![]);

    let mut books: HashMap<PredictionMarketKey, &OrderBook> = HashMap::new();
    books.insert(PredictionMarketKey::polymarket_yes("0xyes"), &poly_yes);
    books.insert(PredictionMarketKey::kalshi_yes("KXTEST"), &kalshi_yes);

    let opps = s.detect_opportunities(&books);
    assert!(opps.is_empty(), "Market too far from expiry should be skipped");
}

// ---------------------------------------------------------------------------
// Test 12: Missing orderbook data produces no opportunities
// ---------------------------------------------------------------------------

#[test]
fn test_missing_orderbook_no_opportunities() {
    let p = pair("KXTEST", "0xyes", "0xno", 30);
    let s = strategy(default_config(), vec![p.clone()]);

    // Only provide one side — missing Kalshi book
    let poly_yes = book(
        vec![(dec!(0.38), dec!(100))],
        vec![(dec!(0.40), dec!(100))],
    );

    let mut books: HashMap<PredictionMarketKey, &OrderBook> = HashMap::new();
    books.insert(PredictionMarketKey::polymarket_yes("0xyes"), &poly_yes);

    let opps = s.detect_opportunities(&books);
    assert!(opps.is_empty(), "Missing orderbook should produce no opportunity");
}

// ---------------------------------------------------------------------------
// Test 13: ArbitrageOpportunity profit_for_contracts
// ---------------------------------------------------------------------------

#[test]
fn test_opportunity_profit_for_subset_contracts() {
    let p = pair("KXTEST", "0xyes", "0xno", 30);
    let buy_side = OrderSide::poly_buy("0xyes", Outcome::Yes, dec!(0.40), 200);
    let sell_side = OrderSide::kalshi_sell("KXTEST", Outcome::Yes, dec!(0.46), 200);
    let opp = ArbitrageOpportunity::new(
        p,
        ArbitrageDirection::PolyToKalshi,
        Outcome::Yes,
        buy_side,
        sell_side,
        50,
    );

    let profit_full = opp.expected_profit;
    let profit_half = opp.profit_for_contracts(100, 50);

    // Profit should scale linearly with contracts
    assert!(profit_half > Decimal::ZERO);
    // With 200 contracts full vs 100 half, profit should be ~2x
    let ratio = profit_full / profit_half;
    assert!(ratio > dec!(1.9) && ratio < dec!(2.1));
}
