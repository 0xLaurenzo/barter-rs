use crate::{
    event::{MarketEvent, MarketIter},
    subscription::trade::PublicTrade,
};
use barter_instrument::{Side, exchange::ExchangeId};
use chrono::Utc;

use super::message::KalshiTrade;

impl<InstrumentKey> From<(ExchangeId, InstrumentKey, KalshiTrade)>
    for MarketIter<InstrumentKey, PublicTrade>
{
    fn from((exchange, instrument, trade): (ExchangeId, InstrumentKey, KalshiTrade)) -> Self {
        Self(vec![Ok(MarketEvent {
            time_exchange: Utc::now(),
            time_received: Utc::now(),
            exchange,
            instrument,
            kind: PublicTrade {
                id: format!("{}-{}", trade.sid, trade.seq),
                price: trade.msg.yes_price as f64 / 100.0,
                amount: trade.msg.count as f64,
                side: match trade.msg.taker_side.as_str() {
                    "yes" => Side::Buy,
                    _ => Side::Sell,
                },
            },
        })])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exchange::kalshi::message::KalshiTradeData;

    #[test]
    fn test_kalshi_trade_to_public_trade_yes() {
        let trade = KalshiTrade {
            sid: 1,
            seq: 42,
            msg: KalshiTradeData {
                market_ticker: "KXBTC-25JAN31-T100000".to_string(),
                yes_price: 40,
                no_price: 60,
                count: 100,
                taker_side: "yes".to_string(),
            },
        };

        let iter = MarketIter::<&str, PublicTrade>::from((ExchangeId::Kalshi, "test", trade));
        let events: Vec<_> = iter.0.into_iter().collect();
        assert_eq!(events.len(), 1);

        let event = events[0].as_ref().unwrap();
        assert_eq!(event.kind.id, "1-42");
        assert!((event.kind.price - 0.40).abs() < f64::EPSILON);
        assert!((event.kind.amount - 100.0).abs() < f64::EPSILON);
        assert_eq!(event.kind.side, Side::Buy);
    }

    #[test]
    fn test_kalshi_trade_to_public_trade_no() {
        let trade = KalshiTrade {
            sid: 2,
            seq: 7,
            msg: KalshiTradeData {
                market_ticker: "KXETH-25JAN31-T5000".to_string(),
                yes_price: 25,
                no_price: 75,
                count: 50,
                taker_side: "no".to_string(),
            },
        };

        let iter = MarketIter::<&str, PublicTrade>::from((ExchangeId::Kalshi, "test", trade));
        let event = iter.0[0].as_ref().unwrap();
        assert_eq!(event.kind.id, "2-7");
        assert!((event.kind.price - 0.25).abs() < f64::EPSILON);
        assert!((event.kind.amount - 50.0).abs() < f64::EPSILON);
        assert_eq!(event.kind.side, Side::Sell);
    }
}
