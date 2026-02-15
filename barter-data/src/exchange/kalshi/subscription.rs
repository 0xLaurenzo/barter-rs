use super::message::KalshiError;
use barter_integration::{Validator, error::SocketError};
use serde::{Deserialize, Serialize};

/// [`Kalshi`](super::Kalshi) message received in response to WebSocket subscription requests.
///
/// ### Raw Payload Examples
/// See docs: <https://trading-api.readme.io/reference/websocket-public-channels>
///
/// #### Subscription Success
/// ```json
/// {
///   "id": 1,
///   "type": "subscribed",
///   "msg": {
///     "channel": "orderbook_delta",
///     "sid": 1
///   }
/// }
/// ```
///
/// #### Subscription Failure
/// ```json
/// {
///   "id": 1,
///   "type": "error",
///   "msg": {
///     "error_message": "Invalid market ticker"
///   }
/// }
/// ```
#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum KalshiSubResponse {
    Subscribed {
        id: u64,
        msg: KalshiSubscribedMsg,
    },
    Error {
        id: u64,
        msg: KalshiError,
    },
}

/// Successful subscription acknowledgment details.
///
/// Actual response: `{"channel":"orderbook_delta","sid":1}`
#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Deserialize, Serialize)]
pub struct KalshiSubscribedMsg {
    pub channel: String,
    #[serde(default)]
    pub sid: Option<u64>,
    #[serde(default)]
    pub market_tickers: Option<Vec<String>>,
}

impl Validator for KalshiSubResponse {
    fn validate(self) -> Result<Self, SocketError>
    where
        Self: Sized,
    {
        match &self {
            KalshiSubResponse::Subscribed { .. } => Ok(self),
            KalshiSubResponse::Error { msg, .. } => Err(SocketError::Subscribe(format!(
                "received failure subscription response: {}",
                msg.message
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod de {
        use super::*;

        #[test]
        fn test_kalshi_sub_response_subscribed() {
            let input = r#"
            {
                "id": 1,
                "type": "subscribed",
                "msg": {
                    "channel": "orderbook_delta",
                    "sid": 1
                }
            }
            "#;

            let response: KalshiSubResponse = serde_json::from_str(input).unwrap();
            match response {
                KalshiSubResponse::Subscribed { id, msg } => {
                    assert_eq!(id, 1);
                    assert_eq!(msg.channel, "orderbook_delta");
                    assert_eq!(msg.sid, Some(1));
                }
                _ => panic!("Expected Subscribed"),
            }
        }

        #[test]
        fn test_kalshi_sub_response_error() {
            let input = r#"
            {
                "id": 1,
                "type": "error",
                "msg": {
                    "error_message": "Invalid market ticker"
                }
            }
            "#;

            let response: KalshiSubResponse = serde_json::from_str(input).unwrap();
            match response {
                KalshiSubResponse::Error { id, msg } => {
                    assert_eq!(id, 1);
                    assert_eq!(msg.message, "Invalid market ticker");
                }
                _ => panic!("Expected Error"),
            }
        }
    }

    #[test]
    fn test_kalshi_sub_response_validate() {
        struct TestCase {
            input_response: KalshiSubResponse,
            is_valid: bool,
        }

        let cases = vec![
            TestCase {
                // TC0: Successful subscription
                input_response: KalshiSubResponse::Subscribed {
                    id: 1,
                    msg: KalshiSubscribedMsg {
                        channel: "orderbook_delta".to_string(),
                        sid: Some(1),
                        market_tickers: None,
                    },
                },
                is_valid: true,
            },
            TestCase {
                // TC1: Failed subscription
                input_response: KalshiSubResponse::Error {
                    id: 1,
                    msg: KalshiError {
                        message: "Invalid market ticker".to_string(),
                    },
                },
                is_valid: false,
            },
        ];

        for (index, test) in cases.into_iter().enumerate() {
            let actual = test.input_response.validate().is_ok();
            assert_eq!(actual, test.is_valid, "TestCase {} failed", index);
        }
    }
}
