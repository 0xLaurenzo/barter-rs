use super::message::PolymarketError;
use barter_integration::{Validator, error::SocketError};
use serde::{Deserialize, Serialize};

/// [`Polymarket`](super::Polymarket) message received in response to WebSocket subscription requests.
///
/// ### Raw Payload Examples
/// See docs: <https://docs.polymarket.com/#websocket-api>
///
/// #### Subscription Success
/// ```json
/// {
///   "type": "subscribed",
///   "channel": "price_book",
///   "assets": ["0x1234...abcd"]
/// }
/// ```
///
/// #### Subscription Failure
/// ```json
/// {
///   "type": "error",
///   "message": "Invalid asset_id"
/// }
/// ```
#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum PolymarketSubResponse {
    Subscribed {
        channel: String,
        #[serde(default)]
        assets: Vec<String>,
    },
    Error(PolymarketError),
}

impl Validator for PolymarketSubResponse {
    fn validate(self) -> Result<Self, SocketError>
    where
        Self: Sized,
    {
        match &self {
            PolymarketSubResponse::Subscribed { .. } => Ok(self),
            PolymarketSubResponse::Error(error) => Err(SocketError::Subscribe(format!(
                "received failure subscription response: {}",
                error.message
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
        fn test_polymarket_sub_response_subscribed() {
            let input = r#"
            {
                "type": "subscribed",
                "channel": "price_book",
                "assets": ["0x1234abcd"]
            }
            "#;

            let response: PolymarketSubResponse = serde_json::from_str(input).unwrap();
            match response {
                PolymarketSubResponse::Subscribed { channel, assets } => {
                    assert_eq!(channel, "price_book");
                    assert_eq!(assets, vec!["0x1234abcd"]);
                }
                _ => panic!("Expected Subscribed"),
            }
        }

        #[test]
        fn test_polymarket_sub_response_error() {
            let input = r#"
            {
                "type": "error",
                "message": "Invalid asset_id"
            }
            "#;

            let response: PolymarketSubResponse = serde_json::from_str(input).unwrap();
            match response {
                PolymarketSubResponse::Error(error) => {
                    assert_eq!(error.message, "Invalid asset_id");
                }
                _ => panic!("Expected Error"),
            }
        }
    }

    #[test]
    fn test_polymarket_sub_response_validate() {
        struct TestCase {
            input_response: PolymarketSubResponse,
            is_valid: bool,
        }

        let cases = vec![
            TestCase {
                // TC0: Successful subscription
                input_response: PolymarketSubResponse::Subscribed {
                    channel: "price_book".to_string(),
                    assets: vec!["0x1234abcd".to_string()],
                },
                is_valid: true,
            },
            TestCase {
                // TC1: Failed subscription
                input_response: PolymarketSubResponse::Error(PolymarketError {
                    message: "Invalid asset_id".to_string(),
                }),
                is_valid: false,
            },
        ];

        for (index, test) in cases.into_iter().enumerate() {
            let actual = test.input_response.validate().is_ok();
            assert_eq!(actual, test.is_valid, "TestCase {} failed", index);
        }
    }
}
