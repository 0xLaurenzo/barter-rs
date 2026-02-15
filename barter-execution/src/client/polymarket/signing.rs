//! Polymarket EIP-712 order signing and API key authentication.
//!
//! Two-layer authentication:
//! 1. API auth (all requests): HMAC-SHA256 with `{timestamp}{method}{path}{body}`
//! 2. Order signing (order submission): EIP-712 typed data signature on the Order struct
//!
//! EIP-712 domain:
//!   name = "Polymarket CTF Exchange"
//!   version = "1"
//!   chainId = 137 (Polygon)
//!   verifyingContract = CTF Exchange or Neg Risk CTF Exchange

use alloy_primitives::{Address, U256, keccak256};
use alloy_signer::SignerSync;
use alloy_signer_local::{MnemonicBuilder, PrivateKeySigner, coins_bip39::English};
use alloy_sol_types::{SolStruct, sol};
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use hmac::{Hmac, Mac};
use sha2::Sha256;

type HmacSha256 = Hmac<Sha256>;

/// CTF Exchange contract address (regular markets) on Polygon mainnet.
const CTF_EXCHANGE: &str = "0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E";

/// Neg Risk CTF Exchange contract address on Polygon mainnet.
const NEG_RISK_CTF_EXCHANGE: &str = "0xC5d563A36AE78145C45a50134d48A1215220f80a";

/// Polygon mainnet chain ID.
const CHAIN_ID: u64 = 137;

// Define the EIP-712 Order struct via alloy sol! macro.
// This generates the type hash and encoding automatically.
sol! {
    #[derive(Debug)]
    struct Order {
        uint256 salt;
        address maker;
        address signer;
        address taker;
        uint256 tokenId;
        uint256 makerAmount;
        uint256 takerAmount;
        uint256 expiration;
        uint256 nonce;
        uint256 feeRateBps;
        uint8 side;
        uint8 signatureType;
    }
}

/// EIP-712 domain separator for Polymarket CTF Exchange.
fn eip712_domain(neg_risk: bool) -> alloy_sol_types::Eip712Domain {
    let contract: Address = if neg_risk {
        NEG_RISK_CTF_EXCHANGE.parse().unwrap()
    } else {
        CTF_EXCHANGE.parse().unwrap()
    };

    alloy_sol_types::Eip712Domain {
        name: Some("Polymarket CTF Exchange".into()),
        version: Some("1".into()),
        chain_id: Some(U256::from(CHAIN_ID)),
        verifying_contract: Some(contract),
        salt: None,
    }
}

/// Parameters for building and signing an order.
#[derive(Debug)]
pub struct OrderParams {
    /// Random salt for uniqueness.
    pub salt: U256,
    /// Maker (wallet) address.
    pub maker: Address,
    /// Signer address (same as maker for EOA).
    pub signer: Address,
    /// Taker address (zero = public order).
    pub taker: Address,
    /// CTF ERC1155 token ID.
    pub token_id: U256,
    /// Maker amount in raw units (6 decimals).
    pub maker_amount: U256,
    /// Taker amount in raw units (6 decimals).
    pub taker_amount: U256,
    /// Expiration timestamp (0 = no expiry).
    pub expiration: U256,
    /// Nonce for onchain cancellations.
    pub nonce: U256,
    /// Fee rate in basis points.
    pub fee_rate_bps: U256,
    /// 0 = BUY, 1 = SELL.
    pub side: u8,
    /// 0 = EOA, 1 = POLY_PROXY, 2 = POLY_GNOSIS_SAFE.
    pub signature_type: u8,
    /// Whether this is a neg risk market.
    pub neg_risk: bool,
}

/// Parse a private key string as either hex or mnemonic phrase.
fn parse_signer(key_or_mnemonic: &str) -> Result<PrivateKeySigner, PolymarketSigningError> {
    let trimmed = key_or_mnemonic.trim();
    if trimmed.contains(' ') {
        // Mnemonic phrase — derive key at BIP-44 path m/44'/60'/0'/0/0
        MnemonicBuilder::<English>::default()
            .phrase(trimmed)
            .index(0u32)
            .map_err(|e| PolymarketSigningError::InvalidKey(format!("mnemonic index: {e}")))?
            .build()
            .map_err(|e| PolymarketSigningError::InvalidKey(format!("mnemonic: {e}")))
    } else {
        // Hex private key
        let hex = trimmed.strip_prefix("0x").unwrap_or(trimmed);
        hex.parse()
            .map_err(|e| PolymarketSigningError::InvalidKey(format!("hex: {e}")))
    }
}

/// Sign a Polymarket order using EIP-712 typed data signing.
///
/// Returns the hex-encoded signature (0x-prefixed, 65 bytes = r + s + v).
pub fn sign_order(
    private_key_hex: &str,
    params: &OrderParams,
) -> Result<String, PolymarketSigningError> {
    let signer = parse_signer(private_key_hex)?;

    let order = Order {
        salt: params.salt,
        maker: params.maker,
        signer: params.signer,
        taker: params.taker,
        tokenId: params.token_id,
        makerAmount: params.maker_amount,
        takerAmount: params.taker_amount,
        expiration: params.expiration,
        nonce: params.nonce,
        feeRateBps: params.fee_rate_bps,
        side: params.side,
        signatureType: params.signature_type,
    };

    let domain = eip712_domain(params.neg_risk);

    // Compute EIP-712 signing hash: keccak256("\x19\x01" || domainSeparator || structHash)
    let domain_separator = domain.hash_struct();
    let struct_hash = order.eip712_hash_struct();
    let signing_hash = keccak256(
        [&[0x19, 0x01], domain_separator.as_slice(), struct_hash.as_slice()].concat(),
    );

    let signature = signer
        .sign_hash_sync(&signing_hash)
        .map_err(|e| PolymarketSigningError::SigningFailed(format!("{e}")))?;

    Ok(format!("0x{}", hex::encode(signature.as_bytes())))
}

/// Errors from Polymarket order signing.
#[derive(Debug, Clone, thiserror::Error)]
pub enum PolymarketSigningError {
    #[error("Invalid private key: {0}")]
    InvalidKey(String),
    #[error("Signing failed: {0}")]
    SigningFailed(String),
}

// --- CLOB Authentication (L1) for API key derivation ---

/// ClobAuth domain name.
const CLOB_DOMAIN_NAME: &str = "ClobAuthDomain";
/// ClobAuth domain version.
const CLOB_DOMAIN_VERSION: &str = "1";
/// Message the wallet attests to.
const MSG_TO_SIGN: &str = "This message attests that I control the given wallet";

sol! {
    #[derive(Debug)]
    struct ClobAuth {
        address address;
        string timestamp;
        uint256 nonce;
        string message;
    }
}

/// Build the EIP-712 domain for CLOB authentication.
fn clob_auth_domain() -> alloy_sol_types::Eip712Domain {
    alloy_sol_types::Eip712Domain {
        name: Some(CLOB_DOMAIN_NAME.into()),
        version: Some(CLOB_DOMAIN_VERSION.into()),
        chain_id: Some(U256::from(CHAIN_ID)),
        verifying_contract: None,
        salt: None,
    }
}

/// L1 authentication headers for CLOB API key derivation/creation.
///
/// Signs the ClobAuth EIP-712 message and returns (address, signature, timestamp, nonce).
pub fn build_l1_auth_headers(
    private_key_hex: &str,
) -> Result<(String, String, String, String), PolymarketSigningError> {
    let signer = parse_signer(private_key_hex)?;

    let address = signer.address();
    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("Time went backwards")
        .as_secs()
        .to_string();
    let nonce = U256::ZERO;

    let auth = ClobAuth {
        address,
        timestamp: timestamp.clone(),
        nonce,
        message: MSG_TO_SIGN.to_string(),
    };

    let domain = clob_auth_domain();
    let domain_separator = domain.hash_struct();
    let struct_hash = auth.eip712_hash_struct();
    let signing_hash = keccak256(
        [&[0x19, 0x01], domain_separator.as_slice(), struct_hash.as_slice()].concat(),
    );

    let signature = signer
        .sign_hash_sync(&signing_hash)
        .map_err(|e| PolymarketSigningError::SigningFailed(format!("{e}")))?;

    Ok((
        format!("{:#x}", address),
        format!("0x{}", hex::encode(signature.as_bytes())),
        timestamp,
        "0".to_string(),
    ))
}

/// Polymarket API credentials derived from the private key.
#[derive(Debug, Clone)]
pub struct PolymarketApiCredentials {
    /// The API key UUID returned by create/derive endpoint.
    pub api_key: String,
    /// Base64-encoded HMAC secret.
    pub api_secret: String,
    /// Passphrase for the API key.
    pub api_passphrase: String,
    /// The Ethereum wallet address (0x-prefixed, checksummed).
    pub wallet_address: String,
}

/// Generate HMAC-SHA256 API authentication headers.
///
/// Signs: `{timestamp}{method}{path}{body}` using the API secret.
pub fn generate_hmac_signature(
    creds: &PolymarketApiCredentials,
    method: &str,
    path: &str,
    body: &str,
) -> (String, String) {
    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("Time went backwards")
        .as_secs()
        .to_string();

    let message = format!("{}{}{}{}", timestamp, method, path, body);

    // API secret may be URL-safe base64 (- and _ instead of + and /)
    let normalized_secret = creds.api_secret.replace('-', "+").replace('_', "/");
    let secret_bytes = BASE64.decode(&normalized_secret).unwrap_or_default();
    let mut mac =
        HmacSha256::new_from_slice(&secret_bytes).expect("HMAC accepts any key length");
    mac.update(message.as_bytes());
    let result = mac.finalize().into_bytes();
    // URL-safe base64: replace '+' with '-', '/' with '_'
    let signature = BASE64.encode(result).replace('+', "-").replace('/', "_");

    (timestamp, signature)
}

/// Build the set of L2 auth headers for a CLOB API request.
pub fn build_auth_headers(
    creds: &PolymarketApiCredentials,
    method: &str,
    path: &str,
    body: &str,
) -> Vec<(String, String)> {
    let (timestamp, signature) = generate_hmac_signature(creds, method, path, body);

    vec![
        ("POLY_ADDRESS".to_string(), creds.wallet_address.clone()),
        ("POLY_SIGNATURE".to_string(), signature),
        ("POLY_TIMESTAMP".to_string(), timestamp),
        ("POLY_API_KEY".to_string(), creds.api_key.clone()),
        ("POLY_PASSPHRASE".to_string(), creds.api_passphrase.clone()),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sign_order_produces_valid_signature() {
        // Use a well-known test private key (DO NOT use in production)
        let test_key = "ac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80";

        let maker: Address = "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266"
            .parse()
            .unwrap();

        let params = OrderParams {
            salt: U256::from(12345u64),
            maker,
            signer: maker,
            taker: Address::ZERO,
            token_id: U256::from(98765u64),
            maker_amount: U256::from(50_000_000u64), // 50 USDC
            taker_amount: U256::from(100_000_000u64), // 100 tokens
            expiration: U256::ZERO,
            nonce: U256::ZERO,
            fee_rate_bps: U256::ZERO,
            side: 0, // BUY
            signature_type: 0, // EOA
            neg_risk: false,
        };

        let sig = sign_order(test_key, &params).unwrap();

        // Signature should be 0x-prefixed, 130 hex chars (65 bytes)
        assert!(sig.starts_with("0x"));
        assert_eq!(sig.len(), 132); // "0x" + 130 hex chars
    }

    #[test]
    fn test_sign_order_neg_risk() {
        let test_key = "ac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80";
        let maker: Address = "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266"
            .parse()
            .unwrap();

        let params = OrderParams {
            salt: U256::from(99999u64),
            maker,
            signer: maker,
            taker: Address::ZERO,
            token_id: U256::from(11111u64),
            maker_amount: U256::from(25_000_000u64),
            taker_amount: U256::from(50_000_000u64),
            expiration: U256::ZERO,
            nonce: U256::ZERO,
            fee_rate_bps: U256::ZERO,
            side: 1, // SELL
            signature_type: 0,
            neg_risk: true,
        };

        let sig_neg = sign_order(test_key, &params).unwrap();

        // Change to non-neg-risk, should produce different signature (different domain)
        let params_regular = OrderParams {
            neg_risk: false,
            ..params
        };
        let sig_regular = sign_order(test_key, &params_regular).unwrap();

        assert_ne!(sig_neg, sig_regular);
    }
}
