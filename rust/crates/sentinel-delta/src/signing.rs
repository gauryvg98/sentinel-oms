//! Delta's request signature.
//!
//! HMAC-SHA256, hex, over `method + timestamp + path + query + body`, carried in
//! `api-key`, `timestamp` and `signature` headers. The timestamp is epoch
//! *seconds* and the venue refuses anything older than five seconds, so a
//! signature is minted per request and never cached.
//!
//! Pure, so it is testable without a network and without a key that can lose
//! money. Everything here takes its clock as an argument.

use hmac::digest::KeyInit;
use hmac::{Hmac, Mac};
use sha2::Sha256;

/// The prehash string Delta signs.
///
/// Assembled by concatenation with no separators, which means the pieces must
/// be exactly right: a query string with a leading `?`, or a body with
/// different whitespace than what is sent, produces a signature that verifies
/// against nothing. That is why the caller passes the *sent* body rather than
/// the values it was built from.
#[must_use]
pub fn prehash(method: &str, timestamp_secs: u64, path: &str, query: &str, body: &str) -> String {
    format!("{method}{timestamp_secs}{path}{query}{body}")
}

/// Hex HMAC-SHA256 of `message` under `secret`.
#[must_use]
pub fn sign(secret: &str, message: &str) -> String {
    // The only way this fails is a key length no HMAC rejects.
    let mut mac = <Hmac<Sha256>>::new_from_slice(secret.as_bytes())
        .unwrap_or_else(|_| unreachable!("HMAC accepts any key length"));
    mac.update(message.as_bytes());
    let bytes = mac.finalize().into_bytes();
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        use core::fmt::Write as _;
        let _ = write!(out, "{byte:02x}");
    }
    out
}

/// The headers one signed request needs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SignedHeaders {
    /// The API key.
    pub api_key: String,
    /// Epoch seconds, as text.
    pub timestamp: String,
    /// Hex HMAC-SHA256 over the prehash.
    pub signature: String,
}

impl SignedHeaders {
    /// Sign one request.
    #[must_use]
    pub fn build(
        api_key: &str,
        secret: &str,
        method: &str,
        timestamp_secs: u64,
        path: &str,
        query: &str,
        body: &str,
    ) -> Self {
        Self {
            api_key: api_key.to_owned(),
            timestamp: timestamp_secs.to_string(),
            signature: sign(secret, &prehash(method, timestamp_secs, path, query, body)),
        }
    }

    /// The websocket handshake signature.
    ///
    /// A different shape from a REST request: the venue signs `GET`, the
    /// timestamp and the literal path `/live`, with no body. Written out here
    /// rather than reusing [`Self::build`] with empty arguments, because the two
    /// are only coincidentally similar and a change to one must not silently
    /// alter the other.
    #[must_use]
    pub fn for_stream(api_key: &str, secret: &str, timestamp_secs: u64) -> Self {
        Self {
            api_key: api_key.to_owned(),
            timestamp: timestamp_secs.to_string(),
            signature: sign(secret, &format!("GET{timestamp_secs}/live")),
        }
    }
}

/// Whether a signature minted at `signed_at` is still inside Delta's window.
///
/// The venue refuses anything older than five seconds. Checked before sending
/// rather than after being refused, because a rejected order and an unsigned
/// one are indistinguishable from the response and only one of them is our
/// fault.
#[must_use]
pub const fn is_fresh(signed_at_secs: u64, now_secs: u64) -> bool {
    now_secs.saturating_sub(signed_at_secs) < 5
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_prehash_concatenates_with_no_separators() {
        assert_eq!(
            prehash("POST", 1_787_761_830, "/v2/orders", "", r#"{"size":3}"#),
            r#"POST1787761830/v2/orders{"size":3}"#
        );
        assert_eq!(
            prehash("GET", 1_787_761_830, "/v2/fills", "?product_id=27", ""),
            "GET1787761830/v2/fills?product_id=27"
        );
    }

    #[test]
    fn hmac_matches_the_published_test_vector() {
        // RFC 4231 case 2: key "Jefe", data "what do ya want for nothing?".
        // If this passes, the signature is HMAC-SHA256 and not something that
        // merely looks like it.
        assert_eq!(
            sign("Jefe", "what do ya want for nothing?"),
            "5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843"
        );
    }

    #[test]
    fn the_signature_is_lowercase_hex_of_the_full_digest() {
        let signature = sign("secret", "message");
        assert_eq!(signature.len(), 64, "32 bytes, two characters each");
        assert!(
            signature
                .chars()
                .all(|c| c.is_ascii_hexdigit() && !c.is_uppercase())
        );
    }

    #[test]
    fn every_field_changes_the_signature() {
        // A signature that ignored one of its inputs would let that field be
        // rewritten in flight — the reason `path` here means path *and* query.
        let base = SignedHeaders::build(
            "key",
            "secret",
            "POST",
            1_787_761_830,
            "/v2/orders",
            "",
            "{}",
        );
        let variants = [
            SignedHeaders::build(
                "key",
                "secret",
                "GET",
                1_787_761_830,
                "/v2/orders",
                "",
                "{}",
            ),
            SignedHeaders::build(
                "key",
                "secret",
                "POST",
                1_787_761_831,
                "/v2/orders",
                "",
                "{}",
            ),
            SignedHeaders::build(
                "key",
                "secret",
                "POST",
                1_787_761_830,
                "/v2/fills",
                "",
                "{}",
            ),
            SignedHeaders::build(
                "key",
                "secret",
                "POST",
                1_787_761_830,
                "/v2/orders",
                "?a=1",
                "{}",
            ),
            SignedHeaders::build(
                "key",
                "secret",
                "POST",
                1_787_761_830,
                "/v2/orders",
                "",
                r#"{"a":1}"#,
            ),
            SignedHeaders::build(
                "key",
                "other",
                "POST",
                1_787_761_830,
                "/v2/orders",
                "",
                "{}",
            ),
        ];
        for variant in variants {
            assert_ne!(variant.signature, base.signature);
        }
    }

    #[test]
    fn the_same_request_signs_the_same_way() {
        let once = SignedHeaders::build("k", "s", "POST", 1_000, "/v2/orders", "", "{}");
        let twice = SignedHeaders::build("k", "s", "POST", 1_000, "/v2/orders", "", "{}");
        assert_eq!(once, twice);
    }

    #[test]
    fn the_stream_handshake_signs_get_timestamp_live() {
        let stream = SignedHeaders::for_stream("key", "secret", 1_787_761_830);
        assert_eq!(stream.signature, sign("secret", "GET1787761830/live"));
        assert_eq!(stream.timestamp, "1787761830");
    }

    #[test]
    fn the_handshake_shape_is_pinned_rather_than_inherited() {
        // Today it happens to equal a REST signature over `GET /live` with no
        // query and no body. That is a coincidence of the two formats, not a
        // relationship: the venue could change either one alone, so the
        // handshake is written out separately and asserted against its own
        // literal above rather than against this.
        let stream = SignedHeaders::for_stream("key", "secret", 1_787_761_830);
        let rest = SignedHeaders::build("key", "secret", "GET", 1_787_761_830, "/live", "", "");
        assert_eq!(
            stream.signature, rest.signature,
            "as of this format version"
        );
    }

    #[test]
    fn freshness_is_the_venues_five_second_window() {
        assert!(is_fresh(1_000, 1_000));
        assert!(is_fresh(1_000, 1_004));
        assert!(!is_fresh(1_000, 1_005));
        // A clock that went backwards is not a stale signature.
        assert!(is_fresh(1_000, 999));
    }

    #[test]
    fn the_timestamp_header_is_seconds_not_milliseconds() {
        // Delta reads epoch seconds. Sending milliseconds produces a signature
        // over a timestamp from the year 58,000 and a refusal that says nothing
        // about why.
        let headers = SignedHeaders::build("k", "s", "GET", 1_787_761_830, "/v2/fills", "", "");
        assert_eq!(headers.timestamp, "1787761830");
        assert_eq!(headers.timestamp.len(), 10);
    }
}
