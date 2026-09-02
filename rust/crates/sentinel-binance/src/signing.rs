//! Binance request signing, and the clock it depends on.
//!
//! HMAC-SHA256 over the **urlencoded query string, exactly as sent**. Not over a
//! canonical form, not over sorted parameters — over the literal bytes on the
//! wire. So the string is built once and both signed and sent, because building
//! it twice is how the two drift and every private call starts returning 401.
//!
//! Timestamps are **milliseconds**, and Binance refuses anything more than
//! `recvWindow` away from *its* clock. A machine whose clock is a second slow
//! gets `-1021` on every order and nothing else — so the offset to the venue's
//! clock is tracked and applied, rather than trusting the local one.
//!
//! Pure, so all of it is testable without a network and without a key that can
//! lose money.

use core::sync::atomic::{AtomicI64, Ordering};

use hmac::{Hmac, Mac};
use sha2::Sha256;

/// How far our timestamp may be from Binance's before it refuses the request.
///
/// Sent explicitly rather than left to the venue's default, because the default
/// has changed before and a signature refused for being late looks exactly like
/// a signature that is wrong.
pub const RECV_WINDOW_MS: u64 = 5_000;

/// A query string being assembled for signing.
///
/// Parameters keep insertion order. Binance signs what it receives, so the
/// order is part of the message: reordering after signing produces a signature
/// that verifies against nothing.
#[derive(Debug, Default, Clone)]
pub struct Query {
    encoded: String,
}

impl Query {
    /// An empty query.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Append a parameter.
    ///
    /// Values are percent-encoded. In practice nothing this adapter sends needs
    /// escaping — symbols, sides and decimal numbers are all unreserved — but a
    /// client order id is chosen upstream, and one containing a `&` would
    /// otherwise silently split into two parameters.
    #[must_use]
    pub fn push(mut self, key: &str, value: &str) -> Self {
        if !self.encoded.is_empty() {
            self.encoded.push('&');
        }
        self.encoded.push_str(key);
        self.encoded.push('=');
        encode_into(&mut self.encoded, value);
        self
    }

    /// Append a parameter only when it is present.
    #[must_use]
    pub fn push_opt(self, key: &str, value: Option<&str>) -> Self {
        match value {
            Some(value) => self.push(key, value),
            None => self,
        }
    }

    /// The query string, unsigned.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.encoded
    }

    /// Whether nothing has been added.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.encoded.is_empty()
    }

    /// Finish: append the timestamp and the receive window, then the signature
    /// over everything before it.
    ///
    /// The signature is last and covers exactly what precedes it, which is why
    /// this returns the finished string rather than the pieces — there is no
    /// way to hold the two apart and still be sure they match.
    #[must_use]
    pub fn sign(self, secret: &str, timestamp_ms: u64) -> String {
        let payload = self
            .push("timestamp", &timestamp_ms.to_string())
            .push("recvWindow", &RECV_WINDOW_MS.to_string())
            .encoded;
        let signature = sign(secret, &payload);
        format!("{payload}&signature={signature}")
    }
}

/// Percent-encode into an existing buffer, escaping everything not unreserved.
fn encode_into(out: &mut String, value: &str) {
    for byte in value.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'~') {
            out.push(char::from(byte));
        } else {
            out.push('%');
            out.push(char::from(hex_digit(byte >> 4)));
            out.push(char::from(hex_digit(byte & 0x0f)));
        }
    }
}

const fn hex_digit(nibble: u8) -> u8 {
    match nibble {
        0..=9 => b'0' + nibble,
        _ => b'A' + (nibble - 10),
    }
}

/// Hex HMAC-SHA256 of `message` under `secret`.
#[must_use]
pub fn sign(secret: &str, message: &str) -> String {
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

/// The venue's clock, as an offset from ours.
///
/// A machine whose clock is a second slow gets `-1021` on every signed call and
/// nothing else — no fills, no cancels, no lookups. Tracking the offset turns
/// that from an outage into a correction, and re-syncing on `-1021` turns a
/// drift that develops later into a hiccup.
///
/// Atomic because the venue worker and the stream thread both read it.
#[derive(Debug, Default)]
pub struct ServerClock {
    offset_ms: AtomicI64,
}

impl ServerClock {
    /// A clock that trusts the local one until told otherwise.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            offset_ms: AtomicI64::new(0),
        }
    }

    /// Record the venue's time against ours, assuming the answer was instant.
    ///
    /// Almost nothing is instant, so prefer [`ServerClock::sync_round_trip`].
    /// This remains for the case where the two readings are genuinely
    /// simultaneous, which in practice means a test.
    pub fn sync(&self, server_time_ms: u64, local_time_ms: u64) {
        self.sync_round_trip(server_time_ms, local_time_ms, local_time_ms);
    }

    /// Record the venue's time against ours, correcting for the round trip.
    ///
    /// `serverTime` describes an instant somewhere between sending the request
    /// and reading the reply — not the instant we sent it. Comparing it against
    /// `sent` books the whole round trip as clock error, which is the wrong
    /// sign in the worst way: it pushes our timestamps *ahead* of the venue,
    /// and Binance rejects a future timestamp with `-1021` no matter how
    /// generous `recvWindow` is. A slow link would present as a broken clock,
    /// and the correction for it would be what actually broke the account.
    ///
    /// So estimate the venue's clock at the midpoint of the exchange, which is
    /// what NTP does and for this exact reason. It assumes the two legs of the
    /// trip are roughly equal; when they are not, the residual error is half
    /// the asymmetry rather than the whole latency.
    pub fn sync_round_trip(&self, server_time_ms: u64, sent_ms: u64, received_ms: u64) {
        let round_trip = received_ms.saturating_sub(sent_ms);
        #[expect(
            clippy::integer_division,
            reason = "halving a millisecond round trip; the lost sub-millisecond \
                      is far below the accuracy this estimate can claim"
        )]
        let half = round_trip / 2;
        let midpoint = sent_ms.saturating_add(half);
        #[expect(
            clippy::cast_possible_wrap,
            reason = "both are epoch milliseconds; their difference is a small \
                      signed offset, nowhere near the i64 bound"
        )]
        let offset = server_time_ms as i64 - midpoint as i64;
        self.offset_ms.store(offset, Ordering::Relaxed);
    }

    /// The venue's time now, given ours.
    #[must_use]
    pub fn venue_time_ms(&self, local_time_ms: u64) -> u64 {
        let offset = self.offset_ms.load(Ordering::Relaxed);
        #[expect(
            clippy::cast_sign_loss,
            clippy::cast_possible_wrap,
            reason = "saturating back into epoch milliseconds, which are far \
                      from either bound"
        )]
        let adjusted = (local_time_ms as i64).saturating_add(offset).max(0) as u64;
        adjusted
    }

    /// The current offset, in milliseconds. For the operator.
    #[must_use]
    pub fn offset_ms(&self) -> i64 {
        self.offset_ms.load(Ordering::Relaxed)
    }
}

/// A decimal rendered the way Binance wants it.
///
/// Plain notation, never scientific: Binance rejects `1E-8` outright, and a
/// quantity that arrives in exponent form is refused with a message about the
/// symbol rather than about the number.
#[must_use]
pub fn format_decimal(value: sentinel_types::Price) -> String {
    value.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hmac_matches_the_published_test_vector() {
        // RFC 4231 case 2. If this passes, the signature is HMAC-SHA256 and not
        // something that merely looks like it.
        assert_eq!(
            sign("Jefe", "what do ya want for nothing?"),
            "5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843"
        );
    }

    #[test]
    fn the_query_keeps_insertion_order() {
        // Binance signs what it receives, so the order is part of the message.
        let query = Query::new()
            .push("symbol", "BTCUSDT")
            .push("side", "BUY")
            .push("quantity", "0.003");
        assert_eq!(query.as_str(), "symbol=BTCUSDT&side=BUY&quantity=0.003");
    }

    #[test]
    fn the_signature_covers_exactly_what_precedes_it() {
        let signed = Query::new()
            .push("symbol", "BTCUSDT")
            .sign("secret", 1_787_761_830_000);
        let (payload, signature) = signed.rsplit_once("&signature=").unwrap();
        assert_eq!(
            payload,
            "symbol=BTCUSDT&timestamp=1787761830000&recvWindow=5000"
        );
        assert_eq!(signature, sign("secret", payload));
    }

    #[test]
    fn the_timestamp_is_milliseconds() {
        // Seconds would put the request 56 years in the past, and Binance would
        // refuse it with -1021 — a message about time, for what looks like a
        // problem with the key.
        let signed = Query::new().sign("secret", 1_787_761_830_000);
        assert!(signed.contains("timestamp=1787761830000"));
        assert!(!signed.contains("timestamp=1787761830&"));
    }

    #[test]
    fn the_receive_window_is_sent_explicitly() {
        // The default has changed before, and a signature refused for being
        // late looks exactly like a signature that is wrong.
        assert!(Query::new().sign("s", 1).contains("recvWindow=5000"));
    }

    #[test]
    fn every_field_changes_the_signature() {
        let sig = |q: Query, ts: u64| {
            q.sign("secret", ts)
                .rsplit_once("&signature=")
                .unwrap()
                .1
                .to_owned()
        };
        let base = sig(Query::new().push("symbol", "BTCUSDT"), 1_000);
        assert_ne!(base, sig(Query::new().push("symbol", "ETHUSDT"), 1_000));
        assert_ne!(base, sig(Query::new().push("symbols", "BTCUSDT"), 1_000));
        assert_ne!(base, sig(Query::new().push("symbol", "BTCUSDT"), 1_001));
        assert_ne!(
            base,
            Query::new()
                .push("symbol", "BTCUSDT")
                .sign("other", 1_000)
                .rsplit_once("&signature=")
                .unwrap()
                .1
        );
    }

    #[test]
    fn reordering_changes_the_signature() {
        // Which is why the string is built once and both signed and sent.
        let a = Query::new().push("a", "1").push("b", "2").sign("s", 1);
        let b = Query::new().push("b", "2").push("a", "1").sign("s", 1);
        assert_ne!(a, b);
    }

    #[test]
    fn values_are_percent_encoded() {
        // A client order id is chosen upstream. One containing an ampersand
        // would otherwise silently split into two parameters.
        let query = Query::new().push("newClientOrderId", "UI&evil=1");
        assert_eq!(query.as_str(), "newClientOrderId=UI%26evil%3D1");

        // And the ids this system actually generates pass through untouched.
        let plain = Query::new().push("newClientOrderId", "UI-4bb5ba826e2e1");
        assert_eq!(plain.as_str(), "newClientOrderId=UI-4bb5ba826e2e1");
    }

    #[test]
    fn optional_parameters_are_absent_rather_than_empty() {
        // An empty `price=` is not the same as no price: Binance reads the
        // first as a malformed number and the second as a market order.
        let query = Query::new()
            .push("symbol", "BTCUSDT")
            .push_opt("price", None)
            .push_opt("timeInForce", Some("GTC"));
        assert_eq!(query.as_str(), "symbol=BTCUSDT&timeInForce=GTC");
    }

    #[test]
    fn the_clock_tracks_the_venue_rather_than_trusting_the_machine() {
        let clock = ServerClock::new();
        assert_eq!(clock.venue_time_ms(1_000), 1_000, "trusts ours until told");

        // Our clock is two seconds slow.
        clock.sync(1_787_761_832_000, 1_787_761_830_000);
        assert_eq!(clock.offset_ms(), 2_000);
        assert_eq!(clock.venue_time_ms(1_787_761_840_000), 1_787_761_842_000);
    }

    #[test]
    fn the_clock_handles_a_machine_that_is_fast() {
        let clock = ServerClock::new();
        clock.sync(1_787_761_828_000, 1_787_761_830_000);
        assert_eq!(clock.offset_ms(), -2_000);
        assert_eq!(clock.venue_time_ms(1_787_761_840_000), 1_787_761_838_000);
    }

    #[test]
    fn a_slow_link_is_not_mistaken_for_a_broken_clock() {
        // The clocks agree exactly. The reply took four seconds to come back,
        // two out and two home, so `serverTime` describes the midpoint.
        let clock = ServerClock::new();
        clock.sync_round_trip(1_787_761_832_000, 1_787_761_830_000, 1_787_761_834_000);
        assert_eq!(clock.offset_ms(), 0, "latency is not skew");

        // The bug this replaced booked the whole trip as offset, which put our
        // timestamps ahead of the venue and earned -1021 on every signed call.
        assert_eq!(
            clock.venue_time_ms(1_787_761_840_000),
            1_787_761_840_000,
            "a correct clock must not be corrected"
        );
    }

    #[test]
    fn a_genuinely_slow_machine_is_still_corrected_across_a_slow_link() {
        // Two seconds slow, measured over a one-second round trip: the venue's
        // clock at the midpoint is 1_787_761_832_500 while ours reads
        // 1_787_761_830_500, so the offset is the real 2s and not 2.5s.
        let clock = ServerClock::new();
        clock.sync_round_trip(1_787_761_832_500, 1_787_761_830_000, 1_787_761_831_000);
        assert_eq!(clock.offset_ms(), 2_000);
        assert_eq!(clock.venue_time_ms(1_787_761_840_000), 1_787_761_842_000);
    }

    #[test]
    fn decimals_never_reach_the_venue_in_exponent_form() {
        // Binance rejects 1E-8 outright, with a message about the symbol.
        use sentinel_types::Price;
        assert_eq!(format_decimal(Price::from_raw(1)), "0.00000001");
        assert_eq!(
            format_decimal(Price::parse("109432.5").unwrap()),
            "109432.5"
        );
        assert_eq!(format_decimal(Price::whole(1)), "1");
        for value in [Price::from_raw(1), Price::from_raw(-1), Price::MAX] {
            let text = format_decimal(value);
            assert!(!text.contains('e') && !text.contains('E'), "{text}");
        }
    }
}
