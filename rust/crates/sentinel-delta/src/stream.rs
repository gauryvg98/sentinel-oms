//! The user stream, on its own thread.
//!
//! A websocket with a `key-auth` handshake, subscribed to orders, trades and
//! margins. It runs on a thread and pushes into a channel; nothing in the
//! engine's path ever waits on it.
//!
//! The stream is at-least-once and it drops. Both are designed for rather than
//! defended against: every execution carries an id the book deduplicates on, and
//! a gap is closed by the stale sweep — which is why this file reconnects
//! quietly instead of trying to guarantee delivery it cannot guarantee.
//!
//! The previous system's socket died with `keepalive ping timeout` every one to
//! five minutes, because the event loop it shared was starved. This one shares
//! nothing: it is a thread that reads a socket and writes a channel.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{Receiver, Sender, channel};
use std::time::Duration;

use sentinel_types::Instrument;
use sentinel_venue::{VenueError, VenueEvent};

use crate::signing::SignedHeaders;
use crate::{Credentials, parse};

/// Backoff between reconnect attempts. Reconnecting instantly against a venue
/// that is refusing connections is how a rate limit becomes a ban.
const RECONNECT_BACKOFF: Duration = Duration::from_millis(500);
/// Cap on that backoff.
const RECONNECT_BACKOFF_CAP: Duration = Duration::from_secs(30);

/// A running stream. Dropping it asks the thread to stop.
#[derive(Debug)]
pub struct StreamHandle {
    running: Arc<AtomicBool>,
}

impl StreamHandle {
    /// Whether the reader thread is still meant to be running.
    #[must_use]
    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::Relaxed)
    }

    /// Ask it to stop. The thread notices at its next read boundary.
    pub fn stop(&self) {
        self.running.store(false, Ordering::Relaxed);
    }
}

impl Drop for StreamHandle {
    fn drop(&mut self) {
        self.stop();
    }
}

/// Start the stream.
///
/// # Errors
/// [`VenueError`] when the thread cannot be started.
pub fn spawn(
    credentials: Credentials,
    instruments: Vec<Instrument>,
) -> Result<(StreamHandle, Receiver<VenueEvent>), VenueError> {
    let (tx, rx) = channel();
    let running = Arc::new(AtomicBool::new(true));
    let flag = Arc::clone(&running);

    std::thread::Builder::new()
        .name("delta-stream".into())
        .spawn(move || run(&credentials, &instruments, &tx, &flag))
        .map_err(|e| VenueError::Transport(format!("stream thread: {e}")))?;

    Ok((StreamHandle { running }, rx))
}

/// Connect, read, reconnect. Forever, until asked to stop.
fn run(
    credentials: &Credentials,
    instruments: &[Instrument],
    tx: &Sender<VenueEvent>,
    running: &AtomicBool,
) {
    let mut backoff = RECONNECT_BACKOFF;
    while running.load(Ordering::Relaxed) {
        match read_once(credentials, instruments, tx, running) {
            // A clean end means we were asked to stop.
            Ok(()) => return,
            Err(_) => {
                // A dropped stream is not an integrity problem: every execution
                // carries an id the book deduplicates on, and anything missed
                // in the gap is found by the stale sweep. So this reconnects
                // rather than escalating.
                std::thread::sleep(backoff);
                backoff = (backoff * 2).min(RECONNECT_BACKOFF_CAP);
            }
        }
    }
}

fn read_once(
    credentials: &Credentials,
    instruments: &[Instrument],
    tx: &Sender<VenueEvent>,
    running: &AtomicBool,
) -> Result<(), VenueError> {
    let (mut socket, _) = tungstenite::connect(&credentials.ws_url)
        .map_err(|e| VenueError::Transport(format!("connect: {e}")))?;

    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_secs());
    let auth = SignedHeaders::for_stream(&credentials.api_key, &credentials.secret, timestamp);

    let handshake = serde_json::json!({
        "type": "auth",
        "payload": {
            "api-key": auth.api_key,
            "signature": auth.signature,
            "timestamp": auth.timestamp,
        }
    });
    socket
        .send(tungstenite::Message::Text(handshake.to_string().into()))
        .map_err(|e| VenueError::Transport(format!("auth: {e}")))?;

    // Private channels need no symbol list; the ticker does. Subscribing to
    // both in one frame keeps the two in step — a socket that carried orders
    // but not marks would leave the book unable to price what it holds.
    let symbols: Vec<&str> = instruments
        .iter()
        .map(sentinel_types::InlineStr::as_str)
        .collect();
    let subscribe = serde_json::json!({
        "type": "subscribe",
        "payload": {
            "channels": [
                {"name": "orders"},
                {"name": "v2/user_trades"},
                {"name": "margins"},
                {"name": "v2/ticker", "symbols": symbols},
            ]
        }
    });
    socket
        .send(tungstenite::Message::Text(subscribe.to_string().into()))
        .map_err(|e| VenueError::Transport(format!("subscribe: {e}")))?;

    while running.load(Ordering::Relaxed) {
        let message = socket
            .read()
            .map_err(|e| VenueError::Transport(format!("read: {e}")))?;
        let text = match message {
            tungstenite::Message::Text(text) => text,
            tungstenite::Message::Ping(payload) => {
                // Answered here, on a thread that does nothing else. The
                // previous system missed these because the loop that owed the
                // reply was busy, and the venue hung up on it every few minutes.
                socket
                    .send(tungstenite::Message::Pong(payload))
                    .map_err(|e| VenueError::Transport(format!("pong: {e}")))?;
                continue;
            }
            tungstenite::Message::Close(_) => {
                return Err(VenueError::Transport("venue closed the stream".into()));
            }
            _ => continue,
        };

        let Ok(frame) = serde_json::from_str::<serde_json::Value>(&text) else {
            continue;
        };
        for event in decode_frame(&frame) {
            // A closed channel means the adapter is gone. Stop, do not spin.
            if tx.send(event).is_err() {
                return Ok(());
            }
        }
    }
    Ok(())
}

/// Turn one stream frame into whatever events it carries.
///
/// Returns a list because a frame can carry none — most of them are
/// acknowledgements and heartbeats — and because a fill frame carries exactly
/// one. Anything unrecognised produces nothing rather than a guess.
#[must_use]
pub fn decode_frame(frame: &serde_json::Value) -> Vec<VenueEvent> {
    let kind = frame.get("type").and_then(serde_json::Value::as_str);
    match kind {
        Some("v2/user_trades" | "user_trades") => decode_trade(frame).into_iter().collect(),
        Some("orders") => decode_order_update(frame).into_iter().collect(),
        Some("v2/ticker") => decode_ticker(frame).into_iter().collect(),
        Some("margins") => decode_margin(frame).into_iter().collect(),
        _ => Vec::new(),
    }
}

fn decode_trade(frame: &serde_json::Value) -> Option<VenueEvent> {
    let client_order_id = frame
        .get("client_order_id")
        .and_then(serde_json::Value::as_str)
        .and_then(|s| sentinel_types::ClientOrderId::new(s).ok())?;
    // The execution id, without which the fill cannot be deduplicated. A trade
    // frame missing it is dropped rather than applied, because applying one
    // twice moves a real position twice — the sweep will find the order.
    let exec_id = frame
        .get("fill_id")
        .or_else(|| frame.get("id"))
        .and_then(|v| parse::integer(v).map(|n| n.to_string()))
        .and_then(|s| sentinel_types::ExecId::new(&s).ok())?;
    let qty = frame.get("size").and_then(parse::quantity)?;
    let price = frame.get("price").and_then(parse::decimal)?;

    Some(VenueEvent::Fill {
        client_order_id,
        exec_id,
        qty,
        price,
    })
}

fn decode_order_update(frame: &serde_json::Value) -> Option<VenueEvent> {
    let client_order_id = frame
        .get("client_order_id")
        .and_then(serde_json::Value::as_str)
        .and_then(|s| sentinel_types::ClientOrderId::new(s).ok())?;
    // Only the cancellation is acted on here. A fill arrives on the trades
    // channel with an execution id; taking it from an order update as well
    // would book the same execution from two sources.
    match frame.get("state").and_then(serde_json::Value::as_str)? {
        "cancelled" => Some(VenueEvent::CancelConfirmed { client_order_id }),
        _ => None,
    }
}

fn decode_ticker(frame: &serde_json::Value) -> Option<VenueEvent> {
    let instrument = frame
        .get("symbol")
        .and_then(serde_json::Value::as_str)
        .and_then(|s| Instrument::new(s).ok())?;
    let price = frame
        .get("mark_price")
        .or_else(|| frame.get("close"))
        .and_then(parse::decimal)?;
    Some(VenueEvent::Mark { instrument, price })
}

fn decode_margin(frame: &serde_json::Value) -> Option<VenueEvent> {
    let asset = frame
        .get("asset_symbol")
        .and_then(serde_json::Value::as_str)
        .and_then(|s| sentinel_record::Asset::new(s).ok())?;
    let amount = frame
        .get("balance")
        .or_else(|| frame.get("available_balance"))
        .and_then(parse::amount)?;
    Some(VenueEvent::Balance { asset, amount })
}

#[cfg(test)]
mod tests {
    use super::*;
    use sentinel_types::{Money, Price, Qty};

    fn json(text: &str) -> serde_json::Value {
        serde_json::from_str(text).unwrap()
    }

    #[test]
    fn a_trade_frame_becomes_a_fill_with_its_execution_id() {
        let frame = json(
            r#"{"type": "v2/user_trades", "client_order_id": "UI-4bb5ba826e2e1",
                 "fill_id": 9911, "size": 2, "price": "109432.5", "side": "buy"}"#,
        );
        assert_eq!(
            decode_frame(&frame),
            vec![VenueEvent::Fill {
                client_order_id: sentinel_types::ClientOrderId::new("UI-4bb5ba826e2e1").unwrap(),
                exec_id: sentinel_types::ExecId::new("9911").unwrap(),
                qty: Qty::whole(2),
                price: Price::parse("109432.5").unwrap(),
            }]
        );
    }

    #[test]
    fn a_trade_without_an_execution_id_is_dropped_not_applied() {
        // Applying an undeduplicable fill twice moves a real position twice.
        // Dropping it costs nothing: the sweep finds the order.
        let frame = json(
            r#"{"type": "v2/user_trades", "client_order_id": "UI-1",
                 "size": 2, "price": "100"}"#,
        );
        assert!(decode_frame(&frame).is_empty());
    }

    #[test]
    fn an_order_update_yields_only_the_cancellation() {
        // A fill arrives on the trades channel with an execution id. Taking it
        // from here as well would book one execution from two sources.
        let cancelled =
            json(r#"{"type": "orders", "client_order_id": "UI-1", "state": "cancelled"}"#);
        assert_eq!(
            decode_frame(&cancelled),
            vec![VenueEvent::CancelConfirmed {
                client_order_id: sentinel_types::ClientOrderId::new("UI-1").unwrap()
            }]
        );

        for state in ["open", "pending", "closed"] {
            let frame = json(&format!(
                r#"{{"type": "orders", "client_order_id": "UI-1", "state": "{state}"}}"#
            ));
            assert!(decode_frame(&frame).is_empty(), "{state}");
        }
    }

    #[test]
    fn a_ticker_frame_becomes_a_mark() {
        let frame = json(r#"{"type": "v2/ticker", "symbol": "BTCUSD", "mark_price": "109432.5"}"#);
        assert_eq!(
            decode_frame(&frame),
            vec![VenueEvent::Mark {
                instrument: Instrument::new("BTCUSD").unwrap(),
                price: Price::parse("109432.5").unwrap(),
            }]
        );
    }

    #[test]
    fn a_margin_frame_becomes_a_balance() {
        let frame = json(r#"{"type": "margins", "asset_symbol": "USD", "balance": "5942.65"}"#);
        assert_eq!(
            decode_frame(&frame),
            vec![VenueEvent::Balance {
                asset: sentinel_record::Asset::new("USD").unwrap(),
                amount: Money::parse("5942.65").unwrap(),
            }]
        );
    }

    #[test]
    fn acknowledgements_and_heartbeats_produce_nothing() {
        for text in [
            r#"{"type": "subscriptions", "channels": []}"#,
            r#"{"type": "heartbeat"}"#,
            r#"{"type": "something_new", "payload": {}}"#,
            r#"{}"#,
        ] {
            assert!(decode_frame(&json(text)).is_empty(), "{text}");
        }
    }

    #[test]
    fn a_frame_missing_a_field_produces_nothing_rather_than_a_guess() {
        for text in [
            r#"{"type": "v2/ticker", "symbol": "BTCUSD"}"#,
            r#"{"type": "v2/ticker", "mark_price": "1"}"#,
            r#"{"type": "margins", "balance": "1"}"#,
            r#"{"type": "orders", "state": "cancelled"}"#,
        ] {
            assert!(decode_frame(&json(text)).is_empty(), "{text}");
        }
    }

    #[test]
    fn the_backoff_is_bounded() {
        // Reconnecting instantly against a venue refusing connections is how a
        // rate limit becomes a ban.
        assert!(RECONNECT_BACKOFF < RECONNECT_BACKOFF_CAP);
        assert!(RECONNECT_BACKOFF_CAP <= Duration::from_secs(60));
    }

    #[test]
    fn dropping_the_handle_stops_the_thread() {
        let running = Arc::new(AtomicBool::new(true));
        let handle = StreamHandle {
            running: Arc::clone(&running),
        };
        assert!(handle.is_running());
        drop(handle);
        assert!(!running.load(Ordering::Relaxed));
    }
}
