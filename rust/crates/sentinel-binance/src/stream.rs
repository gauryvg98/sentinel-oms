//! The user data stream and the mark feed, on one socket and one thread.
//!
//! Binance's private stream is a `listenKey`: POST to open one, connect to it,
//! `PUT` every half hour to keep it alive, and reconnect with a fresh one when
//! it drops. The key expires after about an hour and the venue says so with a
//! `listenKeyExpired` frame — which is a reason to reconnect, not an error.
//!
//! Marks ride the same socket. Binance's combined-stream endpoint takes the
//! `listenKey` as just another stream name, so `/stream?streams=<key>/btcusdt@markPrice@1s`
//! carries both. One socket rather than two is deliberate: a system whose order
//! events and prices can drop independently has a state where it holds a
//! position it cannot price, and no way to notice.
//!
//! It runs on its own thread and pushes into a channel. The previous system's
//! socket died with `keepalive ping timeout` every one to five minutes because
//! it shared an event loop with everything else; this one shares nothing.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{Receiver, Sender, channel};
use std::time::{Duration, Instant};

use sentinel_types::{ClientOrderId, ExecId, Instrument};
use sentinel_venue::{VenueError, VenueEvent};

use crate::parse;

/// How often to refresh the listen key. It lives about an hour; half of that is
/// two chances to succeed before it matters.
const KEEPALIVE_EVERY: Duration = Duration::from_secs(30 * 60);
/// Backoff between reconnect attempts.
const RECONNECT_BACKOFF: Duration = Duration::from_millis(500);
/// Cap on that backoff. Reconnecting hard against a venue that is refusing is
/// how a rate limit becomes a ban.
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

/// What the stream needs in order to open and keep a listen key.
///
/// A trait rather than a reference to the venue, so the thread does not hold
/// anything the venue worker also wants — and so this file can be tested
/// without a network.
pub trait ListenKeys: Send + Sync + 'static {
    /// Open a new listen key.
    ///
    /// # Errors
    /// [`VenueError`] when the venue cannot be reached.
    fn open(&self) -> Result<String, VenueError>;

    /// Keep the current one alive.
    ///
    /// # Errors
    /// [`VenueError`] when the venue cannot be reached. A failure here forces a
    /// reconnect with a fresh key, which is cheap and always correct.
    fn keepalive(&self) -> Result<(), VenueError>;

    /// The websocket base, e.g. `wss://fstream.binance.com`.
    fn ws_base(&self) -> String;

    /// The symbols whose marks to subscribe to.
    fn instruments(&self) -> Vec<Instrument>;
}

/// Start the stream.
///
/// # Errors
/// [`VenueError`] when the thread cannot be started.
pub fn spawn<K: ListenKeys>(
    keys: Arc<K>,
) -> Result<(StreamHandle, Receiver<VenueEvent>), VenueError> {
    let (tx, rx) = channel();
    let running = Arc::new(AtomicBool::new(true));
    let flag = Arc::clone(&running);

    std::thread::Builder::new()
        .name("binance-stream".into())
        .spawn(move || run(&*keys, &tx, &flag))
        .map_err(|e| VenueError::Transport(format!("stream thread: {e}")))?;

    Ok((StreamHandle { running }, rx))
}

/// Connect, read, reconnect. Forever, until asked to stop.
fn run<K: ListenKeys>(keys: &K, tx: &Sender<VenueEvent>, running: &AtomicBool) {
    let mut backoff = RECONNECT_BACKOFF;
    while running.load(Ordering::Relaxed) {
        match read_once(keys, tx, running) {
            // A clean end means we were asked to stop.
            Ok(()) => return,
            Err(_) => {
                // A dropped stream is not an integrity problem: every execution
                // carries a trade id the book deduplicates on, and anything
                // missed in the gap is found by the stale sweep. So this
                // reconnects rather than escalating.
                std::thread::sleep(backoff);
                backoff = (backoff * 2).min(RECONNECT_BACKOFF_CAP);
            }
        }
    }
}

fn read_once<K: ListenKeys>(
    keys: &K,
    tx: &Sender<VenueEvent>,
    running: &AtomicBool,
) -> Result<(), VenueError> {
    let listen_key = keys.open()?;
    let url = combined_stream_url(&keys.ws_base(), &listen_key, &keys.instruments());

    let (mut socket, _) =
        tungstenite::connect(&url).map_err(|e| VenueError::Transport(format!("connect: {e}")))?;
    // Bounded, so the keepalive is reached even when the venue says nothing.
    if let tungstenite::stream::MaybeTlsStream::Plain(stream) = socket.get_ref() {
        let _ = stream.set_read_timeout(Some(Duration::from_secs(30)));
    }

    let mut last_keepalive = Instant::now();

    while running.load(Ordering::Relaxed) {
        if last_keepalive.elapsed() >= KEEPALIVE_EVERY {
            last_keepalive = Instant::now();
            // A failed keepalive forces a reconnect with a fresh key, which is
            // cheap and always correct — unlike nursing one that may already
            // have expired.
            keys.keepalive()?;
        }

        let message = socket
            .read()
            .map_err(|e| VenueError::Transport(format!("read: {e}")))?;
        let text = match message {
            tungstenite::Message::Text(text) => text,
            tungstenite::Message::Ping(payload) => {
                // Answered here, on a thread that does nothing else. The
                // previous system missed these because the loop that owed the
                // reply was busy, and Binance hung up on it every few minutes.
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
        // The combined endpoint wraps every frame; the plain one does not.
        let payload = frame.get("data").unwrap_or(&frame);

        if payload.get("e").and_then(serde_json::Value::as_str) == Some("listenKeyExpired") {
            // Not an error. The venue is telling us to come back with a new key.
            return Err(VenueError::Transport("listen key expired".into()));
        }

        for event in decode_frame(payload) {
            if tx.send(event).is_err() {
                // The adapter is gone. Stop, do not spin.
                return Ok(());
            }
        }
    }
    Ok(())
}

/// The combined-stream URL carrying the private key and every mark.
///
/// One socket for both. A system whose order events and prices can drop
/// independently has a state where it holds a position it cannot price, and no
/// way to notice.
#[must_use]
pub fn combined_stream_url(ws_base: &str, listen_key: &str, instruments: &[Instrument]) -> String {
    let mut streams = String::from(listen_key);
    for instrument in instruments {
        streams.push('/');
        streams.push_str(&instrument.as_str().to_lowercase());
        streams.push_str("@markPrice@1s");
    }
    format!("{ws_base}/stream?streams={streams}")
}

/// Turn one user-data or mark frame into whatever events it carries.
///
/// Returns a list because most frames carry none — acknowledgements, account
/// updates with nothing we track — and anything unrecognised produces nothing
/// rather than a guess.
#[must_use]
pub fn decode_frame(frame: &serde_json::Value) -> Vec<VenueEvent> {
    match frame.get("e").and_then(serde_json::Value::as_str) {
        Some("ORDER_TRADE_UPDATE") => decode_order_update(frame),
        Some("ACCOUNT_UPDATE") => decode_account_update(frame),
        Some("markPriceUpdate") => decode_mark(frame).into_iter().collect(),
        _ => Vec::new(),
    }
}

fn decode_order_update(frame: &serde_json::Value) -> Vec<VenueEvent> {
    let Some(order) = frame.get("o") else {
        return Vec::new();
    };
    let Some(client_order_id) = order
        .get("c")
        .and_then(serde_json::Value::as_str)
        .and_then(|s| ClientOrderId::new(s).ok())
    else {
        return Vec::new();
    };

    match order.get("x").and_then(serde_json::Value::as_str) {
        Some("TRADE") => {
            // The execution id is the *trade* id, not the order id. An order
            // fills many times and each one has to deduplicate separately;
            // keying on the order id would collapse them all into one.
            let Some(exec_id) = order
                .get("t")
                .and_then(parse::integer)
                .and_then(|id| ExecId::new(&id.to_string()).ok())
            else {
                // Undeduplicable. Dropped rather than applied — applying it
                // twice moves a real position twice, and the sweep finds the
                // order anyway.
                return Vec::new();
            };
            let (Some(qty), Some(price)) = (
                order.get("l").and_then(parse::quantity),
                order.get("L").and_then(parse::decimal),
            ) else {
                return Vec::new();
            };
            vec![VenueEvent::Fill {
                client_order_id,
                exec_id,
                qty,
                price,
            }]
        }
        // Self-trade prevention arrives here too, and is a cancellation like
        // any other: the resting remainder is dead and its fills already came
        // through as their own trade events.
        Some("CANCELED" | "EXPIRED") => vec![VenueEvent::CancelConfirmed { client_order_id }],
        _ => Vec::new(),
    }
}

fn decode_account_update(frame: &serde_json::Value) -> Vec<VenueEvent> {
    let Some(balances) = frame
        .get("a")
        .and_then(|a| a.get("B"))
        .and_then(serde_json::Value::as_array)
    else {
        return Vec::new();
    };
    balances
        .iter()
        .filter_map(|entry| {
            let asset = entry
                .get("a")
                .and_then(serde_json::Value::as_str)
                .and_then(|s| sentinel_record::Asset::new(s).ok())?;
            let amount = entry.get("wb").and_then(parse::amount)?;
            Some(VenueEvent::Balance { asset, amount })
        })
        .collect()
}

fn decode_mark(frame: &serde_json::Value) -> Option<VenueEvent> {
    let instrument = frame
        .get("s")
        .and_then(serde_json::Value::as_str)
        .and_then(|s| Instrument::new(s).ok())?;
    let price = frame.get("p").and_then(parse::decimal)?;
    Some(VenueEvent::Mark { instrument, price })
}

#[cfg(test)]
mod tests {
    use super::*;
    use sentinel_types::{Money, Price, Qty};

    fn json(text: &str) -> serde_json::Value {
        serde_json::from_str(text).unwrap()
    }

    #[test]
    fn a_trade_becomes_a_fill_keyed_on_the_trade_id() {
        // The trade id, not the order id. An order fills many times and each
        // one deduplicates separately; keying on the order would collapse them.
        let frame = json(
            r#"{"e":"ORDER_TRADE_UPDATE","E":1787761830000,
                 "o":{"s":"BTCUSDT","c":"UI-4bb5ba826e2e1","S":"BUY","x":"TRADE",
                      "X":"PARTIALLY_FILLED","i":553664,"l":"0.001",
                      "L":"109432.5","t":9911}}"#,
        );
        assert_eq!(
            decode_frame(&frame),
            vec![VenueEvent::Fill {
                client_order_id: ClientOrderId::new("UI-4bb5ba826e2e1").unwrap(),
                exec_id: ExecId::new("9911").unwrap(),
                qty: Qty::parse("0.001").unwrap(),
                price: Price::parse("109432.5").unwrap(),
            }]
        );
    }

    #[test]
    fn two_fills_on_one_order_carry_different_execution_ids() {
        let fill = |trade_id: i64| {
            json(&format!(
                r#"{{"e":"ORDER_TRADE_UPDATE",
                     "o":{{"c":"UI-1","x":"TRADE","l":"0.001","L":"1","t":{trade_id}}}}}"#
            ))
        };
        let first = decode_frame(&fill(1));
        let second = decode_frame(&fill(2));
        assert_ne!(first, second);
    }

    #[test]
    fn a_trade_without_a_trade_id_is_dropped_not_applied() {
        // Applying an undeduplicable fill twice moves a real position twice.
        // Dropping it costs nothing: the sweep finds the order.
        let frame =
            json(r#"{"e":"ORDER_TRADE_UPDATE","o":{"c":"UI-1","x":"TRADE","l":"0.001","L":"1"}}"#);
        assert!(decode_frame(&frame).is_empty());
    }

    #[test]
    fn a_cancellation_is_reported_and_a_new_order_is_not() {
        for execution_type in ["CANCELED", "EXPIRED"] {
            let frame = json(&format!(
                r#"{{"e":"ORDER_TRADE_UPDATE","o":{{"c":"UI-1","x":"{execution_type}"}}}}"#
            ));
            assert_eq!(
                decode_frame(&frame),
                vec![VenueEvent::CancelConfirmed {
                    client_order_id: ClientOrderId::new("UI-1").unwrap()
                }],
                "{execution_type}"
            );
        }
        // NEW is the venue acknowledging, which arrives on the REST reply. A
        // second acknowledgement from here would be the same fact twice.
        let new = json(r#"{"e":"ORDER_TRADE_UPDATE","o":{"c":"UI-1","x":"NEW"}}"#);
        assert!(decode_frame(&new).is_empty());
    }

    #[test]
    fn an_account_update_becomes_balances() {
        let frame = json(
            r#"{"e":"ACCOUNT_UPDATE","a":{"B":[
                 {"a":"USDT","wb":"5942.65","cw":"5942.65"},
                 {"a":"BTC","wb":"0.01","cw":"0.01"}]}}"#,
        );
        assert_eq!(
            decode_frame(&frame),
            vec![
                VenueEvent::Balance {
                    asset: sentinel_record::Asset::new("USDT").unwrap(),
                    amount: Money::parse("5942.65").unwrap(),
                },
                VenueEvent::Balance {
                    asset: sentinel_record::Asset::new("BTC").unwrap(),
                    amount: Money::parse("0.01").unwrap(),
                },
            ]
        );
    }

    #[test]
    fn a_mark_price_update_becomes_a_mark() {
        let frame = json(
            r#"{"e":"markPriceUpdate","E":1787761830000,"s":"BTCUSDT",
                 "p":"109432.5","i":"109430","r":"0.0001","T":1787761860000}"#,
        );
        assert_eq!(
            decode_frame(&frame),
            vec![VenueEvent::Mark {
                instrument: Instrument::new("BTCUSDT").unwrap(),
                price: Price::parse("109432.5").unwrap(),
            }]
        );
    }

    #[test]
    fn unrecognised_frames_produce_nothing_rather_than_a_guess() {
        for text in [
            r#"{"e":"listenKeyExpired"}"#,
            r#"{"e":"TRADE_LITE"}"#,
            r#"{"result":null,"id":1}"#,
            r#"{}"#,
        ] {
            assert!(decode_frame(&json(text)).is_empty(), "{text}");
        }
    }

    #[test]
    fn a_frame_missing_a_field_produces_nothing() {
        for text in [
            r#"{"e":"markPriceUpdate","s":"BTCUSDT"}"#,
            r#"{"e":"markPriceUpdate","p":"1"}"#,
            r#"{"e":"ORDER_TRADE_UPDATE","o":{"x":"TRADE","l":"1","L":"1","t":1}}"#,
            r#"{"e":"ORDER_TRADE_UPDATE"}"#,
        ] {
            assert!(decode_frame(&json(text)).is_empty(), "{text}");
        }
    }

    #[test]
    fn one_socket_carries_the_key_and_every_mark() {
        // A system whose order events and prices can drop independently has a
        // state where it holds a position it cannot price.
        let url = combined_stream_url(
            "wss://fstream.binance.com",
            "abc123",
            &[
                Instrument::new("BTCUSDT").unwrap(),
                Instrument::new("ETHUSDT").unwrap(),
            ],
        );
        assert_eq!(
            url,
            "wss://fstream.binance.com/stream?streams=abc123/btcusdt@markPrice@1s/ethusdt@markPrice@1s"
        );
    }

    #[test]
    fn the_stream_url_works_with_no_symbols() {
        let url = combined_stream_url("wss://fstream.binance.com", "abc123", &[]);
        assert_eq!(url, "wss://fstream.binance.com/stream?streams=abc123");
    }

    #[test]
    fn the_backoff_is_bounded() {
        assert!(RECONNECT_BACKOFF < RECONNECT_BACKOFF_CAP);
        assert!(RECONNECT_BACKOFF_CAP <= Duration::from_secs(60));
        // The key lives about an hour; refreshing at half that is two chances
        // to succeed before it matters.
        assert!(KEEPALIVE_EVERY <= Duration::from_secs(45 * 60));
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
