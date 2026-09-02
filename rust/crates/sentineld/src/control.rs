//! The control socket: how anything outside this process asks it to trade.
//!
//! A Unix socket beside the journal, speaking one JSON object per line. Reads
//! do not come through here at all — a consumer that wants to know what is
//! happening tails the journal, which is an ordered log with a cursor per
//! reader and needs no request-response protocol on top of it.
//!
//! So this carries only the things that *change* something, and there are four:
//! place, cancel, halt, resume. A surface that small is one an operator can
//! hold in their head, and one where every entry point is a line in this file.
//!
//! A Unix socket rather than a TCP port because both ends are in the same
//! machine by construction — one journal, one `flock` — and a port is a thing
//! that can be reached from somewhere else.

use std::io::{BufRead, BufReader, Write};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use sentinel_domain::EconomicOrderIntent;
use sentinel_engine::Command;
use sentinel_record::Note;
use sentinel_types::{Authority, ClientOrderId, Instrument, OrderKind, Price, Qty, Side, TraceId};

use crate::runtime::Runtime;

/// The socket's name, inside the journal directory.
pub const SOCKET_NAME: &str = "control.sock";

/// Where the socket lives for a given journal.
#[must_use]
pub fn socket_path(journal_dir: impl AsRef<Path>) -> PathBuf {
    journal_dir.as_ref().join(SOCKET_NAME)
}

/// Why a line was not a command.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Malformed(String);

impl core::fmt::Display for Malformed {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str(&self.0)
    }
}

/// Parse one line into a command.
///
/// Pure, and public, so the protocol can be tested without a socket — and so
/// the Go client has something to be checked against that is not a running
/// process.
///
/// # Errors
/// [`Malformed`] when the line is not a command this process accepts. Refused
/// rather than partially applied: a place request missing its quantity is not
/// an order for zero.
pub fn parse(line: &str) -> Result<Command, Malformed> {
    let value: serde_json::Value =
        serde_json::from_str(line).map_err(|e| Malformed(e.to_string()))?;
    let field = |name: &str| -> Option<&serde_json::Value> { value.get(name) };
    let text = |name: &str| field(name).and_then(serde_json::Value::as_str);
    let missing = |name: &str| Malformed(format!("missing {name}"));

    match text("op").ok_or_else(|| missing("op"))? {
        "place" => {
            let client_order_id =
                text("client_order_id").ok_or_else(|| missing("client_order_id"))?;
            let instrument = text("instrument").ok_or_else(|| missing("instrument"))?;
            let side = match text("side").ok_or_else(|| missing("side"))? {
                "buy" | "BUY" => Side::Buy,
                "sell" | "SELL" => Side::Sell,
                other => return Err(Malformed(format!("side {other:?}"))),
            };
            let qty = text("qty")
                .and_then(|s| Qty::parse(s).ok())
                .ok_or_else(|| missing("qty"))?;
            // Entry unless it says otherwise. A protective exit has to be asked
            // for by name: the authority decides which guards apply, and a
            // caller that gets it wrong by omission would bypass them.
            let authority = match text("authority") {
                Some("exit" | "protective_exit") => Authority::ProtectiveExit,
                _ => Authority::Entry,
            };
            let limit_price = text("limit_price").and_then(|s| Price::parse(s).ok());
            let stop_price = text("stop_price").and_then(|s| Price::parse(s).ok());
            let kind = if stop_price.is_some() {
                OrderKind::StopMarket
            } else if limit_price.is_some() {
                OrderKind::Limit
            } else {
                OrderKind::Market
            };
            // A trace the caller supplies, or none. Never invented here: a
            // trace id exists to tie an order to the decision that caused it,
            // and one minted at the boundary ties it to nothing.
            let trace_id = field("trace_id")
                .and_then(serde_json::Value::as_u64)
                .map_or(TraceId::NONE, |v| TraceId::from_u128(u128::from(v)));

            let intent = EconomicOrderIntent::new(
                ClientOrderId::new(client_order_id).map_err(|e| Malformed(e.to_string()))?,
                Instrument::new(instrument).map_err(|e| Malformed(e.to_string()))?,
                side,
                qty,
                kind,
                limit_price,
                stop_price,
                authority,
                trace_id,
                None,
            )
            .map_err(|e| Malformed(e.to_string()))?;
            Ok(Command::Place { intent })
        }
        "cancel" => Ok(Command::Cancel {
            client_order_id: ClientOrderId::new(
                text("client_order_id").ok_or_else(|| missing("client_order_id"))?,
            )
            .map_err(|e| Malformed(e.to_string()))?,
            trace_id: TraceId::NONE,
        }),
        "halt" => Ok(Command::Halt {
            note: Note::new(text("note").unwrap_or("halted by operator")).unwrap_or_default(),
        }),
        "resume" => Ok(Command::Resume {
            note: Note::new(text("note").unwrap_or("resumed by operator")).unwrap_or_default(),
        }),
        // A strategy saying what it wants to hold. It changes nothing — the
        // orders that follow come through "place" like anyone else's — but it
        // puts the reasoning in the same log as the consequence, so a stretch
        // of doing nothing has an explanation rather than a silence.
        "decide" => Ok(Command::Decide {
            actor: sentinel_record::Actor::Strategy,
            instrument: sentinel_types::Instrument::new(
                text("instrument").ok_or_else(|| missing("instrument"))?,
            )
            .map_err(|e| Malformed(e.to_string()))?,
            kind: sentinel_record::DecisionKind::StanceDeclared,
            trace_id: TraceId::NONE,
            value: text("value")
                .map(sentinel_types::Money::parse)
                .transpose()
                .map_err(|e| Malformed(e.to_string()))?
                .unwrap_or(sentinel_types::Money::ZERO),
            note: Note::new(text("note").unwrap_or("")).unwrap_or_default(),
        }),
        other => Err(Malformed(format!("unknown op {other:?}"))),
    }
}

/// Serve the control socket until asked to stop.
///
/// # Errors
/// [`std::io::Error`] when the socket cannot be created or the thread started.
pub fn serve(
    journal_dir: impl AsRef<Path>,
    runtime: Arc<Runtime>,
    running: Arc<AtomicBool>,
) -> std::io::Result<()> {
    let path = socket_path(journal_dir);
    // A socket left behind by a crash would refuse to bind. Removing it is safe
    // for exactly the reason the journal lock is: this process has already
    // proven it is the only writer, so nothing else can be listening on it.
    let _ = std::fs::remove_file(&path);
    let listener = UnixListener::bind(&path)?;
    listener.set_nonblocking(true)?;

    std::thread::Builder::new()
        .name("control".into())
        .spawn(move || {
            while running.load(Ordering::Relaxed) {
                match listener.accept() {
                    Ok((stream, _)) => handle(stream, &runtime),
                    Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        std::thread::sleep(std::time::Duration::from_millis(50));
                    }
                    Err(_) => return,
                }
            }
            let _ = std::fs::remove_file(&path);
        })?;
    Ok(())
}

/// One connection: read lines, answer each.
fn handle(stream: UnixStream, runtime: &Runtime) {
    let Ok(mut out) = stream.try_clone() else {
        return;
    };
    let reader = BufReader::new(stream);
    for line in reader.lines() {
        let Ok(line) = line else { return };
        if line.trim().is_empty() {
            continue;
        }
        // Every line gets an answer, including the refusals. A caller that
        // sent something malformed and heard nothing back cannot tell that
        // from a caller whose order was accepted.
        let answer = match parse(&line) {
            Ok(command) => {
                if runtime.submit(command).is_ok() {
                    r#"{"ok":true}"#.to_owned()
                } else {
                    r#"{"ok":false,"error":"the writer has stopped"}"#.to_owned()
                }
            }
            Err(e) => {
                let message = serde_json::Value::String(e.to_string());
                format!(r#"{{"ok":false,"error":{message}}}"#)
            }
        };
        if writeln!(out, "{answer}").is_err() {
            return;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn place(json: &str) -> Result<EconomicOrderIntent, Malformed> {
        match parse(json)? {
            Command::Place { intent } => Ok(intent),
            other => Err(Malformed(format!("expected a placement, got {other:?}"))),
        }
    }

    #[test]
    fn a_market_entry_parses() {
        let intent = place(
            r#"{"op":"place","client_order_id":"UI-1","instrument":"BTCUSD",
                 "side":"buy","qty":"0.003"}"#,
        )
        .unwrap();
        assert_eq!(intent.client_order_id.as_str(), "UI-1");
        assert_eq!(intent.side, Side::Buy);
        assert_eq!(intent.qty, Qty::parse("0.003").unwrap());
        assert_eq!(intent.kind, OrderKind::Market);
        assert_eq!(intent.authority, Authority::Entry);
    }

    #[test]
    fn a_limit_order_takes_its_kind_from_the_price_it_carries() {
        let intent = place(
            r#"{"op":"place","client_order_id":"UI-1","instrument":"BTCUSD",
                 "side":"sell","qty":"0.001","limit_price":"109432.5"}"#,
        )
        .unwrap();
        assert_eq!(intent.kind, OrderKind::Limit);
        assert_eq!(intent.limit_price, Some(Price::parse("109432.5").unwrap()));
    }

    #[test]
    fn a_stop_takes_precedence_and_is_a_stop_market() {
        let intent = place(
            r#"{"op":"place","client_order_id":"SL-1","instrument":"BTCUSD",
                 "side":"sell","qty":"0.001","stop_price":"95000",
                 "authority":"exit"}"#,
        )
        .unwrap();
        assert_eq!(intent.kind, OrderKind::StopMarket);
        assert_eq!(intent.authority, Authority::ProtectiveExit);
    }

    #[test]
    fn an_exit_has_to_be_asked_for_by_name() {
        // The authority decides which guards apply. Defaulting to an exit
        // would let a caller bypass every entry check by omitting a field.
        let intent = place(
            r#"{"op":"place","client_order_id":"UI-1","instrument":"BTCUSD",
                 "side":"sell","qty":"0.001"}"#,
        )
        .unwrap();
        assert_eq!(intent.authority, Authority::Entry);
    }

    #[test]
    fn a_quantity_crosses_as_a_string_and_stays_exact() {
        // JavaScript has one numeric type and it is a float64. A quantity that
        // arrived as a JSON number would already have been rounded by the time
        // this saw it.
        let intent = place(
            r#"{"op":"place","client_order_id":"UI-1","instrument":"BTCUSD",
                 "side":"buy","qty":"0.00000001"}"#,
        )
        .unwrap();
        assert_eq!(intent.qty, Qty::from_raw(1));

        assert!(
            place(
                r#"{"op":"place","client_order_id":"UI-1","instrument":"BTCUSD",
                     "side":"buy","qty":0.00000001}"#
            )
            .is_err(),
            "a number is refused rather than read through a float"
        );
    }

    #[test]
    fn a_missing_field_is_refused_rather_than_defaulted() {
        // An order missing its quantity is not an order for zero.
        for line in [
            r#"{"op":"place","instrument":"BTCUSD","side":"buy","qty":"1"}"#,
            r#"{"op":"place","client_order_id":"UI-1","side":"buy","qty":"1"}"#,
            r#"{"op":"place","client_order_id":"UI-1","instrument":"BTCUSD","qty":"1"}"#,
            r#"{"op":"place","client_order_id":"UI-1","instrument":"BTCUSD","side":"buy"}"#,
            r#"{"op":"place","client_order_id":"UI-1","instrument":"BTCUSD","side":"buy","qty":"0"}"#,
        ] {
            assert!(parse(line).is_err(), "{line}");
        }
    }

    #[test]
    fn cancel_halt_and_resume_parse() {
        assert!(matches!(
            parse(r#"{"op":"cancel","client_order_id":"UI-1"}"#).unwrap(),
            Command::Cancel { .. }
        ));
        assert!(matches!(
            parse(r#"{"op":"halt","note":"divergence"}"#).unwrap(),
            Command::Halt { .. }
        ));
        assert!(matches!(
            parse(r#"{"op":"resume"}"#).unwrap(),
            Command::Resume { .. }
        ));
    }

    #[test]
    fn the_surface_is_exactly_four_operations() {
        // Anything else is refused. A control socket that quietly accepts an
        // unknown verb is one whose surface nobody can state.
        for op in ["tick", "sweep", "positions", "read", "flush", ""] {
            let line = format!(r#"{{"op":"{op}"}}"#);
            assert!(parse(&line).is_err(), "{op}");
        }
    }

    #[test]
    fn a_line_that_is_not_json_is_refused_without_panicking() {
        for line in ["", "not json", "{", "[]", "null", "42"] {
            assert!(parse(line).is_err(), "{line}");
        }
    }

    #[test]
    fn the_socket_sits_beside_the_journal() {
        assert_eq!(
            socket_path("/data/journal"),
            PathBuf::from("/data/journal/control.sock")
        );
    }
}
