//! Point the adapter at the real Binance and see whether it works.
//!
//! ```text
//! cargo run --release -p sentinel-binance --example verify
//! ```
//!
//! **Read-only, structurally.** It uses [`BinanceVenue::connect_read_only`],
//! which syncs the clock and loads the symbol filters and does nothing else —
//! no position mode, no leverage, and no order path anywhere in this file. A
//! tool for looking at an account must not be able to change one, and that has
//! to be a property of what it can call rather than a flag someone remembers.
//!
//! Without keys it checks everything public: the clock, the symbol filters, and
//! the live mark stream. With `BINANCE_FUTURES_KEY` and `BINANCE_FUTURES_SECRET`
//! in the environment it also reads the account — balances, positions, open
//! orders — through the same code the engine uses.
//!
//! `SENTINEL_BINANCE_ENV=production` points it at the real venue. Anything else
//! is Demo Trading.

use std::time::{Duration, Instant};

use sentinel_binance::{BinanceVenue, Credentials, stream, symbol::SymbolRules};
use sentinel_types::{Instrument, Price, Qty};
use sentinel_venue::VenueEvent;

fn main() {
    let production = std::env::var("SENTINEL_BINANCE_ENV").as_deref() == Ok("production");
    let symbols: Vec<Instrument> = std::env::var("SENTINEL_INSTRUMENTS")
        .or_else(|_| std::env::var("SENTINEL_SYMBOLS"))
        .unwrap_or_else(|_| "BTCUSDT".into())
        .split(',')
        .filter_map(|s| Instrument::new(s.trim()).ok())
        .collect();

    let key = std::env::var("BINANCE_FUTURES_KEY")
        .or_else(|_| std::env::var("BINANCE_API_KEY"))
        .unwrap_or_default();
    let secret = std::env::var("BINANCE_FUTURES_SECRET")
        .or_else(|_| std::env::var("BINANCE_API_SECRET"))
        .unwrap_or_default();
    let signed = !key.is_empty() && !secret.is_empty();

    let credentials = if production {
        Credentials::production(&key, &secret)
    } else {
        Credentials::demo(&key, &secret)
    };

    println!(
        "verify: {} — {}, {}",
        credentials,
        if production {
            "PRODUCTION"
        } else {
            "demo trading"
        },
        if signed {
            "keys present, the account will be READ"
        } else {
            "no keys, public checks only"
        }
    );
    println!("verify: nothing in this tool can place, cancel or modify an order\n");

    let mut failures = 0;

    // --- clock and symbol filters, through the real connect path -------------
    let started = Instant::now();
    let venue = match BinanceVenue::connect_read_only(credentials.clone(), &symbols) {
        Ok(venue) => {
            println!(
                "  ok   clock + exchangeInfo in {}ms, offset {}ms",
                started.elapsed().as_millis(),
                venue.clock_offset_ms()
            );
            venue
        }
        Err(e) => {
            println!("  FAIL connect: {e}");
            std::process::exit(1);
        }
    };

    for instrument in &symbols {
        match venue.symbols().get(instrument) {
            Some(rules) => print_rules(&rules),
            None => {
                println!("  FAIL {instrument} is not listed");
                failures += 1;
            }
        }
    }

    // --- the live mark stream, decoded by the same code the engine uses ------
    match sample_marks(&credentials.ws_base, &symbols) {
        Ok(marks) if marks.is_empty() => {
            println!("  FAIL mark stream produced no decodable frames");
            failures += 1;
        }
        Ok(marks) => {
            println!("  ok   mark stream, {} frames decoded", marks.len());
            for (instrument, price) in marks.iter().take(3) {
                println!("       {instrument} {price}");
            }
        }
        Err(e) => {
            println!("  FAIL mark stream: {e}");
            failures += 1;
        }
    }

    // --- the account, only if there are keys ---------------------------------
    if signed {
        match venue.balances() {
            Ok(balances) if balances.is_empty() => println!("  ok   balances: none held"),
            Ok(balances) => {
                println!("  ok   balances");
                for (asset, amount) in balances {
                    println!("       {asset} {amount}");
                }
            }
            Err(e) => {
                println!("  FAIL balances: {e}");
                failures += 1;
            }
        }

        match venue.open_positions() {
            Ok(positions) if positions.is_empty() => println!("  ok   positions: flat"),
            Ok(positions) => {
                println!("  ok   positions");
                for p in positions {
                    println!(
                        "       {} {} @ {}  mark {}  notional {}",
                        p.instrument, p.qty, p.entry_price, p.mark_price, p.notional
                    );
                    println!(
                        "       break-even {}  unrealised {}{}",
                        p.break_even_price,
                        p.unrealized,
                        match p.liquidation_price {
                            Some(liq) => format!("  liquidation {liq}"),
                            None => String::new(),
                        }
                    );
                }
            }
            Err(e) => {
                println!("  FAIL positions: {e}");
                failures += 1;
            }
        }

        match venue.open_orders() {
            Ok(orders) if orders.is_empty() => println!("  ok   open orders: none"),
            Ok(orders) => {
                println!("  ok   open orders");
                for order in orders {
                    println!(
                        "       {} {} {} filled {} of {}",
                        order.symbol, order.order_id, order.state, order.filled_qty, order.qty
                    );
                }
            }
            Err(e) => {
                println!("  FAIL open orders: {e}");
                failures += 1;
            }
        }
    } else {
        println!(
            "\n  skipped the account: set BINANCE_FUTURES_KEY and \
             BINANCE_FUTURES_SECRET to read it"
        );
    }

    println!();
    if failures == 0 {
        println!("verify: everything checked passed");
    } else {
        println!("verify: {failures} check(s) failed");
        std::process::exit(1);
    }
}

fn print_rules(rules: &SymbolRules) {
    println!(
        "  ok   {} step {} minQty {} tick {} minNotional {}",
        rules.symbol, rules.step_size, rules.min_qty, rules.tick_size, rules.min_notional
    );

    // The grid, demonstrated on the size the live configuration would produce.
    // 200 USDT of margin at 100x is ~20,000 notional, which at ~109k is about
    // 0.18 BTC — an arbitrary decimal that has to be snapped before it is
    // placeable.
    let raw = Qty::parse("0.18367").unwrap_or(Qty::ZERO);
    let placeable = rules.round_down(raw);
    println!(
        "       a sized {raw} rounds to {placeable}, which the venue {}",
        match rules.check(placeable, Price::parse("109432.5").unwrap_or(Price::ZERO)) {
            Ok(()) => "accepts".to_owned(),
            Err(e) => format!("would refuse: {e}"),
        }
    );
}

/// Read the public mark stream for a few seconds and decode what arrives.
fn sample_marks(
    ws_base: &str,
    instruments: &[Instrument],
) -> Result<Vec<(Instrument, Price)>, String> {
    if instruments.is_empty() {
        return Ok(Vec::new());
    }
    let streams: Vec<String> = instruments
        .iter()
        .map(|i| format!("{}@markPrice@1s", i.as_str().to_lowercase()))
        .collect();
    let url = format!("{ws_base}/stream?streams={}", streams.join("/"));

    let (mut socket, _) = tungstenite::connect(&url).map_err(|e| e.to_string())?;
    let deadline = Instant::now() + Duration::from_secs(6);
    let mut out = Vec::new();

    while Instant::now() < deadline && out.len() < 6 {
        let message = match socket.read() {
            Ok(message) => message,
            Err(e) => return Err(e.to_string()),
        };
        let tungstenite::Message::Text(text) = message else {
            continue;
        };
        let Ok(frame) = serde_json::from_str::<serde_json::Value>(&text) else {
            continue;
        };
        // The combined endpoint wraps every frame, exactly as the adapter reads it.
        let payload = frame.get("data").unwrap_or(&frame);
        for event in stream::decode_frame(payload) {
            if let VenueEvent::Mark { instrument, price } = event {
                out.push((instrument, price));
            }
        }
    }
    let _ = socket.close(None);
    Ok(out)
}
