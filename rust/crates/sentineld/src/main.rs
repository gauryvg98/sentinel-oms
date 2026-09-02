//! The process.
//!
//! Boot is four steps and they are in this order for a reason:
//!
//! 1. **Claim the journal.** One machine, one file, one `flock`. A second
//!    process is refused here and never gets far enough to write anything.
//! 2. **Rebuild the book** by folding the whole log. Not a recovery path kept
//!    warm for emergencies — it is the only way the book is ever populated.
//! 3. **Connect the venue** and load the products actually traded.
//! 4. **Start the threads**, and only then accept work.
//!
//! Nothing trades before step 2 finishes. That is R1.11 as a property of the
//! binary rather than of a test: there is no window in which this process has
//! opinions about a position it has not yet read from its own log.

mod config;
mod control;
mod runtime;

use std::process::ExitCode;

use sentinel_engine::Engine;
use sentinel_ledger::check;
use sentinel_venue::SimVenue;

use crate::config::{Config, VenueConfig};

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(message) => {
            eprintln!("sentineld: {message}");
            ExitCode::FAILURE
        }
    }
}

/// The runtime's own stop flag, shared with the control thread so closing the
/// socket and stopping the writer are the same event.
fn running_flag(
    runtime: &std::sync::Arc<runtime::Runtime>,
) -> std::sync::Arc<std::sync::atomic::AtomicBool> {
    runtime.stop_flag()
}

fn run() -> Result<(), String> {
    let config = Config::from_env().map_err(|e| e.to_string())?;
    println!("sentineld: {}", config.describe());
    if config.is_live_money() {
        // Said in as many words. An operator should never have to infer it
        // from a URL in a log line.
        println!("sentineld: LIVE MONEY — orders placed here can lose real funds");
    }

    // 1 + 2: claim, and rebuild from the log.
    let engine = Engine::open(
        &config.journal_dir,
        runtime::now(),
        runtime::engine_config(&config),
    )
    .map_err(|e| format!("cannot claim {}: {e}", config.journal_dir))?;

    let report = check(engine.book());
    if !report.holds() {
        // A book that does not hold its own invariants is not one to trade
        // from. Refusing to start is louder than halting after the first order.
        return Err(format!("the rebuilt book is unsound: {report}"));
    }
    println!(
        "sentineld: epoch {}, resumed at seq {}, {} orders, {} fills{}",
        engine.epoch(),
        engine.book().last_seq(),
        engine.book().orders().count(),
        engine.book().fill_count(),
        if engine.is_halted() { ", HALTED" } else { "" }
    );

    // 3 + 4: connect, and start.
    let handle: runtime::Runtime = match &config.venue {
        VenueConfig::Simulator => {
            println!("sentineld: simulator — nothing here reaches a venue");
            runtime::start(
                engine,
                SimVenue::new(),
                config.tick_interval,
                config.sweep_interval,
            )
            .map_err(|e| e.to_string())?
        }
        VenueConfig::Binance {
            api_key,
            secret,
            demo,
            leverage,
        } => {
            let credentials = if *demo {
                sentinel_binance::Credentials::demo(api_key, secret)
            } else {
                sentinel_binance::Credentials::production(api_key, secret)
            };
            let mut venue = sentinel_binance::BinanceVenue::connect(
                credentials,
                &config.instruments,
                *leverage,
            )
            .map_err(|e| format!("cannot reach the venue: {e}"))?;
            venue
                .start_stream()
                .map_err(|e| format!("cannot open the user stream: {e}"))?;
            println!(
                "sentineld: {} symbols loaded, clock offset {}ms, stream up",
                venue.symbols().len(),
                venue.clock_offset_ms()
            );
            runtime::start(engine, venue, config.tick_interval, config.sweep_interval)
                .map_err(|e| e.to_string())?
        }
        VenueConfig::Delta {
            api_key,
            secret,
            testnet,
        } => {
            let credentials = if *testnet {
                sentinel_delta::Credentials::testnet(api_key, secret)
            } else {
                sentinel_delta::Credentials::production(api_key, secret)
            };
            let mut venue = sentinel_delta::DeltaVenue::connect(credentials, &config.instruments)
                .map_err(|e| format!("cannot reach the venue: {e}"))?;
            venue
                .start_stream()
                .map_err(|e| format!("cannot open the user stream: {e}"))?;
            println!(
                "sentineld: {} products loaded, stream up",
                venue.products().len()
            );
            runtime::start(engine, venue, config.tick_interval, config.sweep_interval)
                .map_err(|e| e.to_string())?
        }
    };

    // The control socket is opened last, so nothing can ask this process to
    // trade until the book is rebuilt, sound, and the venue is reachable.
    let handle = std::sync::Arc::new(handle);
    control::serve(
        &config.journal_dir,
        std::sync::Arc::clone(&handle),
        running_flag(&handle),
    )
    .map_err(|e| format!("cannot open the control socket: {e}"))?;

    // Fly sends SIGTERM before it sends SIGKILL. Catching it turns an abrupt
    // death into a drain: the writer finishes what is queued and flushes, so
    // nothing accepted is lost. Not catching it would still be *safe* — a torn
    // tail is truncated on the next claim and the book rebuilds — but "safe"
    // and "lost the last three commands" are different words.
    for signal in [signal_hook::consts::SIGTERM, signal_hook::consts::SIGINT] {
        let flag = handle.stop_flag();
        signal_hook::flag::register(signal, flag)
            .map_err(|e| format!("cannot handle signal {signal}: {e}"))?;
    }

    println!(
        "sentineld: running, control on {}",
        control::socket_path(&config.journal_dir).display()
    );
    // The threads own everything now. This one waits for them to stop, which
    // happens when the writer hits something it must not trade through.
    while handle.is_running() {
        std::thread::sleep(std::time::Duration::from_millis(200));
    }
    // The threads see the same flag and drain on their way out.
    handle.stop();
    std::thread::sleep(std::time::Duration::from_millis(300));
    println!("sentineld: stopped");
    Ok(())
}
