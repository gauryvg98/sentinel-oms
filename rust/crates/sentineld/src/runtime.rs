//! Three threads, and the channels between them.
//!
//! ```text
//!   ticker ──┐
//!            ├──► commands ──► writer ──► requests ──► venue worker ──┐
//!   stream ──┘        ▲                                                │
//!                     └────────────────── answers ─────────────────────┘
//! ```
//!
//! The **writer** owns the journal and the book, and does nothing that can
//! block: no socket, no DNS, no TLS handshake. Its slowest step is one `fsync`
//! shared with every command queued in that instant.
//!
//! The **venue worker** owns the adapter and blocks freely. It performs the
//! requests the writer released and posts the answers back as commands, so a
//! venue that is throttling or unreachable slows down orders and nothing else.
//!
//! The **ticker** is the only clock in the system. Time reaches the engine as a
//! command like any other, which is what makes a replay reproduce.
//!
//! This is the fix for the shape that broke: the previous system held the
//! instrument lock across the venue's HTTP round trip, so an inbound fill
//! queued behind a network call. Here the two cannot queue behind each other,
//! because they are not in the same place.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{RecvTimeoutError, Sender, channel};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use sentinel_engine::{Command, Config as EngineConfig, Engine};
use sentinel_types::Nanos;
use sentinel_venue::{Venue, VenueRequest};

/// How long the writer waits for work before looking at its own housekeeping.
///
/// It blocks on the channel rather than polling. Every loop in this process
/// wakes on something happening; a ticker that fires whether or not there is
/// anything to do adds its own period to every event's latency and burns a core
/// when the market is quiet.
const WRITER_IDLE: Duration = Duration::from_millis(500);

/// The writer is no longer accepting work.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Stopped;

impl core::fmt::Display for Stopped {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str("the writer has stopped")
    }
}

impl std::error::Error for Stopped {}

/// Everything running.
#[derive(Debug)]
pub struct Runtime {
    commands: Sender<Command>,
    running: Arc<AtomicBool>,
}

impl Runtime {
    /// Send a command to the writer.
    ///
    /// # Errors
    /// [`Stopped`] when the writer has already gone. The command is dropped
    /// rather than handed back: there is nothing a caller can do with it, and
    /// returning it would put a 320-byte value in every `Result` on this path.
    pub fn submit(&self, command: Command) -> Result<(), Stopped> {
        self.commands.send(command).map_err(|_| Stopped)
    }

    /// Ask everything to stop.
    ///
    /// Drains before exiting: the writer finishes what is queued and flushes,
    /// so no accepted command is lost at shutdown.
    pub fn stop(&self) {
        self.running.store(false, Ordering::Relaxed);
    }

    /// Whether it is still running.
    #[must_use]
    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::Relaxed)
    }

    /// The stop flag, shared so that anything else attached to this runtime
    /// stops with it rather than outliving it.
    ///
    /// Handed out rather than wrapped in a method, because a signal handler
    /// can do exactly one thing safely and that is set an atomic.
    #[must_use]
    pub fn stop_flag(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.running)
    }
}

/// Wall-clock nanoseconds.
///
/// The gateway boundary. This is the only place in the process that reads a
/// clock for the engine, and the value reaches it as [`Command::Tick`] — so
/// everything below measures time against the log rather than against `now`.
#[must_use]
pub fn now() -> Nanos {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(Nanos::ZERO, |d| {
            Nanos::from_u64(u64::try_from(d.as_nanos()).unwrap_or(u64::MAX))
        })
}

/// Start the writer, the venue worker and the ticker.
///
/// # Errors
/// [`std::io::Error`] when a thread cannot be started.
pub fn start<V>(
    mut engine: Engine,
    mut venue: V,
    tick_interval: Duration,
    sweep_interval: Duration,
) -> std::io::Result<Runtime>
where
    V: Venue + Send + 'static,
{
    let (commands_tx, commands_rx) = channel::<Command>();
    let (requests_tx, requests_rx) = channel::<VenueRequest>();
    let running = Arc::new(AtomicBool::new(true));

    // --- the venue worker ---------------------------------------------------
    let worker_commands = commands_tx.clone();
    let worker_running = Arc::clone(&running);
    std::thread::Builder::new()
        .name("venue".into())
        .spawn(move || {
            let mut events = Vec::new();
            while worker_running.load(Ordering::Relaxed) {
                // Anything the stream pushed, forwarded as commands.
                events.clear();
                venue.drain_events(&mut events);
                for event in events.drain(..) {
                    if worker_commands.send(Command::Streamed { event }).is_err() {
                        return;
                    }
                }

                match requests_rx.recv_timeout(WRITER_IDLE) {
                    Ok(request) => {
                        for answer in perform(&mut venue, request) {
                            if worker_commands.send(answer).is_err() {
                                return;
                            }
                        }
                    }
                    Err(RecvTimeoutError::Timeout) => {}
                    Err(RecvTimeoutError::Disconnected) => return,
                }
            }
        })?;

    // --- the ticker ---------------------------------------------------------
    let ticker_commands = commands_tx.clone();
    let ticker_running = Arc::clone(&running);
    std::thread::Builder::new()
        .name("ticker".into())
        .spawn(move || {
            // Due immediately, so the first sweep runs on the first tick
            // rather than a minute in. The sweep is what asks the venue for
            // the account's positions, and until that answer arrives the
            // engine refuses to open anything — so a slow first sweep is a
            // minute of a live deployment declining to trade, and on a cutover
            // it is a minute before it learns what it has inherited.
            let mut since_sweep = sweep_interval;
            while ticker_running.load(Ordering::Relaxed) {
                std::thread::sleep(tick_interval);
                if ticker_commands.send(Command::Tick { now: now() }).is_err() {
                    return;
                }
                since_sweep += tick_interval;
                if since_sweep >= sweep_interval {
                    since_sweep = Duration::ZERO;
                    if ticker_commands.send(Command::SweepStale).is_err() {
                        return;
                    }
                }
            }
        })?;

    // --- the writer ---------------------------------------------------------
    let writer_running = Arc::clone(&running);
    std::thread::Builder::new()
        .name("writer".into())
        .spawn(move || {
            while writer_running.load(Ordering::Relaxed) {
                // Block for the first command, then take everything else that
                // is already queued. That second part is what makes group
                // commit engage: a burst of commands shares one `fsync`, so the
                // cost per record falls as load rises.
                let first = match commands_rx.recv_timeout(WRITER_IDLE) {
                    Ok(command) => Some(command),
                    Err(RecvTimeoutError::Timeout) => None,
                    Err(RecvTimeoutError::Disconnected) => break,
                };
                let Some(first) = first else { continue };

                let mut batch = vec![first];
                batch.extend(commands_rx.try_iter());

                for command in batch {
                    if let Err(e) = engine.submit(command) {
                        // A failure here is not something to carry on through:
                        // either the log is unwritable or the book has been
                        // shown to be wrong, and both mean the next order would
                        // be placed on numbers we know are untrustworthy.
                        eprintln!("writer: {e}");
                        if e.is_fatal() {
                            writer_running.store(false, Ordering::Relaxed);
                            return;
                        }
                    }
                }

                match engine.flush() {
                    Ok(requests) => {
                        for request in requests {
                            if requests_tx.send(request).is_err() {
                                return;
                            }
                        }
                    }
                    Err(e) => {
                        // A flush that failed leaves memory ahead of a log
                        // still being served from. There is no recovering in
                        // place; the process restarts and rebuilds from what is
                        // actually durable.
                        eprintln!("writer: flush failed, stopping: {e}");
                        writer_running.store(false, Ordering::Relaxed);
                        return;
                    }
                }
            }

            // Drain on the way out, so nothing accepted is lost at shutdown.
            for command in commands_rx.try_iter() {
                let _ = engine.submit(command);
            }
            let _ = engine.flush();
        })?;

    Ok(Runtime {
        commands: commands_tx,
        running,
    })
}

/// Perform one request and turn the answer into commands.
///
/// A call that could not be made produces nothing at all. That is deliberate:
/// silence is what the engine already handles — the order stays where it is and
/// the reconcile backoff asks again — whereas inventing an outcome would tell
/// it something nobody knows.
fn perform<V: Venue>(venue: &mut V, request: VenueRequest) -> Vec<Command> {
    match request {
        VenueRequest::Submit(intent) => venue
            .submit(&intent)
            .ok()
            .map(|outcome| Command::Submitted {
                client_order_id: intent.client_order_id,
                outcome,
            })
            .into_iter()
            .collect(),
        VenueRequest::Cancel {
            client_order_id,
            venue_order_id,
        } => venue
            .cancel(client_order_id, venue_order_id)
            .ok()
            .map(|outcome| Command::Cancelled {
                client_order_id,
                outcome,
            })
            .into_iter()
            .collect(),
        VenueRequest::Lookup { client_order_id } => venue
            .lookup(client_order_id)
            .ok()
            .map(|outcome| Command::LookedUp {
                client_order_id,
                outcome,
            })
            .into_iter()
            .collect(),
        VenueRequest::Positions => venue
            .positions()
            .ok()
            .map(|positions| Command::positions_reported(&positions))
            .into_iter()
            .collect(),
    }
}

/// Build the engine configuration from the process configuration.
#[must_use]
pub fn engine_config(config: &crate::config::Config) -> EngineConfig {
    EngineConfig {
        limits: sentinel_engine::Limits {
            max_position: config.max_position,
            // Configuration cannot grant this. The engine sets it when the
            // venue answers, and starts every boot without it.
            position_confirmed: false,
        },
        stale_after_nanos: u64::try_from(config.stale_after.as_nanos()).unwrap_or(u64::MAX),
        ..EngineConfig::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sentinel_domain::EconomicOrderIntent;
    use sentinel_types::{Authority, ClientOrderId, Instrument, Qty, Side, TraceId};
    use sentinel_venue::SimVenue;

    struct Scratch(std::path::PathBuf);

    impl Scratch {
        fn new(tag: &str) -> Self {
            let dir = std::env::temp_dir().join(format!("sentineld-{tag}-{}", std::process::id()));
            let _ = std::fs::remove_dir_all(&dir);
            Self(dir)
        }
    }

    impl Drop for Scratch {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    fn wait_for(mut predicate: impl FnMut() -> bool) -> bool {
        // Polled with a deadline rather than slept for a fixed time: a fixed
        // sleep is either flaky on a loaded machine or slow on an idle one.
        for _ in 0..200 {
            if predicate() {
                return true;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        false
    }

    #[test]
    fn an_order_travels_from_a_command_to_the_venue_and_back() {
        let scratch = Scratch::new("roundtrip");
        let engine = Engine::open(&scratch.0, now(), EngineConfig::default()).unwrap();
        let venue = SimVenue::new();

        let runtime = start(
            engine,
            venue,
            Duration::from_millis(20),
            Duration::from_secs(60),
        )
        .unwrap();

        // Entries are refused until the venue has said what the account holds.
        // The sweep does this within a tick of boot, but racing it would make
        // the test about timing rather than about the round trip, so the report
        // is delivered explicitly. The gate itself is tested in the engine.
        runtime.submit(Command::positions_reported(&[])).unwrap();

        let intent = EconomicOrderIntent::market(
            ClientOrderId::new("UI-1").unwrap(),
            Instrument::new("BTCUSD").unwrap(),
            Side::Buy,
            Qty::whole(3),
            Authority::Entry,
            TraceId::from_u128(1),
        )
        .unwrap();
        runtime.submit(Command::Place { intent }).unwrap();

        // The order reaches the venue, is acknowledged, and the answer lands
        // back in the log — all without the writer having touched a socket.
        let dir = scratch.0.clone();
        assert!(
            wait_for(|| {
                let Ok(mut tailer) =
                    sentinel_journal::Tailer::open(&dir, sentinel_journal::Seq::ZERO)
                else {
                    return false;
                };
                let Ok(book) = sentinel_ledger::Book::rebuild(&mut tailer) else {
                    return false;
                };
                book.order(&ClientOrderId::new("UI-1").unwrap())
                    .is_some_and(|o| o.core.state == sentinel_domain::OrderState::Working)
            }),
            "the order never reached WORKING"
        );

        runtime.stop();
    }

    #[test]
    fn stopping_is_observable() {
        let scratch = Scratch::new("stop");
        let engine = Engine::open(&scratch.0, now(), EngineConfig::default()).unwrap();
        let runtime = start(
            engine,
            SimVenue::new(),
            Duration::from_millis(20),
            Duration::from_secs(60),
        )
        .unwrap();
        assert!(runtime.is_running());
        runtime.stop();
        assert!(!runtime.is_running());
    }

    #[test]
    fn the_clock_only_moves_forward() {
        let first = now();
        let second = now();
        assert!(second >= first);
        assert!(first.as_u64() > 1_700_000_000_000_000_000, "a real epoch");
    }
}
