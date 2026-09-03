//! Binance USD-M futures.
//!
//! What the deployed system actually trades, and therefore what v2 has to speak
//! on day one.
//!
//! Blocking, because [`Venue`] is called from an I/O worker and never from the
//! writer. The user stream runs on its own thread and pushes into a channel;
//! [`BinanceVenue::drain_events`] moves whatever has arrived and never waits.
//!
//! The parts that can be wrong without a network — the signature, the symbol
//! filters, the response shapes, the stream frames — live in [`signing`],
//! [`symbol`], [`parse`] and [`stream`], and are tested there. What is left
//! here is the transport.
//!
//! Four rules this adapter exists to keep:
//!
//! **A timeout is not an answer.** A call that produces no response, or a 5xx,
//! returns [`SubmitOutcome::TimedOut`] — never an error and never a rejection,
//! because the venue may have taken the order anyway.
//!
//! **Absence has exactly one proof.** `-2013` on a *query*. `-2011` from a
//! cancel reads similarly and means something else entirely: the order may have
//! filled a millisecond ago. It routes to reconciliation, never to "it was not
//! there".
//!
//! **Quantities sit on a grid.** `LOT_SIZE.stepSize`, checked before sending,
//! because a rejection costs a round trip and an order to reconcile afterwards.
//!
//! **One-way position mode.** Set once at boot. In hedge mode a SELL opens a
//! separate short leg instead of reducing the long, and every position number
//! in this system assumes a single signed one.

#![forbid(unsafe_code)]

pub mod parse;
pub mod signing;
pub mod stream;
pub mod symbol;

use std::sync::Arc;
use std::sync::mpsc::{Receiver, TryRecvError};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use sentinel_domain::EconomicOrderIntent;
use sentinel_types::{ClientOrderId, Instrument, OrderKind, Side, VenueOrderId};
use sentinel_venue::{
    CancelOutcome, LookupOutcome, OrderSnapshot, SubmitOutcome, Venue, VenueError, VenueEvent,
    VenuePosition,
};

use crate::signing::{Query, ServerClock, format_decimal};
use crate::symbol::{SymbolRules, Symbols};

pub use stream::StreamHandle;

/// Production REST. This is the one that trades real money.
pub const PROD_REST: &str = "https://fapi.binance.com";
/// Production websocket.
pub const PROD_WS: &str = "wss://fstream.binance.com";
/// Demo Trading REST — the paper environment behind demo.binance.com.
///
/// Note this is *not* `testnet.binancefuture.com`, which is a separate and
/// older system with its own keys. Keys from one do not work on the other, and
/// the failure is a 401 that says nothing about which.
pub const DEMO_REST: &str = "https://demo-fapi.binance.com";
/// Demo Trading websocket.
pub const DEMO_WS: &str = "wss://demo-fstream.binance.com";

/// How long to wait for a response before calling it unanswered.
///
/// Short on purpose. A long timeout does not make an answer more likely; it
/// widens the window in which we do not know, and that window is the thing this
/// system is built to keep small.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(5);

/// Where and as whom to connect.
#[derive(Clone)]
pub struct Credentials {
    /// The API key. Sent as a header on every call.
    pub api_key: String,
    /// The secret. Only ever used to sign; never sent, never logged.
    pub secret: String,
    /// REST base URL.
    pub rest_base: String,
    /// Websocket base URL.
    pub ws_base: String,
}

impl Credentials {
    /// Demo Trading credentials.
    #[must_use]
    pub fn demo(api_key: impl Into<String>, secret: impl Into<String>) -> Self {
        Self {
            api_key: api_key.into(),
            secret: secret.into(),
            rest_base: DEMO_REST.to_owned(),
            ws_base: DEMO_WS.to_owned(),
        }
    }

    /// Production credentials. Real money.
    #[must_use]
    pub fn production(api_key: impl Into<String>, secret: impl Into<String>) -> Self {
        Self {
            api_key: api_key.into(),
            secret: secret.into(),
            rest_base: PROD_REST.to_owned(),
            ws_base: PROD_WS.to_owned(),
        }
    }
}

// Written rather than derived, both of them: a secret in a log line, a panic
// message or an error report is a secret that has left the machine.
impl core::fmt::Debug for Credentials {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "Credentials({})", self.rest_base)
    }
}

impl core::fmt::Display for Credentials {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "binance({})", self.rest_base)
    }
}

/// One open position, as the venue describes it to a person.
///
/// Includes the break-even price, which is the entry plus the fees already paid
/// to get in and out. It is the number that decides whether a position is
/// actually ahead, and it is not the entry price — quoting the entry is how a
/// position that is down by its own commission looks flat.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OpenPosition {
    /// The symbol.
    pub instrument: Instrument,
    /// Signed size. Negative is short.
    pub qty: sentinel_types::Qty,
    /// Average entry.
    pub entry_price: sentinel_types::Price,
    /// Entry plus round-trip fees — where this position actually breaks even.
    pub break_even_price: sentinel_types::Price,
    /// The current mark.
    pub mark_price: sentinel_types::Price,
    /// Where the venue would liquidate, when it says.
    pub liquidation_price: Option<sentinel_types::Price>,
    /// Position value at the mark.
    pub notional: sentinel_types::Money,
    /// The venue's own unrealised figure.
    pub unrealized: sentinel_types::Money,
}

/// Everything the transport needs, shared with the stream thread.
#[derive(Debug)]
struct Transport {
    credentials: Credentials,
    agent: ureq::Agent,
    clock: ServerClock,
    instruments: Vec<Instrument>,
}

/// The adapter.
#[derive(Debug)]
pub struct BinanceVenue {
    transport: Arc<Transport>,
    symbols: Symbols,
    events: Option<Receiver<VenueEvent>>,
    stream: Option<StreamHandle>,
}

impl BinanceVenue {
    /// Connect, sync the clock, load the symbols, and set one-way position mode.
    ///
    /// All four at boot, because each one is a thing that would otherwise fail
    /// at the first order — and the first order is the worst time to discover
    /// that the account is in hedge mode.
    ///
    /// # Errors
    /// [`VenueError`] when the venue cannot be reached or a symbol is not
    /// listed.
    pub fn connect(
        credentials: Credentials,
        instruments: &[Instrument],
        leverage: Option<u32>,
    ) -> Result<Self, VenueError> {
        let agent: ureq::Agent = ureq::Agent::config_builder()
            .timeout_global(Some(REQUEST_TIMEOUT))
            .build()
            .into();
        let transport = Arc::new(Transport {
            credentials,
            agent,
            clock: ServerClock::new(),
            instruments: instruments.to_vec(),
        });

        let mut venue = Self {
            transport,
            symbols: Symbols::new(),
            events: None,
            stream: None,
        };

        venue.sync_clock()?;
        for instrument in instruments {
            let rules = venue.load_symbol(instrument)?;
            venue.symbols.insert(rules);
        }
        venue.set_one_way_mode();
        if let Some(leverage) = leverage {
            venue.set_leverage(leverage);
        }
        Ok(venue)
    }

    /// Connect without changing anything on the account.
    ///
    /// Syncs the clock and loads the symbol filters, and does not set position
    /// mode or leverage. For looking: a tool that inspects an account must not
    /// be able to alter it, and the difference has to be structural rather than
    /// a flag somebody remembers to pass.
    ///
    /// # Errors
    /// [`VenueError`] when the venue cannot be reached or a symbol is not listed.
    pub fn connect_read_only(
        credentials: Credentials,
        instruments: &[Instrument],
    ) -> Result<Self, VenueError> {
        let agent: ureq::Agent = ureq::Agent::config_builder()
            .timeout_global(Some(REQUEST_TIMEOUT))
            .build()
            .into();
        let mut venue = Self {
            transport: Arc::new(Transport {
                credentials,
                agent,
                clock: ServerClock::new(),
                instruments: instruments.to_vec(),
            }),
            symbols: Symbols::new(),
            events: None,
            stream: None,
        };
        venue.sync_clock()?;
        for instrument in instruments {
            let rules = venue.load_symbol(instrument)?;
            venue.symbols.insert(rules);
        }
        Ok(venue)
    }

    /// Open orders, for an operator looking at an account.
    ///
    /// Not part of [`Venue`]: the engine never asks this. It reconciles one
    /// order at a time by the id it authorised, because "what is open" is a
    /// question whose answer includes orders this system did not place.
    ///
    /// # Errors
    /// [`VenueError`] when the venue cannot be reached.
    pub fn open_orders(&self) -> Result<Vec<parse::VenueOrder>, VenueError> {
        let Some(body) = self
            .transport
            .signed("GET", "/fapi/v1/openOrders", Query::new())?
        else {
            return Err(VenueError::Transport("no answer from openOrders".into()));
        };
        if let Some((code, message)) = parse::error_code(&body) {
            return Err(VenueError::Transport(format!("[{code}] {message}")));
        }
        body.as_array()
            .map_or(&[][..], Vec::as_slice)
            .iter()
            .map(parse::venue_order)
            .collect()
    }

    /// Open positions with the detail an operator wants: what it cost, what it
    /// is worth now, and the difference.
    ///
    /// Not part of [`Venue`], which carries only instrument and signed size —
    /// the engine reconciles against quantity and computes its own P&L from its
    /// own fills. This is the venue's opinion, for a person, and the two are
    /// worth being able to compare.
    ///
    /// # Errors
    /// [`VenueError`] when the venue cannot be reached.
    pub fn open_positions(&self) -> Result<Vec<OpenPosition>, VenueError> {
        let Some(body) = self
            .transport
            .signed("GET", "/fapi/v3/positionRisk", Query::new())?
        else {
            return Err(VenueError::Transport("no answer from positionRisk".into()));
        };
        if let Some((code, message)) = parse::error_code(&body) {
            return Err(VenueError::Transport(format!("[{code}] {message}")));
        }

        let null = serde_json::Value::Null;
        let mut out = Vec::new();
        for row in body.as_array().map_or(&[][..], Vec::as_slice) {
            let Some(qty) = parse::quantity(row.get("positionAmt").unwrap_or(&null)) else {
                continue;
            };
            if qty.is_zero() {
                continue;
            }
            let Some(instrument) = row
                .get("symbol")
                .and_then(serde_json::Value::as_str)
                .and_then(|s| Instrument::new(s).ok())
            else {
                continue;
            };
            out.push(OpenPosition {
                instrument,
                qty,
                entry_price: parse::derived_decimal(row.get("entryPrice").unwrap_or(&null))
                    .unwrap_or(sentinel_types::Price::ZERO),
                break_even_price: parse::derived_decimal(
                    row.get("breakEvenPrice").unwrap_or(&null),
                )
                .unwrap_or(sentinel_types::Price::ZERO),
                mark_price: parse::derived_decimal(row.get("markPrice").unwrap_or(&null))
                    .unwrap_or(sentinel_types::Price::ZERO),
                liquidation_price: parse::derived_decimal(
                    row.get("liquidationPrice").unwrap_or(&null),
                )
                .filter(|p| p.is_positive()),
                notional: parse::derived_amount(row.get("notional").unwrap_or(&null))
                    .unwrap_or(sentinel_types::Money::ZERO),
                unrealized: parse::derived_amount(row.get("unRealizedProfit").unwrap_or(&null))
                    .unwrap_or(sentinel_types::Money::ZERO),
            });
        }
        out.sort_unstable_by_key(|p| p.instrument);
        Ok(out)
    }

    /// Wallet balances, for an operator looking at an account.
    ///
    /// # Errors
    /// [`VenueError`] when the venue cannot be reached.
    pub fn balances(
        &self,
    ) -> Result<Vec<(sentinel_record::Asset, sentinel_types::Money)>, VenueError> {
        let Some(body) = self
            .transport
            .signed("GET", "/fapi/v2/balance", Query::new())?
        else {
            return Err(VenueError::Transport("no answer from balance".into()));
        };
        if let Some((code, message)) = parse::error_code(&body) {
            return Err(VenueError::Transport(format!("[{code}] {message}")));
        }
        let mut out = Vec::new();
        for row in body.as_array().map_or(&[][..], Vec::as_slice) {
            let (Some(asset), Some(amount)) = (
                row.get("asset")
                    .and_then(serde_json::Value::as_str)
                    .and_then(|s| sentinel_record::Asset::new(s).ok()),
                row.get("balance").and_then(parse::amount),
            ) else {
                continue;
            };
            if !amount.is_zero() {
                out.push((asset, amount));
            }
        }
        out.sort_unstable_by_key(|(asset, _)| *asset);
        Ok(out)
    }

    /// Start the user stream and the mark feed.
    ///
    /// # Errors
    /// [`VenueError`] when the thread cannot be started.
    pub fn start_stream(&mut self) -> Result<(), VenueError> {
        let (handle, events) = stream::spawn(Arc::clone(&self.transport))?;
        self.stream = Some(handle);
        self.events = Some(events);
        Ok(())
    }

    /// Whether the user stream is running.
    ///
    /// Worth surfacing: with the stream down, fills arrive only through the
    /// stale sweep, which is correct but slow — and an operator watching a
    /// position that is not updating should be able to tell which they see.
    #[must_use]
    pub fn stream_is_running(&self) -> bool {
        self.stream.as_ref().is_some_and(StreamHandle::is_running)
    }

    /// The symbols this adapter knows.
    #[must_use]
    pub const fn symbols(&self) -> &Symbols {
        &self.symbols
    }

    /// The venue's clock offset from ours, in milliseconds.
    #[must_use]
    pub fn clock_offset_ms(&self) -> i64 {
        self.transport.clock.offset_ms()
    }

    fn rules_for(&self, instrument: &Instrument) -> Result<SymbolRules, VenueError> {
        self.symbols
            .get(instrument)
            .ok_or_else(|| VenueError::Malformed(format!("{instrument} is not loaded")))
    }

    /// Ask the venue what time it thinks it is.
    ///
    /// A machine whose clock is a second slow gets `-1021` on every signed call
    /// and nothing else. This turns that from an outage into a correction.
    ///
    /// Asked several times, keeping the sample with the shortest round trip.
    /// One reading on a jittery link is mostly a measurement of the jitter, and
    /// the shortest trip is the one whose midpoint estimate is least wrong —
    /// again NTP's reasoning. The samples are cheap and this runs once at
    /// connect, so there is no reason to guess from one.
    fn sync_clock(&self) -> Result<(), VenueError> {
        /// Enough to discard a single slow reply without delaying a boot.
        const SAMPLES: usize = 5;

        let mut best: Option<(u64, u64, u64, u64)> = None; // trip, server, sent, received
        let mut last_error: Option<VenueError> = None;

        for _ in 0..SAMPLES {
            let sent = Transport::local_ms();
            let body = match self.transport.public("/fapi/v1/time", "") {
                Ok(Some(body)) => body,
                Ok(None) => {
                    last_error = Some(VenueError::Transport("no answer from /fapi/v1/time".into()));
                    continue;
                }
                Err(e) => {
                    last_error = Some(e);
                    continue;
                }
            };
            let received = Transport::local_ms();

            let Some(server) =
                parse::integer(body.get("serverTime").unwrap_or(&serde_json::Value::Null))
            else {
                last_error = Some(VenueError::Malformed("serverTime missing".into()));
                continue;
            };
            #[expect(
                clippy::cast_sign_loss,
                reason = "epoch milliseconds from the venue; a negative one would be \
                          1970 and the request would fail its own window check"
            )]
            let server_ms = server.max(0) as u64;

            let trip = received.saturating_sub(sent);
            if best.is_none_or(|(shortest, _, _, _)| trip < shortest) {
                best = Some((trip, server_ms, sent, received));
            }
        }

        match best {
            Some((_, server_ms, sent, received)) => {
                self.transport
                    .clock
                    .sync_round_trip(server_ms, sent, received);
                Ok(())
            }
            None => Err(last_error
                .unwrap_or_else(|| VenueError::Transport("no answer from /fapi/v1/time".into()))),
        }
    }

    fn load_symbol(&self, instrument: &Instrument) -> Result<SymbolRules, VenueError> {
        let query = format!("symbol={instrument}");
        let body = self
            .transport
            .public("/fapi/v1/exchangeInfo", &query)?
            .ok_or_else(|| VenueError::Transport(format!("no answer loading {instrument}")))?;
        let symbols = body
            .get("symbols")
            .and_then(serde_json::Value::as_array)
            .map_or(&[][..], Vec::as_slice);
        symbols
            .iter()
            .find(|row| {
                row.get("symbol").and_then(serde_json::Value::as_str) == Some(instrument.as_str())
            })
            .ok_or_else(|| VenueError::Malformed(format!("{instrument} is not listed")))
            .and_then(parse::symbol_rules)
    }

    /// One-way position mode, set once.
    ///
    /// Best effort, and deliberately so: the venue answers `-4059` when it is
    /// already set, which is success wearing an error's clothes. A genuine
    /// failure surfaces as a position that does not net, which the invariants
    /// catch — whereas refusing to boot over it would take the account down for
    /// a setting that is almost certainly already right.
    fn set_one_way_mode(&self) {
        let query = Query::new().push("dualSidePosition", "false");
        let _ = self
            .transport
            .signed("POST", "/fapi/v1/positionSide/dual", query);
    }

    /// Per-symbol leverage, set once.
    ///
    /// Best effort for the same reason. Getting it wrong shows up as a margin
    /// rejection on a real order, which is loud enough.
    fn set_leverage(&self, leverage: u32) {
        for instrument in &self.transport.instruments {
            let query = Query::new()
                .push("symbol", instrument.as_str())
                .push("leverage", &leverage.to_string());
            let _ = self.transport.signed("POST", "/fapi/v1/leverage", query);
        }
    }

    /// Look one order up on one symbol.
    ///
    /// Returns `Ok(None)` only for `-2013`, which is the single proof that an
    /// order does not exist.
    fn query_order(
        &self,
        symbol: &Instrument,
        client_order_id: ClientOrderId,
    ) -> Result<Option<parse::VenueOrder>, VenueError> {
        let query = Query::new()
            .push("symbol", symbol.as_str())
            .push("origClientOrderId", client_order_id.as_str());
        let Some(body) = self.transport.signed("GET", "/fapi/v1/order", query)? else {
            return Err(VenueError::Transport("no answer from order query".into()));
        };
        if let Some((code, message)) = parse::error_code(&body) {
            if code == parse::ORDER_DOES_NOT_EXIST {
                return Ok(None);
            }
            if parse::is_retryable(code) {
                if code == parse::TIMESTAMP_SKEW {
                    // Our clock, and fixable. Re-sync so the next call lands.
                    let _ = self.sync_clock();
                }
                return Err(VenueError::Transport(format!("[{code}] {message}")));
            }
            return Err(VenueError::Malformed(format!("[{code}] {message}")));
        }
        parse::venue_order(&body).map(Some)
    }
}

impl Transport {
    /// Wall-clock milliseconds. The only clock read in this crate, and it is
    /// here because the venue demands a timestamp — no value derived from it
    /// reaches the engine.
    fn local_ms() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
    }

    fn timestamp(&self) -> u64 {
        self.clock.venue_time_ms(Self::local_ms())
    }

    /// An unsigned call. Public data only.
    fn public(&self, path: &str, query: &str) -> Result<Option<serde_json::Value>, VenueError> {
        self.send("GET", path, query, false)
    }

    /// A signed call.
    ///
    /// The query string is built once and both signed and sent, because
    /// building it twice is how the two drift and every private call starts
    /// returning 401.
    fn signed(
        &self,
        method: &str,
        path: &str,
        query: Query,
    ) -> Result<Option<serde_json::Value>, VenueError> {
        let signed = query.sign(&self.credentials.secret, self.timestamp());
        self.send(method, path, &signed, true)
    }

    /// Returns `Ok(None)` when the call produced no answer at all.
    ///
    /// That is not an error: the venue may have acted on it, and the caller's
    /// next move depends on knowing the difference. A 4xx *is* an answer — the
    /// body carries the venue's code — so it comes back as a value.
    fn send(
        &self,
        method: &str,
        path: &str,
        query: &str,
        keyed: bool,
    ) -> Result<Option<serde_json::Value>, VenueError> {
        let url = if query.is_empty() {
            format!("{}{path}", self.credentials.rest_base)
        } else {
            format!("{}{path}?{query}", self.credentials.rest_base)
        };

        let mut builder = ureq::http::Request::builder()
            .method(method)
            .uri(&url)
            .header("User-Agent", "sentinel-oms");
        if keyed {
            builder = builder.header("X-MBX-APIKEY", &self.credentials.api_key);
        }
        let request = builder
            .body(())
            .map_err(|e| VenueError::Malformed(e.to_string()))?;

        let response = match self.agent.run(request) {
            Ok(response) => response,
            Err(ureq::Error::StatusCode(code)) => {
                if code == 429 || code == 418 {
                    return Err(VenueError::RateLimited { retry_after: None });
                }
                if code == 401 {
                    return Err(VenueError::Unauthorized);
                }
                if code >= 500 {
                    // The venue's own fault, and it may have acted anyway.
                    return Ok(None);
                }
                // A 4xx carries the venue's code in its body, and that code is
                // the whole answer. ureq does not hand back the body here, so
                // the call is repeated with status errors off.
                return self.read_error_body(method, &url, keyed);
            }
            // No answer. Not an error — the call may have landed.
            Err(_) => return Ok(None),
        };

        Self::read_json(response).map(Some)
    }

    /// Re-run a request that 4xx'd, with status handling off, to read the code.
    fn read_error_body(
        &self,
        method: &str,
        url: &str,
        keyed: bool,
    ) -> Result<Option<serde_json::Value>, VenueError> {
        let agent: ureq::Agent = ureq::Agent::config_builder()
            .timeout_global(Some(REQUEST_TIMEOUT))
            .http_status_as_error(false)
            .build()
            .into();
        let mut builder = ureq::http::Request::builder()
            .method(method)
            .uri(url)
            .header("User-Agent", "sentinel-oms");
        if keyed {
            builder = builder.header("X-MBX-APIKEY", &self.credentials.api_key);
        }
        let request = builder
            .body(())
            .map_err(|e| VenueError::Malformed(e.to_string()))?;
        match agent.run(request) {
            Ok(response) => Self::read_json(response).map(Some),
            Err(_) => Ok(None),
        }
    }

    fn read_json(
        response: ureq::http::Response<ureq::Body>,
    ) -> Result<serde_json::Value, VenueError> {
        let text = response
            .into_body()
            .read_to_string()
            .map_err(|e| VenueError::Transport(e.to_string()))?;
        serde_json::from_str(&text).map_err(|e| {
            VenueError::Malformed(format!(
                "{e}: {}",
                text.chars().take(200).collect::<String>()
            ))
        })
    }
}

impl stream::ListenKeys for Transport {
    fn open(&self) -> Result<String, VenueError> {
        // Key-only, not signed: the listen-key endpoints take the API key
        // header and no signature, and adding one is a 400.
        let body = self
            .send("POST", "/fapi/v1/listenKey", "", true)?
            .ok_or_else(|| VenueError::Transport("no answer opening a listen key".into()))?;
        body.get("listenKey")
            .and_then(serde_json::Value::as_str)
            .map(ToOwned::to_owned)
            .ok_or_else(|| VenueError::Malformed("no listenKey in the response".into()))
    }

    fn keepalive(&self) -> Result<(), VenueError> {
        self.send("PUT", "/fapi/v1/listenKey", "", true)?
            .ok_or_else(|| VenueError::Transport("no answer keeping the key alive".into()))?;
        Ok(())
    }

    fn ws_base(&self) -> String {
        self.credentials.ws_base.clone()
    }

    fn instruments(&self) -> Vec<Instrument> {
        self.instruments.clone()
    }
}

impl Venue for BinanceVenue {
    fn submit(&mut self, intent: &EconomicOrderIntent) -> Result<SubmitOutcome, VenueError> {
        let rules = self.rules_for(&intent.instrument)?;
        // Checked before sending. A rejection costs a round trip, a log line
        // and an order that has to be reconciled — and every one of these
        // conditions is knowable here.
        let reference = intent
            .limit_price
            .or(intent.stop_price)
            .or(intent.quote_at_decision)
            .unwrap_or(sentinel_types::Price::ZERO);
        rules
            .check(intent.qty, reference)
            .map_err(|e| VenueError::Malformed(e.to_string()))?;

        let mut query = Query::new()
            .push("symbol", intent.instrument.as_str())
            .push(
                "side",
                if intent.side == Side::Buy {
                    "BUY"
                } else {
                    "SELL"
                },
            )
            .push(
                "quantity",
                &format_decimal(sentinel_types::Price::from_raw(intent.qty.raw())),
            )
            .push("newClientOrderId", intent.client_order_id.as_str());

        query = match intent.kind {
            OrderKind::StopMarket => {
                let trigger = intent
                    .stop_price
                    .ok_or_else(|| VenueError::Malformed("stop order has no trigger".into()))?;
                // Reduce-only, so a stale trigger can only ever shrink the
                // position. A resting stop that could *open* one is a machine
                // trading while nobody is watching.
                query
                    .push("type", "STOP_MARKET")
                    .push(
                        "stopPrice",
                        &format_decimal(rules.round_price_down(trigger)),
                    )
                    .push("reduceOnly", "true")
            }
            OrderKind::Limit => {
                let limit = intent
                    .limit_price
                    .ok_or_else(|| VenueError::Malformed("limit order has no price".into()))?;
                query
                    .push("type", "LIMIT")
                    .push("timeInForce", "GTC")
                    .push("price", &format_decimal(rules.round_price_down(limit)))
            }
            OrderKind::Market => query.push("type", "MARKET"),
        };

        // No answer means no answer. The venue may have taken it, so this is
        // never an error and never a rejection (R1.3).
        let Some(body) = self.transport.signed("POST", "/fapi/v1/order", query)? else {
            return Ok(SubmitOutcome::TimedOut);
        };
        if let Some((code, message)) = parse::error_code(&body) {
            if parse::is_retryable(code) {
                if code == parse::TIMESTAMP_SKEW {
                    let _ = self.sync_clock();
                }
                return Err(VenueError::Transport(format!("[{code}] {message}")));
            }
            return Ok(SubmitOutcome::Rejected(parse::reject_reason(&body)));
        }
        let order_id = parse::integer(body.get("orderId").unwrap_or(&serde_json::Value::Null))
            .map(|id| id.to_string())
            .unwrap_or_default();
        Ok(SubmitOutcome::Acked(
            VenueOrderId::new(&order_id).unwrap_or_default(),
        ))
    }

    fn cancel(
        &mut self,
        client_order_id: ClientOrderId,
        _venue_order_id: Option<VenueOrderId>,
    ) -> Result<CancelOutcome, VenueError> {
        // Cancelling needs the symbol, and the only thing that survives a
        // restart is the client order id — so the symbol is found by asking,
        // across the symbols this adapter trades.
        let Some((symbol, _)) = self.find_order(client_order_id)? else {
            return Ok(CancelOutcome::Absent);
        };

        let query = Query::new()
            .push("symbol", symbol.as_str())
            .push("origClientOrderId", client_order_id.as_str());
        let Some(body) = self.transport.signed("DELETE", "/fapi/v1/order", query)? else {
            return Ok(CancelOutcome::TimedOut);
        };
        match parse::error_code(&body) {
            None => Ok(CancelOutcome::Requested),
            // "Unknown order sent". It may have filled a millisecond ago, been
            // cancelled, or never existed — three outcomes with different
            // consequences, and this code distinguishes none of them. So it
            // means "go and ask", never "it was not there".
            Some((parse::UNKNOWN_ORDER, _)) => Ok(CancelOutcome::TimedOut),
            Some((code, message)) if parse::is_retryable(code) => {
                Err(VenueError::Transport(format!("[{code}] {message}")))
            }
            Some((code, message)) => Err(VenueError::Malformed(format!(
                "cancel refused [{code}] {message}"
            ))),
        }
    }

    fn lookup(&mut self, client_order_id: ClientOrderId) -> Result<LookupOutcome, VenueError> {
        match self.find_order(client_order_id) {
            Ok(Some((_, order))) => Ok(LookupOutcome::Found(OrderSnapshot {
                state: order.state,
                venue_order_id: VenueOrderId::new(&order.order_id.to_string()).ok(),
                filled_qty: order.filled_qty,
                avg_price: order.avg_price,
            })),
            Ok(None) => Ok(LookupOutcome::Absent),
            // A lookup that could not be made concludes nothing. Reporting
            // absence here would tell the engine the order never existed, and
            // it would be free to place another.
            Err(e) if e.is_retryable() => Ok(LookupOutcome::TimedOut),
            Err(e) => Err(e),
        }
    }

    fn positions(&mut self) -> Result<Vec<VenuePosition>, VenueError> {
        // v3, not v2. v2 returns every symbol the venue lists — 735 rows on a
        // demo account, almost all of them flat — while v3 returns only what is
        // actually held. Cheaper, and it means a parse failure here is about a
        // real position rather than about the 700th zero.
        let Some(body) = self
            .transport
            .signed("GET", "/fapi/v3/positionRisk", Query::new())?
        else {
            return Err(VenueError::Transport("no answer from positionRisk".into()));
        };
        if let Some((code, message)) = parse::error_code(&body) {
            return Err(VenueError::Transport(format!("[{code}] {message}")));
        }
        let rows = body.as_array().map_or(&[][..], Vec::as_slice);

        let mut out = Vec::new();
        for row in rows {
            // Flat first, before anything is parsed. A zero row carries no
            // information and some of them name symbols this system has no
            // type wide enough to hold.
            if parse::quantity(row.get("positionAmt").unwrap_or(&serde_json::Value::Null))
                .is_some_and(|qty| qty.is_zero())
            {
                continue;
            }
            let parsed = parse::venue_position(row)?;
            if parsed.qty.is_zero() {
                continue;
            }
            // Only what this adapter was opened for. A position in something we
            // do not trade is not ours to book — it is somebody else's, on a
            // shared account, and reconciling against it would halt us over a
            // number we have no fills for.
            if self.symbols.get(&parsed.symbol).is_none() {
                continue;
            }
            out.push(VenuePosition {
                instrument: parsed.symbol,
                qty: parsed.qty,
                entry_price: Some(parsed.entry_price).filter(|p| p.is_positive()),
            });
        }
        // Sorted: the venue's ordering is its own business, ours must not vary
        // between runs or a replay stops reproducing.
        out.sort_unstable_by_key(|p| p.instrument);
        Ok(out)
    }

    fn drain_events(&mut self, out: &mut Vec<VenueEvent>) {
        let Some(events) = self.events.as_ref() else {
            return;
        };
        loop {
            match events.try_recv() {
                Ok(event) => out.push(event),
                Err(TryRecvError::Empty | TryRecvError::Disconnected) => return,
            }
        }
    }
}

impl BinanceVenue {
    /// Find an order across the symbols this adapter trades.
    ///
    /// Binance's order endpoints all need a symbol, and the only identifier
    /// that survives a restart is the client order id. Asking each symbol in
    /// turn is a handful of calls on an account trading one instrument, and it
    /// is the alternative to keeping a map that a crash would lose.
    ///
    /// `Ok(None)` means every symbol answered `-2013`, which is the one proof
    /// of absence.
    fn find_order(
        &self,
        client_order_id: ClientOrderId,
    ) -> Result<Option<(Instrument, parse::VenueOrder)>, VenueError> {
        let mut last_error = None;
        for symbol in self.symbols.all() {
            match self.query_order(&symbol, client_order_id) {
                Ok(Some(order)) => return Ok(Some((symbol, order))),
                Ok(None) => {}
                Err(e) => last_error = Some(e),
            }
        }
        // A symbol that could not be asked is not a symbol that said no. If any
        // lookup failed, absence has not been proven.
        match last_error {
            Some(e) => Err(e),
            None => Ok(None),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn demo_and_production_are_different_places() {
        let demo = Credentials::demo("k", "s");
        let prod = Credentials::production("k", "s");
        assert_ne!(demo.rest_base, prod.rest_base);
        assert_ne!(demo.ws_base, prod.ws_base);
        assert!(demo.rest_base.contains("demo"));
        assert_eq!(prod.rest_base, "https://fapi.binance.com");
    }

    #[test]
    fn demo_is_not_the_old_testnet() {
        // Two separate systems with separate keys. Confusing them produces a
        // 401 that says nothing about which one you are talking to.
        assert!(
            !Credentials::demo("k", "s")
                .rest_base
                .contains("binancefuture")
        );
    }

    #[test]
    fn credentials_never_print_the_secret() {
        let credentials = Credentials::production("my-key", "my-very-secret-value");
        for shown in [format!("{credentials}"), format!("{credentials:?}")] {
            assert!(!shown.contains("my-very-secret-value"), "{shown}");
            assert!(!shown.contains("my-key"), "{shown}");
        }
    }

    #[test]
    fn the_request_timeout_is_short_on_purpose() {
        assert!(REQUEST_TIMEOUT <= Duration::from_secs(10));
    }
}
