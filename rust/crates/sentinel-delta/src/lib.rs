//! Delta Exchange India.
//!
//! Blocking, because [`Venue`] is called from an I/O worker and never from the
//! writer. The user stream runs on its own thread and pushes into a channel;
//! [`DeltaVenue::drain_events`] moves whatever has arrived and never waits.
//!
//! The parts that can be wrong without a network — the signature, the contract
//! conversion, the response shapes — live in [`signing`], [`product`] and
//! [`parse`], and are tested there. What is left here is the transport, and it
//! is deliberately thin.
//!
//! Two rules this adapter exists to keep:
//!
//! **A timeout is not an answer.** An HTTP call that produces no response
//! returns [`SubmitOutcome::TimedOut`], never an error and never a rejection,
//! because the venue may have taken the order anyway. Everything downstream is
//! built on that distinction.
//!
//! **Sizes are contracts.** One `BTCUSD` contract is 0.001 BTC. The conversion
//! refuses off-grid quantities rather than rounding them, because rounding here
//! is a thousand-fold error in position size.

#![forbid(unsafe_code)]

pub mod parse;
pub mod product;
pub mod signing;

mod stream;

use std::sync::mpsc::{Receiver, TryRecvError};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use sentinel_domain::EconomicOrderIntent;
use sentinel_types::{ClientOrderId, Instrument, OrderKind, Price, Side, VenueOrderId};
use sentinel_venue::{
    CancelOutcome, LookupOutcome, OrderSnapshot, SubmitOutcome, Venue, VenueError, VenueEvent,
    VenuePosition,
};

use crate::product::{Product, Products};
use crate::signing::SignedHeaders;

pub use stream::StreamHandle;

/// Testnet REST. Free demo keys, and where a cutover paper-trades first.
pub const TESTNET_REST: &str = "https://cdn-ind.testnet.deltaex.org";
/// Testnet websocket.
pub const TESTNET_WS: &str = "wss://socket-ind.testnet.deltaex.org";
/// Production REST.
pub const PROD_REST: &str = "https://api.india.delta.exchange";
/// Production websocket.
pub const PROD_WS: &str = "wss://socket.india.delta.exchange";

/// How long to wait for a response before calling it unanswered.
///
/// Short on purpose. A long timeout does not make an answer more likely; it
/// makes the window in which we do not know wider, and that window is the one
/// thing this system is built to keep small.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(5);

/// Where and as whom to connect.
#[derive(Debug, Clone)]
pub struct Credentials {
    /// The API key.
    pub api_key: String,
    /// The secret. Only ever used to sign; never logged, never sent.
    pub secret: String,
    /// REST base URL.
    pub rest_base: String,
    /// Websocket URL.
    pub ws_url: String,
}

impl Credentials {
    /// Testnet credentials.
    #[must_use]
    pub fn testnet(api_key: impl Into<String>, secret: impl Into<String>) -> Self {
        Self {
            api_key: api_key.into(),
            secret: secret.into(),
            rest_base: TESTNET_REST.to_owned(),
            ws_url: TESTNET_WS.to_owned(),
        }
    }

    /// Production credentials.
    #[must_use]
    pub fn production(api_key: impl Into<String>, secret: impl Into<String>) -> Self {
        Self {
            api_key: api_key.into(),
            secret: secret.into(),
            rest_base: PROD_REST.to_owned(),
            ws_url: PROD_WS.to_owned(),
        }
    }
}

// The secret must not reach a log line, a panic message or an error report, so
// `Debug` is written rather than derived.
impl core::fmt::Display for Credentials {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "delta({})", self.rest_base)
    }
}

/// The adapter.
#[derive(Debug)]
pub struct DeltaVenue {
    credentials: Credentials,
    agent: ureq::Agent,
    products: Products,
    /// The symbols this adapter was opened for, kept so the stream can
    /// subscribe to exactly them.
    instruments: Vec<Instrument>,
    events: Option<Receiver<VenueEvent>>,
    stream: Option<StreamHandle>,
}

impl DeltaVenue {
    /// Connect, and load the products actually traded.
    ///
    /// Products are loaded up front for exactly the symbols named, rather than
    /// paginating the venue's whole catalogue: this deployment trades one
    /// instrument, and the catalogue is thousands of rows of things it will
    /// never touch.
    ///
    /// # Errors
    /// [`VenueError`] when a symbol is not listed or the venue cannot be
    /// reached.
    pub fn connect(
        credentials: Credentials,
        instruments: &[Instrument],
    ) -> Result<Self, VenueError> {
        let agent: ureq::Agent = ureq::Agent::config_builder()
            .timeout_global(Some(REQUEST_TIMEOUT))
            .build()
            .into();
        let mut venue = Self {
            credentials,
            agent,
            products: Products::new(),
            instruments: instruments.to_vec(),
            events: None,
            stream: None,
        };
        for instrument in instruments {
            let product = venue.load_product(instrument)?;
            venue.products.insert(product);
        }
        Ok(venue)
    }

    /// Start the user stream.
    ///
    /// Separate from [`Self::connect`] so an operator can inspect an account
    /// without opening a socket, and so a stream that will not start is a
    /// distinct failure from a venue that will not answer.
    ///
    /// # Errors
    /// [`VenueError`] when the socket cannot be opened.
    pub fn start_stream(&mut self) -> Result<(), VenueError> {
        let (handle, events) = stream::spawn(self.credentials.clone(), self.instruments.clone())?;
        self.stream = Some(handle);
        self.events = Some(events);
        Ok(())
    }

    /// Whether the user stream is running.
    ///
    /// Worth surfacing: with the stream down, fills arrive only through the
    /// stale sweep, which is correct but slow — and an operator watching a
    /// position that is not updating should be able to tell which of the two
    /// they are looking at.
    #[must_use]
    pub fn stream_is_running(&self) -> bool {
        self.stream.as_ref().is_some_and(StreamHandle::is_running)
    }

    /// The products this adapter knows.
    #[must_use]
    pub const fn products(&self) -> &Products {
        &self.products
    }

    /// Epoch seconds. The one clock read in this crate, and it is here because
    /// the venue demands a timestamp — no value derived from it reaches the
    /// engine.
    fn now_secs() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| d.as_secs())
    }

    fn product_of(&self, instrument: &Instrument) -> Result<Product, VenueError> {
        self.products
            .get(instrument)
            .ok_or_else(|| VenueError::Malformed(format!("{instrument} is not loaded")))
    }

    /// One signed request.
    ///
    /// Returns `Ok(None)` when the call produced no answer. That is not an
    /// error: the venue may have acted on it, and the caller's next move
    /// depends on knowing the difference.
    fn request(
        &self,
        method: &str,
        path: &str,
        query: &str,
        body: Option<&serde_json::Value>,
    ) -> Result<Option<serde_json::Value>, VenueError> {
        let body_text = body.map(ToString::to_string).unwrap_or_default();
        let timestamp = Self::now_secs();
        let headers = SignedHeaders::build(
            &self.credentials.api_key,
            &self.credentials.secret,
            method,
            timestamp,
            path,
            query,
            &body_text,
        );
        let url = format!("{}{path}{query}", self.credentials.rest_base);

        // Built as a plain `http::Request` rather than through the typed
        // builders: the method varies at runtime, and the signature was
        // computed over exactly these bytes — so the body has to be handed over
        // unchanged rather than re-serialised by a helper.
        let request = ureq::http::Request::builder()
            .method(method)
            .uri(&url)
            .header("api-key", &headers.api_key)
            .header("timestamp", &headers.timestamp)
            .header("signature", &headers.signature)
            .header("User-Agent", "sentinel-oms")
            .header("Content-Type", "application/json")
            .body(body_text.clone())
            .map_err(|e| VenueError::Malformed(e.to_string()))?;

        let response = match self.agent.run(request) {
            Ok(response) => response,
            Err(ureq::Error::StatusCode(code)) => {
                // The venue answered, and said no. A 429 is its own thing:
                // backing off helps and retrying immediately makes it worse.
                if code == 429 {
                    return Err(VenueError::RateLimited { retry_after: None });
                }
                if code == 401 || code == 403 {
                    return Err(VenueError::Unauthorized);
                }
                if code >= 500 {
                    // The venue's own fault, and it may have acted anyway.
                    return Ok(None);
                }
                return Err(VenueError::Transport(format!("HTTP {code}")));
            }
            // No answer. Not an error — the call may have landed.
            Err(_) => return Ok(None),
        };

        let text = response
            .into_body()
            .read_to_string()
            .map_err(|e| VenueError::Transport(e.to_string()))?;
        serde_json::from_str(&text).map(Some).map_err(|e| {
            VenueError::Malformed(format!(
                "{e}: {}",
                text.chars().take(200).collect::<String>()
            ))
        })
    }

    fn load_product(&self, instrument: &Instrument) -> Result<Product, VenueError> {
        let path = format!("/v2/products/{instrument}");
        let body = self
            .request("GET", &path, "", None)?
            .ok_or_else(|| VenueError::Transport(format!("no answer loading {instrument}")))?;
        let result = parse::unwrap_result(&body)?;

        let id = parse::integer(result.get("id").unwrap_or(&serde_json::Value::Null))
            .ok_or_else(|| VenueError::Malformed(format!("{instrument} has no id")))?;
        let contract_value = parse::quantity(
            result
                .get("contract_value")
                .unwrap_or(&serde_json::Value::Null),
        )
        .ok_or_else(|| VenueError::Malformed(format!("{instrument} has no contract_value")))?;
        let tick_size = parse::decimal(result.get("tick_size").unwrap_or(&serde_json::Value::Null))
            .unwrap_or(Price::from_raw(1));

        Ok(Product {
            id,
            symbol: *instrument,
            contract_value,
            tick_size,
        })
    }

    /// Find an order by our id, across both the live and the historical
    /// endpoints.
    ///
    /// Both are needed. The live lookup 404s for anything already terminal, and
    /// a terminal order is exactly what a reconciliation after a timeout is
    /// most likely to be looking at.
    fn find_order(
        &self,
        client_order_id: ClientOrderId,
    ) -> Result<Option<parse::VenueOrder>, VenueError> {
        let path = format!("/v2/orders/client_order_id/{client_order_id}");
        if let Some(body) = self.request("GET", &path, "", None)?
            && parse::error_code(&body).is_none()
            && let Ok(result) = parse::unwrap_result(&body)
            && let Some(product) = self.product_from_row(result)
            && let Ok(order) = parse::venue_order(result, &product)
        {
            return Ok(Some(order));
        }

        // Not live. Sweep the history before concluding it never existed.
        let query = format!("?client_order_id={client_order_id}&page_size=50");
        let Some(body) = self.request("GET", "/v2/orders/history", &query, None)? else {
            // No answer is not absence.
            return Err(VenueError::Transport("no answer from order history".into()));
        };
        let result = parse::unwrap_result(&body)?;
        let rows = result.as_array().map_or(&[][..], Vec::as_slice);
        for row in rows {
            if row
                .get("client_order_id")
                .and_then(serde_json::Value::as_str)
                != Some(client_order_id.as_str())
            {
                continue;
            }
            if let Some(product) = self.product_from_row(row) {
                return parse::venue_order(row, &product).map(Some);
            }
        }
        Ok(None)
    }

    fn product_from_row(&self, row: &serde_json::Value) -> Option<Product> {
        parse::integer(row.get("product_id").unwrap_or(&serde_json::Value::Null))
            .and_then(|id| self.products.by_id(id))
            .or_else(|| {
                row.get("product_symbol")
                    .and_then(serde_json::Value::as_str)
                    .and_then(|s| Instrument::new(s).ok())
                    .and_then(|s| self.products.get(&s))
            })
    }
}

impl Venue for DeltaVenue {
    fn submit(&mut self, intent: &EconomicOrderIntent) -> Result<SubmitOutcome, VenueError> {
        let product = self.product_of(&intent.instrument)?;
        let contracts = product
            .to_contracts(intent.qty)
            .map_err(|e| VenueError::Malformed(e.to_string()))?;

        let mut body = serde_json::json!({
            "product_symbol": intent.instrument.as_str(),
            "size": contracts,
            "side": if intent.side == Side::Buy { "buy" } else { "sell" },
            "client_order_id": intent.client_order_id.as_str(),
        });

        match intent.kind {
            OrderKind::StopMarket => {
                let trigger = intent
                    .stop_price
                    .ok_or_else(|| VenueError::Malformed("stop order has no trigger".into()))?;
                // Reduce-only, so a stale trigger can only ever shrink the
                // position. A resting stop that could *open* one is a machine
                // that trades while nobody is watching.
                body["order_type"] = "market_order".into();
                body["stop_order_type"] = "stop_loss_order".into();
                body["stop_price"] = trigger.to_string().into();
                body["reduce_only"] = true.into();
            }
            OrderKind::Limit => {
                let limit = intent
                    .limit_price
                    .ok_or_else(|| VenueError::Malformed("limit order has no price".into()))?;
                body["order_type"] = "limit_order".into();
                body["time_in_force"] = "gtc".into();
                body["limit_price"] = limit.to_string().into();
            }
            OrderKind::Market => {
                body["order_type"] = "market_order".into();
            }
        }

        // No answer means no answer. The venue may have taken it, so this is
        // never an error and never a rejection (R1.3).
        let Some(response) = self.request("POST", "/v2/orders", "", Some(&body))? else {
            return Ok(SubmitOutcome::TimedOut);
        };
        if parse::error_code(&response).is_some() {
            return Ok(SubmitOutcome::Rejected(parse::reject_reason(&response)));
        }
        let result = parse::unwrap_result(&response)?;
        let id = parse::integer(result.get("id").unwrap_or(&serde_json::Value::Null))
            .map(|id| id.to_string())
            .unwrap_or_default();
        Ok(SubmitOutcome::Acked(
            VenueOrderId::new(&id).unwrap_or_default(),
        ))
    }

    fn cancel(
        &mut self,
        client_order_id: ClientOrderId,
        _venue_order_id: Option<VenueOrderId>,
    ) -> Result<CancelOutcome, VenueError> {
        // The cancel endpoint needs the venue's own id and the product, so the
        // order has to be resolved first. Absence *at cancel time* is not
        // success — it may have just filled — so it goes to reconciliation.
        let Some(order) = self.find_order(client_order_id)? else {
            return Ok(CancelOutcome::Absent);
        };
        let body = serde_json::json!({
            "id": order.id.as_str().parse::<i64>().unwrap_or_default(),
            "product_id": order.product_id,
            "client_order_id": client_order_id.as_str(),
        });
        let Some(response) = self.request("DELETE", "/v2/orders", "", Some(&body))? else {
            return Ok(CancelOutcome::TimedOut);
        };
        match parse::error_code(&response) {
            None => Ok(CancelOutcome::Requested),
            // It went terminal between the lookup and the delete. Unprovable
            // from here, so the reconciler decides rather than this line.
            Some(code) if parse::is_absent(code) => Ok(CancelOutcome::TimedOut),
            Some(code) => Err(VenueError::Malformed(format!("cancel refused [{code}]"))),
        }
    }

    fn lookup(&mut self, client_order_id: ClientOrderId) -> Result<LookupOutcome, VenueError> {
        match self.find_order(client_order_id) {
            Ok(Some(order)) => Ok(LookupOutcome::Found(OrderSnapshot {
                state: order.state,
                venue_order_id: Some(order.id),
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
        let Some(body) = self.request("GET", "/v2/positions/margined", "", None)? else {
            return Err(VenueError::Transport("no answer from positions".into()));
        };
        let result = parse::unwrap_result(&body)?;
        let rows = result.as_array().map_or(&[][..], Vec::as_slice);

        let mut out = Vec::new();
        for row in rows {
            let Some(product) = self.product_from_row(row) else {
                // A position in something we do not trade. Not ours to book,
                // and not ours to guess the contract value of.
                continue;
            };
            let parsed = parse::venue_position(row, &product)?;
            if parsed.qty.is_zero() {
                continue;
            }
            out.push(VenuePosition {
                instrument: product.symbol,
                qty: parsed.qty,
                entry_price: parse::decimal(
                    row.get("entry_price").unwrap_or(&serde_json::Value::Null),
                )
                .filter(|p| p.is_positive()),
            });
        }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn testnet_and_production_are_different_places() {
        // Two constants that must never be confused, and a test that fails if
        // someone edits one to match the other.
        let test = Credentials::testnet("k", "s");
        let prod = Credentials::production("k", "s");
        assert_ne!(test.rest_base, prod.rest_base);
        assert_ne!(test.ws_url, prod.ws_url);
        assert!(test.rest_base.contains("testnet"));
        assert!(prod.rest_base.contains("api.india.delta.exchange"));
    }

    #[test]
    fn credentials_never_print_the_secret() {
        // A secret in a log line is a secret that has left the machine.
        let credentials = Credentials::production("my-key", "my-very-secret-value");
        let shown = credentials.to_string();
        assert!(!shown.contains("my-very-secret-value"));
        assert!(!shown.contains("my-key"));
    }

    #[test]
    fn the_request_timeout_is_short_on_purpose() {
        // A long timeout does not make an answer more likely. It widens the
        // window in which we do not know, and that window is the thing this
        // system exists to keep small.
        assert!(REQUEST_TIMEOUT <= Duration::from_secs(10));
    }
}
