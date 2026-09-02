//! Turning Delta's JSON into our types.
//!
//! Kept apart from the transport so it can be tested against recorded payloads
//! rather than against a live venue. Every function here is pure, and every
//! shape below was taken from a response the Python adapter actually handled in
//! production — including the ones that are only reachable when something has
//! gone wrong.
//!
//! Numbers arrive as strings and are parsed exactly. A venue field read through
//! `f64` is a rounding error with a settlement date.

use sentinel_domain::{OrderState, RejectReason};
use sentinel_types::{ClientOrderId, ExecId, Money, Price, Qty, Side, VenueOrderId};
use sentinel_venue::VenueError;

use crate::product::Product;

/// What Delta says an order's state is.
///
/// The venue's vocabulary is smaller than ours, and deliberately not mapped
/// one-to-one: `closed` means "no longer live" and says nothing about whether
/// it filled, so the fill quantity decides. Guessing from the word alone is how
/// a cancelled order gets booked as a fill.
#[must_use]
pub fn order_state(venue_state: &str, size: Qty, unfilled: Qty) -> OrderState {
    let filled = size.checked_sub(unfilled).unwrap_or(Qty::ZERO);
    match venue_state {
        "open" | "pending" => {
            if filled.is_positive() {
                OrderState::Partial
            } else {
                OrderState::Working
            }
        }
        "closed" => {
            if filled >= size && size.is_positive() {
                OrderState::Filled
            } else {
                // Closed without filling everything: the remainder is gone.
                OrderState::Canceled
            }
        }
        "cancelled" => OrderState::Canceled,
        // An unknown word is not a state to guess at. Reconciliation asks
        // again rather than booking something on a vocabulary change.
        _ => OrderState::Reconciling,
    }
}

/// Read an exact decimal from a JSON value that may be a string or a number.
///
/// Delta sends most numbers as strings and a few as JSON numbers. A number is
/// re-rendered through its own text rather than through `f64`, so nothing here
/// ever holds a binary float.
#[must_use]
pub fn decimal(value: &serde_json::Value) -> Option<Price> {
    match value {
        serde_json::Value::String(s) => Price::parse(s).ok(),
        serde_json::Value::Number(n) => Price::parse(&n.to_string()).ok(),
        _ => None,
    }
}

/// Read a decimal the venue *derived*, truncating precision we do not model.
///
/// [`decimal`] refuses more than eight decimal places, which is right for
/// anything we send back and wrong for a weighted average the venue computed.
/// See the Binance adapter's note: refusing those made a real entry price
/// display as zero.
#[must_use]
pub fn derived_decimal(value: &serde_json::Value) -> Option<Price> {
    let text = match value {
        serde_json::Value::String(s) => s.clone(),
        serde_json::Value::Number(n) => n.to_string(),
        _ => return None,
    };
    if let Ok(exact) = Price::parse(&text) {
        return Some(exact);
    }
    let (whole, fraction) = text.split_once('.')?;
    let kept = &fraction[..fraction.len().min(sentinel_types::SCALE_DIGITS as usize)];
    Price::parse(&format!("{whole}.{kept}")).ok()
}

/// The same, as a quantity.
#[must_use]
pub fn quantity(value: &serde_json::Value) -> Option<Qty> {
    decimal(value).map(|p| Qty::from_raw(p.raw()))
}

/// The same, as money.
#[must_use]
pub fn amount(value: &serde_json::Value) -> Option<Money> {
    decimal(value).map(|p| Money::from_raw(p.raw()))
}

/// An integer field that may arrive as a string.
#[must_use]
pub fn integer(value: &serde_json::Value) -> Option<i64> {
    match value {
        serde_json::Value::Number(n) => n.as_i64(),
        serde_json::Value::String(s) => s.parse().ok(),
        _ => None,
    }
}

/// Delta's envelope: `{"success": true, "result": ...}`.
///
/// # Errors
/// [`VenueError::Malformed`] when the envelope is not one, carrying the venue's
/// own error code when it gave one — the codes are what an operator searches
/// for, and losing them to a generic message costs an hour every time.
pub fn unwrap_result(body: &serde_json::Value) -> Result<&serde_json::Value, VenueError> {
    if body.get("success").and_then(serde_json::Value::as_bool) == Some(false) {
        let code = error_code(body).unwrap_or("unknown");
        return Err(VenueError::Malformed(format!("venue error [{code}]")));
    }
    body.get("result")
        .ok_or_else(|| VenueError::Malformed("response has no result".into()))
}

/// Delta's error code, when the response carries one.
#[must_use]
pub fn error_code(body: &serde_json::Value) -> Option<&str> {
    body.get("error")
        .and_then(|e| e.get("code"))
        .and_then(serde_json::Value::as_str)
        .or_else(|| body.get("error").and_then(serde_json::Value::as_str))
}

/// Whether a code means "the venue has no such order".
///
/// Distinguished from every other refusal because absence is *conclusive* — it
/// means the intent never became exposure — while anything else leaves the
/// question open. The two lead to opposite decisions.
#[must_use]
pub fn is_absent(code: &str) -> bool {
    matches!(
        code,
        "open_order_not_found"
            | "order_not_found"
            | "invalid_order_id"
            | "immediate_execution_limit_price"
    )
}

/// A refusal, with the venue's code preserved.
#[must_use]
pub fn reject_reason(body: &serde_json::Value) -> RejectReason {
    let code = error_code(body).unwrap_or("unknown");
    // Delta's codes are words, not numbers. The numeric slot stays zero rather
    // than carrying a made-up integer: a fabricated code that looks like the
    // venue's own is worse than an obviously absent one, because somebody will
    // eventually search for it.
    RejectReason::new(0, code)
}

/// One order, as the venue describes it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VenueOrder {
    /// The venue's id.
    pub id: VenueOrderId,
    /// Ours.
    pub client_order_id: ClientOrderId,
    /// The product it is on.
    pub product_id: i64,
    /// State.
    pub state: OrderState,
    /// What has filled, in base quantity.
    pub filled_qty: Qty,
    /// What was ordered, in base quantity.
    pub qty: Qty,
    /// The average price of the fills, when anything has filled.
    ///
    /// So a reconciliation that finds executions we missed books them at what
    /// they actually cost, rather than at whatever the last mark happened to be.
    pub avg_price: Option<Price>,
}

/// Read one order row.
///
/// # Errors
/// [`VenueError::Malformed`] when a field this system depends on is missing.
/// Nothing is defaulted: an order row without a size is not an order of zero.
pub fn venue_order(row: &serde_json::Value, product: &Product) -> Result<VenueOrder, VenueError> {
    let missing = |field: &str| VenueError::Malformed(format!("order row has no {field}"));

    let contracts = integer(row.get("size").unwrap_or(&serde_json::Value::Null))
        .ok_or_else(|| missing("size"))?;
    let unfilled =
        integer(row.get("unfilled_size").unwrap_or(&serde_json::Value::Null)).unwrap_or(0);
    let qty = product.to_qty(contracts);
    let unfilled_qty = product.to_qty(unfilled);

    let state_text = row
        .get("state")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| missing("state"))?;

    Ok(VenueOrder {
        id: row
            .get("id")
            .and_then(integer)
            .map(|id| VenueOrderId::new(&id.to_string()).unwrap_or_default())
            .unwrap_or_default(),
        client_order_id: row
            .get("client_order_id")
            .and_then(serde_json::Value::as_str)
            .and_then(|s| ClientOrderId::new(s).ok())
            .unwrap_or_default(),
        product_id: integer(row.get("product_id").unwrap_or(&serde_json::Value::Null))
            .unwrap_or(product.id),
        state: order_state(state_text, qty, unfilled_qty),
        filled_qty: qty.checked_sub(unfilled_qty).unwrap_or(Qty::ZERO),
        qty,
        avg_price: decimal(
            row.get("average_fill_price")
                .unwrap_or(&serde_json::Value::Null),
        )
        .filter(|p| p.is_positive()),
    })
}

/// One execution.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VenueFill {
    /// The venue's execution id — the dedup key.
    pub exec_id: ExecId,
    /// Which order.
    pub client_order_id: ClientOrderId,
    /// Direction.
    pub side: Side,
    /// Base quantity.
    pub qty: Qty,
    /// Execution price.
    pub price: Price,
}

/// Read one fill row.
///
/// # Errors
/// [`VenueError::Malformed`] when the row lacks an execution id, a size or a
/// price. A fill without an execution id cannot be deduplicated, and applying
/// one twice moves a real position twice.
pub fn venue_fill(row: &serde_json::Value, product: &Product) -> Result<VenueFill, VenueError> {
    let missing = |field: &str| VenueError::Malformed(format!("fill row has no {field}"));

    let exec_id = integer(row.get("id").unwrap_or(&serde_json::Value::Null))
        .map(|id| id.to_string())
        .or_else(|| {
            row.get("id")
                .and_then(serde_json::Value::as_str)
                .map(ToOwned::to_owned)
        })
        .ok_or_else(|| missing("id"))?;

    let contracts = integer(row.get("size").unwrap_or(&serde_json::Value::Null))
        .ok_or_else(|| missing("size"))?;
    let price = decimal(row.get("price").unwrap_or(&serde_json::Value::Null))
        .ok_or_else(|| missing("price"))?;
    let side = match row.get("side").and_then(serde_json::Value::as_str) {
        Some("buy") => Side::Buy,
        Some("sell") => Side::Sell,
        _ => return Err(missing("side")),
    };

    Ok(VenueFill {
        exec_id: ExecId::new(&exec_id).unwrap_or_default(),
        client_order_id: row
            .get("client_order_id")
            .and_then(serde_json::Value::as_str)
            .and_then(|s| ClientOrderId::new(s).ok())
            .unwrap_or_default(),
        side,
        qty: product.to_qty(contracts),
        price,
    })
}

/// One position, as the venue holds it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VenuePositionRow {
    /// The product.
    pub product_id: i64,
    /// Signed base quantity.
    pub qty: Qty,
}

/// Read one position row. Sizes arrive as signed contract counts.
///
/// # Errors
/// [`VenueError::Malformed`] when the row lacks a product or a size.
pub fn venue_position(
    row: &serde_json::Value,
    product: &Product,
) -> Result<VenuePositionRow, VenueError> {
    let product_id = integer(row.get("product_id").unwrap_or(&serde_json::Value::Null))
        .ok_or_else(|| VenueError::Malformed("position row has no product_id".into()))?;
    let contracts = integer(row.get("size").unwrap_or(&serde_json::Value::Null))
        .ok_or_else(|| VenueError::Malformed("position row has no size".into()))?;
    Ok(VenuePositionRow {
        product_id,
        qty: product.to_qty(contracts),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use sentinel_types::Instrument;

    fn btcusd() -> Product {
        Product {
            id: 27,
            symbol: Instrument::new("BTCUSD").unwrap(),
            contract_value: Qty::parse("0.001").unwrap(),
            tick_size: Price::parse("0.5").unwrap(),
        }
    }

    fn json(text: &str) -> serde_json::Value {
        serde_json::from_str(text).unwrap()
    }

    #[test]
    fn numbers_arrive_as_strings_and_stay_exact() {
        assert_eq!(
            decimal(&json(r#""109432.5""#)),
            Some(Price::parse("109432.5").unwrap())
        );
        assert_eq!(
            decimal(&json("109432.5")),
            Some(Price::parse("109432.5").unwrap())
        );
        assert_eq!(decimal(&json(r#""0.00000001""#)), Some(Price::from_raw(1)));
        assert_eq!(decimal(&json("null")), None);
        assert_eq!(decimal(&json(r#""not a number""#)), None);
    }

    #[test]
    fn integers_arrive_either_way() {
        assert_eq!(integer(&json("27")), Some(27));
        assert_eq!(integer(&json(r#""27""#)), Some(27));
        assert_eq!(integer(&json("-3")), Some(-3));
        assert_eq!(integer(&json("null")), None);
    }

    #[test]
    fn a_successful_envelope_yields_its_result() {
        let body = json(r#"{"success": true, "result": {"id": 12345}}"#);
        assert_eq!(
            unwrap_result(&body).unwrap().get("id"),
            Some(&json("12345"))
        );
    }

    #[test]
    fn a_failed_envelope_keeps_the_venues_own_code() {
        // The code is what an operator searches for. Losing it to a generic
        // message costs an hour every time.
        let body = json(r#"{"success": false, "error": {"code": "insufficient_margin"}}"#);
        let err = unwrap_result(&body).unwrap_err();
        assert!(err.to_string().contains("insufficient_margin"));
        assert_eq!(error_code(&body), Some("insufficient_margin"));
        assert_eq!(reject_reason(&body).text.as_str(), "insufficient_margin");
    }

    #[test]
    fn absence_is_distinguished_from_every_other_refusal() {
        // Absence is conclusive — the intent never became exposure. Everything
        // else leaves the question open, and the two lead opposite ways.
        assert!(is_absent("open_order_not_found"));
        assert!(is_absent("order_not_found"));
        assert!(!is_absent("insufficient_margin"));
        assert!(!is_absent("rate_limit_exceeded"));
    }

    #[test]
    fn a_resting_order_reads_as_working() {
        let row = json(
            r#"{"id": 553664, "product_id": 27, "size": 3, "unfilled_size": 3,
                 "state": "open", "client_order_id": "UI-4bb5ba826e2e1"}"#,
        );
        let order = venue_order(&row, &btcusd()).unwrap();
        assert_eq!(order.state, OrderState::Working);
        assert_eq!(order.qty, Qty::parse("0.003").unwrap());
        assert_eq!(order.filled_qty, Qty::ZERO);
        assert_eq!(order.id.as_str(), "553664");
        assert_eq!(order.client_order_id.as_str(), "UI-4bb5ba826e2e1");
    }

    #[test]
    fn a_partly_filled_order_reads_as_partial() {
        let row = json(
            r#"{"id": 553664, "product_id": 27, "size": 10, "unfilled_size": 4,
                 "state": "open", "client_order_id": "UI-1"}"#,
        );
        let order = venue_order(&row, &btcusd()).unwrap();
        assert_eq!(order.state, OrderState::Partial);
        assert_eq!(order.filled_qty, Qty::parse("0.006").unwrap());
    }

    #[test]
    fn a_filled_order_carries_what_it_actually_cost() {
        let row = json(
            r#"{"id": 1, "product_id": 27, "size": 3, "unfilled_size": 0,
                 "state": "closed", "client_order_id": "UI-1",
                 "average_fill_price": "109432.5"}"#,
        );
        let order = venue_order(&row, &btcusd()).unwrap();
        assert_eq!(order.avg_price, Some(Price::parse("109432.5").unwrap()));

        // A zero average is no average, not a price of nothing.
        let unfilled = json(
            r#"{"id": 1, "product_id": 27, "size": 3, "unfilled_size": 3,
                 "state": "open", "client_order_id": "UI-1",
                 "average_fill_price": "0"}"#,
        );
        assert_eq!(venue_order(&unfilled, &btcusd()).unwrap().avg_price, None);
    }

    #[test]
    fn closed_says_nothing_about_filling_so_the_quantity_decides() {
        // The mapping that gets written wrong. "closed" is not "filled", and
        // booking it as one turns a cancelled order into a position.
        let filled = json(
            r#"{"id": 1, "product_id": 27, "size": 3, "unfilled_size": 0,
                 "state": "closed", "client_order_id": "UI-1"}"#,
        );
        assert_eq!(
            venue_order(&filled, &btcusd()).unwrap().state,
            OrderState::Filled
        );

        let partly = json(
            r#"{"id": 1, "product_id": 27, "size": 3, "unfilled_size": 2,
                 "state": "closed", "client_order_id": "UI-1"}"#,
        );
        let order = venue_order(&partly, &btcusd()).unwrap();
        assert_eq!(order.state, OrderState::Canceled, "one filled, two gone");
        assert_eq!(order.filled_qty, Qty::parse("0.001").unwrap());
    }

    #[test]
    fn an_unknown_state_word_is_asked_about_rather_than_guessed_at() {
        // A vocabulary change at the venue must not become a booking here.
        assert_eq!(
            order_state("some_new_word", Qty::whole(3), Qty::ZERO),
            OrderState::Reconciling
        );
    }

    #[test]
    fn an_order_row_without_a_size_is_not_an_order_of_zero() {
        let row = json(r#"{"id": 1, "product_id": 27, "state": "open"}"#);
        assert!(venue_order(&row, &btcusd()).is_err());
    }

    #[test]
    fn a_fill_row_reads_its_execution_id_and_converts_contracts() {
        let row = json(
            r#"{"id": 9911, "order_id": 553664, "size": 2, "price": "109432.5",
                 "side": "buy", "client_order_id": "UI-1"}"#,
        );
        let fill = venue_fill(&row, &btcusd()).unwrap();
        assert_eq!(fill.exec_id.as_str(), "9911");
        assert_eq!(fill.qty, Qty::parse("0.002").unwrap());
        assert_eq!(fill.price, Price::parse("109432.5").unwrap());
        assert_eq!(fill.side, Side::Buy);
    }

    #[test]
    fn a_fill_without_an_execution_id_is_refused() {
        // Without it the fill cannot be deduplicated, and applying one twice
        // moves a real position twice.
        let row = json(r#"{"size": 2, "price": "109432.5", "side": "buy"}"#);
        assert!(venue_fill(&row, &btcusd()).is_err());
    }

    #[test]
    fn a_fill_without_a_side_is_refused_rather_than_assumed() {
        let row = json(r#"{"id": 1, "size": 2, "price": "100"}"#);
        assert!(venue_fill(&row, &btcusd()).is_err());
    }

    #[test]
    fn a_short_position_keeps_its_sign() {
        let row = json(r#"{"product_id": 27, "size": -76, "entry_price": "109432.5"}"#);
        let position = venue_position(&row, &btcusd()).unwrap();
        assert_eq!(position.qty, Qty::parse("-0.076").unwrap());
        assert_eq!(position.product_id, 27);
    }

    #[test]
    fn a_flat_position_is_zero_and_not_missing() {
        let row = json(r#"{"product_id": 27, "size": 0}"#);
        assert_eq!(venue_position(&row, &btcusd()).unwrap().qty, Qty::ZERO);
    }
}
