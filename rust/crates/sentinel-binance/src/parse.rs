//! Turning Binance's JSON into our types.
//!
//! Kept apart from the transport so it can be tested against recorded payloads
//! rather than against a live venue. Every shape below is one the Python
//! adapter actually handled in production, including the ones only reachable
//! when something has gone wrong.
//!
//! Numbers arrive as strings and are parsed exactly. A venue field read through
//! `f64` is a rounding error with a settlement date.

use sentinel_domain::{OrderState, RejectReason};
use sentinel_types::{Instrument, Money, Price, Qty};
use sentinel_venue::VenueError;

use crate::symbol::SymbolRules;

/// The only code that proves an order does not exist.
///
/// `-2013` on a *query*, and nothing else. Absence is conclusive — it means the
/// intent never became exposure — so the bar for concluding it is deliberately
/// one specific number from one specific endpoint.
pub const ORDER_DOES_NOT_EXIST: i32 = -2013;

/// "Unknown order sent", returned by *cancel*.
///
/// Not the same as absence, however similar it reads. The order may have filled
/// a millisecond ago, or been cancelled, or never existed — three outcomes with
/// different consequences, and this code distinguishes none of them. It means
/// "go and ask", never "it was not there".
pub const UNKNOWN_ORDER: i32 = -2011;

/// Timestamp outside the receive window.
///
/// Our clock, not theirs, and fixable: re-sync and retry. Treated as a transport
/// failure rather than a rejection, because the order was never considered.
pub const TIMESTAMP_SKEW: i32 = -1021;

/// Rate limited.
pub const TOO_MANY_REQUESTS: i32 = -1003;

/// Margin is insufficient. The one the sizing caps exist to prevent.
pub const INSUFFICIENT_MARGIN: i32 = -2019;

/// Exceeded the maximum allowable position at current leverage.
pub const EXCEEDS_LEVERAGE_BRACKET: i32 = -2027;

/// Map a Binance order status onto ours.
///
/// `EXPIRED_IN_MATCH` is self-trade prevention: a resting order that would have
/// crossed this account's own is killed in-match. Terminal with no residual —
/// any real fills arrived as separate trade events — so it is a cancellation,
/// and treating it as a rejection would lose the fills that preceded it.
///
/// An unrecognised status maps to `Reconciling` rather than to a guess: a
/// vocabulary change at the venue must not become a booking here.
#[must_use]
pub fn order_state(status: &str) -> OrderState {
    match status {
        "NEW" => OrderState::Working,
        "PARTIALLY_FILLED" => OrderState::Partial,
        "FILLED" => OrderState::Filled,
        "CANCELED" | "EXPIRED" | "EXPIRED_IN_MATCH" => OrderState::Canceled,
        "REJECTED" => OrderState::Rejected,
        _ => OrderState::Reconciling,
    }
}

/// Read an exact decimal from a value that may be a string or a number.
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
/// [`decimal`] refuses more than eight decimal places, which is right for every
/// number we send back: a quantity or a limit price has to be exactly
/// representable or the venue rejects it, and silently rounding one changes what
/// was ordered.
///
/// It is wrong for a number the venue computed. `entryPrice` on a position is a
/// weighted average — Binance returns `78607.99999999999`, eleven places — and
/// so are `avgPrice`, `breakEvenPrice` and `liquidationPrice`. Refusing those
/// meant they arrived as `None` and were displayed as zero, which is how a
/// position with a real entry showed an entry of nothing.
///
/// Truncating an average at 1e-8 discards noise below the eighth decimal of a
/// mean. Truncating an order quantity discards money.
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
    // Cut the fraction to what the scale holds and parse that. Toward zero, so
    // a reported average is never rendered larger than it is.
    let (whole, fraction) = text.split_once('.')?;
    let kept = &fraction[..fraction.len().min(sentinel_types::SCALE_DIGITS as usize)];
    Price::parse(&format!("{whole}.{kept}")).ok()
}

/// The same, as money.
#[must_use]
pub fn derived_amount(value: &serde_json::Value) -> Option<Money> {
    derived_decimal(value).map(|p| Money::from_raw(p.raw()))
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

/// An integer that may arrive as a string.
#[must_use]
pub fn integer(value: &serde_json::Value) -> Option<i64> {
    match value {
        serde_json::Value::Number(n) => n.as_i64(),
        serde_json::Value::String(s) => s.parse().ok(),
        _ => None,
    }
}

/// Binance's error shape: `{"code": -2013, "msg": "Order does not exist."}`.
#[must_use]
pub fn error_code(body: &serde_json::Value) -> Option<(i32, &str)> {
    let code = body.get("code").and_then(serde_json::Value::as_i64)?;
    let message = body
        .get("msg")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("");
    i32::try_from(code).ok().map(|code| (code, message))
}

/// A refusal, with the venue's own code and message preserved.
///
/// The code is what an operator searches for. Losing it to a generic message
/// costs an hour every time, and `-2019` and `-2027` mean quite different
/// things about what to do next.
#[must_use]
pub fn reject_reason(body: &serde_json::Value) -> RejectReason {
    match error_code(body) {
        Some((code, message)) => RejectReason::new(code, message),
        None => RejectReason::new(0, "rejected without a code"),
    }
}

/// Whether a code means the call can be retried unchanged.
#[must_use]
pub const fn is_retryable(code: i32) -> bool {
    matches!(code, TIMESTAMP_SKEW | TOO_MANY_REQUESTS)
}

/// One order, as the venue describes it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VenueOrder {
    /// Binance's own id.
    pub order_id: i64,
    /// The symbol.
    pub symbol: Instrument,
    /// State.
    pub state: OrderState,
    /// What was ordered.
    pub qty: Qty,
    /// What has filled.
    pub filled_qty: Qty,
    /// Average fill price, when anything has filled.
    ///
    /// Carried so a reconciliation that finds fills we missed can book them at
    /// what they actually cost, rather than at whatever the last mark happened
    /// to be.
    pub avg_price: Option<Price>,
}

/// Read one order row from `/fapi/v1/order`.
///
/// # Errors
/// [`VenueError::Malformed`] when a field this system depends on is missing.
/// Nothing is defaulted: an order row without an executed quantity is not an
/// order that has filled nothing.
pub fn venue_order(row: &serde_json::Value) -> Result<VenueOrder, VenueError> {
    let missing = |field: &str| VenueError::Malformed(format!("order row has no {field}"));
    let null = serde_json::Value::Null;

    let status = row
        .get("status")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| missing("status"))?;
    let executed =
        quantity(row.get("executedQty").unwrap_or(&null)).ok_or_else(|| missing("executedQty"))?;
    // Derived: an average fill price is a weighted mean and carries more
    // places than the scale holds.
    let avg = derived_decimal(row.get("avgPrice").unwrap_or(&null)).filter(|p| p.is_positive());

    Ok(VenueOrder {
        order_id: integer(row.get("orderId").unwrap_or(&null)).ok_or_else(|| missing("orderId"))?,
        symbol: row
            .get("symbol")
            .and_then(serde_json::Value::as_str)
            .and_then(|s| Instrument::new(s).ok())
            .ok_or_else(|| missing("symbol"))?,
        state: order_state(status),
        qty: quantity(row.get("origQty").unwrap_or(&null)).unwrap_or(Qty::ZERO),
        filled_qty: executed,
        avg_price: avg,
    })
}

/// One position, as `/fapi/v2/positionRisk` reports it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VenuePositionRow {
    /// The symbol.
    pub symbol: Instrument,
    /// Signed size. Negative is short.
    pub qty: Qty,
    /// Average entry.
    pub entry_price: Price,
}

/// Read one position row.
///
/// # Errors
/// [`VenueError::Malformed`] when the row lacks a symbol or a position amount.
pub fn venue_position(row: &serde_json::Value) -> Result<VenuePositionRow, VenueError> {
    let null = serde_json::Value::Null;
    let symbol = row
        .get("symbol")
        .and_then(serde_json::Value::as_str)
        .and_then(|s| Instrument::new(s).ok())
        .ok_or_else(|| VenueError::Malformed("position row has no symbol".into()))?;
    let qty = quantity(row.get("positionAmt").unwrap_or(&null))
        .ok_or_else(|| VenueError::Malformed("position row has no positionAmt".into()))?;
    Ok(VenuePositionRow {
        symbol,
        qty,
        entry_price: derived_decimal(row.get("entryPrice").unwrap_or(&null)).unwrap_or(Price::ZERO),
    })
}

/// Read the filters for one symbol out of `/fapi/v1/exchangeInfo`.
///
/// # Errors
/// [`VenueError::Malformed`] when the symbol is absent or carries no
/// `LOT_SIZE`. A symbol with no step is one whose orders would all be refused,
/// and defaulting the step to something plausible would make that a surprise
/// at the first order instead of at boot.
pub fn symbol_rules(row: &serde_json::Value) -> Result<SymbolRules, VenueError> {
    let null = serde_json::Value::Null;
    let symbol = row
        .get("symbol")
        .and_then(serde_json::Value::as_str)
        .and_then(|s| Instrument::new(s).ok())
        .ok_or_else(|| VenueError::Malformed("exchangeInfo row has no symbol".into()))?;

    let filters = row
        .get("filters")
        .and_then(serde_json::Value::as_array)
        .map_or(&[][..], Vec::as_slice);
    let find = |kind: &str| {
        filters
            .iter()
            .find(|f| f.get("filterType").and_then(serde_json::Value::as_str) == Some(kind))
    };

    let lot = find("LOT_SIZE")
        .ok_or_else(|| VenueError::Malformed(format!("{symbol} has no LOT_SIZE filter")))?;
    let step_size = quantity(lot.get("stepSize").unwrap_or(&null))
        .ok_or_else(|| VenueError::Malformed(format!("{symbol} has no stepSize")))?;

    Ok(SymbolRules {
        symbol,
        step_size,
        min_qty: quantity(lot.get("minQty").unwrap_or(&null)).unwrap_or(step_size),
        tick_size: find("PRICE_FILTER")
            .and_then(|f| decimal(f.get("tickSize").unwrap_or(&null)))
            .unwrap_or(Price::from_raw(1)),
        // Absent on some symbols, and zero is the honest reading of that: no
        // minimum rather than a guessed one.
        min_notional: find("MIN_NOTIONAL")
            .and_then(|f| {
                amount(f.get("notional").unwrap_or(&null))
                    .or_else(|| amount(f.get("minNotional").unwrap_or(&null)))
            })
            .unwrap_or(Money::ZERO),
        quantity_precision: u32::try_from(
            integer(row.get("quantityPrecision").unwrap_or(&null)).unwrap_or(8),
        )
        .unwrap_or(8),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn json(text: &str) -> serde_json::Value {
        serde_json::from_str(text).unwrap()
    }

    #[test]
    fn numbers_arrive_as_strings_and_stay_exact() {
        assert_eq!(
            decimal(&json(r#""109432.50""#)),
            Some(Price::parse("109432.5").unwrap())
        );
        assert_eq!(
            decimal(&json("109432.5")),
            Some(Price::parse("109432.5").unwrap())
        );
        assert_eq!(decimal(&json(r#""0.00000001""#)), Some(Price::from_raw(1)));
        assert_eq!(decimal(&json("null")), None);
    }

    #[test]
    fn the_status_map_covers_what_binance_sends() {
        assert_eq!(order_state("NEW"), OrderState::Working);
        assert_eq!(order_state("PARTIALLY_FILLED"), OrderState::Partial);
        assert_eq!(order_state("FILLED"), OrderState::Filled);
        assert_eq!(order_state("CANCELED"), OrderState::Canceled);
        assert_eq!(order_state("EXPIRED"), OrderState::Canceled);
        assert_eq!(order_state("REJECTED"), OrderState::Rejected);
    }

    #[test]
    fn self_trade_prevention_is_a_cancellation_not_a_rejection() {
        // EXPIRED_IN_MATCH kills the resting remainder; any real fills arrived
        // as separate trade events. Calling it a rejection would lose them.
        assert_eq!(order_state("EXPIRED_IN_MATCH"), OrderState::Canceled);
    }

    #[test]
    fn an_unknown_status_is_asked_about_rather_than_guessed_at() {
        assert_eq!(order_state("SOME_NEW_WORD"), OrderState::Reconciling);
    }

    #[test]
    fn absence_has_exactly_one_code() {
        // The bar for concluding an order never existed is deliberately one
        // specific number from one specific endpoint.
        assert_eq!(ORDER_DOES_NOT_EXIST, -2013);
        assert_ne!(UNKNOWN_ORDER, ORDER_DOES_NOT_EXIST);
    }

    #[test]
    fn skew_and_throttling_are_retryable_and_margin_is_not() {
        assert!(is_retryable(TIMESTAMP_SKEW));
        assert!(is_retryable(TOO_MANY_REQUESTS));
        assert!(!is_retryable(INSUFFICIENT_MARGIN));
        assert!(!is_retryable(EXCEEDS_LEVERAGE_BRACKET));
        assert!(!is_retryable(ORDER_DOES_NOT_EXIST));
    }

    #[test]
    fn a_rejection_keeps_the_venues_code_and_words() {
        let body = json(r#"{"code": -2019, "msg": "Margin is insufficient."}"#);
        assert_eq!(error_code(&body), Some((-2019, "Margin is insufficient.")));
        let reason = reject_reason(&body);
        assert_eq!(reason.code, -2019);
        assert_eq!(reason.text.as_str(), "Margin is insufficient.");
    }

    #[test]
    fn a_resting_order_reads_as_working() {
        let row = json(
            r#"{"orderId": 553664, "symbol": "BTCUSDT", "status": "NEW",
                 "origQty": "0.003", "executedQty": "0", "avgPrice": "0.00000",
                 "clientOrderId": "UI-4bb5ba826e2e1"}"#,
        );
        let order = venue_order(&row).unwrap();
        assert_eq!(order.state, OrderState::Working);
        assert_eq!(order.qty, Qty::parse("0.003").unwrap());
        assert_eq!(order.filled_qty, Qty::ZERO);
        assert_eq!(order.avg_price, None, "a zero average is no average");
        assert_eq!(order.order_id, 553_664);
    }

    #[test]
    fn a_derived_average_keeps_the_precision_we_model_and_drops_the_rest() {
        // Binance really does send this. Refusing it is what made a position
        // with a real entry display an entry of zero.
        assert_eq!(
            derived_decimal(&json(r#""78607.99999999999""#)),
            Some(Price::parse("78607.99999999").unwrap())
        );
        assert_eq!(
            derived_decimal(&json(r#""78623.72159999999""#)),
            Some(Price::parse("78623.72159999").unwrap())
        );
        // Anything that already fits is untouched.
        assert_eq!(
            derived_decimal(&json(r#""109432.5""#)),
            Some(Price::parse("109432.5").unwrap())
        );
        assert_eq!(derived_decimal(&json(r#""0""#)), Some(Price::ZERO));
        assert_eq!(derived_decimal(&json("null")), None);
        assert_eq!(derived_decimal(&json(r#""not a number""#)), None);
    }

    #[test]
    fn what_we_send_is_still_refused_rather_than_rounded() {
        // The distinction the two functions exist for. A quantity or a limit
        // price has to be exactly representable or the venue rejects it, and
        // silently rounding one changes what was ordered.
        assert_eq!(decimal(&json(r#""78607.99999999999""#)), None);
        assert_eq!(quantity(&json(r#""0.0000000012""#)), None);
    }

    #[test]
    fn a_filled_order_carries_what_it_actually_cost() {
        // So a reconciliation that finds fills we missed books them at their
        // price rather than at whatever the last mark happened to be.
        let row = json(
            r#"{"orderId": 1, "symbol": "BTCUSDT", "status": "FILLED",
                 "origQty": "0.003", "executedQty": "0.003", "avgPrice": "109432.5"}"#,
        );
        let order = venue_order(&row).unwrap();
        assert_eq!(order.state, OrderState::Filled);
        assert_eq!(order.filled_qty, Qty::parse("0.003").unwrap());
        assert_eq!(order.avg_price, Some(Price::parse("109432.5").unwrap()));
    }

    #[test]
    fn an_order_row_without_an_executed_quantity_is_refused() {
        // Not an order that has filled nothing — an order we cannot read.
        let row = json(r#"{"orderId": 1, "symbol": "BTCUSDT", "status": "NEW"}"#);
        assert!(venue_order(&row).is_err());
    }

    #[test]
    fn a_short_position_keeps_its_sign() {
        let row = json(
            r#"{"symbol": "BTCUSDT", "positionAmt": "-0.076",
                 "entryPrice": "109432.5", "markPrice": "109500"}"#,
        );
        let position = venue_position(&row).unwrap();
        assert_eq!(position.qty, Qty::parse("-0.076").unwrap());
        assert_eq!(position.entry_price, Price::parse("109432.5").unwrap());
    }

    #[test]
    fn a_flat_position_is_zero_and_not_missing() {
        let row = json(r#"{"symbol": "BTCUSDT", "positionAmt": "0", "entryPrice": "0"}"#);
        assert_eq!(venue_position(&row).unwrap().qty, Qty::ZERO);
    }

    #[test]
    fn the_filters_read_off_exchange_info() {
        let row = json(
            r#"{"symbol": "BTCUSDT", "quantityPrecision": 3, "filters": [
                 {"filterType": "PRICE_FILTER", "tickSize": "0.10"},
                 {"filterType": "LOT_SIZE", "stepSize": "0.001", "minQty": "0.001"},
                 {"filterType": "MIN_NOTIONAL", "notional": "100"}]}"#,
        );
        let rules = symbol_rules(&row).unwrap();
        assert_eq!(rules.step_size, Qty::parse("0.001").unwrap());
        assert_eq!(rules.min_qty, Qty::parse("0.001").unwrap());
        assert_eq!(rules.tick_size, Price::parse("0.1").unwrap());
        assert_eq!(rules.min_notional, Money::whole(100));
        assert_eq!(rules.quantity_precision, 3);
    }

    #[test]
    fn a_symbol_with_no_lot_size_is_refused_at_boot() {
        // A symbol with no step is one whose orders would all be refused.
        // Defaulting it would make that a surprise at the first order.
        let row = json(r#"{"symbol": "BTCUSDT", "filters": []}"#);
        assert!(symbol_rules(&row).is_err());
    }

    #[test]
    fn an_absent_minimum_notional_is_zero_rather_than_guessed() {
        let row = json(
            r#"{"symbol": "BTCUSDT", "filters": [
                 {"filterType": "LOT_SIZE", "stepSize": "0.001", "minQty": "0.001"}]}"#,
        );
        let rules = symbol_rules(&row).unwrap();
        assert_eq!(rules.min_notional, Money::ZERO);
    }
}
