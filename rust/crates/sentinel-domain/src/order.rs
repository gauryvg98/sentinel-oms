//! The minimal order snapshot the domain reasons about.

use sentinel_types::{ClientOrderId, Instrument, Qty, Side, VenueOrderId};

use crate::state::OrderState;

/// What the lifecycle needs to know about an order, and nothing else.
///
/// Average cost, fees and realised P&L are projections derived from fills and
/// live in the ledger. They are excluded here on purpose: a transition rule
/// that could read them would eventually depend on them, and then the state
/// machine would no longer be testable without a ledger.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OrderCore {
    /// What the venue knows us by, and the key every reconciliation uses.
    pub client_order_id: ClientOrderId,
    /// What is being traded.
    pub instrument: Instrument,
    /// Direction. Quantities are unsigned; this carries the sign.
    pub side: Side,
    /// What was authorised. Never changes — a replace is a new order.
    pub qty: Qty,
    /// What has filled so far. Never exceeds `qty`, by construction.
    pub filled_qty: Qty,
    /// Where the order is.
    pub state: OrderState,
    /// What the venue calls it. Absent until acknowledged — which is exactly
    /// why it cannot be the reconciliation key.
    pub venue_order_id: Option<VenueOrderId>,
}

impl OrderCore {
    /// A fresh order, durable and unsubmitted.
    #[must_use]
    pub const fn created(
        client_order_id: ClientOrderId,
        instrument: Instrument,
        side: Side,
        qty: Qty,
    ) -> Self {
        Self {
            client_order_id,
            instrument,
            side,
            qty,
            filled_qty: Qty::ZERO,
            state: OrderState::Created,
            venue_order_id: None,
        }
    }

    /// Quantity still outstanding on this order.
    ///
    /// Local belief, and named nowhere in a sizing decision. A replacement is
    /// sized from *conclusively remaining* quantity — what the venue confirmed
    /// (R1.8) — and this value is what we merely believe.
    ///
    /// Floored at zero. The transition function clamps a tolerated over-match
    /// so this cannot arise from a fill, but a projection rebuilt from a log
    /// written by an older format could carry one, and a negative remainder
    /// reads as a short position everywhere it is used.
    #[must_use]
    pub fn remaining_qty(&self) -> Qty {
        self.qty
            .checked_sub(self.filled_qty)
            .unwrap_or(Qty::ZERO)
            .max(Qty::ZERO)
    }

    /// Whether the order is finished, absent new evidence.
    #[must_use]
    pub const fn is_terminal(&self) -> bool {
        self.state.is_terminal()
    }

    /// Whether this order blocks new entries on its instrument (R1.4).
    #[must_use]
    pub const fn locks_instrument(&self) -> bool {
        self.state.locks_instrument()
    }

    /// The signed position delta this order's fills have produced.
    #[must_use]
    pub fn signed_filled(&self) -> Qty {
        Qty::from_raw(self.filled_qty.raw().saturating_mul(self.side.sign()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn order() -> OrderCore {
        OrderCore::created(
            ClientOrderId::new("UI-4bb5ba826e2e1").unwrap(),
            Instrument::new("BTCUSD").unwrap(),
            Side::Buy,
            Qty::whole(10),
        )
    }

    #[test]
    fn a_new_order_is_created_unfilled_and_unnamed() {
        let o = order();
        assert_eq!(o.state, OrderState::Created);
        assert_eq!(o.filled_qty, Qty::ZERO);
        assert_eq!(o.venue_order_id, None);
        assert_eq!(o.remaining_qty(), Qty::whole(10));
    }

    #[test]
    fn remaining_never_goes_negative() {
        // Over-match is tolerated and clamped elsewhere; if one ever reached
        // here, a negative remainder would read as a short position.
        let mut o = order();
        o.filled_qty = Qty::whole(12);
        assert_eq!(o.remaining_qty(), Qty::ZERO);
    }

    #[test]
    fn sells_produce_negative_position_deltas() {
        let mut o = order();
        o.filled_qty = Qty::whole(4);
        assert_eq!(o.signed_filled(), Qty::whole(4));
        o.side = Side::Sell;
        assert_eq!(o.signed_filled(), Qty::whole(-4));
    }
}
