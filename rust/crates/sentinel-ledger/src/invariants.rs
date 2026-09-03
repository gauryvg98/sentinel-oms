//! The invariants, as runnable assertions.
//!
//! Four statements that must be true of the book at every instant. They are
//! checked in-process after each batch and — more usefully — from *outside* the
//! process by `cmd/adversary`, because an in-process assertion shares the
//! engine's assumptions: if the check and the thing it checks are wrong the
//! same way, the assertion agrees with the bug.
//!
//! In the Python system these were four full-table scans against Postgres,
//! every three seconds, on the same connection pool the hot path used. Here
//! they are folds over memory, so running them more often costs nothing worth
//! measuring.

use std::collections::{BTreeMap, BTreeSet};

use sentinel_types::{ClientOrderId, Instrument, Money, Qty, Side};

use crate::book::Book;

/// An invariant that does not hold.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Violation {
    /// A position does not equal the sum of its fills.
    ///
    /// The one that catches a posting applied twice, a fill dropped, or
    /// arithmetic that went astray — the same job the zero-sum check does in a
    /// double-entry ledger.
    PositionDisagreesWithFills {
        /// Where.
        instrument: Instrument,
        /// What the position says.
        position: Qty,
        /// What the fills add up to.
        fills: Qty,
    },
    /// An order has recorded more filled quantity than was authorised.
    OrderOverfilled {
        /// Which order.
        client_order_id: ClientOrderId,
        /// What was authorised.
        ordered: Qty,
        /// What is recorded as filled.
        filled: Qty,
    },
    /// Live protective exits add up to more than the position they protect.
    ///
    /// R1.10 stated as a property of the whole book rather than of one
    /// decision: if this holds, no sequence of exits can have over-exited,
    /// whatever any individual guard did.
    ExitsExceedPosition {
        /// Where.
        instrument: Instrument,
        /// Outstanding exit quantity.
        exits: Qty,
        /// The position it is supposed to be closing.
        position: Qty,
    },
    /// An order exists that no chain authorised.
    ///
    /// Every order is traceable to the decision that made it. An untraced one
    /// means exposure appeared without an audit path to whatever asked for it.
    UntracedOrder {
        /// Which order.
        client_order_id: ClientOrderId,
    },
    /// A cost basis is held against a position that is flat.
    ///
    /// Cosmetic-looking and not: a residual basis on nothing is reported as
    /// running P&L, so a flat account shows a number it did not earn.
    BasisWithoutPosition {
        /// Where.
        instrument: Instrument,
        /// The basis left behind.
        basis: Money,
    },
}

impl core::fmt::Display for Violation {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::PositionDisagreesWithFills {
                instrument,
                position,
                fills,
            } => write!(
                f,
                "{instrument}: position {position} but fills sum to {fills}"
            ),
            Self::OrderOverfilled {
                client_order_id,
                ordered,
                filled,
            } => write!(
                f,
                "{client_order_id}: filled {filled} against an order of {ordered}"
            ),
            Self::ExitsExceedPosition {
                instrument,
                exits,
                position,
            } => write!(
                f,
                "{instrument}: {exits} of live exits against a position of {position}"
            ),
            Self::UntracedOrder { client_order_id } => {
                write!(f, "{client_order_id}: no trace id")
            }
            Self::BasisWithoutPosition { instrument, basis } => {
                write!(f, "{instrument}: cost basis {basis} on a flat position")
            }
        }
    }
}

impl core::error::Error for Violation {}

/// What a check found.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Report {
    /// Everything that does not hold. Empty is the only acceptable value.
    pub violations: Vec<Violation>,
}

impl Report {
    /// Whether every invariant holds.
    #[must_use]
    pub fn holds(&self) -> bool {
        self.violations.is_empty()
    }
}

impl core::fmt::Display for Report {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        if self.holds() {
            return f.write_str("all invariants hold");
        }
        for (i, v) in self.violations.iter().enumerate() {
            if i > 0 {
                f.write_str("; ")?;
            }
            write!(f, "{v}")?;
        }
        Ok(())
    }
}

/// Check every invariant.
///
/// Returns everything wrong rather than the first thing wrong. An operator
/// looking at a broken account wants the shape of the damage, and two
/// violations that always appear together say something one does not.
#[must_use]
pub fn check(book: &Book) -> Report {
    let mut violations = Vec::new();
    check_positions_equal_fills(book, &mut violations);
    check_no_overfill(book, &mut violations);
    check_exits_bounded(book, &mut violations);
    check_orders_are_traced(book, &mut violations);
    check_flat_means_flat(book, &mut violations);
    Report { violations }
}

/// The position in each instrument is the signed sum of its fills.
///
/// Both sides are gathered — every instrument that has a position and every
/// instrument that has a fill — so a position with no fills behind it is caught
/// as surely as fills with no position in front of them.
fn check_positions_equal_fills(book: &Book, out: &mut Vec<Violation>) {
    let mut sums: BTreeMap<Instrument, Qty> = BTreeMap::new();
    for (instrument, _) in book.all_positions() {
        sums.entry(instrument).or_insert(Qty::ZERO);
    }
    for fill in book.all_fills() {
        let entry = sums.entry(fill.instrument).or_insert(Qty::ZERO);
        *entry = entry
            .checked_add(signed(fill.side, fill.qty))
            .unwrap_or(*entry);
    }

    for (instrument, fills) in sums {
        let position = book.position(&instrument).qty;
        if position != fills {
            out.push(Violation::PositionDisagreesWithFills {
                instrument,
                position,
                fills,
            });
        }
    }
}

/// No order records more filled quantity than it authorised.
fn check_no_overfill(book: &Book, out: &mut Vec<Violation>) {
    for order in book.orders() {
        if order.core.filled_qty > order.core.qty {
            out.push(Violation::OrderOverfilled {
                client_order_id: order.core.client_order_id,
                ordered: order.core.qty,
                filled: order.core.filled_qty,
            });
        }
    }
}

/// Live protective exits never add up to more than the position they close.
fn check_exits_bounded(book: &Book, out: &mut Vec<Violation>) {
    let mut instruments: BTreeSet<Instrument> = BTreeSet::new();
    for order in book.live_orders().filter(|o| o.is_exit()) {
        instruments.insert(order.core.instrument);
    }
    for instrument in instruments {
        let exits = book.committed_to_exits(&instrument);
        let position = book.position(&instrument);
        if exits > position.abs_qty() {
            out.push(Violation::ExitsExceedPosition {
                instrument,
                exits,
                position: position.qty,
            });
        }
    }
}

/// Every order is traceable to the decision that authorised it.
fn check_orders_are_traced(book: &Book, out: &mut Vec<Violation>) {
    for order in book.orders() {
        if order.trace_id == sentinel_types::TraceId::NONE {
            out.push(Violation::UntracedOrder {
                client_order_id: order.core.client_order_id,
            });
        }
    }
}

/// A flat position holds no cost basis.
fn check_flat_means_flat(book: &Book, out: &mut Vec<Violation>) {
    for (instrument, position) in book.all_positions() {
        if position.is_flat() && !position.cost_basis.is_zero() {
            out.push(Violation::BasisWithoutPosition {
                instrument,
                basis: position.cost_basis,
            });
        }
    }
}

/// A side's sign, for callers assembling their own sums.
#[must_use]
pub const fn signed(side: Side, qty: Qty) -> Qty {
    Qty::from_raw(qty.raw().saturating_mul(side.sign()))
}
