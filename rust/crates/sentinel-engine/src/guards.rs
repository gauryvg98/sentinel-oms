//! Exposure guards: the reasons an order is refused before it exists.
//!
//! These are integrity rules, not risk appetite. Position sizing lives in
//! `sentinel-risk` and answers "how much"; this answers "may this exist at
//! all", and the difference is that these cannot be tuned. A guard with a knob
//! on it is a guard someone turns off at three in the morning.
//!
//! Every one of them is a pure function of the book and the intent, so a
//! refusal is reproducible from the log rather than depending on when it ran.

use sentinel_domain::EconomicOrderIntent;
use sentinel_ledger::Book;
use sentinel_record::Note;
use sentinel_types::{ClientOrderId, Qty};

/// Why an order may not exist.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Refusal {
    /// The account has stopped trading.
    ///
    /// Entries only. A halt that also disarmed the stops would turn a
    /// bookkeeping problem into an unbounded position (R1.13).
    Halted,
    /// Something on this instrument is in a state whose truth we cannot prove.
    ///
    /// R1.4. Submitting against exposure that may or may not exist is exactly
    /// the duplicate this system refuses to be capable of.
    InstrumentLocked {
        /// The order holding it, so the operator knows where to look.
        held_by: ClientOrderId,
    },
    /// A live entry already exists on this instrument.
    ///
    /// R1.9. Two signals aiming at the same exposure resolve to one order, and
    /// the second is refused rather than netted — netting is a decision, and
    /// the strategy did not make it.
    EntryAlreadyLive {
        /// The entry that got there first.
        existing: ClientOrderId,
    },
    /// The exit is for more than the position that is not already spoken for.
    ///
    /// R1.10. Bounded by the *reconciled* position minus what live exits
    /// already cover, so a stop and a take-profit cannot between them close
    /// more than is held.
    ExitExceedsPosition {
        /// What was asked for.
        requested: Qty,
        /// What is actually available to close.
        available: Qty,
    },
    /// The order would take the position past the fat-finger cap.
    ExceedsMaxPosition {
        /// What the position would become.
        would_be: Qty,
        /// The cap.
        cap: Qty,
    },
    /// The venue has not yet said what the account holds.
    ///
    /// An empty book means "we have booked nothing", which is not the same as
    /// "the account holds nothing" — on a first boot, after a restore, or on a
    /// cutover from another system, the two differ and only the venue knows
    /// how. Opening exposure on that assumption is how a position gets built
    /// on top of one nobody remembered.
    ///
    /// Entries only: an exit against a position we can see is still allowed,
    /// because refusing to close is the more dangerous failure (R1.13).
    PositionUnconfirmed,
}

impl Refusal {
    /// A short explanation, for the decision record.
    #[must_use]
    pub fn note(self) -> Note {
        match self {
            Self::Halted => Note::new("account halted").unwrap_or_default(),
            Self::InstrumentLocked { held_by } => {
                Note::new(&format!("instrument locked by {held_by}")).unwrap_or_default()
            }
            Self::EntryAlreadyLive { existing } => {
                Note::new(&format!("entry {existing} is already live")).unwrap_or_default()
            }
            Self::ExitExceedsPosition {
                requested,
                available,
            } => Note::new(&format!("exit {requested} exceeds available {available}"))
                .unwrap_or_default(),
            Self::ExceedsMaxPosition { would_be, cap } => {
                Note::new(&format!("position {would_be} exceeds cap {cap}")).unwrap_or_default()
            }
            Self::PositionUnconfirmed => {
                Note::new("the venue has not yet reported the position").unwrap_or_default()
            }
        }
    }

    /// The quantity the refusal turned on, for the decision record.
    #[must_use]
    pub const fn value(self) -> Qty {
        match self {
            Self::ExitExceedsPosition { available, .. } => available,
            Self::ExceedsMaxPosition { cap, .. } => cap,
            _ => Qty::ZERO,
        }
    }
}

impl core::fmt::Display for Refusal {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str(self.note().as_str())
    }
}

/// What the guards decided.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Verdict {
    /// Place it as asked.
    Allow,
    /// Place it, but for less.
    ///
    /// Only exits are ever clamped, and only downward. An entry that does not
    /// fit is refused rather than shrunk, because a smaller entry is a
    /// different trade and nobody decided to make it — whereas a smaller exit
    /// is the same decision applied to the position that is actually there.
    Clamp {
        /// The quantity that will actually be sent.
        to: Qty,
    },
    /// Do not place it.
    Refuse(Refusal),
}

/// Limits that are not tunable at runtime.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Limits {
    /// Largest absolute position, per instrument. A fat-finger backstop, not a
    /// risk model: sizing decides how much to trade, and this only decides what
    /// is obviously wrong.
    pub max_position: Option<Qty>,
    /// Whether the venue has told us what the account holds since this process
    /// started.
    ///
    /// Starts false and is set by the first position report. Entries wait for
    /// it; exits do not. A venue that never answers therefore stops the system
    /// opening anything, which is the direction a failure here should fail in.
    pub position_confirmed: bool,
}

/// Decide whether an intent may become an order.
#[must_use]
pub fn evaluate(
    book: &Book,
    intent: &EconomicOrderIntent,
    limits: Limits,
    halted: bool,
) -> Verdict {
    if intent.opens_exposure() {
        evaluate_entry(book, intent, limits, halted)
    } else {
        evaluate_exit(book, intent)
    }
}

fn evaluate_entry(
    book: &Book,
    intent: &EconomicOrderIntent,
    limits: Limits,
    halted: bool,
) -> Verdict {
    if halted {
        return Verdict::Refuse(Refusal::Halted);
    }
    if !limits.position_confirmed {
        return Verdict::Refuse(Refusal::PositionUnconfirmed);
    }
    if let Some(holder) = book.locking_order(&intent.instrument) {
        return Verdict::Refuse(Refusal::InstrumentLocked {
            held_by: holder.core.client_order_id,
        });
    }
    if let Some(existing) = book
        .live_orders_on(&intent.instrument)
        .find(|o| !o.is_exit())
    {
        return Verdict::Refuse(Refusal::EntryAlreadyLive {
            existing: existing.core.client_order_id,
        });
    }
    if let Some(cap) = limits.max_position {
        let would_be = book
            .position(&intent.instrument)
            .qty
            .checked_add(intent.signed_qty())
            .unwrap_or(Qty::MAX);
        if would_be.abs() > cap {
            return Verdict::Refuse(Refusal::ExceedsMaxPosition { would_be, cap });
        }
    }
    Verdict::Allow
}

fn evaluate_exit(book: &Book, intent: &EconomicOrderIntent) -> Verdict {
    // Bounded by what the book actually holds, minus what live exits already
    // cover. An exit is never refused for being too large — it is reduced to
    // the position that is there, which is the same decision applied honestly.
    let position = book.position(&intent.instrument);
    let held = position.abs_qty();
    let committed = book.committed_to_exits(&intent.instrument);
    let available = held
        .checked_sub(committed)
        .unwrap_or(Qty::ZERO)
        .max(Qty::ZERO);

    // An exit must close, never open. Selling when short would add exposure
    // under an authority that exists to remove it.
    let closes = position.qty.is_positive() == (intent.side == sentinel_types::Side::Sell);
    if held.is_zero() || !closes {
        return Verdict::Refuse(Refusal::ExitExceedsPosition {
            requested: intent.qty,
            available: Qty::ZERO,
        });
    }

    if available.is_zero() {
        return Verdict::Refuse(Refusal::ExitExceedsPosition {
            requested: intent.qty,
            available,
        });
    }
    if intent.qty > available {
        return Verdict::Clamp { to: available };
    }
    Verdict::Allow
}

#[cfg(test)]
mod tests {

    /// Limits for a process that has already heard from the venue.
    ///
    /// Every guard below this line is about something other than whether the
    /// position is known, so they start from a state where it is.
    fn confirmed() -> Limits {
        Limits {
            position_confirmed: true,
            ..Limits::default()
        }
    }
    use super::*;
    use sentinel_types::{Authority, Instrument, Side, TraceId};

    fn coid(s: &str) -> ClientOrderId {
        ClientOrderId::new(s).unwrap()
    }

    fn btc() -> Instrument {
        Instrument::new("BTCUSD").unwrap()
    }

    fn intent(id: &str, side: Side, qty: i64, authority: Authority) -> EconomicOrderIntent {
        EconomicOrderIntent::market(
            coid(id),
            btc(),
            side,
            Qty::whole(qty),
            authority,
            TraceId::from_u128(1),
        )
        .unwrap()
    }

    #[test]
    fn an_empty_book_allows_an_entry() {
        let book = Book::new();
        assert_eq!(
            evaluate(
                &book,
                &intent("UI-1", Side::Buy, 3, Authority::Entry),
                confirmed(),
                false
            ),
            Verdict::Allow
        );
    }

    #[test]
    fn a_halt_refuses_entries_and_not_exits() {
        // R1.13 stated where it is enforced: a halt that disarmed the stops
        // would turn a bookkeeping problem into an unbounded position.
        let book = Book::new();
        assert_eq!(
            evaluate(
                &book,
                &intent("UI-1", Side::Buy, 3, Authority::Entry),
                confirmed(),
                true
            ),
            Verdict::Refuse(Refusal::Halted)
        );
        // The exit is refused here only because the book is flat — the halt
        // itself never reaches the exit path.
        assert!(matches!(
            evaluate(
                &book,
                &intent("SL-1", Side::Sell, 3, Authority::ProtectiveExit),
                confirmed(),
                true
            ),
            Verdict::Refuse(Refusal::ExitExceedsPosition { .. })
        ));
    }

    #[test]
    fn an_exit_against_no_position_is_refused() {
        let book = Book::new();
        assert!(matches!(
            evaluate(
                &book,
                &intent("SL-1", Side::Sell, 3, Authority::ProtectiveExit),
                confirmed(),
                false
            ),
            Verdict::Refuse(Refusal::ExitExceedsPosition { .. })
        ));
    }

    #[test]
    fn a_refusal_explains_itself() {
        let refusal = Refusal::InstrumentLocked {
            held_by: coid("UI-4bb5ba826e2e1"),
        };
        assert_eq!(refusal.to_string(), "instrument locked by UI-4bb5ba826e2e1");
    }
}
