//! A venue that does exactly what it is told to do wrong.
//!
//! Every failure the integrity scenarios depend on is a scripted [`Fault`], so
//! "a submission that timed out but was accepted anyway" is a line of setup
//! rather than a race someone has to reproduce. Nothing here reads a clock or a
//! random number: the same script produces the same run, every time.

use std::collections::{HashMap, VecDeque};

use sentinel_domain::{EconomicOrderIntent, OrderState, RejectReason};
use sentinel_types::{
    ClientOrderId, ExecId, Instrument, Money, Price, Qty, SCALE, Side, VenueOrderId,
};

use crate::{
    CancelOutcome, LookupOutcome, OrderSnapshot, SubmitOutcome, Venue, VenueError, VenueEvent,
    VenuePosition,
};

/// What the venue should do wrong for one order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Fault {
    /// Answer nothing, and accept the order anyway.
    ///
    /// The failure the whole design exists for: the caller has no venue order
    /// id, no answer, and real exposure. Treating it as a rejection and
    /// resubmitting is how a position gets doubled.
    TimeoutButAccepted,
    /// Answer nothing, and do not accept the order.
    ///
    /// Indistinguishable from the case above at the moment it happens, which is
    /// exactly why neither may be assumed.
    TimeoutAndAbsent,
    /// Refuse the order.
    Reject(RejectReason),
    /// Accept it, and never mention it on the stream again.
    ///
    /// A stream gap. No reactive trigger fires, because none of them exist
    /// without an event arriving — so the only thing that finds this order is
    /// the stale sweep.
    AcceptButSilent,
    /// Fail the call itself.
    Transport,
    /// Rate limit the call.
    RateLimited,
}

/// One order, as the simulated venue holds it.
#[derive(Debug, Clone, Copy)]
struct SimOrder {
    venue_order_id: VenueOrderId,
    instrument: Instrument,
    side: Side,
    qty: Qty,
    filled: Qty,
    /// Total cost of the fills, so an average can be reported without storing
    /// one — the same reason the ledger keeps a cost basis rather than a mean.
    filled_cost: Money,
    state: OrderState,
    /// Whether the client was ever told this order exists.
    acknowledged: bool,
    /// Whether executions on it reach the stream.
    silent: bool,
}

/// The average price of `cost` spread over `qty`, or `None` when nothing filled.
///
/// Derived rather than accumulated, for the same reason the ledger keeps a cost
/// basis: an average that is updated in place compounds the rounding of every
/// fill before it.
fn average_price(cost: Money, qty: Qty) -> Option<Price> {
    if !qty.is_positive() {
        return None;
    }
    let scaled = i128::from(cost.raw()) * i128::from(SCALE);
    #[expect(
        clippy::integer_division,
        reason = "cost per unit; the single division, truncating toward zero"
    )]
    let per_unit = scaled / i128::from(qty.raw());
    i64::try_from(per_unit).ok().map(Price::from_raw)
}

/// A deterministic venue.
#[derive(Debug, Default)]
pub struct SimVenue {
    orders: HashMap<ClientOrderId, SimOrder>,
    faults: HashMap<ClientOrderId, Fault>,
    stream: VecDeque<VenueEvent>,
    positions: HashMap<Instrument, Qty>,
    next_id: u64,
    next_exec: u64,
    /// Set when the venue should claim a position we never opened, so a boot
    /// divergence can be scripted (R1.12).
    forced_positions: Option<Vec<VenuePosition>>,
    submits: u32,
    lookups: u32,
    cancels: u32,
}

impl SimVenue {
    /// A venue that behaves.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Script a fault for one order, applied when it is submitted.
    pub fn script(&mut self, client_order_id: ClientOrderId, fault: Fault) {
        self.faults.insert(client_order_id, fault);
    }

    /// Fill an order the way a venue would, pushing the execution to the stream.
    ///
    /// # Panics
    /// If the order does not exist. A test filling an order the venue never
    /// took is a test that has already gone wrong, and returning an error would
    /// let it carry on.
    pub fn fill(&mut self, client_order_id: ClientOrderId, qty: Qty, price: Price) {
        let exec_id = self.next_exec_id();
        let order = self
            .orders
            .get_mut(&client_order_id)
            .expect("filling an order the venue never took");
        order.filled = order.filled.checked_add(qty).unwrap_or(order.filled);
        if let Ok(cost) = price.notional(qty) {
            order.filled_cost = order
                .filled_cost
                .checked_add(cost)
                .unwrap_or(order.filled_cost);
        }
        order.state = if order.filled >= order.qty {
            OrderState::Filled
        } else {
            OrderState::Partial
        };
        let (instrument, side, silent) = (order.instrument, order.side, order.silent);

        let entry = self.positions.entry(instrument).or_insert(Qty::ZERO);
        let signed = Qty::from_raw(qty.raw().saturating_mul(side.sign()));
        *entry = entry.checked_add(signed).unwrap_or(*entry);

        if !silent {
            self.stream.push_back(VenueEvent::Fill {
                client_order_id,
                exec_id,
                qty,
                price,
            });
        }
    }

    /// Deliver an execution twice, as an at-least-once stream does.
    ///
    /// # Panics
    /// If the order does not exist.
    pub fn fill_twice(&mut self, client_order_id: ClientOrderId, qty: Qty, price: Price) {
        self.fill(client_order_id, qty, price);
        let last = self
            .stream
            .back()
            .copied()
            .expect("the fill should have reached the stream");
        self.stream.push_back(last);
    }

    /// Cancel an order the way a venue would, unsolicited.
    ///
    /// Self-trade prevention and exchange-side cancels arrive this way, and the
    /// lifecycle has to accept them without our having asked.
    pub fn cancel_unsolicited(&mut self, client_order_id: ClientOrderId) {
        if let Some(order) = self.orders.get_mut(&client_order_id) {
            order.state = OrderState::Canceled;
            self.stream
                .push_back(VenueEvent::CancelConfirmed { client_order_id });
        }
    }

    /// Push a price.
    pub fn mark(&mut self, instrument: Instrument, price: Price) {
        self.stream
            .push_back(VenueEvent::Mark { instrument, price });
    }

    /// Push a balance.
    pub fn balance(&mut self, asset: sentinel_record::Asset, amount: Money) {
        self.stream.push_back(VenueEvent::Balance { asset, amount });
    }

    /// The average price the simulated venue filled this instrument at.
    fn entry_price(&self, instrument: &Instrument) -> Option<Price> {
        let mut cost = Money::ZERO;
        let mut qty = Qty::ZERO;
        let mut ids: Vec<_> = self.orders.keys().collect();
        ids.sort_unstable();
        for id in ids {
            let Some(order) = self.orders.get(id) else {
                continue;
            };
            if order.instrument != *instrument {
                continue;
            }
            cost = cost.checked_add(order.filled_cost.abs()).unwrap_or(cost);
            qty = qty.checked_add(order.filled).unwrap_or(qty);
        }
        average_price(cost, qty)
    }

    /// Claim a position the client has no record of, for a boot divergence.
    pub fn force_positions(&mut self, positions: Vec<VenuePosition>) {
        self.forced_positions = Some(positions);
    }

    /// Whether the venue holds this order at all.
    #[must_use]
    pub fn knows(&self, client_order_id: &ClientOrderId) -> bool {
        self.orders.contains_key(client_order_id)
    }

    /// Whether the client was ever told this order exists.
    ///
    /// False for an order the venue accepted behind a timeout. Exposure the
    /// client cannot know about from anything it was sent is the whole shape of
    /// the problem, and a test asserting on it should be able to say so.
    #[must_use]
    pub fn was_acknowledged(&self, client_order_id: &ClientOrderId) -> bool {
        self.orders
            .get(client_order_id)
            .is_some_and(|o| o.acknowledged)
    }

    /// Calls made, for tests that assert the engine did not retry.
    #[must_use]
    pub const fn call_counts(&self) -> (u32, u32, u32) {
        (self.submits, self.lookups, self.cancels)
    }

    fn next_venue_id(&mut self) -> VenueOrderId {
        self.next_id += 1;
        VenueOrderId::new(&format!("V-{}", self.next_id)).unwrap_or_default()
    }

    fn next_exec_id(&mut self) -> ExecId {
        self.next_exec += 1;
        ExecId::new(&format!("E-{}", self.next_exec)).unwrap_or_default()
    }
}

impl Venue for SimVenue {
    fn submit(&mut self, intent: &EconomicOrderIntent) -> Result<SubmitOutcome, VenueError> {
        self.submits += 1;
        let coid = intent.client_order_id;

        // A fault is consumed on use, so an order that times out and is later
        // reconciled does not time out again on the reconciliation.
        match self.faults.remove(&coid) {
            Some(Fault::Transport) => {
                return Err(VenueError::Transport("simulated".into()));
            }
            Some(Fault::RateLimited) => {
                return Err(VenueError::RateLimited { retry_after: None });
            }
            Some(Fault::Reject(reason)) => return Ok(SubmitOutcome::Rejected(reason)),
            Some(Fault::TimeoutAndAbsent) => return Ok(SubmitOutcome::TimedOut),
            Some(Fault::TimeoutButAccepted) => {
                let venue_order_id = self.next_venue_id();
                self.orders.insert(
                    coid,
                    SimOrder {
                        venue_order_id,
                        instrument: intent.instrument,
                        side: intent.side,
                        qty: intent.qty,
                        filled: Qty::ZERO,
                        filled_cost: Money::ZERO,
                        state: OrderState::Working,
                        // The client was never told. Only a lookup can find it.
                        acknowledged: false,
                        silent: false,
                    },
                );
                return Ok(SubmitOutcome::TimedOut);
            }
            Some(Fault::AcceptButSilent) => {
                let venue_order_id = self.next_venue_id();
                self.orders.insert(
                    coid,
                    SimOrder {
                        venue_order_id,
                        instrument: intent.instrument,
                        side: intent.side,
                        qty: intent.qty,
                        filled: Qty::ZERO,
                        filled_cost: Money::ZERO,
                        state: OrderState::Working,
                        acknowledged: true,
                        silent: true,
                    },
                );
                return Ok(SubmitOutcome::Acked(venue_order_id));
            }
            None => {}
        }

        // A venue rejects a duplicate client order id rather than opening a
        // second position under it. Ours never sends one — the ledger refuses
        // first — but a venue that quietly accepted it would hide that.
        if self.orders.contains_key(&coid) {
            return Ok(SubmitOutcome::Rejected(RejectReason::new(
                -4015,
                "duplicate client order id",
            )));
        }

        let venue_order_id = self.next_venue_id();
        self.orders.insert(
            coid,
            SimOrder {
                venue_order_id,
                instrument: intent.instrument,
                side: intent.side,
                qty: intent.qty,
                filled: Qty::ZERO,
                filled_cost: Money::ZERO,
                state: OrderState::Working,
                acknowledged: true,
                silent: false,
            },
        );
        Ok(SubmitOutcome::Acked(venue_order_id))
    }

    fn cancel(
        &mut self,
        client_order_id: ClientOrderId,
        _venue_order_id: Option<VenueOrderId>,
    ) -> Result<CancelOutcome, VenueError> {
        self.cancels += 1;
        match self.faults.remove(&client_order_id) {
            Some(Fault::Transport) => return Err(VenueError::Transport("simulated".into())),
            Some(Fault::TimeoutAndAbsent | Fault::TimeoutButAccepted) => {
                return Ok(CancelOutcome::TimedOut);
            }
            _ => {}
        }
        let Some(order) = self.orders.get_mut(&client_order_id) else {
            return Ok(CancelOutcome::Absent);
        };
        if order.state.is_terminal() {
            return Ok(CancelOutcome::Absent);
        }
        order.state = OrderState::Canceled;
        let silent = order.silent;
        if !silent {
            self.stream
                .push_back(VenueEvent::CancelConfirmed { client_order_id });
        }
        Ok(CancelOutcome::Requested)
    }

    fn lookup(&mut self, client_order_id: ClientOrderId) -> Result<LookupOutcome, VenueError> {
        self.lookups += 1;
        match self.faults.remove(&client_order_id) {
            Some(Fault::Transport) => return Err(VenueError::Transport("simulated".into())),
            Some(Fault::RateLimited) => {
                return Err(VenueError::RateLimited { retry_after: None });
            }
            Some(Fault::TimeoutAndAbsent | Fault::TimeoutButAccepted) => {
                return Ok(LookupOutcome::TimedOut);
            }
            _ => {}
        }
        // Looked up by client order id, which is the point: an order that was
        // never acknowledged has no venue id to look it up by, and those are
        // precisely the orders that need finding.
        let Some(order) = self.orders.get(&client_order_id) else {
            return Ok(LookupOutcome::Absent);
        };
        Ok(LookupOutcome::Found(OrderSnapshot {
            state: order.state,
            venue_order_id: Some(order.venue_order_id),
            filled_qty: order.filled,
            avg_price: average_price(order.filled_cost, order.filled),
        }))
    }

    fn positions(&mut self) -> Result<Vec<VenuePosition>, VenueError> {
        if let Some(forced) = &self.forced_positions {
            return Ok(forced.clone());
        }
        let mut out: Vec<_> = self
            .positions
            .iter()
            .filter(|(_, q)| !q.is_zero())
            .map(|(instrument, qty)| VenuePosition {
                instrument: *instrument,
                qty: *qty,
                entry_price: self.entry_price(instrument),
            })
            .collect();
        // Sorted: a venue's ordering is its own business, but ours must not
        // vary between runs or a replay stops reproducing.
        out.sort_unstable_by_key(|p| p.instrument);
        Ok(out)
    }

    fn drain_events(&mut self, out: &mut Vec<VenueEvent>) {
        out.extend(self.stream.drain(..));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sentinel_types::{Authority, TraceId};

    fn coid(s: &str) -> ClientOrderId {
        ClientOrderId::new(s).unwrap()
    }

    fn intent(id: &str, side: Side, qty: i64) -> EconomicOrderIntent {
        EconomicOrderIntent::market(
            coid(id),
            Instrument::new("BTCUSD").unwrap(),
            side,
            Qty::whole(qty),
            Authority::Entry,
            TraceId::from_u128(1),
        )
        .unwrap()
    }

    fn drained(v: &mut SimVenue) -> Vec<VenueEvent> {
        let mut out = Vec::new();
        v.drain_events(&mut out);
        out
    }

    #[test]
    fn a_plain_submission_is_acknowledged_and_findable() {
        let mut v = SimVenue::new();
        let outcome = v.submit(&intent("UI-1", Side::Buy, 3)).unwrap();
        assert!(matches!(outcome, SubmitOutcome::Acked(_)));
        assert!(matches!(
            v.lookup(coid("UI-1")).unwrap(),
            LookupOutcome::Found(_)
        ));
    }

    /// The failure the design exists for: no answer, and real exposure.
    #[test]
    fn a_timeout_that_was_accepted_is_findable_by_client_order_id() {
        let mut v = SimVenue::new();
        v.script(coid("UI-1"), Fault::TimeoutButAccepted);
        assert_eq!(
            v.submit(&intent("UI-1", Side::Buy, 3)).unwrap(),
            SubmitOutcome::TimedOut
        );

        // Real exposure the client was never told about.
        assert!(v.knows(&coid("UI-1")));
        assert!(!v.was_acknowledged(&coid("UI-1")));

        // The client has no venue id. Looking it up by ours is the only way,
        // and it finds a live order.
        match v.lookup(coid("UI-1")).unwrap() {
            LookupOutcome::Found(snapshot) => {
                assert_eq!(snapshot.state, OrderState::Working);
                assert!(snapshot.venue_order_id.is_some());
            }
            other => panic!("expected to find it, got {other:?}"),
        }
    }

    #[test]
    fn a_timeout_that_was_not_accepted_is_conclusively_absent() {
        let mut v = SimVenue::new();
        v.script(coid("UI-1"), Fault::TimeoutAndAbsent);
        assert_eq!(
            v.submit(&intent("UI-1", Side::Buy, 3)).unwrap(),
            SubmitOutcome::TimedOut
        );
        assert_eq!(v.lookup(coid("UI-1")).unwrap(), LookupOutcome::Absent);
    }

    #[test]
    fn the_two_timeouts_are_indistinguishable_at_the_moment_they_happen() {
        // Which is exactly why neither may be assumed, and why the only legal
        // next move is to ask.
        let mut accepted = SimVenue::new();
        accepted.script(coid("UI-1"), Fault::TimeoutButAccepted);
        let mut absent = SimVenue::new();
        absent.script(coid("UI-1"), Fault::TimeoutAndAbsent);

        assert_eq!(
            accepted.submit(&intent("UI-1", Side::Buy, 3)).unwrap(),
            absent.submit(&intent("UI-1", Side::Buy, 3)).unwrap()
        );
    }

    #[test]
    fn a_silent_order_reaches_the_stream_never() {
        let mut v = SimVenue::new();
        v.script(coid("UI-1"), Fault::AcceptButSilent);
        v.submit(&intent("UI-1", Side::Buy, 3)).unwrap();
        v.fill(coid("UI-1"), Qty::whole(3), Price::whole(100));

        assert!(drained(&mut v).is_empty(), "the stream gap");
        // But the venue knows, and a lookup finds the truth.
        match v.lookup(coid("UI-1")).unwrap() {
            LookupOutcome::Found(s) => assert_eq!(s.filled_qty, Qty::whole(3)),
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn a_duplicate_execution_appears_twice_on_the_stream() {
        let mut v = SimVenue::new();
        v.submit(&intent("UI-1", Side::Buy, 3)).unwrap();
        v.fill_twice(coid("UI-1"), Qty::whole(1), Price::whole(100));

        let events = drained(&mut v);
        assert_eq!(events.len(), 2);
        assert_eq!(events[0], events[1], "same execution id, twice");
    }

    #[test]
    fn a_rejection_leaves_no_order_behind() {
        let mut v = SimVenue::new();
        v.script(
            coid("UI-1"),
            Fault::Reject(RejectReason::new(-2019, "insufficient margin")),
        );
        assert!(matches!(
            v.submit(&intent("UI-1", Side::Buy, 3)).unwrap(),
            SubmitOutcome::Rejected(_)
        ));
        assert_eq!(v.lookup(coid("UI-1")).unwrap(), LookupOutcome::Absent);
    }

    #[test]
    fn a_transport_failure_is_an_error_not_an_outcome() {
        // The distinction the engine branches on: an error means the call did
        // not happen and can be retried; a timeout means it may have.
        let mut v = SimVenue::new();
        v.script(coid("UI-1"), Fault::Transport);
        let err = v.submit(&intent("UI-1", Side::Buy, 3)).unwrap_err();
        assert!(err.is_retryable());
    }

    #[test]
    fn a_fault_is_consumed_so_a_retry_can_succeed() {
        let mut v = SimVenue::new();
        v.script(coid("UI-1"), Fault::TimeoutAndAbsent);
        assert_eq!(
            v.submit(&intent("UI-1", Side::Buy, 3)).unwrap(),
            SubmitOutcome::TimedOut
        );
        // The second call is a plain one, so a scripted transport failure does
        // not become a permanent one.
        assert!(matches!(
            v.submit(&intent("UI-1", Side::Buy, 3)).unwrap(),
            SubmitOutcome::Acked(_)
        ));
    }

    #[test]
    fn positions_follow_the_fills() {
        let mut v = SimVenue::new();
        v.submit(&intent("UI-1", Side::Buy, 3)).unwrap();
        v.fill(coid("UI-1"), Qty::whole(3), Price::whole(100));
        v.submit(&intent("UI-2", Side::Sell, 1)).unwrap();
        v.fill(coid("UI-2"), Qty::whole(1), Price::whole(110));

        let positions = v.positions().unwrap();
        assert_eq!(positions.len(), 1);
        assert_eq!(positions[0].qty, Qty::whole(2));
    }

    #[test]
    fn positions_can_be_forced_to_disagree_with_us() {
        // A boot divergence: the venue holds something we have no record of.
        let mut v = SimVenue::new();
        v.force_positions(vec![VenuePosition {
            instrument: Instrument::new("BTCUSD").unwrap(),
            qty: Qty::whole(-5),
            entry_price: Some(Price::whole(100)),
        }]);
        assert_eq!(v.positions().unwrap()[0].qty, Qty::whole(-5));
    }

    #[test]
    fn positions_come_back_in_a_stable_order() {
        let mut v = SimVenue::new();
        for (i, symbol) in ["SOLUSD", "BTCUSD", "ETHUSD"].into_iter().enumerate() {
            let intent = EconomicOrderIntent::market(
                coid(&format!("UI-{i}")),
                Instrument::new(symbol).unwrap(),
                Side::Buy,
                Qty::whole(1),
                Authority::Entry,
                TraceId::from_u128(1),
            )
            .unwrap();
            v.submit(&intent).unwrap();
            v.fill(coid(&format!("UI-{i}")), Qty::whole(1), Price::whole(1));
        }
        let names: Vec<_> = v
            .positions()
            .unwrap()
            .iter()
            .map(|p| p.instrument.to_string())
            .collect();
        assert_eq!(names, ["BTCUSD", "ETHUSD", "SOLUSD"]);
    }

    #[test]
    fn an_unsolicited_cancel_arrives_without_our_asking() {
        let mut v = SimVenue::new();
        v.submit(&intent("UI-1", Side::Buy, 3)).unwrap();
        v.cancel_unsolicited(coid("UI-1"));
        assert_eq!(
            drained(&mut v),
            vec![VenueEvent::CancelConfirmed {
                client_order_id: coid("UI-1")
            }]
        );
        assert_eq!(v.call_counts().2, 0, "we never asked");
    }

    #[test]
    fn the_venue_refuses_a_reused_client_order_id() {
        let mut v = SimVenue::new();
        v.submit(&intent("UI-1", Side::Buy, 3)).unwrap();
        assert!(matches!(
            v.submit(&intent("UI-1", Side::Buy, 3)).unwrap(),
            SubmitOutcome::Rejected(_)
        ));
    }

    #[test]
    fn the_same_script_produces_the_same_run() {
        let run = || {
            let mut v = SimVenue::new();
            v.script(coid("UI-1"), Fault::TimeoutButAccepted);
            let first = v.submit(&intent("UI-1", Side::Buy, 3)).unwrap();
            v.submit(&intent("UI-2", Side::Sell, 1)).unwrap();
            v.fill(coid("UI-2"), Qty::whole(1), Price::whole(100));
            (first, drained(&mut v), v.positions().unwrap())
        };
        assert_eq!(run(), run());
    }
}
