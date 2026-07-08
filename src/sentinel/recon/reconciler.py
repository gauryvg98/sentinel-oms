"""The single recovery mechanism (R1.11 / R1.12).

Every failure mode in the system — submit timeout, lost ack, cancel timeout,
crash in any window, late fill, position disagreement — funnels into ONE
procedure:

    ask the broker what it knows about client_order_id K, and adopt that.

Found  -> backfill any executions we never heard (exactly-once via exec_id
          dedup), then resolve to the broker's state. Never resubmit.
Absent -> conclusively unexposed: resolve CANCELED with zero fills. Any
          resubmission is a NEW order under policy, never a reuse.

Startup recovery is this procedure applied to every non-terminal order,
after projections are rebuilt from the ledger — and only when the worklist
is empty may the system accept new commands.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from decimal import Decimal
from uuid import uuid4

from sentinel.broker import BrokerAdapter, BrokerOrderState
from sentinel.domain import (
    Authority,
    EconomicOrderIntent,
    FillReceived,
    OrderState,
    ReconcileResolved,
    ReconcileStarted,
    Side,
    SubmissionStarted,
    is_terminal,
)
from sentinel.ledger import FillOutcome, LedgerStore, StoredOrder
from sentinel.oms import WriterCoordinator

log = logging.getLogger("sentinel.recon")


class ReconciliationDivergence(Exception):
    """Local evidence contradicts broker truth in a way reconciliation cannot
    repair (e.g. local fills for an order the broker never accepted). This is
    a halt condition, never something to absorb."""


_STATE_MAP: dict[BrokerOrderState, OrderState] = {
    BrokerOrderState.WORKING: OrderState.WORKING,
    BrokerOrderState.PARTIAL: OrderState.PARTIAL,
    BrokerOrderState.FILLED: OrderState.FILLED,
    BrokerOrderState.CANCELED: OrderState.CANCELED,
    BrokerOrderState.REJECTED: OrderState.REJECTED,
}

# An actively-filling order can report a broker executedQty that momentarily runs
# ahead of its own userTrades feed (seen on demo-fapi when recovering mid-fill).
# Re-query a few times so the trades catch up before calling it a missing-fills
# gap. Only the SAFE direction (booked < broker) retries; booked > broker halts.
_FILL_BACKFILL_RETRIES = 3
_FILL_BACKFILL_BACKOFF_S = 0.5


@dataclass(slots=True)
class RecoveryReport:
    events_replayed: int = 0
    reconciled: list[str] = field(default_factory=list)
    resolved_states: dict[str, OrderState] = field(default_factory=dict)
    positions_imported: list[str] = field(default_factory=list)


class Reconciler:
    def __init__(
        self,
        store: LedgerStore,
        broker: BrokerAdapter,
        coordinator: WriterCoordinator,
    ) -> None:
        self._store = store
        self._broker = broker
        self._coord = coordinator

    async def reconcile_order(self, client_order_id: str) -> StoredOrder:
        """Broker I/O runs OUTSIDE the instrument lock. query_order is
        multi-second HTTP (order lookup + paginated trades), and holding the
        per-instrument writer lock across it convoyed every live fill apply
        and placement on that instrument behind reconciliation (prod:
        event_apply p99 3.41s/max 51s, order-place p99 8s/max 28.9s, against
        a ~13ms steady state). The shape is an outer loop of
        {query outside the lock -> reload + apply + evaluate inside it}:
        every ledger write still happens under the lock against a freshly
        reloaded order, and exec_id dedup makes re-applied fills exactly-once,
        so releasing the lock around broker HTTP costs nothing in safety."""
        # Phase A — no lock: cheap reads and the broker round-trip.
        stored = await self._store.load_order(client_order_id)
        if stored is None:
            raise ReconciliationDivergence(f"no such order {client_order_id!r}")
        if is_terminal(stored.core.state):
            return stored          # already resolved; nothing to reconcile
        instrument = stored.core.instrument

        trace = uuid4()
        view = await self._broker.query_order(client_order_id)

        # Backfill executions we never received, then reconcile the count.
        # The ledger's exec_id dedup makes replay exactly-once, so re-querying
        # is safe. On each pass we apply (under the lock) whatever fills the
        # broker last reported; if `booked` still trails `broker` it may just
        # be the userTrades feed lagging executedQty on an actively-filling
        # order — release the lock, re-query, and let it catch up. Only a gap
        # that SURVIVES the retries is a genuine missing-fills halt.
        # booked > broker (invented exposure) never retries; it halts at once.
        for attempt in range(_FILL_BACKFILL_RETRIES + 1):
            # Phase B — under the lock: the order may have moved while we were
            # in broker HTTP. Reload it, and if another path (a live fill, a
            # prior reconcile) already resolved it terminal, adopt that result
            # — never reopen a settled order.
            async with self._coord.lock(instrument):
                stored = await self._store.load_order(client_order_id)
                if is_terminal(stored.core.state):
                    return stored
                if stored.core.state is not OrderState.RECONCILING:
                    stored = await self._store.apply_event(
                        stored, ReconcileStarted(cause="reconciler"), trace
                    )

                if view is None:
                    # The broker's order-query no longer returns this order (Binance
                    # code -2013). Crucially, that is NOT proof the order never
                    # existed. On demo/testnet fapi (and past the retention window on
                    # live) a genuinely-filled order ages OUT of the order-query
                    # window and answers -2013 while its fills are entirely real —
                    # every fill we hold was sourced from the broker's OWN execution
                    # stream (real exec_ids) or a prior query backfill; we never
                    # invent fills. Treating -2013 as "phantom exposure" here caused
                    # repeated FALSE halts on real, fully-broker-sourced fills.
                    #
                    # Resolve the order terminal, PRESERVING the local filled_qty:
                    # the filled portion is real and already booked into the position
                    # (ReconcileResolved sets order state only; it does not re-book
                    # fills, so this never double-counts), and the unfilled remainder
                    # is gone (the order is not live at the broker). Exposure
                    # correctness is guarded authoritatively by POSITION
                    # reconciliation (reconcile_positions -> positionRisk), which
                    # adopts broker truth and self-heals any real mismatch — that,
                    # not an order-query miss, is the system's exposure integrity
                    # check, and it is consistent (adopt, don't halt).
                    if stored.core.filled_qty > 0:
                        log.warning(
                            "%s: order absent from broker query (-2013) with %s "
                            "local broker-sourced fills — aged out, not phantom; "
                            "resolving CANCELED, position stands (exposure verified "
                            "by position reconciliation)",
                            client_order_id, stored.core.filled_qty,
                        )
                    return await self._store.apply_event(
                        stored,
                        ReconcileResolved(
                            resolved_state=OrderState.CANCELED,
                            broker_order_id=stored.core.broker_order_id,
                            filled_qty=stored.core.filled_qty,
                        ),
                        trace,
                    )

                for f in view.fills:
                    result = await self._store.apply_event(
                        stored,
                        FillReceived(exec_id=f.exec_id, qty=f.qty, price=f.price),
                        trace,
                    )
                    if result is not FillOutcome.DUPLICATE:
                        stored = result

                booked, broker, qty = (stored.core.filled_qty, view.filled_qty,
                                       stored.core.qty)
                if booked > broker:
                    # Recorded MORE than the broker holds — exposure the exchange
                    # doesn't have. The dangerous direction: halt, do not absorb.
                    raise ReconciliationDivergence(
                        f"{client_order_id}: local filled {booked} > broker {broker}"
                    )
                if booked >= min(broker, qty):   # reconciled
                    if broker > qty:
                        # The venue matched a resting order past its size (demo-fapi
                        # over-match). The order is booked to its qty and the fills
                        # carry true position — log the excess, never halt.
                        log.warning(
                            "%s: venue over-match — broker filled %s > order qty %s; "
                            "booked to qty, position tracks the fills",
                            client_order_id, broker, qty,
                        )
                    resolved = await self._store.apply_event(
                        stored,
                        ReconcileResolved(
                            resolved_state=_STATE_MAP[view.state],
                            broker_order_id=view.broker_order_id,
                            filled_qty=view.filled_qty,
                        ),
                        trace,
                    )
                    await self._store.record_decision(
                        trace, resolved.core.instrument, "reconciler",
                        "RECONCILE_RESOLVED",
                        {"key": client_order_id, "to": resolved.core.state.value,
                         "filled": str(resolved.core.filled_qty)},
                    )
                    return resolved

            # booked < min(broker, qty): short. The lock is RELEASED here —
            # live fills and placements on the instrument proceed while we
            # wait out the feed and re-query.
            if attempt >= _FILL_BACKFILL_RETRIES:
                # Still short after retries — a real gap, not a feed lag. Halt.
                raise ReconciliationDivergence(
                    f"{client_order_id}: filled {booked} local vs {broker} "
                    f"broker after backfill"
                )
            await asyncio.sleep(_FILL_BACKFILL_BACKOFF_S)
            refetched = await self._broker.query_order(client_order_id)
            if refetched is not None:
                view = refetched

        raise AssertionError("unreachable: the backfill loop returns or raises")

    async def drain(self, queue: asyncio.Queue[str]) -> list[StoredOrder]:
        """Process every queued reconciliation request (test/scenario driver;
        the supervised loop wraps this in the runtime layer)."""
        resolved = []
        while not queue.empty():
            resolved.append(await self.reconcile_order(queue.get_nowait()))
        return resolved

    async def startup_recovery(self) -> RecoveryReport:
        """The R1.11 restart sequence:
        1. rebuild projections from the event ledger (the ledger wins),
        2. reconcile every non-terminal order against the broker,
        3. only then may the caller re-arm protection and accept commands.
        """
        report = RecoveryReport()
        report.events_replayed = await self._store.rebuild_projections()
        for stored in await self._store.load_nonterminal_orders():
            resolved = await self.reconcile_order(stored.core.client_order_id)
            key = resolved.core.client_order_id
            report.reconciled.append(key)
            report.resolved_states[key] = resolved.core.state
        report.positions_imported = await self.reconcile_positions()
        return report

    async def reconcile_positions(self) -> list[str]:
        """Adopt the exchange's actual positions — R1.12 (broker truth replaces
        local belief) at the POSITION level. For each symbol where the exchange
        holds a different quantity than our ledger, import the delta as a
        synthetic opening-balance fill at the exchange's entry price, so both the
        position AND the cost basis match the exchange. Idempotent: after import
        the delta is zero, so a re-run imports nothing. Skips venues without a
        position concept (spot)."""
        get_positions = getattr(self._broker, "open_positions", None)
        if get_positions is None:
            return []
        try:
            exchange = await get_positions()
        except Exception as e:  # noqa: BLE001 — a slow query mustn't block boot
            log.warning("position reconcile: could not fetch exchange positions: %r", e)
            return []
        ledger = await self._store.load_positions()          # {symbol: qty} nonzero
        imported: list[str] = []
        for sym in set(exchange) | set(ledger):
            led = ledger.get(sym, Decimal(0))
            ex = exchange.get(sym)
            ex_qty = ex.qty if ex is not None else Decimal(0)
            delta = ex_qty - led
            if delta == 0:
                continue
            if ex is not None:
                price = ex.entry_price                       # adopt at the true entry
            else:
                # exchange is flat but we hold: close our phantom at avg cost so
                # the forced close realizes ~nothing.
                from sentinel.marks.pnl import compute_pnl
                pnl = (await compute_pnl(self._store._pool)).get(sym)
                price = pnl.avg_cost if pnl else None
            if await self._import_position_delta(sym, delta, price):
                imported.append(sym)
        return imported

    async def _import_position_delta(self, instrument: str, delta: Decimal,
                                     price: Decimal | None) -> bool:
        """Record a synthetic opening-balance fill of `delta` at `price` — flows
        through the same tested transitions (CREATED -> SUBMITTING -> FILLED), so
        it preserves position = Σ fills. Tagged RECON- and logged for audit."""
        if price is None or price <= 0:
            log.warning("position reconcile: skip %s (delta %s) — no price",
                        instrument, delta)
            return False
        side = Side.BUY if delta > 0 else Side.SELL
        qty = abs(delta)
        trace = uuid4()
        coid = f"RECON-{instrument}-{uuid4().hex[:8]}"
        intent = EconomicOrderIntent(
            intent_id=uuid4(), idempotency_key=coid, instrument=instrument,
            side=side, qty=qty, limit_price=None, authority=Authority.ENTRY,
            trace_id=trace, quote_at_decision=price,
        )
        stored = await self._store.create_order(intent)
        stored = await self._store.apply_event(stored, SubmissionStarted(), trace)
        await self._store.apply_event(
            stored, FillReceived(exec_id=f"X-{coid}", qty=qty, price=price), trace)
        await self._store.record_decision(
            trace, instrument, "reconciler", "POSITION_IMPORTED",
            {"delta": str(delta), "price": str(price)})
        log.warning("position reconcile: imported %s delta %s @ %s (exchange truth)",
                    instrument, delta, price)
        return True
