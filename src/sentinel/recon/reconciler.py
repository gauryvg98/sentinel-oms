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
    FillReceived,
    OrderState,
    ReconcileResolved,
    ReconcileStarted,
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


@dataclass(slots=True)
class RecoveryReport:
    events_replayed: int = 0
    reconciled: list[str] = field(default_factory=list)
    resolved_states: dict[str, OrderState] = field(default_factory=dict)


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
        stored = await self._store.load_order(client_order_id)
        if stored is None:
            raise ReconciliationDivergence(f"no such order {client_order_id!r}")

        async with self._coord.lock(stored.core.instrument):
            stored = await self._store.load_order(client_order_id)
            trace = uuid4()

            if stored.core.state is not OrderState.RECONCILING:
                stored = await self._store.apply_event(
                    stored, ReconcileStarted(cause="reconciler"), trace
                )

            view = await self._broker.query_order(client_order_id)

            if view is None:
                # Conclusively absent: the broker never accepted this order.
                if stored.core.filled_qty > 0:
                    raise ReconciliationDivergence(
                        f"{client_order_id}: local fills exist but broker has "
                        f"no such order — halt, do not absorb"
                    )
                return await self._store.apply_event(
                    stored,
                    ReconcileResolved(
                        resolved_state=OrderState.CANCELED,
                        broker_order_id=None,
                        filled_qty=Decimal(0),
                    ),
                    trace,
                )

            # Backfill executions we never received. The ledger's exec_id
            # dedup makes this exactly-once no matter how often it reruns.
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
                # We've recorded MORE than the broker shows — we invented
                # exposure the exchange doesn't hold. The dangerous direction:
                # halt, do not absorb.
                raise ReconciliationDivergence(
                    f"{client_order_id}: local filled {booked} > broker {broker}"
                )
            if booked < min(broker, qty):
                # Fills that SHOULD fit in the order are still missing after
                # backfill — a genuine gap, not a mere over-match. Halt.
                raise ReconciliationDivergence(
                    f"{client_order_id}: filled {booked} local vs {broker} broker "
                    f"after backfill"
                )
            if broker > qty:
                # The venue matched a resting order past its size (demo-fapi
                # over-match). The order is booked to its qty and the fills carry
                # true position — log the excess, never halt.
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
        return report
