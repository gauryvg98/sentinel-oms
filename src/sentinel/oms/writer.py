"""OrderEngine — the single-writer pipeline from intent to broker and back.

Every state-changing path runs under the instrument lock:
  place:   guards -> durable intent -> SUBMITTING -> broker -> ACK/REJECT/UNKNOWN
  cancel:  CANCEL_PENDING -> broker cancel request
  events:  fills / cancel confirms applied through the ledger (dedup inside)

The engine never resolves UNKNOWN itself — it only parks orders there and
queues them for the reconciler. Timeout handling is therefore three lines:
apply SubmissionTimedOut, enqueue for reconciliation, return. No retry exists
on this path AT ALL (R1.3): the code that could blind-resubmit was never written.
"""

from __future__ import annotations

import asyncio
import logging
from decimal import Decimal
from uuid import UUID, uuid4

log = logging.getLogger("sentinel.oms")

from sentinel.broker import (
    BrokerAdapter,
    BrokerCancelConfirmed,
    BrokerEvent,
    BrokerFill,
    BrokerReject,
    BrokerTimeout,
)
from sentinel.domain import (
    BrokerAcked,
    BrokerRejected,
    CancelConfirmed,
    CancelRequested,
    FillReceived,
    OrderState,
    ReconcileStarted,
    RequiresReconciliation,
    SubmissionStarted,
    SubmissionTimedOut,
    EconomicOrderIntent,
)
from sentinel.ledger import FillOutcome, LedgerStore, StoredOrder

from .coordinator import WriterCoordinator
from .guards import ExposureGuards


class OrderEngine:
    def __init__(
        self,
        store: LedgerStore,
        broker: BrokerAdapter,
        coordinator: WriterCoordinator | None = None,
        *,
        max_position: Decimal | None = None,
    ) -> None:
        self._store = store
        self._broker = broker
        self._coord = coordinator or WriterCoordinator()
        self._guards = ExposureGuards(store, max_position=max_position)
        # client_order_ids awaiting reconciliation (consumed by recon layer).
        self.needs_reconcile: asyncio.Queue[str] = asyncio.Queue()

    # ----------------------------------------------------------------- place

    async def place(self, intent: EconomicOrderIntent) -> StoredOrder:
        async with self._coord.lock(intent.instrument):
            # Idempotency outranks guards: a replay of an existing intent is
            # not new exposure, so it must short-circuit BEFORE the guards
            # can refuse it (guards exist to block NEW exposure only).
            existing = await self._store.load_order(intent.idempotency_key)
            if existing is not None:
                # If it's frozen mid-submission (crash window), reconciliation
                # — not a resubmit — is the only safe wake-up.
                if existing.core.state is OrderState.SUBMITTING:
                    existing = await self._store.apply_event(
                        existing, ReconcileStarted(cause="replayed mid-submission"),
                        intent.trace_id,
                    )
                    await self.needs_reconcile.put(existing.core.client_order_id)
                return existing

            intent = await self._guards.apply(intent)
            stored = await self._store.create_order(intent)

            stored = await self._store.apply_event(
                stored, SubmissionStarted(), intent.trace_id
            )
            try:
                broker_id = await self._broker.submit(
                    client_order_id=stored.core.client_order_id,
                    instrument=intent.instrument,
                    side=intent.side,
                    qty=stored.core.qty,
                    limit_price=intent.limit_price,
                )
            except BrokerReject as e:
                return await self._store.apply_event(
                    stored, BrokerRejected(reason=e.reason), intent.trace_id
                )
            except BrokerTimeout:
                # NOT a rejection (R1.3). Park in UNKNOWN — which also holds
                # the instrument against new entries (R1.4) — and hand to recon.
                stored = await self._store.apply_event(
                    stored, SubmissionTimedOut(), intent.trace_id
                )
                await self.needs_reconcile.put(stored.core.client_order_id)
                return stored
            return await self._store.apply_event(
                stored, BrokerAcked(broker_order_id=broker_id), intent.trace_id
            )

    # ---------------------------------------------------------------- cancel

    async def request_cancel(self, client_order_id: str, trace_id: UUID) -> StoredOrder:
        stored = await self._require(client_order_id)
        async with self._coord.lock(stored.core.instrument):
            stored = await self._require(client_order_id)  # reload under lock
            stored = await self._store.apply_event(
                stored, CancelRequested(), trace_id
            )
            try:
                await self._broker.cancel(client_order_id)
            except BrokerTimeout:
                # Cancel outcome unprovable: the order may fill or cancel.
                # Reconciliation resolves it; we never assume either way.
                stored = await self._store.apply_event(
                    stored, ReconcileStarted(cause="cancel timeout"), trace_id
                )
                await self.needs_reconcile.put(client_order_id)
            return stored

    # ---------------------------------------------------------- broker events

    async def on_broker_event(self, event: BrokerEvent) -> None:
        stored = await self._store.load_order(event.client_order_id)
        if stored is None:
            # An execution/update for an order we have NO intent for — e.g. an
            # orphan order left on the account by another process. Surface it
            # loudly, but DISOWN and continue: one stray id must not take the
            # whole fleet down. We never book exposure we didn't create; any real
            # exposure it produced is caught by POSITION reconciliation against
            # broker truth (the considered retry-then-halt path), not by crashing
            # the event-apply loop here.
            log.error(
                "disowning broker event for unknown client_order_id %r (%s) — "
                "not in the ledger; continuing",
                event.client_order_id, type(event).__name__,
            )
            return
        async with self._coord.lock(stored.core.instrument):
            stored = await self._store.load_order(event.client_order_id)
            trace = uuid4()
            match event:
                case BrokerFill(exec_id=e, qty=q, price=p):
                    try:
                        result = await self._store.apply_event(
                            stored,
                            FillReceived(exec_id=e, qty=q, price=p),
                            trace,
                        )
                        if result is FillOutcome.DUPLICATE:
                            return  # replayed delivery: full no-op (R1.2/R1.7)
                    except RequiresReconciliation:
                        # Late fill for a terminal/unknown order (R1.7):
                        # evidence, not application — reopen via recon.
                        stored = await self._store.apply_event(
                            stored, ReconcileStarted(cause=f"late fill {e}"), trace
                        )
                        await self.needs_reconcile.put(stored.core.client_order_id)
                case BrokerCancelConfirmed():
                    stored = await self._store.apply_event(
                        stored, CancelConfirmed(), trace
                    )

    # -------------------------------------------------------------- internals

    async def _require(self, client_order_id: str) -> StoredOrder:
        stored = await self._store.load_order(client_order_id)
        if stored is None:
            raise RuntimeError(f"no such order {client_order_id!r}")
        return stored
