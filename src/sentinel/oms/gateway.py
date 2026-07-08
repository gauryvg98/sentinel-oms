"""Command gateway — the idempotent front door.

Thin by design: durable command dedup (R1.2), then delegate to the engine.
A replayed command returns the existing order's current state — the caller
cannot tell (and must not care) whether it raced a duplicate.
"""

from __future__ import annotations

from uuid import UUID

from sentinel.domain import EconomicOrderIntent, OrderState
from sentinel.ledger import LedgerStore, StoredOrder

from .errors import PlacementBlocked
from .writer import OrderEngine


class CommandGateway:
    def __init__(self, store: LedgerStore, engine: OrderEngine) -> None:
        self._store = store
        self._engine = engine

    async def place(
        self, command_id: UUID, intent: EconomicOrderIntent
    ) -> StoredOrder:
        fresh = await self._store.record_command(
            command_id,
            intent.trace_id,
            "PLACE",
            {
                "idempotency_key": intent.idempotency_key,
                "instrument": intent.instrument,
                "side": intent.side.value,
                "qty": str(intent.qty),           # requested (pre-clamp) — audit
                "authority": intent.authority.value,
                "quote_at_decision": str(intent.quote_at_decision)
                if intent.quote_at_decision is not None else None,
                "stop_price": str(intent.stop_price)
                if intent.stop_price is not None else None,
            },
        )
        if not fresh:
            existing = await self._store.load_order(intent.idempotency_key)
            if existing is not None:
                return existing
            # Crash landed between command insert and order creation: the
            # intent pipeline is idempotent, so falling through is safe.
        try:
            stored = await self._engine.place(intent)
        except PlacementBlocked as e:
            # The trades that DIDN'T happen are audit-worthy too.
            await self._store.record_decision(
                intent.trace_id, intent.instrument, "guards",
                "PLACE_BLOCKED",
                {"key": intent.idempotency_key, "reason": type(e).__name__,
                 "message": str(e)},
            )
            raise
        decision = (
            "PARKED_UNKNOWN" if stored.core.state is OrderState.UNKNOWN
            else f"{stored.authority}_PLACED"
        )
        detail = {"key": stored.core.client_order_id, "qty": str(stored.core.qty)}
        if stored.core.qty != intent.qty:
            detail["requested_qty"] = str(intent.qty)   # exit was clamped
        await self._store.record_decision(
            intent.trace_id, intent.instrument, "gateway", decision, detail
        )
        return stored

    async def cancel(
        self, command_id: UUID, client_order_id: str, trace_id: UUID
    ) -> StoredOrder:
        fresh = await self._store.record_command(
            command_id, trace_id, "CANCEL", {"client_order_id": client_order_id}
        )
        if not fresh:
            stored = await self._store.load_order(client_order_id)
            if stored is not None:
                return stored
        return await self._engine.request_cancel(client_order_id, trace_id)
