"""ScriptedBroker — a deterministic broker with real broker-side truth.

Time is virtual: nothing happens except inside ``step()``, which the test (or
load harness) drives. The simulator maintains its OWN order/position state —
the "broker truth" the reconciler converges against — which is what lets it
accept an order whose ack it dropped (R1.14) or keep executing during a
cancel window (R1.6).
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from decimal import Decimal
from typing import AsyncIterator

from sentinel.domain import Side

from ..adapter import (
    BrokerCancelConfirmed,
    BrokerEvent,
    BrokerFill,
    BrokerOrderState,
    BrokerOrderView,
    BrokerReject,
    BrokerTimeout,
)
from .script import BrokerScript, CancelBehavior

_DEFAULT_CANCEL = CancelBehavior()


@dataclass(slots=True)
class _SimOrder:
    client_order_id: str
    broker_order_id: str
    instrument: str
    side: Side
    qty: Decimal
    filled_qty: Decimal = Decimal(0)
    state: BrokerOrderState = BrokerOrderState.WORKING
    cancel_confirm_at: int | None = None
    fills: list[BrokerFill] = field(default_factory=list)


class ScriptedBroker:
    def __init__(self, script: BrokerScript) -> None:
        self._script = script
        self._step = 0
        self._orders: dict[str, _SimOrder] = {}
        self._pending: list[tuple[int, BrokerEvent]] = []  # (deliver_at, event)
        self._emitted: list[BrokerEvent] = []              # take_events() buffer
        self._queue: asyncio.Queue[BrokerEvent] = asyncio.Queue()
        self._broker_seq = 0
        self._exec_seq: dict[str, int] = {}

    # ------------------------------------------------------------ adapter API

    async def submit(
        self,
        *,
        client_order_id: str,
        instrument: str,
        side: Side,
        qty: Decimal,
        limit_price: Decimal | None,
    ) -> str:
        behavior = self._script.submits.get(client_order_id)
        if behavior and behavior.reject_reason is not None:
            raise BrokerReject(behavior.reject_reason)

        register = True
        if behavior and behavior.timeout and not behavior.accept_on_timeout:
            register = False

        if register:
            if client_order_id in self._orders:
                # Broker-side idempotency on client id: same order, same id.
                return self._orders[client_order_id].broker_order_id
            self._broker_seq += 1
            order = _SimOrder(
                client_order_id=client_order_id,
                broker_order_id=f"SIM-{self._broker_seq}",
                instrument=instrument,
                side=side,
                qty=qty,
            )
            self._orders[client_order_id] = order

        if behavior and behavior.timeout:
            # The caller gets nothing — no id, no proof. Broker truth may
            # nevertheless hold a WORKING order (accept_on_timeout).
            raise BrokerTimeout(f"submit {client_order_id}: no response")
        return self._orders[client_order_id].broker_order_id

    async def cancel(self, client_order_id: str) -> None:
        behavior = self._script.cancels.get(client_order_id) or _DEFAULT_CANCEL
        if behavior.timeout:
            raise BrokerTimeout(f"cancel {client_order_id}: no response")
        order = self._orders.get(client_order_id)
        if order is None or order.state in (
            BrokerOrderState.FILLED,
            BrokerOrderState.CANCELED,
            BrokerOrderState.REJECTED,
        ):
            return  # nothing live to cancel; brokers ack this quietly
        order.cancel_confirm_at = self._step + max(1, behavior.confirm_after_steps)

    async def query_order(self, client_order_id: str) -> BrokerOrderView | None:
        order = self._orders.get(client_order_id)
        if order is None:
            return None  # conclusively absent: never accepted
        return BrokerOrderView(
            client_order_id=order.client_order_id,
            broker_order_id=order.broker_order_id,
            state=order.state,
            filled_qty=order.filled_qty,
            fills=tuple(order.fills),
        )

    async def query_positions(self) -> dict[str, Decimal]:
        positions: dict[str, Decimal] = {}
        for order in self._orders.values():
            for f in order.fills:
                signed = f.qty if order.side is Side.BUY else -f.qty
                positions[order.instrument] = (
                    positions.get(order.instrument, Decimal(0)) + signed
                )
        return positions

    async def events(self) -> AsyncIterator[BrokerEvent]:
        while True:
            yield await self._queue.get()

    # -------------------------------------------------------------- test API

    def step(self) -> None:
        """Advance virtual time by one tick. Deterministic: cancel confirms,
        then executions, then deliveries — always in that order, always in
        schedule order."""
        self._step += 1

        for order in self._orders.values():
            if (
                order.cancel_confirm_at == self._step
                and order.state in (BrokerOrderState.WORKING, BrokerOrderState.PARTIAL)
            ):
                order.state = BrokerOrderState.CANCELED
                self._deliver_now_or_later(
                    BrokerCancelConfirmed(client_order_id=order.client_order_id), None
                )

        for key, scheduled in self._script.fills.items():
            order = self._orders.get(key)
            if order is None:
                continue
            for sf in scheduled:
                if sf.execute_at != self._step:
                    continue
                if order.state not in (
                    BrokerOrderState.WORKING,
                    BrokerOrderState.PARTIAL,
                ):
                    continue  # canceled/filled broker-side: execution can't happen
                n = self._exec_seq.get(key, 0) + 1
                self._exec_seq[key] = n
                fill = BrokerFill(
                    client_order_id=key,
                    exec_id=f"{key}-E{n}",
                    qty=sf.qty,
                    price=sf.price,
                )
                order.filled_qty += sf.qty
                order.fills.append(fill)
                order.state = (
                    BrokerOrderState.FILLED
                    if order.filled_qty >= order.qty
                    else BrokerOrderState.PARTIAL
                )
                self._deliver_now_or_later(fill, sf.deliver_at)

        for redelivery in self._script.redeliveries:
            if redelivery.at_step != self._step:
                continue
            for order in self._orders.values():
                for f in order.fills:
                    if f.exec_id == redelivery.exec_id:
                        self._emit(f)

        still_pending: list[tuple[int, BrokerEvent]] = []
        for deliver_at, event in self._pending:
            if deliver_at <= self._step:
                self._emit(event)
            else:
                still_pending.append((deliver_at, event))
        self._pending = still_pending

    def take_events(self) -> list[BrokerEvent]:
        """Drain everything delivered so far (deterministic test access)."""
        out, self._emitted = self._emitted, []
        return out

    @property
    def current_step(self) -> int:
        return self._step

    # ------------------------------------------------------------- internals

    def _deliver_now_or_later(self, event: BrokerEvent, deliver_at: int | None) -> None:
        if deliver_at is None or deliver_at <= self._step:
            self._emit(event)
        else:
            self._pending.append((deliver_at, event))

    def _emit(self, event: BrokerEvent) -> None:
        self._emitted.append(event)
        self._queue.put_nowait(event)
