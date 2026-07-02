"""Scenario harness: one place to build the full stack against a scripted
broker, so the acceptance tests read like the requirements they prove."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from uuid import uuid4

from sentinel.broker.sim import BrokerScript, ScriptedBroker
from sentinel.domain import Authority, EconomicOrderIntent, Side
from sentinel.ledger import LedgerStore
from sentinel.oms import CommandGateway, OrderEngine, WriterCoordinator
from sentinel.protect import ProtectiveExitSupervisor
from sentinel.recon import Reconciler


@dataclass(slots=True)
class Stack:
    store: LedgerStore
    sim: ScriptedBroker
    engine: OrderEngine
    gateway: CommandGateway
    recon: Reconciler
    protect: ProtectiveExitSupervisor

    async def pump(self, steps: int = 1) -> None:
        """Advance virtual time and apply everything the broker delivered."""
        for _ in range(steps):
            self.sim.step()
            for event in self.sim.take_events():
                await self.engine.on_broker_event(event)

    def restart(self) -> "Stack":
        """A 'process restart': fresh objects, same database pool and same
        broker truth — exactly what survives a real crash."""
        return build(self.store._pool, sim=self.sim)  # noqa: SLF001


def build(pool, script: BrokerScript | None = None,
          sim: ScriptedBroker | None = None) -> Stack:
    store = LedgerStore(pool)
    sim = sim or ScriptedBroker(script or BrokerScript())
    coord = WriterCoordinator()
    engine = OrderEngine(store, sim, coord)
    return Stack(
        store=store,
        sim=sim,
        engine=engine,
        gateway=CommandGateway(store, engine),
        recon=Reconciler(store, sim, coord),
        protect=ProtectiveExitSupervisor(store, engine),
    )


def intent(key="K1", qty="4", side=Side.BUY, authority=Authority.ENTRY,
           instrument="IDX-OPT", limit="4.20") -> EconomicOrderIntent:
    return EconomicOrderIntent(
        intent_id=uuid4(),
        idempotency_key=key,
        instrument=instrument,
        side=side,
        qty=Decimal(qty),
        limit_price=Decimal(limit) if limit else None,
        authority=authority,
        trace_id=uuid4(),
    )
