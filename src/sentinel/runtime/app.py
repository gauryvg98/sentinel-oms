"""SentinelApp — the assembled system under supervision.

Startup order is the R1.11 contract:
    1. startup recovery (rebuild -> reconcile everything non-terminal)
    2. re-arm protection
    3. only then start consumers and accept commands

Shutdown is the no-lost-events contract (R2): stop intake, drain the broker
event queue through the engine, cancel supervised tasks awaiting cleanup.
"""

from __future__ import annotations

import asyncio

import asyncpg

from sentinel.broker import BrokerAdapter
from sentinel.ledger import LedgerStore
from sentinel.metrics import MetricsRegistry
from sentinel.oms import CommandGateway, OrderEngine, WriterCoordinator
from sentinel.protect import ProtectiveExitSupervisor
from sentinel.recon import Reconciler, RecoveryReport

from .supervisor import TaskSupervisor


class SentinelApp:
    def __init__(
        self,
        pool: asyncpg.Pool,
        broker: BrokerAdapter,
        *,
        event_queue_size: int = 1024,
    ) -> None:
        self.store = LedgerStore(pool)
        self.coordinator = WriterCoordinator()
        self.engine = OrderEngine(self.store, broker, self.coordinator)
        self.gateway = CommandGateway(self.store, self.engine)
        self.recon = Reconciler(self.store, broker, self.coordinator)
        self.protect = ProtectiveExitSupervisor(self.store, self.engine)
        self.supervisor = TaskSupervisor()
        self.metrics = MetricsRegistry()
        self._broker = broker
        # Bounded: a fill storm exerts backpressure instead of ballooning
        # memory; accepted events are never dropped, producers wait.
        self._events: asyncio.Queue = asyncio.Queue(maxsize=event_queue_size)
        self._accepting = asyncio.Event()

    @property
    def accepting(self) -> bool:
        return self._accepting.is_set()

    # ------------------------------------------------------------ lifecycle

    async def start(self) -> RecoveryReport:
        report = await self.recon.startup_recovery()      # 1. recover
        await self.protect.ensure_protection()            # 2. re-arm
        self.supervisor.spawn("broker-intake", self._broker_intake)
        self.supervisor.spawn("event-apply", self._event_apply)
        self.supervisor.spawn("reconcile", self._reconcile_loop, restart=True)
        self._accepting.set()                             # 3. open for business
        return report

    async def stop(self) -> None:
        self._accepting.clear()                           # stop intake first
        await self._events.join()                         # drain accepted events
        await self.supervisor.shutdown()                  # cancel + await cleanup

    # ------------------------------------------------------------ consumers

    async def _broker_intake(self) -> None:
        async for event in self._broker.events():
            await self._events.put(event)                 # backpressure point

    async def _event_apply(self) -> None:
        while True:
            event = await self._events.get()
            self.metrics.gauge("queue_depth", self._events.qsize())
            try:
                with self.metrics.timer("event_apply_ms"):
                    await self.engine.on_broker_event(event)
                self.metrics.inc("events_applied")
            finally:
                self._events.task_done()                  # drain-accounting

    async def _reconcile_loop(self) -> None:
        while True:
            key = await self.engine.needs_reconcile.get()
            await self.recon.reconcile_order(key)
            self.metrics.inc("reconciliations")
