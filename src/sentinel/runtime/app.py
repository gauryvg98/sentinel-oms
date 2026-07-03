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
from decimal import Decimal

import asyncpg

from sentinel.broker import BrokerAdapter, BrokerBalanceUpdate
from sentinel.ledger import LedgerStore
from sentinel.metrics import MetricsRegistry
from sentinel.oms import CommandGateway, OrderEngine, WriterCoordinator
from sentinel.protect import ProtectiveExitSupervisor
from sentinel.recon import Reconciler, RecoveryReport

from .supervisor import TaskSupervisor


class ChangeSignal:
    """Event-driven UI notifier. bump() on any state change; wait_past() blocks
    a viewer until the world is newer than what it last rendered. A monotonic
    revision + Condition (not a bare Event) so multiple viewers fan out
    correctly and no wakeup is lost."""

    def __init__(self) -> None:
        self._cond = asyncio.Condition()
        self.revision = 0

    async def bump(self) -> None:
        async with self._cond:
            self.revision += 1
            self._cond.notify_all()

    async def wait_past(self, seen: int, *, timeout: float) -> int:
        async with self._cond:
            try:
                await asyncio.wait_for(
                    self._cond.wait_for(lambda: self.revision > seen), timeout
                )
            except asyncio.TimeoutError:
                pass  # heartbeat: return current revision even if unchanged
            return self.revision


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
        self.changes = ChangeSignal()          # event-driven UI notifications
        # Account balances: seeded once from REST, then kept live by pushed
        # outboundAccountPosition events. No polling. Partial updates merge.
        self.latest_balances: dict[str, Decimal] = {}
        self._broker = broker
        # Bounded: a fill storm exerts backpressure instead of ballooning
        # memory; accepted events are never dropped, producers wait.
        self._events: asyncio.Queue = asyncio.Queue(maxsize=event_queue_size)
        self._accepting = asyncio.Event()

    @property
    def accepting(self) -> bool:
        return self._accepting.is_set()

    @property
    def broker(self) -> BrokerAdapter:
        return self._broker

    # ------------------------------------------------------------ lifecycle

    async def start(self, *, arm_protection: bool = True) -> RecoveryReport:
        # Consumers FIRST: anything recovery or re-arming submits must have
        # its execution reports heard — a fill that lands before the stream
        # subscribes is a silent divergence (found live against Binance).
        self.supervisor.spawn("broker-intake", self._broker_intake)
        self.supervisor.spawn("event-apply", self._event_apply)
        self.supervisor.spawn("reconcile", self._reconcile_loop, restart=True)
        report = await self.recon.startup_recovery()      # 1. recover
        if arm_protection:
            # NOTE: with market-style exits, auto-arm means flatten-on-boot
            # against a real broker. Manual/paper terminals pass False and
            # keep exit authority on the human or the strategy.
            await self.protect.ensure_protection()        # 2. re-arm
        # Seed balances ONCE from REST (consumers already running, so any
        # push during the seed just merges on top — the seed is full truth).
        try:
            self.latest_balances = await self._broker.query_positions()
        except Exception:  # noqa: BLE001 — a slow seed shouldn't block startup
            pass
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
                if isinstance(event, BrokerBalanceUpdate):
                    # Account state, not order lifecycle — never touches the
                    # OMS engine or the ledger. Merge (partial) and notify.
                    self.latest_balances.update(event.balances)
                    self.metrics.inc("balance_updates")
                else:
                    with self.metrics.timer("event_apply_ms"):
                        await self.engine.on_broker_event(event)
                    self.metrics.inc("events_applied")
                await self.changes.bump()                 # push to the UI
            finally:
                self._events.task_done()                  # drain-accounting

    async def _reconcile_loop(self) -> None:
        while True:
            key = await self.engine.needs_reconcile.get()
            await self.recon.reconcile_order(key)
            self.metrics.inc("reconciliations")
            await self.changes.bump()
