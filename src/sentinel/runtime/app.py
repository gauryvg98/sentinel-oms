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

from sentinel.broker import BrokerAdapter, BrokerBalanceUpdate, BrokerTimeout
from sentinel.ledger import LedgerStore
from sentinel.metrics import MetricsRegistry
from sentinel.oms import CommandGateway, OrderEngine, WriterCoordinator
from sentinel.protect import ProtectiveExitSupervisor
from sentinel.recon import Reconciler, ReconciliationDivergence, RecoveryReport

from .single_writer import SingleWriterLock
from .supervisor import TaskSupervisor

# Reconcile retry policy: a transient broker/network error must NOT strand the
# order (which would hold the instrument forever). Re-enqueue with backoff up
# to a cap; beyond the cap the failure is treated as fatal, not transient.
_RECON_BACKOFF_S = 2.0
_RECON_MAX_RETRIES = 5


class ChangeSignal:
    """Event-driven UI notifier, per TOPIC. bump(topic) on a state change;
    wait_past() blocks a viewer until the world is newer than what it last
    rendered, and topics_since() tells it WHICH topics moved so it can patch
    just those (one bot card, or 'account') instead of reflushing everything.
    A monotonic revision + Condition (not a bare Event) so multiple viewers fan
    out correctly and no wakeup is lost."""

    def __init__(self) -> None:
        self._cond = asyncio.Condition()
        self.revision = 0
        self._topics: dict[str, int] = {}     # topic -> revision at last change

    async def bump(self, topic: str = "account") -> None:
        async with self._cond:
            self.revision += 1
            self._topics[topic] = self.revision
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

    def topics_since(self, seen: int) -> list[str]:
        """Topics whose last change is newer than `seen` — the set a viewer at
        revision `seen` still needs to be told about."""
        return [t for t, rev in self._topics.items() if rev > seen]


class SentinelApp:
    def __init__(
        self,
        pool: asyncpg.Pool,
        broker: BrokerAdapter,
        *,
        event_queue_size: int = 1024,
        dsn: str | None = None,
        account: str = "sentinel",
        max_position: Decimal | None = None,
    ) -> None:
        # dsn set -> enforce single-writer: this process must win an
        # account-scoped advisory lock at start() or refuse to boot.
        self._writer_lock = SingleWriterLock(dsn, account) if dsn else None
        self.store = LedgerStore(pool)
        self.coordinator = WriterCoordinator()
        self.engine = OrderEngine(self.store, broker, self.coordinator,
                                  max_position=max_position)
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
        # FIRST of all: claim exclusive ownership of the account. If another
        # process holds it, raise AnotherWriterActive and boot nothing — the
        # two-writer interleave that HALTED us before can't even begin.
        if self._writer_lock is not None:
            await self._writer_lock.acquire()
            self.supervisor.spawn(
                "writer-lock", self._writer_lock.guard, restart=False
            )  # losing the lock is fatal: halt, don't retry

        # Consumers next: anything recovery or re-arming submits must have
        # its execution reports heard — a fill that lands before the stream
        # subscribes is a silent divergence (found live against Binance).
        self.supervisor.spawn("broker-intake", self._broker_intake)
        self.supervisor.spawn("event-apply", self._event_apply)
        # restart=False: this loop is integrity-critical. It handles its OWN
        # transient retries internally (see _reconcile_loop); the only thing
        # that exits it is a ReconciliationDivergence or an exhausted retry —
        # both of which SHOULD halt the account, not silently respawn.
        self.supervisor.spawn("reconcile", self._reconcile_loop, restart=False)
        report = await self.recon.startup_recovery()      # 1. recover
        if report.positions_imported:
            print(f"  RECONCILED positions from exchange: "
                  f"{', '.join(report.positions_imported)}", flush=True)
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
        if self._writer_lock is not None:
            await self._writer_lock.release()             # free the account

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
        """Single recovery mechanism. Two failure modes, handled distinctly:

        - ReconciliationDivergence: the broker and the ledger disagree on
          exposure. This is the designated halt-and-scream condition ("halt,
          do not absorb"). Let it propagate — spawned restart=False, so the
          supervisor halts. NEVER swallow it.
        - Connectivity (BrokerTimeout: connect/read timeout, transport error):
          we learned NOTHING — no answer from the broker. Halting here is pure
          downside: it stops SL/TP management while we STILL can't see the
          broker, so it raises risk instead of reducing it. Retry indefinitely
          with backoff; the order stays RECONCILING (which safely blocks new
          orders on that symbol — correct while we're blind), and resolves the
          moment the network returns. Only a real disagreement halts.
        - Other transient (broker 5xx, unexpected shape): bounded retries then
          escalate to fatal rather than looping on forever.

        reconcile_order is idempotent (exec_id dedup), so every retry is safe."""
        attempts: dict[str, int] = {}
        while True:
            key = await self.engine.needs_reconcile.get()
            try:
                await self.recon.reconcile_order(key)
            except ReconciliationDivergence:
                raise  # integrity-critical: propagate -> supervisor halts
            except BrokerTimeout:
                # Pure connectivity failure — do NOT count toward the halt cap.
                self.metrics.inc("reconcile_timeouts")
                await asyncio.sleep(_RECON_BACKOFF_S)
                await self.engine.needs_reconcile.put(key)
                continue
            except Exception:  # noqa: BLE001 — transient; do not strand
                n = attempts.get(key, 0) + 1
                if n > _RECON_MAX_RETRIES:
                    raise  # persistent failure: halt loudly, don't loop silently
                attempts[key] = n
                self.metrics.inc("reconcile_retries")
                await asyncio.sleep(_RECON_BACKOFF_S)
                await self.engine.needs_reconcile.put(key)
                continue
            attempts.pop(key, None)
            self.metrics.inc("reconciliations")
            await self.changes.bump()
