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
import logging
from decimal import Decimal
from uuid import uuid4

import asyncpg

from sentinel.broker import BrokerAdapter, BrokerBalanceUpdate, BrokerTimeout
from sentinel.domain import is_terminal
from sentinel.ledger import LedgerStore
from sentinel.metrics import MetricsRegistry
from sentinel.oms import CommandGateway, OrderEngine, WriterCoordinator
from sentinel.protect import ProtectiveExitSupervisor
from sentinel.recon import Reconciler, ReconciliationDivergence, RecoveryReport

from .single_writer import SingleWriterLock
from .supervisor import TaskSupervisor

log = logging.getLogger("sentinel.runtime")

# Reconcile retry policy: a transient broker/network error must NOT strand the
# order (which would hold the instrument forever). Re-enqueue with backoff up
# to a cap; beyond the cap the failure is treated as fatal, not transient.
_RECON_BACKOFF_S = 2.0
_RECON_MAX_RETRIES = 5
# BrokerTimeout re-queue delay escalates PER KEY (base -> double -> cap) and
# resets on success. A fixed 2s delay meant a batch of unreconcilable orders
# (broker throttling) re-hit the broker every 2s each — worsening the very
# throttling that caused the timeouts, while every attempt held the instrument
# lock (prod storm: event_apply max 391s, order-place max 428s, lock convoy).
_RECON_BACKOFF_CAP_S = 60.0

# Transient-DB retry for event application: a managed-Postgres proxy can kill
# a live connection mid-query (proven in prod by the writer-lock InterfaceError
# halt). Losing a connection is NOT an integrity violation — retry briefly
# before treating it as fatal. Retrying on_broker_event is safe: it reloads
# order state fresh each attempt, fills dedup by exec_id, and a replayed
# CancelConfirmed on a CANCELED order is an idempotent no-op — so even the
# ambiguous case (commit landed, error surfaced) reapplies cleanly as a no-op.
_DB_RETRYABLE = (ConnectionError, OSError, TimeoutError,
                 asyncpg.InterfaceError, asyncpg.PostgresConnectionError)
_DB_RETRY_BACKOFF_S = (0.5, 1.0, 2.0, 4.0, 8.0)   # ~15s, then halt for real

# Balance re-seed: latest_balances is seeded once then stream-merged, so a
# balance event lost in a user-stream gap skews POSITION SIZING until reboot.
# Re-seed from REST periodically — the account-side twin of the order sweep.
_BALANCE_RESEED_S = 300.0

# Stale-order sweep: the liveness backstop for LOST broker events. Every
# reactive reconcile trigger (timeout, late fill, duplicate) presumes an event
# ARRIVES; a fill dropped in a user-stream gap produces no trigger at all, so
# the order sits in a live state forever — found in prod as a filled MARKET
# protective exit stuck WORKING/0, pinning its position as committed-to-exits
# and blocking every SL/TP flatten. Sweep non-terminal orders untouched for
# _SWEEP_STALE_AGE_S onto the reconcile queue; reconcile_order is idempotent
# (exec_id dedup), so sweeping a genuinely-resting order just re-affirms it.
# The age comfortably exceeds any honest in-flight window (submission timeout,
# reconcile round-trip), so the sweep only ever touches abandoned state.
_SWEEP_INTERVAL_S = 30.0
_SWEEP_STALE_AGE_S = 120.0


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

    async def _record_halt(self, reason: str) -> None:
        """Durable halt evidence: every halt writes a decision_log row with
        its reason. supervisor.halted is in-memory and the CRITICAL log line
        scrolls away — the ledger row is what survives a restart."""
        await self.store.record_decision(
            uuid4(), "ACCOUNT", "supervisor", "HALTED", {"reason": reason}
        )

    async def start(self, *, arm_protection: bool = True) -> RecoveryReport:
        # Halts must leave durable evidence, and tasks spawned below can halt
        # us — so wire the hook before the first spawn.
        self.supervisor.on_halt = self._record_halt
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
        # broker-intake restart=True: the stream iterator failing is
        # CONNECTIVITY, not divergence — respawn (with the supervisor's
        # backoff) re-enters events(), which reconnects; missed-in-gap events
        # are repaired by the stale-order sweep + reconciliation.
        self.supervisor.spawn("broker-intake", self._broker_intake, restart=True)
        self.supervisor.spawn("event-apply", self._event_apply)
        # restart=False: this loop is integrity-critical. It handles its OWN
        # transient retries internally (see _reconcile_loop); the only thing
        # that exits it is a ReconciliationDivergence or an exhausted retry —
        # both of which SHOULD halt the account, not silently respawn.
        self.supervisor.spawn("reconcile", self._reconcile_loop, restart=False)
        # Liveness backstop: periodically re-queue orders stranded by lost
        # broker events (stream-gap fills). Just a nudger — safe to restart.
        self.supervisor.spawn("stale-sweep", self._stale_sweep_loop,
                              restart=True)
        # Sizing-input backstop: re-seed balances from REST so a balance event
        # lost in a stream gap can't skew position sizing until the next boot.
        self.supervisor.spawn("balance-reseed", self._balance_reseed_loop,
                              restart=True)
        try:
            report = await self.recon.startup_recovery()      # 1. recover
        except ReconciliationDivergence as e:
            # A genuine boot divergence must HALT (don't absorb) — but halting
            # must NOT crash the process into a Fly restart loop (which serves
            # nothing and hides the reason). Trip the halt flag so the board comes
            # up read-only and refuses to trade, and keep serving so the operator
            # can see and investigate. Same "halt, don't absorb" outcome as a
            # runtime divergence — just without the opaque crash-loop.
            print(f"  STARTUP HALT (serving read-only, not trading): {e}",
                  flush=True)
            self.supervisor.halted.set()
            self.supervisor.alert(f"STARTUP HALT (boot divergence): {e}")
            # Inline (not via the async hook): the DB just served recovery,
            # and start() should not proceed until the halt row is durable.
            await self._record_halt(f"STARTUP HALT (boot divergence): {e}")
            report = RecoveryReport()
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
                        await self._apply_with_db_retry(event)
                    self.metrics.inc("events_applied")
                await self.changes.bump()                 # push to the UI
            finally:
                self._events.task_done()                  # drain-accounting

    async def _apply_with_db_retry(self, event) -> None:
        """on_broker_event, retrying briefly through connection-class DB
        errors (proxy-killed connections). Domain violations pass straight
        through — those are the genuine halt conditions. Retry is safe: state
        reloads fresh per attempt, fills dedup on exec_id, duplicate cancel
        confirms are no-ops (see _DB_RETRYABLE note above)."""
        for delay in _DB_RETRY_BACKOFF_S:
            try:
                await self.engine.on_broker_event(event)
                return
            except _DB_RETRYABLE as e:
                log.warning("event apply hit a transient DB error (%r); "
                            "retrying in %.1fs", e, delay)
                self.metrics.inc("event_apply_db_retries")
                await asyncio.sleep(delay)
        await self.engine.on_broker_event(event)   # last try: raise = halt

    async def _balance_reseed_loop(self) -> None:
        while True:
            await asyncio.sleep(_BALANCE_RESEED_S)
            try:
                self.latest_balances = await self._broker.query_positions()
            except Exception as e:  # noqa: BLE001 — a nudger outlives blips
                log.warning("balance re-seed failed (%r); next pass", e)
                continue
            await self.changes.bump()

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

        reconcile_order is idempotent (exec_id dedup), so every retry is safe.

        Pending-set lifecycle: a key enters engine.pending_reconcile in
        enqueue_reconcile (the only producer entry point) and leaves it HERE,
        only once its attempt concludes — resolved, or found terminal. The
        re-queue paths below deliberately bypass enqueue_reconcile and keep
        the key in the set: it is still owed a reconcile, and a fresh trigger
        arriving mid-retry must stay a no-op (see writer.py for the storm
        this prevents)."""
        attempts: dict[str, int] = {}
        timeout_backoff: dict[str, float] = {}   # per-key escalating delay
        while True:
            key = await self.engine.needs_reconcile.get()
            self.metrics.gauge("reconcile_queue_depth",
                               self.engine.needs_reconcile.qsize())
            # Terminal short-circuit: a queued key can be resolved before we
            # get to it (broker event applied while it waited, or a sweep of
            # an order a prior reconcile already closed). Reload first and
            # skip without ANY broker traffic — during throttling, redundant
            # queries are exactly the load that keeps the throttling alive.
            stored = await self.store.load_order(key)
            if stored is not None and is_terminal(stored.core.state):
                self.engine.pending_reconcile.discard(key)
                attempts.pop(key, None)
                timeout_backoff.pop(key, None)
                self.metrics.inc("reconcile_terminal_skips")
                continue
            try:
                await self.recon.reconcile_order(key)
            except ReconciliationDivergence:
                raise  # integrity-critical: propagate -> supervisor halts
            except BrokerTimeout:
                # Pure connectivity failure — do NOT count toward the halt cap,
                # but DO back off per key (double to a cap, reset on success):
                # retrying a throttled broker every 2s only feeds the throttle.
                self.metrics.inc("reconcile_timeouts")
                delay = timeout_backoff.get(key, _RECON_BACKOFF_S)
                timeout_backoff[key] = min(delay * 2, _RECON_BACKOFF_CAP_S)
                await asyncio.sleep(delay)
                await self.engine.needs_reconcile.put(key)  # stays pending
                continue
            except Exception:  # noqa: BLE001 — transient; do not strand
                n = attempts.get(key, 0) + 1
                if n > _RECON_MAX_RETRIES:
                    raise  # persistent failure: halt loudly, don't loop silently
                attempts[key] = n
                self.metrics.inc("reconcile_retries")
                await asyncio.sleep(_RECON_BACKOFF_S)
                await self.engine.needs_reconcile.put(key)  # stays pending
                continue
            attempts.pop(key, None)
            timeout_backoff.pop(key, None)
            self.engine.pending_reconcile.discard(key)   # concluded: resolvable again
            self.metrics.inc("reconciliations")
            await self.changes.bump()

    async def sweep_stale_orders(self) -> list[str]:
        """One sweep pass: queue every non-terminal order untouched for
        _SWEEP_STALE_AGE_S for reconciliation against broker truth, skipping
        keys already queued/in-flight. Returns the NEWLY queued keys.
        Idempotent and cheap — a swept order's first reconcile event bumps
        updated_at, so it will not be re-swept for another full staleness
        window even if it legitimately keeps resting."""
        stale = await self.store.load_stale_nonterminal(_SWEEP_STALE_AGE_S)
        keys: list[str] = []
        for s in stale:
            # Dedup via the pending set: an order stuck RECONCILING gets no
            # new event, so updated_at never bumps and it matches EVERY sweep
            # pass until it resolves. Re-queuing it each pass was the storm's
            # amplifier (11,409 sweep enqueues vs 2,159 completed reconciles
            # in prod) — enqueue_reconcile makes those passes no-ops.
            if not await self.engine.enqueue_reconcile(s.core.client_order_id):
                continue
            keys.append(s.core.client_order_id)
            log.warning(
                "stale-order sweep: %s %s on %s untouched > %.0fs — a broker "
                "event was likely lost (stream gap); reconciling",
                s.core.client_order_id, s.core.state.value,
                s.core.instrument, _SWEEP_STALE_AGE_S,
            )
            self.metrics.inc("stale_sweeps")
        self.metrics.gauge("reconcile_queue_depth",
                           self.engine.needs_reconcile.qsize())
        return keys

    async def _stale_sweep_loop(self) -> None:
        while True:
            await asyncio.sleep(_SWEEP_INTERVAL_S)
            try:
                await self.sweep_stale_orders()
            except Exception as e:  # noqa: BLE001 — a nudger must outlive DB blips
                log.warning("stale sweep pass failed (%r); next pass retries", e)
