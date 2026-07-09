"""Runtime evidence (R2): supervision, cancellation propagation, fail-loud
policies, graceful shutdown with zero lost accepted events, and the full
supervised app lifecycle."""

from __future__ import annotations

import asyncio
from decimal import Decimal
from uuid import uuid4

import pytest

from sentinel.runtime import TaskSupervisor


# ------------------------------------------------------------- supervision


async def test_failure_of_critical_task_halts_loudly():
    sup = TaskSupervisor()

    async def doomed():
        raise RuntimeError("boom")

    sup.spawn("critical", doomed, restart=False)
    await asyncio.wait_for(sup.halted.wait(), timeout=1)
    assert sup.failures[0].name == "critical"
    assert isinstance(sup.failures[0].error, RuntimeError)


async def test_restartable_task_is_respawned(monkeypatch):
    from sentinel.runtime import supervisor as sup_mod
    monkeypatch.setattr(sup_mod, "_RESTART_BASE_S", 0.01)  # fast for the test

    sup = TaskSupervisor()
    runs = 0
    done = asyncio.Event()

    async def flaky():
        nonlocal runs
        runs += 1
        if runs < 3:
            raise RuntimeError(f"failure {runs}")
        done.set()
        await asyncio.sleep(3600)                 # healthy: parks forever

    sup.spawn("flaky", flaky, restart=True)
    await asyncio.wait_for(done.wait(), timeout=1)
    assert runs == 3
    assert len(sup.failures) == 2                 # both failures recorded
    assert not sup.halted.is_set()                # restarts, no halt
    await sup.shutdown()


async def test_restart_backs_off_instead_of_crash_looping(monkeypatch):
    """A task failing on entry must NOT respawn instantly (a tight crash loop
    starves the event loop). The respawn is deferred by the backoff."""
    from sentinel.runtime import supervisor as sup_mod
    monkeypatch.setattr(sup_mod, "_RESTART_BASE_S", 0.3)

    sup = TaskSupervisor()
    runs = 0

    async def always_fails():
        nonlocal runs
        runs += 1
        raise RuntimeError("boom")

    sup.spawn("crashy", always_fails, restart=True)
    await asyncio.sleep(0.1)      # first failure recorded, respawn pending
    assert runs == 1              # NOT respawned yet — backoff holds it
    await asyncio.sleep(0.4)      # past the 0.3s backoff
    assert runs == 2              # exactly one deferred respawn
    await sup.shutdown()


async def test_shutdown_cancels_a_pending_backoff_restart(monkeypatch):
    """A restart scheduled during backoff must not resurrect the task after
    shutdown() tore the supervisor down — no orphan revival."""
    from sentinel.runtime import supervisor as sup_mod
    monkeypatch.setattr(sup_mod, "_RESTART_BASE_S", 0.2)

    sup = TaskSupervisor()
    runs = 0

    async def fails_once():
        nonlocal runs
        runs += 1
        raise RuntimeError("boom")

    sup.spawn("doomed", fails_once, restart=True)
    await asyncio.sleep(0.05)     # failed; respawn parked in backoff
    await sup.shutdown()          # drops the child from the roster
    await asyncio.sleep(0.4)      # backoff elapses AFTER shutdown
    assert runs == 1              # never resurrected


async def test_cancellation_propagates_and_cleanup_runs():
    sup = TaskSupervisor()
    cleaned = asyncio.Event()
    started = asyncio.Event()

    async def worker():
        started.set()
        try:
            await asyncio.sleep(3600)
        finally:
            cleaned.set()                         # MUST run on cancellation

    sup.spawn("worker", worker)
    await asyncio.wait_for(started.wait(), timeout=1)
    await sup.shutdown()
    assert cleaned.is_set()
    assert sup.failures == []                     # cancellation is not failure


async def test_clean_exit_is_not_a_failure():
    sup = TaskSupervisor()

    async def finite():
        return None

    sup.spawn("finite", finite)
    await asyncio.sleep(0.01)
    assert sup.failures == [] and not sup.halted.is_set()


async def test_halt_invokes_on_halt_with_the_reason():
    """A halt must leave durable evidence: the on_halt hook receives the
    halt reason so the app can write it to the ledger."""
    sup = TaskSupervisor()
    recorded: list[str] = []
    seen = asyncio.Event()

    async def on_halt(message: str) -> None:
        recorded.append(message)
        seen.set()

    sup.on_halt = on_halt

    async def doomed():
        raise RuntimeError("boom")

    sup.spawn("critical", doomed, restart=False)
    await asyncio.wait_for(sup.halted.wait(), timeout=1)
    await asyncio.wait_for(seen.wait(), timeout=1)
    assert recorded == ["task critical failed: RuntimeError('boom')"]


async def test_on_halt_failure_never_cascades():
    """The halt record failing (ledger down, exactly when things are bad)
    must not take anything else with it — the halt itself already stands."""
    sup = TaskSupervisor()
    called = asyncio.Event()

    async def broken_on_halt(message: str) -> None:
        called.set()
        raise RuntimeError("ledger unavailable")

    sup.on_halt = broken_on_halt

    async def doomed():
        raise RuntimeError("boom")

    sup.spawn("critical", doomed, restart=False)
    await asyncio.wait_for(sup.halted.wait(), timeout=1)
    await asyncio.wait_for(called.wait(), timeout=1)
    await asyncio.sleep(0.01)          # let the record task finish/fail
    assert sup.halted.is_set()
    assert len(sup.failures) == 1      # the hook failure adds no failure


async def test_on_halt_not_invoked_for_restarted_tasks(monkeypatch):
    """on_halt fires on the HALT branch and nowhere else: a restart=True
    failure respawns without writing a halt record."""
    from sentinel.runtime import supervisor as sup_mod
    monkeypatch.setattr(sup_mod, "_RESTART_BASE_S", 0.01)

    sup = TaskSupervisor()
    halt_calls: list[str] = []

    async def on_halt(message: str) -> None:
        halt_calls.append(message)

    sup.on_halt = on_halt
    runs = 0
    done = asyncio.Event()

    async def flaky():
        nonlocal runs
        runs += 1
        if runs < 2:
            raise RuntimeError("transient")
        done.set()
        await asyncio.sleep(3600)

    sup.spawn("flaky", flaky, restart=True)
    await asyncio.wait_for(done.wait(), timeout=1)
    assert halt_calls == []            # restarts are not halts
    await sup.shutdown()


# ------------------------------------------------- graceful drain semantics


async def test_bounded_queue_drains_fully_before_shutdown():
    """The no-lost-events property in miniature: everything accepted into the
    bounded queue is processed before shutdown completes."""
    queue: asyncio.Queue[int] = asyncio.Queue(maxsize=8)
    processed: list[int] = []
    sup = TaskSupervisor()

    async def consumer():
        while True:
            item = await queue.get()
            try:
                await asyncio.sleep(0)            # yield: simulate work
                processed.append(item)
            finally:
                queue.task_done()

    sup.spawn("consumer", consumer)
    for i in range(50):
        await queue.put(i)                        # backpressure engages at 8

    await queue.join()                            # drain barrier
    await sup.shutdown()
    assert processed == list(range(50))           # all accepted, none lost


# --------------------------------------------------------- full app lifecycle


async def test_app_lifecycle_end_to_end(pool):
    """SentinelApp: recover -> re-arm -> consume live events through the
    supervised loops -> graceful stop with a drained queue."""
    from sentinel.broker.sim import BrokerScript, ScriptedBroker
    from sentinel.domain import Authority, EconomicOrderIntent, OrderState, Side
    from sentinel.runtime import SentinelApp

    script = BrokerScript()
    script.fill("K1", qty="2", price="4.20", at_step=1)
    sim = ScriptedBroker(script)
    app = SentinelApp(pool, sim)

    report = await app.start()
    assert report.reconciled == [] and app.accepting

    stored = await app.gateway.place(
        uuid4(),
        EconomicOrderIntent(
            intent_id=uuid4(), idempotency_key="K1", instrument="IDX-OPT",
            side=Side.BUY, qty=Decimal("2"), limit_price=Decimal("4.20"),
            authority=Authority.ENTRY, trace_id=uuid4(),
        ),
    )
    assert stored.core.state is OrderState.WORKING

    sim.step()                                    # fill flows: sim -> queue ->
    await asyncio.sleep(0.05)                     # intake -> apply (live tasks)

    final = await app.store.load_order("K1")
    assert final.core.state is OrderState.FILLED
    assert await app.store.get_position("IDX-OPT") == 2

    await app.stop()
    assert not app.accepting
    assert app.supervisor.failures == []          # clean lifecycle throughout


async def test_halt_writes_a_durable_decision_row(pool):
    """start() wires supervisor.on_halt to the ledger: firing it lands a
    decision_log row carrying the reason — the evidence that survives a
    restart, unlike the in-memory halted flag or the log buffer."""
    from sentinel.broker.sim import BrokerScript, ScriptedBroker
    from sentinel.runtime import SentinelApp

    app = SentinelApp(pool, ScriptedBroker(BrokerScript()))
    await app.start()
    assert app.supervisor.on_halt is not None     # wired by start()

    # Fire the hook exactly as the supervisor's halt branch would.
    await app.supervisor.on_halt("task reconcile failed: divergence")

    halts = [d for d in await app.store.recent_decisions()
             if d["decision"] == "HALTED"]
    assert len(halts) == 1
    assert halts[0]["actor"] == "supervisor"
    assert halts[0]["instrument"] == "ACCOUNT"
    assert halts[0]["detail"] == {
        "reason": "task reconcile failed: divergence"}
    await app.stop()
