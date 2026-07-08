"""The live reconcile loop's failure policy — the integrity-critical part.

A transient error must NOT strand the order (re-enqueue + retry); a
ReconciliationDivergence (broker vs ledger disagree on exposure) MUST propagate
so the supervisor halts — never silently absorbed. We drive the real
`SentinelApp._reconcile_loop` with a duck-typed self so no DB/broker is needed.
"""

from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest

from sentinel.broker import BrokerTimeout
from sentinel.domain import OrderState
from sentinel.oms import OrderEngine
from sentinel.recon import ReconciliationDivergence
from sentinel.runtime import app as app_module
from sentinel.runtime.app import SentinelApp


class _Recon:
    def __init__(self, script: list) -> None:
        self.script = script          # each item: None (ok) or an Exception
        self.calls: list[str] = []

    async def reconcile_order(self, key: str) -> None:
        self.calls.append(key)
        outcome = self.script.pop(0)
        if isinstance(outcome, Exception):
            raise outcome


class _Metrics:
    def __init__(self) -> None:
        self.counts: dict[str, int] = {}
        self.gauges: dict[str, float] = {}

    def inc(self, key: str, n: int = 1) -> None:
        self.counts[key] = self.counts.get(key, 0) + n

    def gauge(self, key: str, value: float) -> None:
        self.gauges[key] = value


def _stored(key: str, state: OrderState = OrderState.RECONCILING):
    """Just enough of a StoredOrder for the loop/sweep to read."""
    return SimpleNamespace(core=SimpleNamespace(
        client_order_id=key, state=state, instrument="IDX-OPT"))


def _fake(script: list, orders: dict | None = None):
    q: asyncio.Queue = asyncio.Queue()
    ledger = orders if orders is not None else {}

    async def bump() -> None:
        return None

    async def load_order(key: str):
        return ledger.get(key)

    return SimpleNamespace(
        engine=SimpleNamespace(needs_reconcile=q, pending_reconcile=set()),
        recon=_Recon(script),
        metrics=_Metrics(),
        changes=SimpleNamespace(bump=bump),
        store=SimpleNamespace(load_order=load_order),
    )


async def test_divergence_propagates_and_is_never_absorbed():
    """The halt-and-scream condition must escape the loop, not restart it."""
    app = _fake([ReconciliationDivergence("broker has no such order")])
    app.engine.needs_reconcile.put_nowait("K1")
    with pytest.raises(ReconciliationDivergence):
        await SentinelApp._reconcile_loop(app)
    assert app.recon.calls == ["K1"]


async def test_transient_error_is_retried_not_stranded(monkeypatch):
    """A blip re-enqueues the key and eventually reconciles it — the order is
    never left in RECONCILING (which would hold the instrument forever)."""
    monkeypatch.setattr(app_module, "_RECON_BACKOFF_S", 0.0)
    app = _fake([RuntimeError("blip"), RuntimeError("blip"), None])
    app.engine.needs_reconcile.put_nowait("K1")

    task = asyncio.create_task(SentinelApp._reconcile_loop(app))
    await asyncio.sleep(0.05)                       # let it churn through retries
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass

    assert app.recon.calls == ["K1", "K1", "K1"]    # two retries then success
    assert app.metrics.counts.get("reconcile_retries") == 2
    assert app.metrics.counts.get("reconciliations") == 1


async def test_broker_timeout_retries_forever_and_never_halts(monkeypatch):
    """A pure connectivity timeout tells us nothing dangerous — halting would
    only stop SL/TP management while still blind. It must retry past the cap,
    never escalating to a halt."""
    monkeypatch.setattr(app_module, "_RECON_BACKOFF_S", 0.0)
    monkeypatch.setattr(app_module, "_RECON_MAX_RETRIES", 3)
    # 10 straight timeouts — well past the cap — then success. Must not raise.
    app = _fake([BrokerTimeout("GET /fapi/v1/order: ConnectTimeout")] * 10 + [None])
    app.engine.needs_reconcile.put_nowait("K1")

    task = asyncio.create_task(SentinelApp._reconcile_loop(app))
    await asyncio.sleep(0.05)
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass

    assert app.recon.calls.count("K1") >= 11         # retried every timeout + resolved
    assert app.metrics.counts.get("reconcile_timeouts", 0) >= 10
    assert app.metrics.counts.get("reconciliations") == 1
    assert "reconcile_retries" not in app.metrics.counts   # not the halt-counting path


async def test_persistent_failure_escalates_to_halt(monkeypatch):
    """Past the retry cap, a still-failing reconcile is fatal — we halt loudly
    rather than loop on it forever."""
    monkeypatch.setattr(app_module, "_RECON_BACKOFF_S", 0.0)
    monkeypatch.setattr(app_module, "_RECON_MAX_RETRIES", 3)
    app = _fake([RuntimeError("down")] * 4)         # 3 retries, 4th escalates
    app.engine.needs_reconcile.put_nowait("K1")
    with pytest.raises(RuntimeError):
        await SentinelApp._reconcile_loop(app)
    assert len(app.recon.calls) == 4


# ------------------------------------------------- queue-storm regression
# Prod incident: 11,409 stale-sweep enqueues vs 2,159 completed reconciliations
# — the sweep re-queued stuck-RECONCILING keys every pass while the timeout
# path re-queued them every 2s, and the redundant broker calls fed the very
# throttling causing the timeouts. These pin the dedup/backoff behavior.


async def test_duplicate_enqueue_is_noop_while_pending():
    """One queued reconcile resolves the order no matter how many triggers
    fired — a key already pending must not be queued again."""
    engine = OrderEngine(store=None, broker=None)   # ctor wires no I/O
    assert await engine.enqueue_reconcile("K1") is True
    assert await engine.enqueue_reconcile("K1") is False    # still pending
    assert engine.needs_reconcile.qsize() == 1
    # Once the loop concludes the attempt (its discard), the key is
    # enqueueable again — dedup suppresses duplicates, not future triggers.
    engine.pending_reconcile.discard("K1")
    assert await engine.enqueue_reconcile("K1") is True


async def test_sweep_does_not_readd_a_pending_key():
    """A stuck-RECONCILING order gets no new event, so updated_at never bumps
    and it matches EVERY sweep pass — each pass must be a no-op for it."""
    engine = OrderEngine(store=None, broker=None)
    await engine.enqueue_reconcile("K1")            # already queued (in-flight)
    stale = [_stored("K1"), _stored("K2", OrderState.WORKING)]

    async def load_stale_nonterminal(older_than_s: float):
        return stale

    app = SimpleNamespace(
        engine=engine, metrics=_Metrics(),
        store=SimpleNamespace(load_stale_nonterminal=load_stale_nonterminal),
    )
    assert await SentinelApp.sweep_stale_orders(app) == ["K2"]  # K1 skipped
    assert engine.needs_reconcile.qsize() == 2      # K1 once + K2 — no dupes
    # The amplification case: repeated passes while both are still pending.
    assert await SentinelApp.sweep_stale_orders(app) == []
    assert engine.needs_reconcile.qsize() == 2
    assert app.metrics.counts.get("stale_sweeps") == 1   # only K2, only once
    assert app.metrics.gauges.get("reconcile_queue_depth") == 2


async def test_terminal_order_is_skipped_without_a_broker_call():
    """A queued key resolved before the loop reaches it (event applied while
    it waited) must be dropped with ZERO broker traffic — during throttling,
    redundant queries are exactly the load that keeps the throttling alive."""
    app = _fake([], orders={"K1": _stored("K1", OrderState.FILLED)})
    app.engine.pending_reconcile.add("K1")
    app.engine.needs_reconcile.put_nowait("K1")

    task = asyncio.create_task(SentinelApp._reconcile_loop(app))
    await asyncio.sleep(0.05)
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass

    assert app.recon.calls == []                    # no reconcile, no broker
    assert "K1" not in app.engine.pending_reconcile  # concluded: re-armable
    assert app.metrics.counts.get("reconcile_terminal_skips") == 1


async def test_timeout_backoff_escalates_per_key_then_resets(monkeypatch):
    """BrokerTimeout re-queue delay doubles per key (2s -> 4s -> 8s ...) and
    resets to base after a success — a fixed 2s delay hammered a throttling
    broker in lockstep with the very throttle causing the timeouts."""
    real_sleep = asyncio.sleep
    delays: list[float] = []

    async def recording_sleep(delay: float) -> None:
        delays.append(delay)
        await real_sleep(0)

    monkeypatch.setattr(app_module.asyncio, "sleep", recording_sleep)
    timeout = BrokerTimeout("GET /fapi/v1/order: ConnectTimeout")
    app = _fake([timeout] * 3 + [None] + [timeout, None])
    app.engine.pending_reconcile.add("K1")
    app.engine.needs_reconcile.put_nowait("K1")
    task = asyncio.create_task(SentinelApp._reconcile_loop(app))

    async def settle(reconciliations: int) -> None:
        for _ in range(200):
            if app.metrics.counts.get("reconciliations", 0) >= reconciliations:
                return
            await real_sleep(0)
        raise AssertionError("loop did not settle")

    await settle(1)
    assert delays == [2.0, 4.0, 8.0]                # escalates while timing out
    app.engine.needs_reconcile.put_nowait("K1")     # next incident, same key
    app.engine.pending_reconcile.add("K1")
    await settle(2)
    assert delays == [2.0, 4.0, 8.0, 2.0]           # reset by the success

    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass
