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

    def inc(self, key: str, n: int = 1) -> None:
        self.counts[key] = self.counts.get(key, 0) + n


def _fake(script: list):
    q: asyncio.Queue = asyncio.Queue()

    async def bump() -> None:
        return None

    return SimpleNamespace(
        engine=SimpleNamespace(needs_reconcile=q),
        recon=_Recon(script),
        metrics=_Metrics(),
        changes=SimpleNamespace(bump=bump),
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
