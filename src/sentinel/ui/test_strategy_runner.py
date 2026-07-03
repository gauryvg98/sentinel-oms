"""StrategyRunner.react tests — the reconcile-to-stance rule with fake order
callables (no DB, no broker). Proves it brings actual position into line with
the desired stance, including PRE-EXISTING positions, and never acts paused."""

from __future__ import annotations

from decimal import Decimal

import pytest

from sentinel.strategy import Decision, Stance
from sentinel.ui.strategy_runner import StrategyRunner


class _Strat:
    name = "fake"

    def on_bar(self, close):
        return Decision(Stance.FLAT)


def make(running: bool = True):
    calls = {"enter": 0, "exit": 0}

    async def enter():
        calls["enter"] += 1
        return {"placed": "K1", "qty": "0.001"}

    async def exit_():
        calls["exit"] += 1
        return {"placed": "X1", "qty": "0.001"}

    async def noop():
        return None

    r = StrategyRunner(
        _Strat(), market=None,
        position_fn=noop, enter_fn=enter, exit_fn=exit_, on_change=noop,
    )
    r.running = running
    return r, calls


async def test_wants_long_while_flat_enters():
    r, calls = make()
    action = await r.react(Decision(Stance.LONG), Decimal(0))
    assert calls == {"enter": 1, "exit": 0}
    assert action.startswith("ENTER")


async def test_wants_long_while_already_long_is_a_noop():
    """Pre-existing position that matches the stance: leave it alone."""
    r, calls = make()
    assert await r.react(Decision(Stance.LONG), Decimal("0.002")) is None
    assert calls["enter"] == 0


async def test_wants_flat_while_long_closes_a_preexisting_position():
    """The key case: you're holding when the strategy wants flat -> it closes,
    without needing an edge/crossover."""
    r, calls = make()
    action = await r.react(Decision(Stance.FLAT), Decimal("0.002"))
    assert calls == {"enter": 0, "exit": 1}
    assert action.startswith("EXIT")


async def test_wants_flat_while_flat_is_a_noop():
    r, calls = make()
    assert await r.react(Decision(Stance.FLAT), Decimal(0)) is None
    assert calls == {"enter": 0, "exit": 0}


async def test_no_opinion_never_acts_even_holding():
    """Warming up (stance None) must NOT flatten an existing position."""
    r, calls = make()
    assert await r.react(Decision(None), Decimal("0.002")) is None
    assert await r.react(Decision(None), Decimal(0)) is None
    assert calls == {"enter": 0, "exit": 0}


async def test_paused_runner_ignores_stance():
    r, calls = make(running=False)
    assert await r.react(Decision(Stance.LONG), Decimal(0)) is None
    assert await r.react(Decision(Stance.FLAT), Decimal("0.002")) is None
    assert calls == {"enter": 0, "exit": 0}


async def test_action_string_reports_rejection():
    r, _ = make()

    async def rejected():
        return {"rejected": "K1", "reason": "insufficient balance"}

    r._enter_fn = rejected  # noqa: SLF001
    action = await r.react(Decision(Stance.LONG), Decimal(0))
    assert "rejected" in action and "insufficient" in action


async def test_reconcile_now_acts_on_current_stance():
    """On start, reconcile_now brings position into line immediately."""
    calls = {"enter": 0}

    async def enter():
        calls["enter"] += 1
        return {"placed": "K1", "qty": "0.001"}

    async def exit_():
        return {}

    async def flat():
        return Decimal(0)

    async def noop():
        return None

    r = StrategyRunner(_Strat(), market=None, position_fn=flat,
                       enter_fn=enter, exit_fn=exit_, on_change=noop)
    r.running = True
    r.last_decision = Decision(Stance.LONG)
    action = await r.reconcile_now()
    assert action.startswith("ENTER") and calls["enter"] == 1
    assert r.last_action == action


async def test_reconcile_now_noop_without_a_decision():
    r, _ = make()
    r.last_decision = None
    assert await r.reconcile_now() is None
