"""StrategyRunner.react tests — the decision->action rule, with fake order
callables (no DB, no broker). Proves the runner only enters-when-flat and
exits-when-long, and never acts while paused."""

from __future__ import annotations

from decimal import Decimal

import pytest

from sentinel.strategy import Decision, Signal
from sentinel.ui.strategy_runner import StrategyRunner


class _Strat:
    name = "fake"

    def on_bar(self, close):
        return Decision(Signal.HOLD)


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


async def test_enter_when_flat_places_a_buy():
    r, calls = make()
    action = await r.react(Decision(Signal.ENTER), Decimal(0))
    assert calls == {"enter": 1, "exit": 0}
    assert action.startswith("ENTER")


async def test_enter_when_already_long_is_a_noop():
    r, calls = make()
    action = await r.react(Decision(Signal.ENTER), Decimal("0.002"))
    assert calls["enter"] == 0 and action is None


async def test_exit_when_long_places_a_sell():
    r, calls = make()
    action = await r.react(Decision(Signal.EXIT), Decimal("0.002"))
    assert calls == {"enter": 0, "exit": 1}
    assert action.startswith("EXIT")


async def test_exit_when_flat_is_a_noop():
    r, calls = make()
    action = await r.react(Decision(Signal.EXIT), Decimal(0))
    assert calls["exit"] == 0 and action is None


async def test_hold_never_acts():
    r, calls = make()
    assert await r.react(Decision(Signal.HOLD), Decimal(0)) is None
    assert await r.react(Decision(Signal.HOLD), Decimal("0.002")) is None
    assert calls == {"enter": 0, "exit": 0}


async def test_paused_runner_ignores_signals():
    r, calls = make(running=False)
    assert await r.react(Decision(Signal.ENTER), Decimal(0)) is None
    assert await r.react(Decision(Signal.EXIT), Decimal("0.002")) is None
    assert calls == {"enter": 0, "exit": 0}


async def test_action_string_reports_rejection():
    r, _ = make()

    async def rejected():
        return {"rejected": "K1", "reason": "insufficient balance"}

    r._enter_fn = rejected  # noqa: SLF001
    action = await r.react(Decision(Signal.ENTER), Decimal(0))
    assert "rejected" in action and "insufficient" in action
