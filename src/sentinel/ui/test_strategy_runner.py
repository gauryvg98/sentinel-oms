"""StrategyRunner.react tests — the reconcile-to-stance rule with fake order
callables (no DB, no broker). Proves it brings actual position into line with
the desired stance, including PRE-EXISTING positions, and never acts paused."""

from __future__ import annotations

from decimal import Decimal

import pytest

from types import SimpleNamespace

from sentinel.strategy import Decision, Stance
from sentinel.strategy.sma import SmaCross
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


def _bars(closes):
    return [{"t": i, "c": str(c)} for i, c in enumerate(closes, start=1)]


async def test_reseed_forgets_the_old_timeframes_bars():
    """After a timeframe switch, the stance must be computed PURELY from the
    new interval's bars — the old interval's closes must not linger in the SMA,
    and the bar cursor must point at the new interval's last closed bar."""
    strat = SmaCross(fast=2, slow=3)
    market = SimpleNamespace(candles=[])

    async def zero():
        return Decimal(0)

    async def noop():
        return None

    r = StrategyRunner(strat, market, position_fn=zero,
                       enter_fn=noop, exit_fn=noop, on_change=noop)
    r.running = False                                  # seed only, no trading

    # New timeframe #1: a downtrend -> FLAT. (closes on bars 1..4 = 100..97)
    market.candles = _bars([100, 99, 98, 97, 96])
    await r.reseed()
    assert r.last_decision.stance is Stance.FLAT

    # New timeframe #2: an uptrend. If the down closes still lingered in the
    # deque the average would be muddied; a clean reset yields LONG.
    market.candles = _bars([100, 101, 102, 103, 104])
    await r.reseed()
    assert r.last_decision.stance is Stance.LONG
    assert r._last_closed_t == market.candles[-2]["t"]   # cursor on new tf


async def test_reseed_while_running_reconciles_to_fresh_stance():
    """Switching timeframe while running shouldn't stall: it acts on the new
    stance immediately instead of waiting up to a full interval."""
    calls = {"enter": 0}

    async def flat():
        return Decimal(0)

    async def enter():
        calls["enter"] += 1
        return {"placed": "K1", "qty": "0.001"}

    async def noop():
        return None

    strat = SmaCross(fast=2, slow=3)
    market = SimpleNamespace(candles=_bars([100, 101, 102, 103, 104]))  # uptrend
    r = StrategyRunner(strat, market, position_fn=flat,
                       enter_fn=enter, exit_fn=noop, on_change=noop)
    r.running = True
    await r.reseed()
    assert r.last_decision.stance is Stance.LONG and calls["enter"] == 1
