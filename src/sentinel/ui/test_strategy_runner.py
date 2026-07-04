"""Peg-to-touch runner tests.

Two layers: the PURE `plan_action` (every place/cancel/reprice/exit case, no
I/O) and a few `reconcile_now` integration checks with fake order callables
(proving the plan is executed and the guards' shape is respected)."""

from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace

from sentinel.strategy import Decision, Stance
from sentinel.strategy.sma import SmaCross
from sentinel.ui.strategy_runner import StrategyRunner, plan_action

B = Decimal("15")            # budget (USDT)
T = Decimal("100")           # touch -> target = 15/100 = 0.15 BTC


def entry(state="WORKING", price="100", key="K1"):
    return {"key": key, "qty": Decimal("0.15"), "filled": Decimal(0),
            "state": state, "limit_price": Decimal(price) if price else None}


# ------------------------------------------------------------ plan_action (pure)

def test_no_opinion_is_noop():
    assert plan_action(None, Decimal(0), T, None, B).kind == "noop"


def test_flat_with_position_exits():
    assert plan_action(Stance.FLAT, Decimal("0.15"), T, None, B).kind == "exit"


def test_flat_with_unfilled_entry_cancels_it():
    p = plan_action(Stance.FLAT, Decimal(0), T, entry(), B)
    assert p.kind == "cancel" and p.cancel_key == "K1"


def test_flat_and_flat_is_noop():
    assert plan_action(Stance.FLAT, Decimal(0), T, None, B).kind == "noop"


def test_long_without_a_touch_cannot_peg():
    assert plan_action(Stance.LONG, Decimal(0), None, None, B).kind == "noop"


def test_long_flat_no_entry_places_at_touch():
    p = plan_action(Stance.LONG, Decimal(0), T, None, B)
    assert p.kind == "place" and p.price == T and p.qty == Decimal("0.15")


def test_long_places_only_the_remaining_after_a_partial():
    p = plan_action(Stance.LONG, Decimal("0.075"), T, None, B)   # half already filled
    assert p.kind == "place" and p.qty == Decimal("0.075")


def test_long_at_target_is_noop():
    assert plan_action(Stance.LONG, Decimal("0.15"), T, None, B).kind == "noop"


def test_long_at_target_cancels_a_leftover_resting_entry():
    p = plan_action(Stance.LONG, Decimal("0.15"), T, entry(), B)
    assert p.kind == "cancel"


def test_pegged_entry_within_band_is_left_alone():
    p = plan_action(Stance.LONG, Decimal(0), T, entry(price="99.99"), B)  # ~1bp
    assert p.kind == "noop"


def test_drifted_entry_is_cancelled_to_reprice():
    p = plan_action(Stance.LONG, Decimal(0), T, entry(price="99"), B)     # 100bp
    assert p.kind == "cancel" and p.reason == "re-peg to touch"


def test_entry_in_flight_is_never_touched():
    for st in ("SUBMITTING", "CANCEL_PENDING", "UNKNOWN", "RECONCILING"):
        assert plan_action(Stance.LONG, Decimal(0), T, entry(state=st, price="99"),
                           B).kind == "noop"


def test_orphan_entry_without_a_price_is_left_alone():
    p = plan_action(Stance.LONG, Decimal(0), T, entry(price=None), B)
    assert p.kind == "noop"


# --------------------------------------------------------- reconcile (with fakes)

class _Strat:
    name = "fake"

    def __init__(self, stance):
        self._stance = stance

    def on_bar(self, close):
        return Decision(self._stance)


def make(*, stance=Stance.LONG, position="0", touch=T, entry_=None,
         budget=B, running=True):
    calls = {"place": [], "cancel": [], "exit": 0}

    async def position_fn():
        return Decimal(position)

    async def open_entry_fn():
        return entry_

    async def place_entry_fn(qty, price):
        calls["place"].append((qty, price))
        return {"placed": "K1", "qty": str(qty)}

    async def cancel_fn(key):
        calls["cancel"].append(key)
        return {"canceling": key}

    async def exit_fn():
        calls["exit"] += 1
        return {"placed": "X1", "qty": "0.15"}

    async def on_change():
        return None

    r = StrategyRunner(
        _Strat(stance), market=SimpleNamespace(candles=[]),
        position_fn=position_fn, open_entry_fn=open_entry_fn,
        place_entry_fn=place_entry_fn, cancel_fn=cancel_fn, exit_fn=exit_fn,
        touch_fn=lambda: touch, budget_fn=lambda: budget, on_change=on_change,
    )
    r.running = running
    r.last_decision = Decision(stance) if stance is not None else None
    return r, calls


async def test_reconcile_places_a_maker_entry_when_long_and_flat():
    r, calls = make(stance=Stance.LONG, position="0")
    action = await r.reconcile_now()
    assert calls["place"] == [(Decimal("0.15"), T)]
    assert action.startswith("PEG") and r.last_action == action


async def test_reconcile_exits_at_market_and_cancels_any_resting_entry():
    r, calls = make(stance=Stance.FLAT, position="0.15", entry_=entry())
    action = await r.reconcile_now()
    assert calls["exit"] == 1 and calls["cancel"] == ["K1"]   # cancel then flatten
    assert action.startswith("EXIT")


async def test_reconcile_reprices_a_drifted_entry():
    r, calls = make(stance=Stance.LONG, position="0", entry_=entry(price="99"))
    await r.reconcile_now()
    assert calls["cancel"] == ["K1"] and calls["place"] == []


async def test_paused_runner_does_nothing():
    r, calls = make(stance=Stance.LONG, position="0", running=False)
    assert await r.reconcile_now() is None
    assert calls["place"] == [] and calls["exit"] == 0


# ---------------------------------------------------------------- reseed (SMA)

def _bars(closes):
    return [{"t": i, "c": str(c)} for i, c in enumerate(closes, start=1)]


def _sma_runner(market, *, running):
    async def zero():
        return Decimal(0)

    async def none_():
        return None

    async def noop(*a):
        return None

    async def place(qty, price):
        return {"placed": "K1", "qty": str(qty)}

    r = StrategyRunner(
        SmaCross(fast=2, slow=3), market,
        position_fn=zero, open_entry_fn=none_, place_entry_fn=place,
        cancel_fn=noop, exit_fn=noop, touch_fn=lambda: Decimal("100"),
        budget_fn=lambda: B, on_change=noop,
    )
    r.running = running
    return r


async def test_reseed_forgets_the_old_timeframes_bars():
    market = SimpleNamespace(candles=[])
    r = _sma_runner(market, running=False)          # seed only, no trading

    market.candles = _bars([100, 99, 98, 97, 96])   # downtrend -> FLAT
    await r.reseed()
    assert r.last_decision.stance is Stance.FLAT

    market.candles = _bars([100, 101, 102, 103, 104])   # uptrend -> LONG if clean
    await r.reseed()
    assert r.last_decision.stance is Stance.LONG
    assert r._last_closed_t == market.candles[-2]["t"]


async def test_reseed_while_running_acts_on_the_fresh_stance():
    placed = []

    async def place(qty, price):
        placed.append((qty, price))
        return {"placed": "K1", "qty": str(qty)}

    async def zero():
        return Decimal(0)

    async def none_():
        return None

    async def noop(*a):
        return None

    market = SimpleNamespace(candles=_bars([100, 101, 102, 103, 104]))  # uptrend
    r = StrategyRunner(
        SmaCross(fast=2, slow=3), market,
        position_fn=zero, open_entry_fn=none_, place_entry_fn=place,
        cancel_fn=noop, exit_fn=noop, touch_fn=lambda: Decimal("100"),
        budget_fn=lambda: B, on_change=noop,
    )
    r.running = True
    await r.reseed()
    assert r.last_decision.stance is Stance.LONG and len(placed) == 1
