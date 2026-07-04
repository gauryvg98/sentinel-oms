"""Sized peg-to-touch runner tests.

Three layers: the PURE `plan_action` (reconcile position -> target: place / trim
/ re-peg / exit, with the no-trade band), the runner's stance+conviction ->
target sizing, and a few reconcile integration checks with fake callables."""

from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace

from sentinel.strategy import Decision, Stance
from sentinel.strategy.sma import SmaCross
from sentinel.ui.strategy_runner import StrategyRunner, plan_action

T = Decimal("100")           # touch
TGT = Decimal("0.15")        # target qty (band @ 20% = 0.03)
B = Decimal("15")            # budget -> 15/100 = 0.15


def entry(state="WORKING", price="100", key="K1"):
    return {"key": key, "qty": TGT, "filled": Decimal(0),
            "state": state, "limit_price": Decimal(price) if price else None}


# ------------------------------------------------------------ plan_action (pure)

def test_no_opinion_is_noop():
    assert plan_action(Decimal(0), T, None, None).kind == "noop"


def test_target_flat_with_position_exits():
    assert plan_action(Decimal("0.15"), T, None, Decimal(0)).kind == "exit"


def test_target_flat_with_unfilled_entry_cancels_it():
    assert plan_action(Decimal(0), T, entry(), Decimal(0)).kind == "cancel"


def test_under_target_places_the_shortfall_at_touch():
    p = plan_action(Decimal(0), T, None, TGT)
    assert p.kind == "place" and p.price == T and p.qty == Decimal("0.15")


def test_under_target_after_a_partial_places_only_the_remainder():
    p = plan_action(Decimal("0.075"), T, None, TGT)     # gap 0.075 > band 0.03
    assert p.kind == "place" and p.qty == Decimal("0.075")


def test_over_target_beyond_band_trims_the_excess_at_market():
    p = plan_action(Decimal("0.20"), T, None, TGT)      # gap -0.05, |gap| > 0.03
    assert p.kind == "trim" and p.qty == Decimal("0.05")


def test_small_drift_inside_the_band_does_not_churn():
    assert plan_action(Decimal("0.16"), T, None, TGT).kind == "noop"   # over by 0.01
    assert plan_action(Decimal("0.14"), T, None, TGT).kind == "noop"   # under by 0.01


def test_at_target_cancels_a_leftover_resting_entry():
    assert plan_action(Decimal("0.15"), T, entry(), TGT).kind == "cancel"


def test_pegged_entry_within_band_is_left_alone():
    assert plan_action(Decimal(0), T, entry(price="99.99"), TGT).kind == "noop"


def test_drifted_entry_is_cancelled_to_reprice():
    p = plan_action(Decimal(0), T, entry(price="99"), TGT)
    assert p.kind == "cancel" and p.reason == "re-peg to touch"


def test_entry_in_flight_is_never_touched():
    for st in ("SUBMITTING", "CANCEL_PENDING", "UNKNOWN", "RECONCILING"):
        assert plan_action(Decimal(0), T, entry(state=st, price="99"), TGT).kind == "noop"


def test_orphan_entry_without_a_price_is_left_alone():
    assert plan_action(Decimal(0), T, entry(price=None), TGT).kind == "noop"


def test_under_target_without_a_touch_cannot_peg():
    assert plan_action(Decimal(0), None, None, TGT).kind == "noop"


def test_full_reprice_sequence_is_a_one_bar_lag():
    """Bar N: drifted -> cancel. Bar N+1: the cancel confirmed (entry gone),
    the bid moved -> place afresh at the NEW touch. Proves the peg re-prices
    without ever having two live entries."""
    bar_n = plan_action(Decimal(0), T, entry(price="99"), TGT)
    assert bar_n.kind == "cancel"
    new_touch = Decimal("101")
    bar_n1 = plan_action(Decimal(0), new_touch, None, TGT)   # entry now terminal/absent
    assert bar_n1.kind == "place" and bar_n1.price == new_touch


# ----------------------------------------------------------- sizing (_target)

def _runner(**kw):
    async def anum():
        return Decimal(0)

    async def adict():
        return None

    async def aok(*a):
        return {"placed": "K1"}

    base = dict(
        position_fn=anum, open_entry_fn=adict, place_entry_fn=aok, trim_fn=aok,
        cancel_fn=aok, exit_fn=aok, touch_fn=lambda: T, budget_fn=lambda: B,
        on_change=aok,
    )
    base.update(kw)
    return StrategyRunner(SimpleNamespace(name="x"), SimpleNamespace(candles=[]), **base)


def test_target_defaults_to_full_budget_without_a_weight():
    r = _runner()
    r.last_decision = Decision(Stance.LONG, {"fast": "1", "slow": "1"})
    assert r._target(T) == Decimal("0.15")             # 1.0 * 15 / 100


def test_conviction_scales_the_target():
    r = _runner()
    r.last_decision = Decision(Stance.LONG, {"target_weight": "0.5"})
    assert r._target(T) == Decimal("0.075")            # 0.5 * 15 / 100


def test_conviction_is_clamped_to_unit_interval():
    r = _runner()
    r.last_decision = Decision(Stance.LONG, {"target_weight": "2"})
    assert r._target(T) == Decimal("0.15")             # clamped to 1.0


def test_flat_targets_zero_without_needing_a_price():
    r = _runner()
    r.last_decision = Decision(Stance.FLAT)
    assert r._target(None) == Decimal(0)               # exits never block on the feed


def test_no_opinion_targets_none():
    r = _runner()
    r.last_decision = Decision(None)
    assert r._target(T) is None


# --------------------------------------------------------- reconcile (with fakes)

class _Strat:
    name = "fake"

    def __init__(self, stance, detail=None):
        self._d = Decision(stance, detail or {})

    def on_bar(self, close):
        return self._d


def make(*, stance=Stance.LONG, detail=None, position="0", entry_=None,
         running=True):
    calls = {"place": [], "trim": [], "cancel": [], "exit": 0}

    async def position_fn():
        return Decimal(position)

    async def open_entry_fn():
        return entry_

    async def place_entry_fn(qty, price):
        calls["place"].append((qty, price))
        return {"placed": "K1", "qty": str(qty)}

    async def trim_fn(qty):
        calls["trim"].append(qty)
        return {"placed": "T1", "qty": str(qty)}

    async def cancel_fn(key):
        calls["cancel"].append(key)
        return {"canceling": key}

    async def exit_fn():
        calls["exit"] += 1
        return {"placed": "X1", "qty": "0.15"}

    async def on_change():
        return None

    r = StrategyRunner(
        _Strat(stance, detail), bars=SimpleNamespace(candles=[]),
        position_fn=position_fn, open_entry_fn=open_entry_fn,
        place_entry_fn=place_entry_fn, trim_fn=trim_fn, cancel_fn=cancel_fn,
        exit_fn=exit_fn, touch_fn=lambda: T, budget_fn=lambda: B,
        on_change=on_change,
    )
    r.running = running
    r.last_decision = Decision(stance, detail or {}) if stance is not None else None
    return r, calls


async def test_reconcile_places_a_maker_entry_when_under_target():
    r, calls = make(position="0")
    action = await r.reconcile_now()
    assert calls["place"] == [(Decimal("0.15"), T)] and action.startswith("PEG")


async def test_reconcile_trims_at_market_when_over_target():
    r, calls = make(position="0.20")
    action = await r.reconcile_now()
    assert calls["trim"] == [Decimal("0.05")] and action.startswith("TRIM")


async def test_reconcile_exits_and_cancels_any_resting_entry():
    r, calls = make(stance=Stance.FLAT, position="0.15", entry_=entry())
    action = await r.reconcile_now()
    assert calls["exit"] == 1 and calls["cancel"] == ["K1"]
    assert action.startswith("EXIT")


async def test_reconcile_reprices_a_drifted_entry():
    r, calls = make(position="0", entry_=entry(price="99"))
    await r.reconcile_now()
    assert calls["cancel"] == ["K1"] and calls["place"] == []


async def test_half_conviction_sizes_the_entry_down():
    r, calls = make(detail={"target_weight": "0.5"}, position="0")
    await r.reconcile_now()
    assert calls["place"] == [(Decimal("0.075"), T)]   # half budget


async def test_paused_runner_does_nothing():
    r, calls = make(position="0", running=False)
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

    r = StrategyRunner(
        SmaCross(fast=2, slow=3), market,
        position_fn=zero, open_entry_fn=none_, place_entry_fn=noop, trim_fn=noop,
        cancel_fn=noop, exit_fn=noop, touch_fn=lambda: T, budget_fn=lambda: B,
        on_change=noop,
    )
    r.running = running
    return r


async def test_reseed_forgets_the_old_timeframes_bars():
    market = SimpleNamespace(candles=[])
    r = _sma_runner(market, running=False)

    market.candles = _bars([100, 99, 98, 97, 96])           # downtrend -> FLAT
    await r.reseed()
    assert r.last_decision.stance is Stance.FLAT

    market.candles = _bars([100, 101, 102, 103, 104])       # uptrend -> LONG if clean
    await r.reseed()
    assert r.last_decision.stance is Stance.LONG
    assert r._last_closed_t == market.candles[-2]["t"]


async def test_reseed_while_running_acts_on_the_fresh_stance():
    placed = []

    async def place(qty, price):
        placed.append((qty, price))
        return {"placed": "K1"}

    async def zero():
        return Decimal(0)

    async def none_():
        return None

    async def noop(*a):
        return None

    market = SimpleNamespace(candles=_bars([100, 101, 102, 103, 104]))
    r = StrategyRunner(
        SmaCross(fast=2, slow=3), market,
        position_fn=zero, open_entry_fn=none_, place_entry_fn=place, trim_fn=noop,
        cancel_fn=noop, exit_fn=noop, touch_fn=lambda: T, budget_fn=lambda: B,
        on_change=noop,
    )
    r.running = True
    await r.reseed()
    assert r.last_decision.stance is Stance.LONG and len(placed) == 1
