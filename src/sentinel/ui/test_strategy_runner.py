"""Signed peg-to-touch runner tests: reconcile position -> a SIGNED target
(open/reduce, long AND short), stance+conviction sizing, and reconcile
integration with fake order callables."""

from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace

from sentinel.strategy import Decision, Stance
from sentinel.strategy.sma import SmaCross
from sentinel.ui.strategy_runner import StrategyRunner, plan_action

PX = Decimal("100")          # bid == ask (no spread) unless a test says otherwise
TGT = Decimal("0.15")        # target magnitude (band @ 20% = 0.03)
B = Decimal("15")            # budget -> 15/100 = 0.15


def entry(side="BUY", state="WORKING", price="100", key="K1"):
    return {"key": key, "side": side, "qty": TGT, "filled": Decimal(0),
            "state": state, "limit_price": Decimal(price) if price else None}


# ------------------------------------------------------------ plan_action (pure)

def test_no_opinion_is_noop():
    assert plan_action(Decimal(0), PX, PX, None, None).kind == "noop"


def test_open_long_from_flat():
    p = plan_action(Decimal(0), PX, PX, None, TGT)
    assert p.kind == "open" and p.side == "BUY" and p.qty == Decimal("0.15")


def test_open_short_from_flat():
    p = plan_action(Decimal(0), PX, PX, None, -TGT)
    assert p.kind == "open" and p.side == "SELL" and p.qty == Decimal("0.15")


def test_qty_is_floored_to_the_venue_lot_step():
    # Perps trade in 0.001 BTC lots; a raw target of 0.00318 must floor to
    # 0.003, never reach the exchange as 0.00318 (Binance rejects -1111).
    p = plan_action(Decimal(0), PX, PX, None, Decimal("0.00318"),
                    lot_step=Decimal("0.001"))
    assert p.kind == "open" and p.qty == Decimal("0.003")
    assert p.qty.as_tuple().exponent >= -3          # <= 3 decimal places


def test_add_to_a_long_pegs_the_remainder():
    p = plan_action(Decimal("0.075"), PX, PX, None, TGT)     # gap 0.075 > band
    assert p.kind == "open" and p.side == "BUY" and p.qty == Decimal("0.075")


def test_reduce_an_oversized_long_at_market():
    p = plan_action(Decimal("0.20"), PX, PX, None, TGT)      # gap -0.05
    assert p.kind == "reduce" and p.side == "SELL" and p.qty == Decimal("0.05")


def test_flatten_long():
    p = plan_action(Decimal("0.15"), PX, PX, None, Decimal(0))
    assert p.kind == "reduce" and p.side == "SELL" and p.qty == Decimal("0.15")


def test_cover_part_of_a_short():
    p = plan_action(Decimal("-0.20"), PX, PX, None, -TGT)    # gap +0.05
    assert p.kind == "reduce" and p.side == "BUY" and p.qty == Decimal("0.05")


def test_flatten_short_buys_to_cover():
    p = plan_action(Decimal("-0.15"), PX, PX, None, Decimal(0))
    assert p.kind == "reduce" and p.side == "BUY" and p.qty == Decimal("0.15")


def test_flip_long_to_short_reduces_to_zero_first():
    # position +0.15, target -0.15: sell only down to zero this bar; the short
    # opens next bar (the peg's one-bar cadence).
    p = plan_action(Decimal("0.15"), PX, PX, None, -TGT)
    assert p.kind == "reduce" and p.side == "SELL" and p.qty == Decimal("0.15")


def test_small_drift_inside_the_band_does_not_churn():
    assert plan_action(Decimal("0.16"), PX, PX, None, TGT).kind == "noop"
    assert plan_action(Decimal("0.14"), PX, PX, None, TGT).kind == "noop"


def test_long_pegs_at_bid_short_pegs_at_ask():
    bid, ask = Decimal("99"), Decimal("101")
    assert plan_action(Decimal(0), bid, ask, None, TGT).price == bid    # buy the bid
    assert plan_action(Decimal(0), bid, ask, None, -TGT).price == ask   # sell the ask


def test_pegged_entry_within_band_is_left_alone():
    assert plan_action(Decimal(0), PX, PX, entry(price="99.99"), TGT).kind == "noop"


def test_drifted_entry_is_cancelled_to_reprice():
    p = plan_action(Decimal(0), PX, PX, entry(price="99"), TGT)
    assert p.kind == "cancel" and p.reason == "re-peg to touch"


def test_entry_on_the_wrong_side_is_cancelled():
    # want short now, but a BUY entry is resting -> cancel it.
    p = plan_action(Decimal(0), PX, PX, entry(side="BUY"), -TGT)
    assert p.kind == "cancel" and p.reason == "entry wrong side"


def test_entry_in_flight_is_never_touched():
    for st in ("SUBMITTING", "CANCEL_PENDING", "UNKNOWN", "RECONCILING"):
        assert plan_action(Decimal(0), PX, PX, entry(state=st, price="99"),
                           TGT).kind == "noop"


def test_open_without_a_touch_cannot_peg():
    assert plan_action(Decimal(0), None, None, None, TGT).kind == "noop"


# ----------------------------------------------------------- sizing (_target)

def _runner(*, allow_short=False, budget=B):
    async def anum():
        return Decimal(0)

    async def adict():
        return None

    async def aok(*a):
        return {"placed": "K1"}

    return StrategyRunner(
        SimpleNamespace(name="x"), SimpleNamespace(candles=[]),
        position_fn=anum, open_entry_fn=adict, place_entry_fn=aok,
        reduce_sell_fn=aok, cancel_fn=aok, bid_fn=lambda: PX, ask_fn=lambda: PX,
        budget_fn=lambda: budget, on_change=aok, allow_short=allow_short)


def test_long_targets_positive():
    r = _runner()
    r.last_decision = Decision(Stance.LONG, {})
    assert r._target(PX, PX) == Decimal("0.15")


def test_short_targets_negative_when_allowed():
    r = _runner(allow_short=True)
    r.last_decision = Decision(Stance.SHORT, {})
    assert r._target(PX, PX) == Decimal("-0.15")


def test_short_clamps_to_flat_on_spot():
    r = _runner(allow_short=False)
    r.last_decision = Decision(Stance.SHORT, {})
    assert r._target(PX, PX) == Decimal(0)             # spot can't short


def test_conviction_scales_the_target():
    r = _runner(allow_short=True)
    r.last_decision = Decision(Stance.SHORT, {"target_weight": "0.5"})
    assert r._target(PX, PX) == Decimal("-0.075")


def test_flat_and_no_opinion():
    r = _runner()
    r.last_decision = Decision(Stance.FLAT)
    assert r._target(PX, PX) == Decimal(0)
    r.last_decision = Decision(None)
    assert r._target(PX, PX) is None


# --------------------------------------------------------- reconcile (with fakes)

class _Strat:
    name = "fake"

    def __init__(self, stance, detail=None):
        self._d = Decision(stance, detail or {})

    def on_bar(self, close):
        return self._d


def make(*, stance=Stance.LONG, detail=None, position="0", entry_=None,
         running=True, allow_short=True):
    calls = {"open_buy": [], "open_sell": [], "reduce_sell": [],
             "reduce_buy": [], "cancel": []}

    async def position_fn():
        return Decimal(position)

    async def open_entry_fn():
        return entry_

    async def place_entry_fn(qty, price):
        calls["open_buy"].append((qty, price)); return {"placed": "K1", "qty": str(qty)}

    async def place_short_fn(qty, price):
        calls["open_sell"].append((qty, price)); return {"placed": "S1", "qty": str(qty)}

    async def reduce_sell_fn(qty):
        calls["reduce_sell"].append(qty); return {"placed": "R1", "qty": str(qty)}

    async def reduce_buy_fn(qty):
        calls["reduce_buy"].append(qty); return {"placed": "C1", "qty": str(qty)}

    async def cancel_fn(key):
        calls["cancel"].append(key); return {"canceling": key}

    async def on_change():
        return None

    r = StrategyRunner(
        _Strat(stance, detail), SimpleNamespace(candles=[]),
        position_fn=position_fn, open_entry_fn=open_entry_fn,
        place_entry_fn=place_entry_fn, reduce_sell_fn=reduce_sell_fn,
        cancel_fn=cancel_fn, bid_fn=lambda: PX, ask_fn=lambda: PX,
        budget_fn=lambda: B, on_change=on_change,
        place_short_fn=place_short_fn, reduce_buy_fn=reduce_buy_fn,
        allow_short=allow_short)
    r.running = running
    r.last_decision = Decision(stance, detail or {}) if stance is not None else None
    return r, calls


async def test_reconcile_opens_a_long_maker_when_flat():
    r, c = make(stance=Stance.LONG, position="0")
    a = await r.reconcile_now()
    assert c["open_buy"] == [(Decimal("0.15"), PX)] and a.startswith("PEG BUY")


async def test_reconcile_opens_a_short_maker_when_flat():
    r, c = make(stance=Stance.SHORT, position="0")
    a = await r.reconcile_now()
    assert c["open_sell"] == [(Decimal("0.15"), PX)] and a.startswith("PEG SELL")


async def test_reconcile_reduces_an_oversized_long_at_market():
    r, c = make(stance=Stance.LONG, position="0.20")
    a = await r.reconcile_now()
    assert c["reduce_sell"] == [Decimal("0.05")] and a.startswith("REDUCE SELL")


async def test_reconcile_covers_a_short_at_market():
    r, c = make(stance=Stance.SHORT, position="-0.20")
    a = await r.reconcile_now()
    assert c["reduce_buy"] == [Decimal("0.05")] and a.startswith("REDUCE BUY")


async def test_short_stance_on_spot_does_not_short():
    r, c = make(stance=Stance.SHORT, position="0", allow_short=False)
    assert await r.reconcile_now() is None            # target 0, flat -> noop
    assert c["open_sell"] == [] and c["reduce_sell"] == []


async def test_half_conviction_sizes_the_entry_down():
    r, c = make(detail={"target_weight": "0.5"}, position="0")
    await r.reconcile_now()
    assert c["open_buy"] == [(Decimal("0.075"), PX)]


async def test_paused_runner_does_nothing():
    r, c = make(position="0", running=False)
    assert await r.reconcile_now() is None


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
        position_fn=zero, open_entry_fn=none_, place_entry_fn=noop,
        reduce_sell_fn=noop, cancel_fn=noop, bid_fn=lambda: PX, ask_fn=lambda: PX,
        budget_fn=lambda: B, on_change=noop)
    r.running = running
    return r


async def test_reseed_forgets_the_old_timeframes_bars():
    market = SimpleNamespace(candles=[])
    r = _sma_runner(market, running=False)

    market.candles = _bars([100, 99, 98, 97, 96])           # downtrend -> FLAT
    await r.reseed()
    assert r.last_decision.stance is Stance.FLAT

    market.candles = _bars([100, 101, 102, 103, 104])       # uptrend -> LONG
    await r.reseed()
    assert r.last_decision.stance is Stance.LONG
    assert r._last_closed_t == market.candles[-2]["t"]


def test_snapshot_carries_view_and_bar_aligned_series():
    market = SimpleNamespace(candles=_bars([100, 101, 102, 103, 104, 105]),
                             interval="1m")
    r = _sma_runner(market, running=False)
    r._seed_from_history()
    snap = r.snapshot()
    assert snap["interval"] == "1m"
    assert {"fast", "slow"} <= {row["key"] for row in snap["view"]["rows"]}
    pts = snap["series"]["fast"]
    assert pts and all("t" in p and "v" in p for p in pts)


async def test_set_strategy_swaps_and_reseeds():
    market = SimpleNamespace(candles=_bars([100, 101, 102, 103, 104]), interval="1m")
    r = _sma_runner(market, running=False)
    r._seed_from_history()

    class _Other:
        name = "other"

        def on_bar(self, close):
            return Decision(Stance.FLAT, {"k": "v"})

    await r.set_strategy(_Other())
    assert r.strategy.name == "other" and r.last_decision.detail == {"k": "v"}
