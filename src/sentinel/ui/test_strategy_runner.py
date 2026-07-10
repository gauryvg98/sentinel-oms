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
         running=True, allow_short=True, market_execution=False,
         strategy=None, candles=None):
    calls = {"open_buy": [], "open_sell": [], "reduce_sell": [],
             "reduce_buy": [], "cancel": [],
             "mkt_buy": [], "mkt_sell": []}

    async def position_fn():
        return Decimal(position) if isinstance(position, str) else position()

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

    async def market_buy_fn(qty):
        calls["mkt_buy"].append(qty); return {"placed": "MB1", "qty": str(qty)}

    async def market_sell_fn(qty):
        calls["mkt_sell"].append(qty); return {"placed": "MS1", "qty": str(qty)}

    async def on_change():
        return None

    bars = SimpleNamespace(candles=candles if candles is not None else [])
    r = StrategyRunner(
        strategy if strategy is not None else _Strat(stance, detail), bars,
        position_fn=position_fn, open_entry_fn=open_entry_fn,
        place_entry_fn=place_entry_fn, reduce_sell_fn=reduce_sell_fn,
        cancel_fn=cancel_fn, bid_fn=lambda: PX, ask_fn=lambda: PX,
        budget_fn=lambda: B, on_change=on_change,
        place_short_fn=place_short_fn, reduce_buy_fn=reduce_buy_fn,
        allow_short=allow_short, market_execution=market_execution,
        place_market_buy_fn=market_buy_fn, place_market_sell_fn=market_sell_fn)
    r.running = running
    if strategy is None:
        r.last_decision = Decision(stance, detail or {}) if stance is not None else None
    return r, calls, bars


async def test_reconcile_opens_a_long_maker_when_flat():
    r, c, _bars = make(stance=Stance.LONG, position="0")
    a = await r.reconcile_now()
    assert c["open_buy"] == [(Decimal("0.15"), PX)] and a.startswith("PEG BUY")


async def test_reconcile_opens_a_short_maker_when_flat():
    r, c, _bars = make(stance=Stance.SHORT, position="0")
    a = await r.reconcile_now()
    assert c["open_sell"] == [(Decimal("0.15"), PX)] and a.startswith("PEG SELL")


async def test_reconcile_reduces_an_oversized_long_at_market():
    r, c, _bars = make(stance=Stance.LONG, position="0.20")
    a = await r.reconcile_now()
    assert c["reduce_sell"] == [Decimal("0.05")] and a.startswith("REDUCE SELL")


async def test_reconcile_covers_a_short_at_market():
    r, c, _bars = make(stance=Stance.SHORT, position="-0.20")
    a = await r.reconcile_now()
    assert c["reduce_buy"] == [Decimal("0.05")] and a.startswith("REDUCE BUY")


async def test_short_stance_on_spot_does_not_short():
    r, c, _bars = make(stance=Stance.SHORT, position="0", allow_short=False)
    assert await r.reconcile_now() is None            # target 0, flat -> noop
    assert c["open_sell"] == [] and c["reduce_sell"] == []


async def test_half_conviction_sizes_the_entry_down():
    r, c, _bars = make(detail={"target_weight": "0.5"}, position="0")
    await r.reconcile_now()
    assert c["open_buy"] == [(Decimal("0.075"), PX)]


async def test_paused_runner_does_nothing():
    r, c, _bars = make(position="0", running=False)
    assert await r.reconcile_now() is None


# ------------------------------------------------ market-execution mode (reactive)
#
# Off (default = peg): the "open" branch rests a maker peg and the run loop
# evaluates the signal ONLY on bar close. On ("market"): the open branch fires a
# MARKET entry, the signal is re-evaluated intra-bar off the forming bar every
# poll, and a stance flip executes both legs at once (market reduce, then market
# open). SLs stay software-market either way. These follow the fake-fn patterns
# above; the intra-bar/flip tests drive one iteration of run()'s per-poll body.

async def _one_poll(r):
    """Run exactly one iteration of the market-mode per-poll body (the loop in
    run(), minus the sleep/supervision) so intra-bar behaviour is testable
    without the infinite loop. Mirrors run() step for step."""
    await r._check_brackets()
    candles = r._bars.candles
    if len(candles) < 2:
        return
    closed = candles[-2]
    new_close = r._last_closed_t is None or closed["t"] > r._last_closed_t
    if new_close:
        r._last_closed_t = closed["t"]
        if r._suppressed is not None:
            r._suppress_age += 1
        r.last_decision = r._feed(closed)
        r._history.append({"t": closed["t"], "detail": r.last_decision.detail})
    if r._market_execution:
        d = r._decide_intrabar()
        if d is not None:
            r.last_decision = d
        await r.reconcile_now()


async def test_market_mode_places_a_market_entry_not_a_peg():
    r, c, _bars = make(stance=Stance.LONG, position="0", market_execution=True)
    a = await r.reconcile_now()
    # MARKET open path taken (limit_price=None), NOT a resting peg.
    assert c["mkt_buy"] == [Decimal("0.15")] and c["open_buy"] == []
    assert a.startswith("MARKET BUY")


async def test_market_mode_short_open_is_a_market_sell():
    r, c, _bars = make(stance=Stance.SHORT, position="0", market_execution=True)
    a = await r.reconcile_now()
    assert c["mkt_sell"] == [Decimal("0.15")] and c["open_sell"] == []
    assert a.startswith("MARKET SELL")


async def test_peg_mode_still_places_a_peg_limit():
    # Flag OFF -> byte-identical peg behaviour: resting maker limit, no market fn.
    r, c, _bars = make(stance=Stance.LONG, position="0", market_execution=False)
    a = await r.reconcile_now()
    assert c["open_buy"] == [(Decimal("0.15"), PX)] and c["mkt_buy"] == []
    assert a.startswith("PEG BUY")


def _ohlcv(closes):
    """OHLCV candle series (SMA reads only close). t is bar-aligned."""
    return [{"t": i, "h": str(c), "l": str(c), "c": str(c)}
            for i, c in enumerate(closes, start=1)]


async def test_intrabar_reevaluates_and_acts_between_bar_closes():
    # Closed bars 100,99,98 -> SMA(2/3) fast<=slow -> FLAT (no position).
    # A FORMING bar whose close climbs to 200 lifts fast>slow -> LONG intra-bar:
    # market mode must OPEN off the forming bar without waiting for it to close.
    candles = _ohlcv([100, 99, 98]) + [{"t": 4, "h": "98", "l": "98", "c": "98"}]
    r, c, bars = make(strategy=SmaCross(fast=2, slow=3), position="0",
                      market_execution=True, candles=candles)
    r._seed_from_history()
    hist_before = len(r._history)

    await _one_poll(r)                        # forming close 98 -> still FLAT
    assert c["mkt_buy"] == [] and r.last_decision.stance is Stance.FLAT
    closed_t_before = r._last_closed_t

    bars.candles[-1] = {"t": 4, "h": "200", "l": "200", "c": "200"}  # forming climbs
    await _one_poll(r)                        # intra-bar flip to LONG -> market open
    assert r.last_decision.stance is Stance.LONG
    assert c["mkt_buy"] == [Decimal("0.15")]  # acted BETWEEN closes
    # Bookkeeping untouched by the forming-bar peek: no new close was booked.
    assert r._last_closed_t == closed_t_before
    assert len(r._history) == hist_before     # only the real closed decisions


async def test_intrabar_history_and_suppression_advance_only_on_close():
    # Suppress LONG (as after a stop); with a forming bar re-evaluated every poll,
    # _suppress_age and _history must advance ONLY when a real bar closes.
    candles = _ohlcv([100, 101, 102]) + [{"t": 4, "h": "103", "l": "103", "c": "103"}]
    r, c, bars = make(strategy=SmaCross(fast=2, slow=3), position="0",
                      market_execution=True, candles=candles)
    r._seed_from_history()
    hist0, age0 = len(r._history), r._suppress_age
    r._suppressed = Stance.LONG

    await _one_poll(r)                        # forming bar peeked, no new close
    assert r._suppress_age == age0 and len(r._history) == hist0

    await _one_poll(r)                        # still the same forming bar
    assert r._suppress_age == age0 and len(r._history) == hist0

    # A REAL close appears (the forming bar closes, a new one forms) -> advance.
    bars.candles[-1] = {"t": 4, "h": "103", "l": "103", "c": "103"}
    bars.candles.append({"t": 5, "h": "104", "l": "104", "c": "104"})
    await _one_poll(r)
    assert r._suppress_age == age0 + 1 and len(r._history) == hist0 + 1


async def test_long_to_short_flip_closes_and_opens_instantly_in_market_mode():
    # LONG position; strategy now says SHORT. Market mode must reduce the long to
    # zero AND open the short in the SAME reconcile pass — no waiting for a bar
    # close between the two legs (the peg's one-bar cadence).
    pos = {"q": Decimal("0.15")}

    r, c, bars = make(stance=Stance.SHORT, position=lambda: pos["q"],
                      market_execution=True, allow_short=True)
    r.last_decision = Decision(Stance.SHORT, {})

    # First pass reduces the long to zero (market), then the position is flat...
    a1 = await r.reconcile_now()
    assert c["reduce_sell"] == [Decimal("0.15")]    # long closed at market
    pos["q"] = Decimal("0")                          # reduce booked -> flat

    # ...second pass opens the short at market — both legs fire promptly, no bar
    # close needed between them.
    a2 = await r.reconcile_now()
    assert c["mkt_sell"] == [Decimal("0.15")]       # short opened at market
    assert a1.startswith("REDUCE SELL") and a2.startswith("MARKET SELL")


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


# ------------------------------------------ exchange-native hard-stop backstop

from sentinel.risk import RiskParams  # noqa: E402


def make_hard_stop(monkeypatch, *, position="0.15", entry_price="100",
                   stop_dist="1", pct="0.005", trail=True, env=True):
    """A risk-layer runner with the backstop enabled: fake fns recording ONE
    ordered call log (so cancel-vs-flatten ordering is provable), an
    injectable clock for the 30s repeg limit, and mutable pos/price/results.
    Long stance, entry 100, stop_dist 1 -> software stop 99, backstop 98.5."""
    if env:
        monkeypatch.setenv("SENTINEL_HARD_STOP_PCT", pct)
    else:
        monkeypatch.delenv("SENTINEL_HARD_STOP_PCT", raising=False)
    state = {"pos": Decimal(position), "price": Decimal("100"),
             "reduce_result": None}
    seq: list[tuple] = []
    n = {"stop": 0}

    async def position_fn():
        return state["pos"]

    async def open_entry_fn():
        return None

    async def entry_fn():
        return Decimal(entry_price)

    async def place_entry_fn(qty, price):
        seq.append(("open_buy", qty)); return {"placed": "K1", "qty": str(qty)}

    async def place_short_fn(qty, price):
        seq.append(("open_sell", qty)); return {"placed": "S1", "qty": str(qty)}

    async def reduce_sell_fn(qty):
        seq.append(("reduce_sell", qty))
        return state["reduce_result"] or {"placed": "R1", "qty": str(qty)}

    async def reduce_buy_fn(qty):
        seq.append(("reduce_buy", qty))
        return state["reduce_result"] or {"placed": "C1", "qty": str(qty)}

    async def cancel_fn(key):
        seq.append(("cancel", key)); return {"canceling": key}

    async def place_stop_fn(side, qty, stop):
        n["stop"] += 1
        seq.append(("place_stop", side, qty, stop))
        return {"placed": f"BS{n['stop']}", "qty": str(qty)}

    async def on_change():
        return None

    d = {"stop_dist": stop_dist}
    r = StrategyRunner(
        _Strat(Stance.LONG, d), SimpleNamespace(candles=[]),
        position_fn=position_fn, open_entry_fn=open_entry_fn,
        place_entry_fn=place_entry_fn, reduce_sell_fn=reduce_sell_fn,
        cancel_fn=cancel_fn,
        bid_fn=lambda: state["price"], ask_fn=lambda: state["price"],
        budget_fn=lambda: B, on_change=on_change,
        place_short_fn=place_short_fn, reduce_buy_fn=reduce_buy_fn,
        allow_short=True, lot_step=Decimal("0.001"),
        equity_fn=lambda: Decimal("1000"),
        risk_params=RiskParams(risk_pct=Decimal("0.01"),
                               max_leverage=Decimal("5"),
                               stop_atr_mult=Decimal("2"),
                               fallback_stop_pct=Decimal("0.01"),
                               rr=Decimal("0"), trail=trail),
        entry_fn=entry_fn,
        place_stop_fn=place_stop_fn, price_tick=Decimal("0.01"))
    r.running = True
    r.last_decision = Decision(Stance.LONG, d)
    clock = {"t": 0.0}
    r._now = lambda: clock["t"]
    return r, state, seq, clock


async def test_backstop_places_once_behind_the_software_stop(monkeypatch):
    r, state, seq, clock = make_hard_stop(monkeypatch)
    await r._check_brackets()
    # software stop 99, extra 0.5% of price -> reduce-only stop rests at 98.5
    assert seq == [("place_stop", "SELL", Decimal("0.15"), Decimal("98.5"))]
    await r._check_brackets()                     # same level, same qty ->
    await r._check_brackets()                     # nothing new goes out
    assert len(seq) == 1
    assert r._backstop["key"] == "BS1" and r._backstop["stop"] == Decimal("98.5")


async def test_backstop_off_without_the_env_gate(monkeypatch):
    r, state, seq, clock = make_hard_stop(monkeypatch, env=False)
    await r._check_brackets()
    assert seq == [] and r._backstop is None


async def test_backstop_repeg_only_tighter_and_only_every_30s(monkeypatch):
    r, state, seq, clock = make_hard_stop(monkeypatch)
    await r._check_brackets()                     # rests @ 98.5
    state["price"] = Decimal("102")               # trail ratchets stop to 101
    await r._check_brackets()                     # tighter, but < 30s -> hold
    assert [k for k, *_ in seq] == ["place_stop"]
    clock["t"] = 31.0
    await r._check_brackets()                     # repeg: cancel this poll...
    assert seq[-1] == ("cancel", "BS1") and r._backstop is None
    await r._check_brackets()                     # ...replacement next poll
    # desired = 101 - 0.5% of 102 = 100.49 (tick-quantized)
    assert seq[-1] == ("place_stop", "SELL", Decimal("0.15"), Decimal("100.49"))


async def test_backstop_never_loosens_even_when_the_stop_widens(monkeypatch):
    # Non-trail mode: the static bracket CAN loosen when the strategy's stop
    # geometry widens — the backstop is MONOTONIC and must not follow it down.
    r, state, seq, clock = make_hard_stop(monkeypatch, trail=False)
    await r._check_brackets()                     # stop 99 -> backstop 98.5
    assert seq[-1][3] == Decimal("98.5")
    r.last_decision = Decision(Stance.LONG, {"stop_dist": "2"})  # stop now 98
    clock["t"] = 61.0                             # rate limit long expired
    await r._check_brackets()
    assert [k for k, *_ in seq] == ["place_stop"]           # no looser repeg
    assert r._backstop["stop"] == Decimal("98.5")


async def test_backstop_cancels_on_flat(monkeypatch):
    r, state, seq, clock = make_hard_stop(monkeypatch)
    await r._check_brackets()
    state["pos"] = Decimal("0")
    await r._check_brackets()
    assert seq[-1] == ("cancel", "BS1")
    assert r._backstop is None and r._backstop_floor is None


async def test_backstop_repegs_qty_on_position_resize(monkeypatch):
    r, state, seq, clock = make_hard_stop(monkeypatch)
    await r._check_brackets()                     # SELL 0.15 @ 98.5
    state["pos"] = Decimal("0.20")                # added >= 1 lot to the long
    clock["t"] = 31.0
    await r._check_brackets()                     # resize -> cancel...
    assert seq[-1] == ("cancel", "BS1")
    await r._check_brackets()                     # ...replace at the new qty
    assert seq[-1] == ("place_stop", "SELL", Decimal("0.20"), Decimal("98.5"))


async def test_backstop_flips_with_the_position(monkeypatch):
    r, state, seq, clock = make_hard_stop(monkeypatch)
    await r._check_brackets()                     # long backstop (SELL stop)
    state["pos"] = Decimal("-0.15")               # flipped short
    await r._check_brackets()
    i = seq.index(("cancel", "BS1"))              # old side canceled first,
    assert seq[i + 1][0] == "place_stop"          # then a BUY stop behind the
    assert seq[i + 1][1] == "BUY"                 # short's software stop
    assert seq[i + 1][3] == Decimal("101.5")      # 101 + 0.5% of price


async def test_software_stop_cancels_backstop_before_flatten(monkeypatch):
    """THE guard trap: the resting backstop counts toward open_exit_remaining,
    so when the SOFTWARE stop fires the runner must cancel the backstop FIRST
    and only then send the market flatten — and a flatten refused because the
    cancel confirm hasn't booked yet must be RETRIED next poll with the trail
    state intact (not cleared into a looser re-derived stop, not suppressed)."""
    r, state, seq, clock = make_hard_stop(monkeypatch)
    await r._check_brackets()                     # backstop resting @ 98.5
    state["price"] = Decimal("99")                # software stop (99) breached
    state["reduce_result"] = {"blocked": "NothingToExit",
                              "reason": "committed to exits"}
    await r._check_brackets()
    assert seq[1:] == [("cancel", "BS1"), ("reduce_sell", Decimal("0.15"))]
    assert r._suppressed is None and r._trail is not None    # no wedge
    state["reduce_result"] = None                 # cancel confirm booked now
    await r._check_brackets()                     # breach re-fires -> retry
    assert seq[-1] == ("reduce_sell", Decimal("0.15"))
    assert seq.count(("reduce_sell", Decimal("0.15"))) == 2
    assert r._suppressed is Stance.LONG and r._trail is None
    assert r._backstop is None


async def test_strategy_reduce_cancels_backstop_first(monkeypatch):
    """Same trap on the NORMAL exit path: a strategy-driven reduce would be
    clamped to nothing while the backstop claims the whole position — the
    runner cancels the backstop before any reduce goes out."""
    r, state, seq, clock = make_hard_stop(monkeypatch)
    await r._check_brackets()                     # backstop resting
    r.last_decision = Decision(Stance.FLAT, {})   # strategy wants out
    await r.reconcile_now()
    i = seq.index(("cancel", "BS1"))
    assert seq[i + 1] == ("reduce_sell", Decimal("0.15"))


def _rejecting_hard_stop(monkeypatch, reason):
    """A backstop runner whose place_stop ALWAYS rejects with `reason` — the
    real shape place_stop returns on a broker reject: {"rejected", "reason"}.
    Records every attempt so a flood is visible as call count."""
    r, state, seq, clock = make_hard_stop(monkeypatch)

    async def rejecting(side, qty, stop):
        seq.append(("place_stop", side, qty, stop))
        return {"rejected": "BX", "reason": reason}

    r._place_stop_fn = rejecting
    return r, state, seq, clock


async def test_rejecting_backstop_retries_at_most_once_per_repeg_window(monkeypatch):
    """A transient reject leaves _backstop None; without attempt-time backoff
    the venue is re-hit EVERY poll (the 579-reject flood). It must be retried
    at most once per _BACKSTOP_REPEG_S, regardless of failure."""
    r, state, seq, clock = _rejecting_hard_stop(
        monkeypatch, reason="[-1001] internal error, try again")
    for _ in range(20):                           # 20 polls, clock frozen at 0
        await r._check_brackets()
    assert seq.count(("place_stop", "SELL", Decimal("0.15"), Decimal("98.5"))) == 1
    clock["t"] = 31.0                             # window elapsed -> one more try
    await r._check_brackets()
    assert len([s for s in seq if s[0] == "place_stop"]) == 2
    assert r._backstop is None                    # still never placed


async def test_unsupported_reject_disables_backstop_entirely(monkeypatch):
    """A -4120 'not supported' reject is permanent — the manager must set the
    unsupported flag and NEVER attempt again this process, even across many
    polls and elapsed repeg windows. place_stop called exactly once."""
    r, state, seq, clock = _rejecting_hard_stop(
        monkeypatch, reason="[-4120] Order type not supported for this endpoint")
    await r._check_brackets()
    assert r._hard_stop_unsupported is True
    for t in (31.0, 62.0, 999.0):                 # windows come and go
        clock["t"] = t
        for _ in range(5):
            await r._check_brackets()
    assert len([s for s in seq if s[0] == "place_stop"]) == 1
    assert r._backstop is None
