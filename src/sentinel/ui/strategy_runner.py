"""StrategyRunner — turns a pure strategy's decisions into real orders.

It runs as a supervised task, ALWAYS feeding every closed bar to the strategy
(so the indicators stay warm even while paused), but only ACTING when started.
Actions go through injected callables that hit the normal CommandGateway — so
the strategy faces the exact same guards a human does. A bad strategy trades
badly; it cannot bypass the safety layer or corrupt state.

SIGNED target-position model. The strategy states a desired STANCE (LONG / FLAT
/ SHORT) and an optional CONVICTION (`detail["target_weight"]` in [0,1]); the
runner turns that into a SIGNED target quantity — +weight*budget/price for LONG,
-weight*budget/price for SHORT — and reconciles the actual position toward it:
  - move AWAY from zero, toward the target's side : peg a resting maker order
  - move TOWARD zero (reduce / flip)             : market (want less risk now)
A flip (long -> short) reduces to zero one bar, then opens the other side the
next — the peg's natural one-bar cadence. A no-trade band absorbs small drift so
conviction/price jitter doesn't churn.

SPOT can't short, so on a spot venue `allow_short=False` clamps a SHORT stance to
FLAT (safe, behavior unchanged). Perps set allow_short=True and wire the
short-side callables. plan_action is pure -> every case is tested without a broker.
"""

from __future__ import annotations

import os
import time

from dataclasses import dataclass
from decimal import ROUND_DOWN, ROUND_UP, Decimal, InvalidOperation
from typing import Awaitable, Callable

from sentinel.strategy import Bar, Decision, Stance, Strategy

_RESTING = ("WORKING", "PARTIAL")             # cleanly resting -> safe to re-price
_DEFAULT_REPRICE_FRAC = Decimal("0.0005")     # 5 bps: re-peg once the touch drifts
_DEFAULT_REBALANCE_FRAC = Decimal("0.20")     # no-trade band around target (anti-churn)
_DEFAULT_LOT_STEP = Decimal("0.00001")
_DEFAULT_PRICE_TICK = Decimal("0.00000001")
_HISTORY_CAP = 400

# Hard-stop backstop pacing: re-peg the exchange-side stop only once the
# desired level has ratcheted >= 5 bps of price tighter, and never more often
# than every 30s — a cancel+place per trail tick would spam the venue.
_BACKSTOP_REPEG_FRAC = Decimal("0.0005")
_BACKSTOP_REPEG_S = 30.0


@dataclass(frozen=True, slots=True)
class Plan:
    """What the runner should do this bar.
    kind: "open" (maker, toward the target's side) | "reduce" (market, toward
          zero) | "cancel" | "noop". side/qty/price describe the order."""
    kind: str
    side: str | None = None          # "BUY" | "SELL"
    qty: Decimal | None = None
    price: Decimal | None = None      # peg price for an open
    cancel_key: str | None = None
    reason: str = ""


def plan_action(
    position: Decimal,
    bid: Decimal | None,
    ask: Decimal | None,
    entry: dict | None,               # store.open_entry(): key/side/qty/filled/state/limit_price
    target: Decimal | None,           # SIGNED desired position (None = no opinion)
    *,
    reprice_frac: Decimal = _DEFAULT_REPRICE_FRAC,
    rebalance_frac: Decimal = _DEFAULT_REBALANCE_FRAC,
    lot_step: Decimal = _DEFAULT_LOT_STEP,
) -> Plan:
    """Pure reconciliation of actual position -> a SIGNED target. No I/O."""
    if target is None:
        return Plan("noop", reason="no opinion")

    gap = target - position
    band = abs(target) * rebalance_frac
    if abs(gap) <= band:                          # at target (within the band)
        if entry is not None and entry["state"] in _RESTING:
            return Plan("cancel", cancel_key=entry["key"], reason="at target: cancel remainder")
        return Plan("noop", reason="at target")

    # Reduce? gap moves the position TOWARD zero (opposite sign to the position).
    if position != 0 and (gap > 0) != (position > 0):
        reduce = min(abs(gap), abs(position)).quantize(lot_step, rounding=ROUND_DOWN)
        if reduce <= 0:
            return Plan("noop", reason="dust")
        side = "SELL" if position > 0 else "BUY"   # sell to shed a long, buy to cover a short
        return Plan("reduce", side=side, qty=reduce, reason="reduce toward target")

    # Otherwise open/add AWAY from zero, toward the target's side -> maker peg.
    add = abs(gap).quantize(lot_step, rounding=ROUND_DOWN)
    if add <= 0:
        return Plan("noop", reason="within a lot of target")
    side = "BUY" if gap > 0 else "SELL"
    touch = bid if side == "BUY" else ask          # join the near side (maker)
    if touch is None or touch <= 0:
        return Plan("noop", reason="no touch price to peg to")
    if entry is None:
        return Plan("open", side=side, qty=add, price=touch, reason="place maker entry")
    if entry["state"] not in _RESTING:
        return Plan("noop", reason=f"entry in flight ({entry['state']})")
    if entry.get("side") not in (None, side):      # resting order on the wrong side
        return Plan("cancel", cancel_key=entry["key"], reason="entry wrong side")
    resting = entry["limit_price"]
    if resting is None:
        return Plan("noop", reason="entry has no limit price, leaving it")
    if abs(touch - resting) / touch > reprice_frac:
        return Plan("cancel", cancel_key=entry["key"], reason="re-peg to touch")
    return Plan("noop", reason="pegged")


class StrategyRunner:
    def __init__(
        self,
        strategy: Strategy,
        bars,                                     # bar source (.candles); BarFeed
        *,
        position_fn: Callable[[], Awaitable[Decimal]],
        open_entry_fn: Callable[[], Awaitable[dict | None]],
        place_entry_fn: Callable[[Decimal, Decimal], Awaitable[dict]],  # maker BUY at bid
        reduce_sell_fn: Callable[[Decimal], Awaitable[dict]],           # market SELL qty
        cancel_fn: Callable[[str], Awaitable[dict]],
        bid_fn: Callable[[], Decimal | None],
        ask_fn: Callable[[], Decimal | None],
        budget_fn: Callable[[], Decimal],         # risk budget in USDT (100% conviction)
        on_change: Callable[[], Awaitable[None]],
        place_short_fn: Callable[[Decimal, Decimal], Awaitable[dict]] | None = None,  # maker SELL at ask
        reduce_buy_fn: Callable[[Decimal], Awaitable[dict]] | None = None,            # market BUY (cover)
        allow_short: bool = False,
        poll_s: float = 2.0,
        reprice_frac: Decimal = _DEFAULT_REPRICE_FRAC,
        rebalance_frac: Decimal = _DEFAULT_REBALANCE_FRAC,
        lot_step: Decimal = _DEFAULT_LOT_STEP,
        equity_fn: Callable[[], Decimal] | None = None,   # account equity (USDT)
        risk_params=None,                                 # RiskParams -> risk-based sizing
        entry_fn: Callable[[], Awaitable[Decimal | None]] | None = None,  # position avg cost
        # (price) -> TOTAL |position| qty the bot's real margin can carry
        # (held + resting + free headroom). None -> open-loop sizing as before.
        margin_cap_fn: Callable[[Decimal], Awaitable[Decimal | None]] | None = None,
        # (side, qty, stop) -> reduce-only STOP_MARKET resting ON the exchange
        # (Terminal.place_stop). None -> no exchange-native backstop possible.
        place_stop_fn: Callable[[str, Decimal, Decimal],
                                Awaitable[dict]] | None = None,
        price_tick: Decimal = _DEFAULT_PRICE_TICK,
    ) -> None:
        self.strategy = strategy
        self._bars = bars
        self._position_fn = position_fn
        self._open_entry_fn = open_entry_fn
        self._place_entry_fn = place_entry_fn
        self._reduce_sell_fn = reduce_sell_fn
        self._place_short_fn = place_short_fn
        self._reduce_buy_fn = reduce_buy_fn
        self._cancel_fn = cancel_fn
        self._bid_fn = bid_fn
        self._ask_fn = ask_fn
        self._budget_fn = budget_fn
        self._on_change = on_change
        self._allow_short = allow_short
        self._poll_s = poll_s
        self._reprice_frac = reprice_frac
        self._rebalance_frac = rebalance_frac
        self._lot_step = lot_step
        self._equity_fn = equity_fn
        self._risk_params = risk_params
        self._entry_fn = entry_fn
        self._margin_cap_fn = margin_cap_fn
        self._suppressed = None          # stance we exited on a stop/TP — no re-entry until it flips
        # Bars-based re-entry: >0 lets a bot re-enter the SAME regime after N
        # closed bars post-stop (0 = classic wait-for-flip only).
        self._reentry_bars = int(os.environ.get("SENTINEL_REENTRY_BARS", "0"))
        self._suppress_age = 0           # closed bars since suppression began
        self._last_bracket: dict | None = None   # current SL/TP levels, for the UI
        self._trail: dict | None = None          # ratchet state {sign, hwm, stop}
        # Exchange-native hard-stop backstop (catastrophe insurance: process
        # death leaves a REAL stop on the venue). Gated on SENTINEL_HARD_STOP_PCT
        # = fraction of price of EXTRA distance beyond the software stop
        # (unset/0 = off). The software trail is strictly tighter, so in normal
        # operation it always exits first and the backstop is canceled.
        self._place_stop_fn = place_stop_fn
        self._price_tick = price_tick
        try:
            self._hard_stop_pct = Decimal(
                os.environ.get("SENTINEL_HARD_STOP_PCT", "0") or "0")
        except InvalidOperation:
            self._hard_stop_pct = Decimal(0)
        self._backstop: dict | None = None   # live stop {key, side, stop, qty, at}
        # Tighten-only memory across cancel/replace cycles (mirrors the trail
        # ratchet): a replacement may never rest looser than any level this
        # position's backstop has already held.
        self._backstop_floor: dict | None = None       # {side, level}
        self._now = time.monotonic          # injectable clock (repeg rate limit)

        self.running = False
        self.last_decision: Decision | None = None
        self.last_action: str | None = None
        self._last_closed_t: int | None = None
        self._history: list[dict] = []

    def start(self) -> None:
        self.running = True

    def stop(self) -> None:
        self.running = False

    @staticmethod
    def _weight(d: Decision) -> Decimal:
        try:
            w = Decimal(str(d.detail.get("target_weight", "1")))
        except (InvalidOperation, AttributeError, TypeError):
            w = Decimal("1")
        return max(Decimal(0), min(Decimal(1), w))

    def _target(self, bid: Decimal | None, ask: Decimal | None,
                margin_qty_cap: Decimal | None = None) -> Decimal | None:
        """Signed desired position from stance + conviction. LONG -> +qty (sized
        off the bid, the side we'd buy into); SHORT -> -qty (off the ask). FLAT
        -> 0 (needs no price, so exits never block on the feed). A SHORT on a
        spot venue (allow_short=False) clamps to 0 — you can't short spot.

        `margin_qty_cap` (resolved by the async caller — this stays sync/pure)
        is the closed-loop clamp on the TOTAL target magnitude: held + resting
        + what free margin can still add. It caps the target, it never shrinks
        it below the current position — the cap already INCLUDES the position,
        so a fully-margined position targets itself (noop), not a flatten."""
        d = self.last_decision
        if d is None or d.stance is None:
            return None
        if d.stance is Stance.FLAT:
            return Decimal(0)
        # Re-entry suppression: after a stop/TP exit, stay flat until the stance
        # FLIPS — don't immediately pile back into the same trade we just exited.
        if self._suppressed is not None:
            if d.stance == self._suppressed:
                # Same regime: stay flat — unless bar-based re-entry is on and
                # enough bars have closed since the stop (the market got N bars
                # to prove the move was a wick, not a trend against us).
                if self._reentry_bars > 0 and self._suppress_age >= self._reentry_bars:
                    self._suppressed = None
                else:
                    return Decimal(0)
            self._suppressed = None
        if d.stance is Stance.SHORT and not self._allow_short:
            return Decimal(0)
        price = bid if d.stance is Stance.LONG else ask
        if price is None or price <= 0:
            return None
        if self._risk_params is not None and self._equity_fn is not None:
            # Risk layer: size so a stop-out costs risk_pct·conviction of equity,
            # capped by leverage. Uses the SAME stop distance as the SL (strategy
            # geometry or ATR), so size and stop stay consistent.
            from sentinel.risk import risk_sized_qty
            raw = risk_sized_qty(
                self._risk_params, equity=self._equity_fn(), price=price,
                stop_dist=self._stop_dist(price), conviction=self._weight(d),
                margin_qty_cap=margin_qty_cap)
        else:
            raw = self._weight(d) * self._budget_fn() / price   # fixed-notional budget
        qty = raw.quantize(self._lot_step, rounding=ROUND_DOWN)
        return -qty if d.stance is Stance.SHORT else qty

    def _feed(self, c: dict) -> Decision:
        ohlcv = getattr(self.strategy, "on_bar_ohlcv", None)
        if callable(ohlcv):
            return ohlcv(Bar(high=Decimal(str(c["h"])), low=Decimal(str(c["l"])),
                             close=Decimal(str(c["c"]))))
        return self.strategy.on_bar(Decimal(str(c["c"])))

    def _seed_from_history(self) -> None:
        reset = getattr(self.strategy, "reset", None)
        if callable(reset):
            reset()
        self.last_decision = None
        self._last_closed_t = None
        self._history = []
        for c in self._bars.candles[:-1]:
            self.last_decision = self._feed(c)
            self._last_closed_t = c["t"]
            self._history.append({"t": c["t"], "detail": self.last_decision.detail})
        del self._history[:-_HISTORY_CAP]

    async def set_strategy(self, strategy: Strategy) -> None:
        self.strategy = strategy
        await self.reseed()

    async def reseed(self) -> None:
        self._seed_from_history()
        if self.running:
            await self.reconcile_now()

    async def reconcile_now(self) -> str | None:
        if not self.running or self.last_decision is None:
            return None
        bid, ask = self._bid_fn(), self._ask_fn()
        # Closed-loop margin clamp: ask the bot how much TOTAL position its real
        # free margin can carry (position + resting-order margin already netted
        # out) BEFORE sizing. Resolved here because it needs I/O and _target is
        # sync. Either touch price works as the reference — the cap's 10% safety
        # buffer dwarfs a spread's worth of difference.
        margin_qty_cap = None
        if self._margin_cap_fn is not None and (ref := bid or ask):
            margin_qty_cap = await self._margin_cap_fn(ref)
        plan = plan_action(
            await self._position_fn(), bid, ask, await self._open_entry_fn(),
            self._target(bid, ask, margin_qty_cap),
            reprice_frac=self._reprice_frac, rebalance_frac=self._rebalance_frac,
            lot_step=self._lot_step,
        )
        action = await self._execute(plan)
        if action is not None:
            self.last_action = action
            await self._on_change()
        return action

    def _stop_dist(self, price: Decimal):
        """Effective stop distance for BOTH sizing and the SL: the strategy's own
        geometry (last decision's detail['stop_dist']) when it provides one, else
        the risk layer's ATR-based stop. None without risk params."""
        if self._risk_params is None:
            return None
        d = self.last_decision
        raw = d.detail.get("stop_dist") if d and isinstance(d.detail, dict) else None
        try:
            sd = Decimal(str(raw)) if raw is not None else None
        except (InvalidOperation, TypeError):
            sd = None
        if sd and sd > 0:
            return sd
        from sentinel.risk import atr, stop_distance
        return stop_distance(self._risk_params, price, atr(self._bars.candles))

    async def _check_brackets(self) -> None:
        """Risk-layer protective exits. If the mark has breached the open
        position's stop-loss or take-profit, flatten at market and suppress
        re-entry until the signal flips. Also stashes the live SL/TP levels for
        the UI. No-op without risk params, a position, an entry price, or while
        paused."""
        if not self.running or self._risk_params is None:
            # Deliberately leaves any resting exchange-side backstop alone: a
            # paused runner with a position is EXACTLY when venue-side
            # protection matters most, and reduce-only means it stays harmless.
            self._last_bracket = None
            self._trail = None
            return
        pos = await self._position_fn()
        if pos == 0:
            await self._cancel_backstop()        # flat: nothing left to protect
            self._backstop_floor = None
            self._last_bracket = None
            self._trail = None
            return
        entry = await self._entry_fn() if self._entry_fn is not None else None
        price = self._bid_fn() or self._ask_fn()
        if entry is None or entry <= 0 or price is None or price <= 0:
            return
        from sentinel.risk import brackets, breached
        p = self._risk_params
        d = self.last_decision
        sd = self._stop_dist(price)
        if not sd or sd <= 0:
            return
        is_long = pos > 0
        if p.trail:
            # Ratcheting trail: stop follows the peak at sd and only tightens;
            # no fixed TP — the trail IS the profit-taker (gains run, and a
            # run's giveback is bounded to ~sd from its best price).
            from sentinel.risk.model import trail_ratchet
            sign = 1 if is_long else -1
            t = self._trail
            if t is None or t.get("sign") != sign:
                t = {"sign": sign, "hwm": None, "stop": None}
            t["hwm"], stop = trail_ratchet(is_long, entry, price, sd,
                                           t["hwm"], t["stop"])
            t["stop"] = stop
            self._trail = t
            take = None
        else:
            stop, take = brackets(entry, is_long, sd, p.rr)
        self._last_bracket = {"stop": stop, "take": take, "is_long": is_long,
                              "trail": bool(p.trail)}
        hit = breached(is_long, price, stop, take)
        if hit is None:
            await self._manage_backstop(pos, price, stop, is_long)
            return
        # SOFTWARE stop/TP fired -> flatten at market. The resting backstop
        # counts toward store.open_exit_remaining, so it MUST be canceled
        # FIRST or clamp_exit would refuse the flatten (NothingToExit).
        await self._cancel_backstop()
        qty = abs(pos)                       # clamp_exit trims off any residue
        r: dict | None = None
        if is_long and self._reduce_sell_fn is not None:
            r = await self._reduce_sell_fn(qty)
        elif not is_long and self._reduce_buy_fn is not None:
            r = await self._reduce_buy_fn(qty)
        if isinstance(r, dict) and \
                any(k in r for k in ("blocked", "rejected", "error")):
            # Flatten refused — typically NothingToExit while the backstop's
            # cancel confirm hasn't booked yet. Keep the trail/bracket state
            # UNTOUCHED so the breach re-fires next poll and the flatten is
            # retried; clearing it here would re-derive a looser trail from
            # the current price and silently wedge the position open.
            self.last_action = (f"{hit} hit @ {format(price.normalize(), 'f')}"
                                f" — flatten {self._fmt(r)}; retrying")
            await self._on_change()
            return
        self._suppressed = d.stance if d is not None else None
        self._suppress_age = 0
        self._last_bracket = None
        self._trail = None
        self._backstop_floor = None
        self.last_action = f"{hit} hit @ {format(price.normalize(), 'f')} — flattened"
        await self._on_change()

    async def _cancel_backstop(self) -> None:
        """Cancel the live exchange-side backstop, if any. Clears the tracking
        eagerly — the cancel confirm books asynchronously, and until it does
        the order still counts against never-over-exit (callers know this and
        retry whatever the backstop was blocking on the next poll)."""
        bs, self._backstop = self._backstop, None
        if bs is not None:
            await self._cancel_fn(bs["key"])

    async def _manage_backstop(self, pos: Decimal, price: Decimal,
                               stop: Decimal, is_long: bool) -> None:
        """Exchange-NATIVE trailing hard-stop: a reduce-only STOP_MARKET
        resting ON the venue, SENTINEL_HARD_STOP_PCT of price BEYOND the
        software stop. Catastrophe insurance — if this process dies, the
        position still has a real stop; in normal operation the strictly
        tighter software trail exits first (canceling this order first).

        Lifecycle per poll, only while the software stop is known and NOT
        breached: place when missing; cancel-and-replace when the trail
        ratcheted the desired level >= 5 bps of price tighter OR |position|
        resized >= one lot (both rate-limited to one repeg per 30s; the
        replacement goes out on a LATER poll, once the cancel confirm frees
        the never-over-exit budget); cancel on flip. MONOTONIC like the trail:
        the desired level only ever tightens."""
        if self._place_stop_fn is None or self._hard_stop_pct <= 0:
            return
        side = "SELL" if is_long else "BUY"
        if self._backstop is not None and self._backstop["side"] != side:
            await self._cancel_backstop()        # position flipped
        if self._backstop_floor is not None and \
                self._backstop_floor["side"] != side:
            self._backstop_floor = None
        # Desired trigger: the extra margin beyond the software stop, snapped
        # to the tick grid AWAY from the mark (never tighter than asked), then
        # floored against every level already rested at (tighten-only).
        extra = price * self._hard_stop_pct
        desired = stop - extra if is_long else stop + extra
        if self._price_tick > 0:
            desired = (desired / self._price_tick).to_integral_value(
                rounding=ROUND_DOWN if is_long else ROUND_UP) * self._price_tick
        if self._backstop_floor is not None:
            level = self._backstop_floor["level"]
            desired = max(desired, level) if is_long else min(desired, level)
        if desired <= 0:
            return
        qty = abs(pos)
        bs = self._backstop
        if bs is None:
            r = await self._place_stop_fn(side, qty, desired)
            if isinstance(r, dict) and "placed" in r:
                self._backstop = {"key": r["placed"], "side": side,
                                  "stop": desired, "qty": qty, "at": self._now()}
                self._backstop_floor = {"side": side, "level": desired}
            return                               # blocked/rejected: retry next poll
        if self._now() - bs["at"] < _BACKSTOP_REPEG_S:
            return                               # repeg rate limit
        tighter = desired - bs["stop"] if is_long else bs["stop"] - desired
        resized = abs(qty - bs["qty"]) >= self._lot_step
        if resized or tighter >= price * _BACKSTOP_REPEG_FRAC:
            await self._cancel_backstop()        # replacement placed next poll

    async def _execute(self, plan: Plan) -> str | None:
        if plan.kind == "noop":
            return None
        if plan.kind == "cancel":
            await self._cancel_fn(plan.cancel_key)
            return f"CANCEL {plan.cancel_key} — {plan.reason}"
        if plan.kind == "open":
            if plan.side == "BUY":
                r = await self._place_entry_fn(plan.qty, plan.price)
            elif self._place_short_fn is not None:
                r = await self._place_short_fn(plan.qty, plan.price)
            else:
                return "BLOCKED short-open (spot venue)"
            return f"PEG {plan.side} {self._fmt(r)} @ {format(plan.price.normalize(), 'f')}"
        if plan.kind == "reduce":
            # A resting backstop claims the WHOLE position in never-over-exit
            # accounting, so any strategy reduce would clamp to nothing
            # (NothingToExit). Cancel it first; this reduce may still come
            # back blocked this cycle (cancel confirm not yet booked) — the
            # next reconcile re-plans and retries, and the backstop is
            # re-placed by _check_brackets if a position remains.
            await self._cancel_backstop()
            if plan.side == "SELL":
                r = await self._reduce_sell_fn(plan.qty)
            elif self._reduce_buy_fn is not None:
                r = await self._reduce_buy_fn(plan.qty)
            else:
                return "BLOCKED cover (spot venue)"
            return f"REDUCE {plan.side} {self._fmt(r)}"
        return None

    @staticmethod
    def _fmt(result: dict) -> str:
        if "placed" in result:
            return f"placed {result.get('qty', '')}".strip()
        if "pending" in result:
            return "pending (reconciling)"
        if "rejected" in result:
            return f"rejected ({result['reason']})"
        if "blocked" in result:
            return f"blocked ({result.get('reason', result['blocked'])})"
        if "error" in result:
            return f"error ({result['error']})"
        return str(result)

    async def run(self) -> None:
        self._seed_from_history()

        import asyncio

        while True:
            await asyncio.sleep(self._poll_s)
            await self._check_brackets()          # SL/TP on every poll, not just bar closes
            candles = self._bars.candles
            if len(candles) < 2:
                continue
            closed = candles[-2]
            if self._last_closed_t is not None and closed["t"] <= self._last_closed_t:
                continue
            self._last_closed_t = closed["t"]

            if self._suppressed is not None:
                self._suppress_age += 1          # one more closed bar in timeout
            self.last_decision = self._feed(closed)
            self._history.append({"t": closed["t"], "detail": self.last_decision.detail})
            del self._history[:-_HISTORY_CAP]
            await self.reconcile_now()
            await self._on_change()

    def _view_spec(self) -> dict:
        vs = getattr(self.strategy, "view_spec", None)
        return vs() if callable(vs) else {"rows": [], "overlays": []}

    def _series(self, view: dict) -> dict:
        keys: set[str] = set()
        for ov in view.get("overlays", []):
            if ov["kind"] == "line":
                keys.add(ov["key"])
            elif ov["kind"] == "band":
                keys.update((ov["upper"], ov["lower"]))
        out: dict[str, list] = {k: [] for k in keys}
        for h in self._history:
            for k in keys:
                v = h["detail"].get(k)
                if v is None:
                    continue
                try:
                    out[k].append({"t": h["t"], "v": float(v)})
                except (ValueError, TypeError):
                    pass
        return out

    def snapshot(self) -> dict:
        d = self.last_decision
        directional = d and d.stance in (Stance.LONG, Stance.SHORT)
        return {
            "name": self.strategy.name,
            "running": self.running,
            "stance": d.stance.value if d and d.stance else None,
            "detail": d.detail if d else {},
            "last_action": self.last_action,
            "weight": str(self._weight(d)) if directional else None,
            "view": (view := self._view_spec()),
            "series": self._series(view),
            "interval": getattr(self._bars, "interval", None),
            "risk": self._risk_view(),
        }

    def _risk_view(self) -> dict | None:
        """What the risk layer is doing right now — for the UI. None when sizing
        is the plain fixed-notional budget."""
        p = self._risk_params
        if p is None:
            return None
        from sentinel.risk import atr
        candles = getattr(self._bars, "candles", [])
        price = self._bid_fn() or self._ask_fn()
        a = atr(candles)
        sd = self._stop_dist(price) if price else None
        # Did the strategy supply its own stop geometry, or did we fall back?
        d = self.last_decision
        strat_stop = bool(d and isinstance(d.detail, dict) and d.detail.get("stop_dist"))
        b = self._last_bracket
        return {
            "mode": "risk-based",
            "risk_pct": str(p.risk_pct),
            "max_leverage": str(p.max_leverage),
            "stop_atr_mult": str(p.stop_atr_mult),
            "rr": str(p.rr),
            "stop_source": "strategy" if strat_stop else "atr",
            "atr": str(a) if a is not None else None,
            "stop_dist": str(sd) if sd is not None else None,
            "stop_pct": (str((sd / price).quantize(Decimal("0.0001")))
                         if sd is not None and price else None),
            "stop_price": str(b["stop"]) if b else None,
            # take may be None when rr<=0 (take-profit disabled — ride to flip).
            "take_price": str(b["take"]) if b and b.get("take") is not None else None,
            # exchange-native hard-stop backstop currently resting on the venue
            "hard_stop_price": (str(self._backstop["stop"])
                                if self._backstop else None),
        }
