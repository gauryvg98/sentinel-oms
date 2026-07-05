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

from dataclasses import dataclass
from decimal import ROUND_DOWN, Decimal, InvalidOperation
from typing import Awaitable, Callable

from sentinel.strategy import Bar, Decision, Stance, Strategy

_RESTING = ("WORKING", "PARTIAL")             # cleanly resting -> safe to re-price
_DEFAULT_REPRICE_FRAC = Decimal("0.0005")     # 5 bps: re-peg once the touch drifts
_DEFAULT_REBALANCE_FRAC = Decimal("0.20")     # no-trade band around target (anti-churn)
_DEFAULT_LOT_STEP = Decimal("0.00001")
_HISTORY_CAP = 400


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

    def _target(self, bid: Decimal | None, ask: Decimal | None) -> Decimal | None:
        """Signed desired position from stance + conviction. LONG -> +qty (sized
        off the bid, the side we'd buy into); SHORT -> -qty (off the ask). FLAT
        -> 0 (needs no price, so exits never block on the feed). A SHORT on a
        spot venue (allow_short=False) clamps to 0 — you can't short spot."""
        d = self.last_decision
        if d is None or d.stance is None:
            return None
        if d.stance is Stance.FLAT:
            return Decimal(0)
        if d.stance is Stance.SHORT and not self._allow_short:
            return Decimal(0)
        price = bid if d.stance is Stance.LONG else ask
        if price is None or price <= 0:
            return None
        qty = (self._weight(d) * self._budget_fn() / price).quantize(
            self._lot_step, rounding=ROUND_DOWN)
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
        plan = plan_action(
            await self._position_fn(), bid, ask, await self._open_entry_fn(),
            self._target(bid, ask),
            reprice_frac=self._reprice_frac, rebalance_frac=self._rebalance_frac,
            lot_step=self._lot_step,
        )
        action = await self._execute(plan)
        if action is not None:
            self.last_action = action
            await self._on_change()
        return action

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
            candles = self._bars.candles
            if len(candles) < 2:
                continue
            closed = candles[-2]
            if self._last_closed_t is not None and closed["t"] <= self._last_closed_t:
                continue
            self._last_closed_t = closed["t"]

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
        }
