"""StrategyRunner — turns a pure strategy's decisions into real orders.

It runs as a supervised task, ALWAYS feeding every closed bar to the strategy
(so the indicators stay warm even while paused), but only ACTING when started.
Actions go through injected callables that hit the normal CommandGateway — so
the strategy faces the exact same guards a human does: single-writer,
never-over-exit, duplicate-entry. A bad strategy trades badly; it cannot bypass
the safety layer or corrupt state.

SIZED TARGET-POSITION model. The strategy states a desired STANCE and an
optional CONVICTION (`detail["target_weight"]` in [0,1]); the runner turns that
into a target quantity — `weight * budget / touch` — and reconciles the actual
position toward it:
  - under target : peg a resting maker BUY (join the bid, re-price on drift)
  - over  target : trim the excess at market
  - flat / weight 0 : close the position
A no-trade band around the target absorbs small drift so conviction jitter (and
price drift) doesn't churn. Entries are makers; reductions are market (when
you want less risk you want it now, not a price). The place/trim/cancel decision
is a PURE function (`plan_action`), so every case is tested without a broker.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import ROUND_DOWN, Decimal, InvalidOperation
from typing import Awaitable, Callable

from sentinel.strategy import Bar, Decision, Stance, Strategy

# A working entry is safe to re-price only when cleanly resting. In any in-flight
# state (SUBMITTING / CANCEL_PENDING / UNKNOWN / RECONCILING) we wait: touching
# it would race the broker or the duplicate-entry guard.
_RESTING = ("WORKING", "PARTIAL")

_DEFAULT_REPRICE_FRAC = Decimal("0.0005")     # 5 bps: re-peg once the touch drifts
_DEFAULT_REBALANCE_FRAC = Decimal("0.20")     # no-trade band around target (anti-churn)
_DEFAULT_LOT_STEP = Decimal("0.00001")        # BTCUSDT lot step


@dataclass(frozen=True, slots=True)
class Plan:
    """What the runner should do this bar. One primary action; the executor
    handles any incidental cleanup (e.g. cancelling a resting entry on exit)."""
    kind: str                       # "place" | "trim" | "cancel" | "exit" | "noop"
    qty: Decimal | None = None
    price: Decimal | None = None
    cancel_key: str | None = None
    reason: str = ""


def plan_action(
    position: Decimal,
    touch: Decimal | None,
    entry: dict | None,             # store.open_entry(): key/qty/filled/state/limit_price
    target: Decimal | None,         # desired position qty (0 = flat, None = no opinion)
    *,
    reprice_frac: Decimal = _DEFAULT_REPRICE_FRAC,
    rebalance_frac: Decimal = _DEFAULT_REBALANCE_FRAC,
    lot_step: Decimal = _DEFAULT_LOT_STEP,
) -> Plan:
    """Pure reconciliation of actual position -> target. No I/O, no clock."""
    if target is None:
        return Plan("noop", reason="no opinion")

    # Flatten entirely.
    if target <= 0:
        if position > 0:
            return Plan("exit", reason="target flat: close position")
        if entry is not None:
            return Plan("cancel", cancel_key=entry["key"], reason="target flat: drop entry")
        return Plan("noop", reason="flat")

    gap = target - position                     # >0 need more, <0 hold too much
    band = target * rebalance_frac              # ignore drift smaller than this

    # Over target beyond the band: trim the excess at market.
    if -gap > band:
        excess = (-gap).quantize(lot_step, rounding=ROUND_DOWN)
        if excess > 0:
            return Plan("trim", qty=excess, reason="over target: trim to size")

    # Under target beyond the band: peg a maker entry for the shortfall.
    if gap > band:
        if touch is None or touch <= 0:
            return Plan("noop", reason="no touch price to peg to")
        remaining = gap.quantize(lot_step, rounding=ROUND_DOWN)
        if remaining <= 0:
            return Plan("noop", reason="within a lot of target")
        if entry is None:
            return Plan("place", qty=remaining, price=touch,
                        reason="place maker entry at touch")
        if entry["state"] not in _RESTING:
            return Plan("noop", reason=f"entry in flight ({entry['state']})")
        resting = entry["limit_price"]
        if resting is None:
            return Plan("noop", reason="entry has no limit price, leaving it")
        if abs(touch - resting) / touch > reprice_frac:
            return Plan("cancel", cancel_key=entry["key"], reason="re-peg to touch")
        return Plan("noop", reason="pegged")

    # Within the no-trade band: at target. Cancel any leftover resting entry.
    if entry is not None and entry["state"] in _RESTING:
        return Plan("cancel", cancel_key=entry["key"], reason="at target: cancel remainder")
    return Plan("noop", reason="at target")


class StrategyRunner:
    def __init__(
        self,
        strategy: Strategy,
        bars,                                     # bar source (.candles); BarFeed
        *,
        position_fn: Callable[[], Awaitable[Decimal]],
        open_entry_fn: Callable[[], Awaitable[dict | None]],
        place_entry_fn: Callable[[Decimal, Decimal], Awaitable[dict]],  # (qty, price)
        trim_fn: Callable[[Decimal], Awaitable[dict]],                  # market sell qty
        cancel_fn: Callable[[str], Awaitable[dict]],
        exit_fn: Callable[[], Awaitable[dict]],   # market close (all)
        touch_fn: Callable[[], Decimal | None],
        budget_fn: Callable[[], Decimal],         # risk budget in USDT (100% conviction)
        on_change: Callable[[], Awaitable[None]],
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
        self._trim_fn = trim_fn
        self._cancel_fn = cancel_fn
        self._exit_fn = exit_fn
        self._touch_fn = touch_fn
        self._budget_fn = budget_fn
        self._on_change = on_change
        self._poll_s = poll_s
        self._reprice_frac = reprice_frac
        self._rebalance_frac = rebalance_frac
        self._lot_step = lot_step

        self.running = False
        self.last_decision: Decision | None = None
        self.last_action: str | None = None
        self._last_closed_t: int | None = None

    def start(self) -> None:
        self.running = True

    def stop(self) -> None:
        self.running = False

    @staticmethod
    def _weight(d: Decision) -> Decimal:
        """Conviction in [0,1] from the decision detail; 1.0 (full budget) when
        the strategy doesn't express one (e.g. plain SMA -> unchanged behavior)."""
        try:
            w = Decimal(str(d.detail.get("target_weight", "1")))
        except (InvalidOperation, AttributeError, TypeError):
            w = Decimal("1")
        return max(Decimal(0), min(Decimal(1), w))

    def _target(self, touch: Decimal | None) -> Decimal | None:
        """Desired position qty from the current stance + conviction. None =
        no opinion (leave everything alone). 0 = flat. LONG = weight*budget/touch
        (needs a price to size); FLAT needs no price so exits never block on the
        feed."""
        d = self.last_decision
        if d is None or d.stance is None:
            return None
        if d.stance is Stance.FLAT:
            return Decimal(0)
        if touch is None or touch <= 0:
            return None
        qty = self._weight(d) * self._budget_fn() / touch
        return qty.quantize(self._lot_step, rounding=ROUND_DOWN)

    def _feed(self, c: dict) -> Decision:
        """Feed one closed candle to the strategy, preferring the OHLC path
        (true range for ATR/ADX, highs/lows for Donchian) when the strategy
        exposes it, else the bare close. Both are pure."""
        ohlcv = getattr(self.strategy, "on_bar_ohlcv", None)
        if callable(ohlcv):
            return ohlcv(Bar(high=Decimal(str(c["h"])),
                             low=Decimal(str(c["l"])),
                             close=Decimal(str(c["c"]))))
        return self.strategy.on_bar(Decimal(str(c["c"])))

    def _seed_from_history(self) -> None:
        """Warm the strategy from closed history (all but the forming bar),
        leaving last_decision + the bar cursor set to the latest CLOSED bar.
        Pure/synchronous — no awaits — so it is atomic w.r.t. the run() loop."""
        reset = getattr(self.strategy, "reset", None)
        if callable(reset):
            reset()
        self.last_decision = None
        self._last_closed_t = None
        for c in self._bars.candles[:-1]:
            self.last_decision = self._feed(c)
            self._last_closed_t = c["t"]

    async def reseed(self) -> None:
        """Reset + re-warm the strategy from the current bar history, then (if
        running) reconcile to the fresh stance immediately. The hook for a
        strategy-interval change: after the bar source's clock changes, this
        forgets old-interval indicator state so the signal isn't a mix of two
        timeframes and the runner doesn't stall for a full interval."""
        self._seed_from_history()
        if self.running:
            await self.reconcile_now()

    async def reconcile_now(self) -> str | None:
        """Bring the broker position into line with the sized target. Pulls its
        own inputs, so the run loop and the /strategy/start path share it."""
        if not self.running or self.last_decision is None:
            return None
        touch = self._touch_fn()
        plan = plan_action(
            await self._position_fn(),
            touch,
            await self._open_entry_fn(),
            self._target(touch),
            reprice_frac=self._reprice_frac,
            rebalance_frac=self._rebalance_frac,
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
        if plan.kind == "place":
            r = await self._place_entry_fn(plan.qty, plan.price)
            return f"PEG {self._fmt(r)} @ {format(plan.price.normalize(), 'f')}"
        if plan.kind == "trim":
            return f"TRIM {self._fmt(await self._trim_fn(plan.qty))}"
        if plan.kind == "cancel":
            await self._cancel_fn(plan.cancel_key)
            return f"CANCEL {plan.cancel_key} — {plan.reason}"
        if plan.kind == "exit":
            entry = await self._open_entry_fn()      # don't leave a maker live
            if entry is not None:
                await self._cancel_fn(entry["key"])
            return f"EXIT {self._fmt(await self._exit_fn())}"
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
        """Supervised task. Seed the strategy from history so it's warm, then
        reconcile on each newly-closed bar."""
        self._seed_from_history()

        import asyncio

        while True:
            await asyncio.sleep(self._poll_s)
            candles = self._bars.candles
            if len(candles) < 2:
                continue
            closed = candles[-2]                  # most recently completed bar
            if self._last_closed_t is not None and closed["t"] <= self._last_closed_t:
                continue
            self._last_closed_t = closed["t"]

            self.last_decision = self._feed(closed)
            await self.reconcile_now()
            await self._on_change()               # always push the fresh decision

    def snapshot(self) -> dict:
        d = self.last_decision
        return {
            "name": self.strategy.name,
            "running": self.running,
            "stance": d.stance.value if d and d.stance else None,
            "detail": d.detail if d else {},
            "last_action": self.last_action,
            # Conviction actually applied (1.0 if the strategy expresses none).
            "weight": str(self._weight(d)) if d and d.stance is Stance.LONG else None,
            # Periods for the chart overlay (None if the strategy isn't SMA).
            "fast_period": getattr(self.strategy, "fast_period", None),
            "slow_period": getattr(self.strategy, "slow_period", None),
        }
