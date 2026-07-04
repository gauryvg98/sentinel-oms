"""StrategyRunner — turns a pure strategy's decisions into real orders.

It runs as a supervised task, ALWAYS feeding every closed bar to the strategy
(so the indicators stay warm even while paused), but only ACTING when started.
Actions go through injected callables that hit the normal CommandGateway — so
the strategy faces the exact same guards a human does: single-writer,
never-over-exit, duplicate-entry. A bad strategy trades badly; it cannot bypass
the safety layer or corrupt state.

Execution is PEG-TO-TOUCH maker entries: the entry is a resting LIMIT order
pegged to the touch and re-priced as the touch drifts, so the strategy pays the
maker side instead of the spread. The EXIT stays a market order — when you want
out you want certainty, not a price. The place/cancel/reprice decision is pulled
out as a PURE function (`plan_action`) so every case is testable without a
broker; the run loop is just "detect a newly-closed bar, feed it, reconcile."
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import ROUND_DOWN, Decimal
from typing import Awaitable, Callable

from sentinel.strategy import Decision, Stance, Strategy

# A working entry is safe to re-price only when it is cleanly resting. In any
# in-flight state (SUBMITTING / CANCEL_PENDING / UNKNOWN / RECONCILING) we wait:
# touching it would race the broker or the duplicate-entry guard.
_RESTING = ("WORKING", "PARTIAL")

_DEFAULT_REPRICE_FRAC = Decimal("0.0005")   # 5 bps: re-peg once the touch drifts
_DEFAULT_LOT_STEP = Decimal("0.00001")      # BTCUSDT lot step


@dataclass(frozen=True, slots=True)
class Plan:
    """What the runner should do this bar. One primary action; the executor
    handles any incidental cleanup (e.g. cancelling a resting entry on exit)."""
    kind: str                       # "place" | "cancel" | "exit" | "noop"
    qty: Decimal | None = None
    price: Decimal | None = None
    cancel_key: str | None = None
    reason: str = ""


def plan_action(
    stance: Stance | None,
    position: Decimal,
    touch: Decimal | None,
    entry: dict | None,             # store.open_entry(): key/qty/filled/state/limit_price
    budget: Decimal,                # entry size in quote (USDT)
    *,
    reprice_frac: Decimal = _DEFAULT_REPRICE_FRAC,
    lot_step: Decimal = _DEFAULT_LOT_STEP,
) -> Plan:
    """Pure peg-to-touch reconciliation: actual position + resting entry ->
    the one thing to do this bar. No I/O, no clock — trivially testable."""
    if stance is None:
        return Plan("noop", reason="no opinion")

    if stance is Stance.FLAT:
        if position > 0:
            return Plan("exit", reason="flat: close position")
        if entry is not None:
            return Plan("cancel", cancel_key=entry["key"],
                        reason="flat: abandon unfilled entry")
        return Plan("noop", reason="flat")

    # stance LONG: hold `budget` worth via a maker limit pegged to the touch.
    if touch is None or touch <= 0:
        return Plan("noop", reason="no touch price to peg to")

    target = (budget / touch).quantize(lot_step, rounding=ROUND_DOWN)
    remaining = (target - position).quantize(lot_step, rounding=ROUND_DOWN)
    if remaining <= 0:                                   # at target (within a lot)
        if entry is not None and entry["state"] in _RESTING:
            return Plan("cancel", cancel_key=entry["key"],
                        reason="target reached: cancel remainder")
        return Plan("noop", reason="at target")

    if entry is None:
        return Plan("place", qty=remaining, price=touch,
                    reason="place maker entry at touch")

    # A working entry exists — re-peg it or wait.
    if entry["state"] not in _RESTING:
        return Plan("noop", reason=f"entry in flight ({entry['state']})")
    resting = entry["limit_price"]
    if resting is None:                                  # orphan / not our maker
        return Plan("noop", reason="entry has no limit price, leaving it")
    if abs(touch - resting) / touch > reprice_frac:
        # Cancel now; the replace lands next bar once the cancel is confirmed
        # and the duplicate-entry guard clears. See the module docstring.
        return Plan("cancel", cancel_key=entry["key"], reason="re-peg to touch")
    return Plan("noop", reason="pegged")


class StrategyRunner:
    def __init__(
        self,
        strategy: Strategy,
        market,                                   # ui.market.MarketData
        *,
        position_fn: Callable[[], Awaitable[Decimal]],
        open_entry_fn: Callable[[], Awaitable[dict | None]],
        place_entry_fn: Callable[[Decimal, Decimal], Awaitable[dict]],  # (qty, price)
        cancel_fn: Callable[[str], Awaitable[dict]],
        exit_fn: Callable[[], Awaitable[dict]],   # market close
        touch_fn: Callable[[], Decimal | None],
        budget_fn: Callable[[], Decimal],         # entry size in USDT
        on_change: Callable[[], Awaitable[None]],
        poll_s: float = 2.0,
        reprice_frac: Decimal = _DEFAULT_REPRICE_FRAC,
        lot_step: Decimal = _DEFAULT_LOT_STEP,
    ) -> None:
        self.strategy = strategy
        self.market = market
        self._position_fn = position_fn
        self._open_entry_fn = open_entry_fn
        self._place_entry_fn = place_entry_fn
        self._cancel_fn = cancel_fn
        self._exit_fn = exit_fn
        self._touch_fn = touch_fn
        self._budget_fn = budget_fn
        self._on_change = on_change
        self._poll_s = poll_s
        self._reprice_frac = reprice_frac
        self._lot_step = lot_step

        self.running = False
        self.last_decision: Decision | None = None
        self.last_action: str | None = None
        self._last_closed_t: int | None = None

    def start(self) -> None:
        self.running = True

    def stop(self) -> None:
        self.running = False

    def _seed_from_history(self) -> None:
        """Warm the strategy from closed history (all but the forming bar),
        leaving last_decision + the bar cursor set to the latest CLOSED bar.
        Pure/synchronous — no awaits — so it is atomic w.r.t. the run() loop."""
        reset = getattr(self.strategy, "reset", None)
        if callable(reset):
            reset()
        self.last_decision = None
        self._last_closed_t = None
        for c in self.market.candles[:-1]:
            self.last_decision = self.strategy.on_bar(Decimal(str(c["c"])))
            self._last_closed_t = c["t"]

    async def reseed(self) -> None:
        """The chart timeframe changed under us: market.candles are now a
        DIFFERENT interval's bars, but the strategy's indicator state and our
        bar cursor were built on the old one. Reset and re-warm from the new
        history; if running, bring the position into line with the fresh stance
        now (else it stalls for up to a full interval / trades a mixed signal)."""
        self._seed_from_history()
        if self.running:
            await self.reconcile_now()

    async def reconcile_now(self) -> str | None:
        """Bring broker state into line with the current stance via a
        peg-to-touch maker entry (market exit). Pulls its own inputs, so the
        run loop and the /strategy/start path share one code path."""
        if not self.running or self.last_decision is None:
            return None
        plan = plan_action(
            self.last_decision.stance,
            await self._position_fn(),
            self._touch_fn(),
            await self._open_entry_fn(),
            self._budget_fn(),
            reprice_frac=self._reprice_frac,
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
        if plan.kind == "cancel":
            await self._cancel_fn(plan.cancel_key)
            return f"CANCEL {plan.cancel_key} — {plan.reason}"
        if plan.kind == "exit":
            # Don't leave a maker order live after deciding to be flat.
            entry = await self._open_entry_fn()
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
            candles = self.market.candles
            if len(candles) < 2:
                continue
            closed = candles[-2]                  # most recently completed bar
            if self._last_closed_t is not None and closed["t"] <= self._last_closed_t:
                continue
            self._last_closed_t = closed["t"]

            self.last_decision = self.strategy.on_bar(Decimal(str(closed["c"])))
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
            # Periods for the chart overlay (None if the strategy isn't SMA).
            "fast_period": getattr(self.strategy, "fast_period", None),
            "slow_period": getattr(self.strategy, "slow_period", None),
        }
