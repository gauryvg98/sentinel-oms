"""StrategyRunner — turns a pure strategy's decisions into real orders.

It runs as a supervised task, ALWAYS feeding every closed bar to the strategy
(so the indicators stay warm even while paused), but only ACTING when started.
Actions go through injected buy/sell callables that hit the normal
CommandGateway — so the strategy faces the exact same guards a human does:
single-writer, never-over-exit, duplicate-entry. A bad strategy trades badly;
it cannot bypass the safety layer or corrupt state.

The decision->action rule (`react`) is pulled out pure and testable; the run
loop is just "detect a newly-closed bar, feed it, react."
"""

from __future__ import annotations

from decimal import Decimal
from typing import Awaitable, Callable

from sentinel.strategy import Decision, Stance, Strategy


class StrategyRunner:
    def __init__(
        self,
        strategy: Strategy,
        market,                                   # ui.market.MarketData
        *,
        position_fn: Callable[[], Awaitable[Decimal]],
        enter_fn: Callable[[], Awaitable[dict]],  # place a BUY (open/add)
        exit_fn: Callable[[], Awaitable[dict]],   # place a SELL (close)
        on_change: Callable[[], Awaitable[None]],
        poll_s: float = 2.0,
    ) -> None:
        self.strategy = strategy
        self.market = market
        self._position_fn = position_fn
        self._enter_fn = enter_fn
        self._exit_fn = exit_fn
        self._on_change = on_change
        self._poll_s = poll_s

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
        bar cursor were built on the old one. Left alone, the runner would (a)
        feed the new interval's closes into an indicator still holding the old
        interval's — a mixed-timeframe garbage signal — and (b) stall (the new
        closed bar's open-time can be < the old cursor, so it wouldn't act for
        up to a full interval). Reset and re-warm from the new history; if
        running, bring the position into line with the fresh stance now."""
        self._seed_from_history()
        if self.running:
            await self.reconcile_now()

    async def reconcile_now(self) -> str | None:
        """Reconcile against the CURRENT stance immediately (used on start), so
        the bot acts on existing position/state without waiting for the next
        bar. Start it wanting LONG while flat -> it enters now; start it while
        holding a position it wants FLAT -> it closes now."""
        if self.last_decision is None:
            return None
        action = await self.react(self.last_decision, await self._position_fn())
        if action is not None:
            self.last_action = action
            await self._on_change()
        return action

    async def react(self, decision: Decision, position: Decimal) -> str | None:
        """Reconcile actual position -> desired stance. Acts only on a
        mismatch; a matched stance (or no opinion / paused) is a no-op. This
        is what makes it account for pre-existing positions: whatever you're
        holding when the strategy warms up, the next bar brings it into line."""
        if not self.running or decision.stance is None:
            return None
        if decision.stance is Stance.LONG and position <= 0:
            return f"ENTER {self._fmt(await self._enter_fn())}"
        if decision.stance is Stance.FLAT and position > 0:
            return f"EXIT {self._fmt(await self._exit_fn())}"
        return None

    @staticmethod
    def _fmt(result: dict) -> str:
        if "placed" in result:
            return f"placed {result.get('qty', '')}".strip()
        if "rejected" in result:
            return f"rejected ({result['reason']})"
        if "blocked" in result:
            return f"blocked ({result.get('reason', result['blocked'])})"
        return str(result)

    async def run(self) -> None:
        """Supervised task. Seed the strategy from history so it's warm, then
        act on each newly-closed bar."""
        self._seed_from_history()                 # leaves a live stance for
                                                  # reconcile_now() on start

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

            decision = self.strategy.on_bar(Decimal(str(closed["c"])))
            self.last_decision = decision
            action = await self.react(decision, await self._position_fn())
            if action is not None:
                self.last_action = action
            await self._on_change()

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
