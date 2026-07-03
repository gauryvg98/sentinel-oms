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

from sentinel.strategy import Decision, Signal, Strategy


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

    async def react(self, decision: Decision, position: Decimal) -> str | None:
        """Pure-ish decision -> action. Only ENTER-when-flat and EXIT-when-long
        do anything; every other case is a deliberate no-op."""
        if not self.running or decision.signal is Signal.HOLD:
            return None
        if decision.signal is Signal.ENTER and position <= 0:
            return f"ENTER {self._fmt(await self._enter_fn())}"
        if decision.signal is Signal.EXIT and position > 0:
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
        for c in self.market.candles[:-1]:        # all but the forming bar
            self.strategy.on_bar(Decimal(str(c["c"])))
            self._last_closed_t = c["t"]

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
            "signal": d.signal.value if d else None,
            "detail": d.detail if d else {},
            "last_action": self.last_action,
        }
