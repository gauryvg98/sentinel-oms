"""BarFeed — closed OHLC bars at a FIXED interval: the strategy's own clock.

Deliberately separate from the chart's MarketData. The chart timeframe is a
VIEWING choice the operator flips around; the strategy must decide on ONE stable
interval — its Donchian / ADX / vol windows are calibrated to a specific bar
size — regardless of what's on screen. So the strategy reads its bars here while
the live touch/mark for pegging still comes from MarketData. Kline stream only.
"""

from __future__ import annotations

import asyncio
import json
import logging

import httpx

from .market import REST_BASE, STREAM_BASE

log = logging.getLogger("sentinel.bars")

HISTORY = 300  # enough to warm the longest strategy window (Donchian 55, etc.)


class BarFeed:
    def __init__(self, symbol: str, interval: str, *,
                 rest_base: str = REST_BASE, stream_base: str = STREAM_BASE,
                 kline_path: str = "/api/v3/klines") -> None:
        self.symbol = symbol
        self.interval = interval
        self._rest = rest_base
        self._stream = stream_base
        self._kline_path = kline_path          # /fapi/v1/klines on futures
        self.candles: list[dict] = []          # {t, o, h, l, c} — t in seconds

    async def load_history(self) -> None:
        async with httpx.AsyncClient(base_url=self._rest, timeout=10) as http:
            resp = await http.get(
                self._kline_path,
                params={"symbol": self.symbol, "interval": self.interval,
                        "limit": HISTORY},
            )
            resp.raise_for_status()
        self.candles = [
            {"t": k[0] // 1000, "o": float(k[1]), "h": float(k[2]),
             "l": float(k[3]), "c": float(k[4])}
            for k in resp.json()
        ]

    def _ingest(self, k: dict) -> None:
        candle = {"t": k["t"] // 1000, "o": float(k["o"]), "h": float(k["h"]),
                  "l": float(k["l"]), "c": float(k["c"])}
        if self.candles and self.candles[-1]["t"] == candle["t"]:
            self.candles[-1] = candle
        else:
            self.candles.append(candle)
            del self.candles[:-HISTORY]

    async def run(self) -> None:
        """Supervised task: stream the forming candle at the fixed interval,
        reconnect forever. No interval switching — this feed's clock is fixed."""
        import websockets

        backoff = 1.0
        url = f"{self._stream}/ws/{self.symbol.lower()}@kline_{self.interval}"
        while True:
            try:
                async with websockets.connect(url) as ws:
                    log.info("strategy bars: %s %s", self.symbol, self.interval)
                    backoff = 1.0
                    async for raw in ws:
                        k = json.loads(raw).get("k")
                        if k:
                            self._ingest(k)
            except asyncio.CancelledError:
                raise
            except Exception as e:  # noqa: BLE001
                log.warning("strategy bar stream dropped (%r); reconnecting", e)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30.0)
