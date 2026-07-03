"""Live market data for the terminal: candles + last price, one symbol.

Public testnet endpoints (no auth): REST klines for history, the kline_1m
stream for the forming candle. Doubles as the MarkFeed for P&L — the same
price the chart shows is the price the unrealized P&L marks against.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from decimal import Decimal

import httpx

from sentinel.marks import Mark

log = logging.getLogger("sentinel.market")

REST_BASE = "https://testnet.binance.vision"
STREAM_BASE = "wss://stream.testnet.binance.vision"
HISTORY = 180  # minutes of 1m candles


class MarketData:
    def __init__(self, symbol: str, *, rest_base: str = REST_BASE,
                 stream_base: str = STREAM_BASE) -> None:
        self.symbol = symbol
        self._rest = rest_base
        self._stream = stream_base
        self.candles: list[dict] = []          # {t, o, h, l, c} — t in seconds
        self._price: Decimal | None = None
        self._price_ts: float = 0.0
        # Async callback fired on every price/candle tick, so the UI pushes
        # in real time instead of on a timer. Wired to app.changes.bump.
        self.on_change = None

    # ------------------------------------------------------------- MarkFeed

    def latest(self, instrument: str) -> Mark | None:
        if instrument != self.symbol or self._price is None:
            return None
        return Mark(instrument=instrument, price=self._price, ts=self._price_ts)

    @property
    def price_age_s(self) -> float | None:
        return round(time.time() - self._price_ts, 1) if self._price_ts else None

    # ------------------------------------------------------------ lifecycle

    async def load_history(self) -> None:
        async with httpx.AsyncClient(base_url=self._rest, timeout=10) as http:
            resp = await http.get(
                "/api/v3/klines",
                params={"symbol": self.symbol, "interval": "1m", "limit": HISTORY},
            )
            resp.raise_for_status()
        self.candles = [
            {"t": k[0] // 1000, "o": float(k[1]), "h": float(k[2]),
             "l": float(k[3]), "c": float(k[4])}
            for k in resp.json()
        ]
        if self.candles:
            self._price = Decimal(str(self.candles[-1]["c"]))
            self._price_ts = time.time()

    async def run(self) -> None:
        """Supervised task: stream the forming candle, reconnect forever."""
        import websockets

        backoff = 1.0
        url = f"{self._stream}/ws/{self.symbol.lower()}@kline_1m"
        while True:
            try:
                async with websockets.connect(url) as ws:
                    log.info("market stream connected: %s", self.symbol)
                    backoff = 1.0
                    async for raw in ws:
                        k = json.loads(raw).get("k")
                        if not k:
                            continue
                        candle = {"t": k["t"] // 1000, "o": float(k["o"]),
                                  "h": float(k["h"]), "l": float(k["l"]),
                                  "c": float(k["c"])}
                        if self.candles and self.candles[-1]["t"] == candle["t"]:
                            self.candles[-1] = candle
                        else:
                            self.candles.append(candle)
                            del self.candles[:-HISTORY]
                        self._price = Decimal(k["c"])
                        self._price_ts = time.time()
                        if self.on_change is not None:
                            await self.on_change()
            except asyncio.CancelledError:
                raise
            except Exception as e:  # noqa: BLE001
                log.warning("market stream dropped (%r); reconnecting", e)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30.0)
