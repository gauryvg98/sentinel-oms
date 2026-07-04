"""Live market data for the terminal: candles + best bid/ask, one symbol.

Public testnet endpoints (no auth): REST klines for history, then ONE combined
socket carrying @kline (the forming candle) and @bookTicker (best bid/ask,
pushed continuously — not just on trades). bookTicker keeps the mark fresh
between sparse testnet trades (no "feed age 30s" freeze) and gives the
peg-to-touch strategy a real bid to rest a maker order on. Doubles as the
MarkFeed for P&L — the same price the chart shows is the mark P&L uses.
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
HISTORY = 200  # candles kept, any interval
VALID_INTERVALS = ("1m", "5m", "15m", "1h", "4h", "1d")
# Beyond this, the last price is treated as DEAD — latest() returns None so
# unrealized P&L is not marked against a stale quote (which would show a phantom
# gain/loss during a feed outage). Generous because testnet's @kline_1m only
# ticks on trades: 30-60s gaps are normal liquidity, not an outage. A real
# continuous feed (@bookTicker) would let this drop to a few seconds.
MAX_MARK_AGE_S = 180.0
# The book (bid/ask) has its OWN, much tighter freshness gate: @bookTicker fires
# many times a second in a live market, so if it goes quiet for even this long
# the top of book is suspect — best_bid/ask return None and the peg falls back
# to last price rather than resting a maker on a stale quote.
MAX_BOOK_AGE_S = 15.0


class MarketData:
    def __init__(self, symbol: str, *, rest_base: str = REST_BASE,
                 stream_base: str = STREAM_BASE) -> None:
        self.symbol = symbol
        self._rest = rest_base
        self._stream = stream_base
        self.interval = "1m"
        self.candles: list[dict] = []          # {t, o, h, l, c} — t in seconds
        self._price: Decimal | None = None
        self._price_ts: float = 0.0
        self._bid: Decimal | None = None       # best bid/ask from @bookTicker
        self._ask: Decimal | None = None
        self._book_ts: float = 0.0
        self._last_bump: float = 0.0           # UI-push throttle (book is chatty)
        self._ws = None                        # live stream, closed on tf switch
        # Async callback fired on price/candle tick, so the UI pushes in real
        # time instead of on a timer. Wired to app.changes.bump.
        self.on_change = None

    # ------------------------------------------------------------- MarkFeed

    def latest(self, instrument: str) -> Mark | None:
        if instrument != self.symbol or self._price is None:
            return None
        if time.time() - self._price_ts > MAX_MARK_AGE_S:
            return None   # stale: don't let dead price drive unrealized P&L
        return Mark(instrument=instrument, price=self._price, ts=self._price_ts)

    def best_bid(self) -> Decimal | None:
        """Best bid, or None if the book feed is stale. A maker BUY rests HERE
        (join the bid) so it doesn't cross and take."""
        if self._bid is None or time.time() - self._book_ts > MAX_BOOK_AGE_S:
            return None
        return self._bid

    def best_ask(self) -> Decimal | None:
        if self._ask is None or time.time() - self._book_ts > MAX_BOOK_AGE_S:
            return None
        return self._ask

    @property
    def price_age_s(self) -> float | None:
        return round(time.time() - self._price_ts, 1) if self._price_ts else None

    # ------------------------------------------------------------ lifecycle

    async def set_interval(self, interval: str) -> None:
        """Switch chart timeframe: load the new interval's history, then drop
        the live stream so run() reconnects on the new interval."""
        if interval not in VALID_INTERVALS or interval == self.interval:
            return
        self.interval = interval
        await self.load_history()              # replaces candles for the new tf
        ws = self._ws
        if ws is not None:
            try:
                await ws.close()               # forces run() to reconnect
            except Exception:  # noqa: BLE001
                pass
        if self.on_change is not None:
            await self.on_change()

    async def load_history(self) -> None:
        async with httpx.AsyncClient(base_url=self._rest, timeout=10) as http:
            resp = await http.get(
                "/api/v3/klines",
                params={"symbol": self.symbol, "interval": self.interval,
                        "limit": HISTORY},
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

    def _ingest_book(self, d: dict) -> None:
        """@bookTicker: best bid/ask. Drives a continuous MID mark, so the
        price stays live between trades."""
        try:
            self._bid, self._ask = Decimal(d["b"]), Decimal(d["a"])
        except (KeyError, TypeError):
            return
        self._book_ts = time.time()
        self._price = (self._bid + self._ask) / 2
        self._price_ts = self._book_ts

    def _ingest_kline(self, k: dict | None) -> None:
        """@kline: the forming candle. Also keeps the mark alive from trades in
        case the book feed is ever unavailable."""
        if not k:
            return
        candle = {"t": k["t"] // 1000, "o": float(k["o"]), "h": float(k["h"]),
                  "l": float(k["l"]), "c": float(k["c"])}
        if self.candles and self.candles[-1]["t"] == candle["t"]:
            self.candles[-1] = candle
        else:
            self.candles.append(candle)
            del self.candles[:-HISTORY]
        self._price = Decimal(k["c"])
        self._price_ts = time.time()

    async def _bump_throttled(self) -> None:
        """bookTicker can fire many times a second; coalesce UI pushes to a few
        per second (trading reads bid/ask directly, not via the push)."""
        now = time.time()
        if now - self._last_bump >= 0.25 and self.on_change is not None:
            self._last_bump = now
            await self.on_change()

    async def run(self) -> None:
        """Supervised task: one combined socket for @kline + @bookTicker,
        reconnect forever. Reads self.interval each connection, so a timeframe
        switch (which closes the socket) simply reconnects on the new interval;
        @bookTicker is interval-independent and rides along unchanged."""
        import websockets

        backoff = 1.0
        while True:
            interval = self.interval
            sym = self.symbol.lower()
            url = (f"{self._stream}/stream?streams="
                   f"{sym}@kline_{interval}/{sym}@bookTicker")
            try:
                async with websockets.connect(url) as ws:
                    self._ws = ws
                    log.info("market stream: %s %s + bookTicker", self.symbol, interval)
                    backoff = 1.0
                    async for raw in ws:
                        if self.interval != interval:
                            break              # switched — stale frames ignored
                        msg = json.loads(raw)
                        stream, data = msg.get("stream", ""), msg.get("data", {})
                        if stream.endswith("@bookTicker"):
                            self._ingest_book(data)
                        elif "@kline" in stream:
                            self._ingest_kline(data.get("k"))
                        else:
                            continue
                        await self._bump_throttled()
            except asyncio.CancelledError:
                raise
            except Exception as e:  # noqa: BLE001
                if self.interval != interval:
                    continue                   # deliberate close on tf switch
                log.warning("market stream dropped (%r); reconnecting", e)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30.0)
