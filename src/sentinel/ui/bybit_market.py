"""Bybit V5 public market data — chart candles + best bid/ask + strategy bars,
so a Bybit venue prices its peg against the SAME book its orders land in (no
cross-venue basis). Mirrors the MarketData / BarFeed interface the terminal and
runner already use; only the wire format differs (Bybit's /v5/market/* REST +
the public linear WS with tickers.* / kline.* topics, intervals as codes).
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from decimal import Decimal

import httpx

from sentinel.marks import Mark

from .market import HISTORY, MAX_BOOK_AGE_S, MAX_MARK_AGE_S

log = logging.getLogger("sentinel.bybit.market")

REST_BASE = "https://api-testnet.bybit.com"
WS_PUBLIC = "wss://stream-testnet.bybit.com/v5/public/linear"

# Sentinel interval string -> Bybit interval code.
_IV = {"1m": "1", "3m": "3", "5m": "5", "15m": "15", "30m": "30",
       "1h": "60", "2h": "120", "4h": "240", "1d": "D", "1w": "W"}
VALID_INTERVALS = ("1m", "5m", "15m", "1h", "4h", "1d")


async def _fetch_klines(rest: str, symbol: str, interval: str,
                        limit: int) -> list[dict]:
    async with httpx.AsyncClient(base_url=rest, timeout=10) as http:
        resp = await http.get("/v5/market/kline", params={
            "category": "linear", "symbol": symbol,
            "interval": _IV.get(interval, "1"), "limit": limit})
        resp.raise_for_status()
    rows = resp.json()["result"]["list"]           # [startMs,o,h,l,c,vol,turnover], NEWEST-first
    return [{"t": int(k[0]) // 1000, "o": float(k[1]), "h": float(k[2]),
             "l": float(k[3]), "c": float(k[4])}
            for k in reversed(rows)]                 # -> oldest-first, like Binance


def _kline_candle(k: dict) -> dict:
    return {"t": int(k["start"]) // 1000, "o": float(k["open"]), "h": float(k["high"]),
            "l": float(k["low"]), "c": float(k["close"])}


async def _ping(ws) -> None:
    while True:
        await asyncio.sleep(20)
        await ws.send(json.dumps({"op": "ping"}))


class BybitMarketData:
    """Chart candles + best bid/ask for the terminal (and the peg). Same public
    surface as ui.market.MarketData."""

    def __init__(self, symbol: str, *, rest_base: str = REST_BASE,
                 ws_url: str = WS_PUBLIC) -> None:
        self.symbol = symbol
        self._rest = rest_base
        self._ws_url = ws_url
        self.interval = "1m"
        self.candles: list[dict] = []
        self._price: Decimal | None = None
        self._price_ts: float = 0.0
        self._bid: Decimal | None = None
        self._ask: Decimal | None = None
        self._book_ts: float = 0.0
        self._last_bump: float = 0.0
        self._ws = None
        self.on_change = None

    def latest(self, instrument: str) -> Mark | None:
        if instrument != self.symbol or self._price is None:
            return None
        if time.time() - self._price_ts > MAX_MARK_AGE_S:
            return None
        return Mark(instrument=instrument, price=self._price, ts=self._price_ts)

    def best_bid(self) -> Decimal | None:
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

    async def set_interval(self, interval: str) -> None:
        if interval not in VALID_INTERVALS or interval == self.interval:
            return
        self.interval = interval
        await self.load_history()
        if self._ws is not None:
            try:
                await self._ws.close()
            except Exception:  # noqa: BLE001
                pass
        if self.on_change is not None:
            await self.on_change()

    async def load_history(self) -> None:
        self.candles = await _fetch_klines(self._rest, self.symbol, self.interval, HISTORY)
        if self.candles:
            self._price = Decimal(str(self.candles[-1]["c"]))
            self._price_ts = time.time()

    def _ingest(self, msg: dict) -> None:
        topic = msg.get("topic", "")
        if topic.startswith("tickers."):
            d = msg.get("data", {})               # delta frames carry only changed fields
            if d.get("bid1Price"):
                self._bid = Decimal(d["bid1Price"])
            if d.get("ask1Price"):
                self._ask = Decimal(d["ask1Price"])
            if self._bid is not None and self._ask is not None:
                self._price = (self._bid + self._ask) / 2
                self._price_ts = self._book_ts = time.time()
        elif topic.startswith("kline."):
            for k in msg.get("data", []):
                c = _kline_candle(k)
                if self.candles and self.candles[-1]["t"] == c["t"]:
                    self.candles[-1] = c
                else:
                    self.candles.append(c)
                    del self.candles[:-HISTORY]
                self._price = Decimal(str(k["close"]))
                self._price_ts = time.time()

    async def _bump(self) -> None:
        now = time.time()
        if now - self._last_bump >= 0.25 and self.on_change is not None:
            self._last_bump = now
            await self.on_change()

    async def run(self) -> None:
        import websockets

        backoff = 1.0
        while True:
            interval = self.interval
            args = [f"tickers.{self.symbol}", f"kline.{_IV.get(interval, '1')}.{self.symbol}"]
            try:
                async with websockets.connect(self._ws_url) as ws:
                    self._ws = ws
                    await ws.send(json.dumps({"op": "subscribe", "args": args}))
                    log.info("bybit market stream: %s %s", self.symbol, interval)
                    backoff = 1.0
                    ping = asyncio.create_task(_ping(ws))
                    try:
                        async for raw in ws:
                            if self.interval != interval:
                                break
                            self._ingest(json.loads(raw))
                            await self._bump()
                    finally:
                        ping.cancel()
            except asyncio.CancelledError:
                raise
            except Exception as e:  # noqa: BLE001
                if self.interval != interval:
                    continue
                log.warning("bybit market stream dropped (%r); reconnecting", e)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30.0)


class BybitBarFeed:
    """Closed OHLC bars at a FIXED interval — the strategy's clock. Kline only."""

    def __init__(self, symbol: str, interval: str, *,
                 rest_base: str = REST_BASE, ws_url: str = WS_PUBLIC) -> None:
        self.symbol = symbol
        self.interval = interval
        self._rest = rest_base
        self._ws_url = ws_url
        self.candles: list[dict] = []

    async def load_history(self) -> None:
        self.candles = await _fetch_klines(self._rest, self.symbol, self.interval, HISTORY)

    def _ingest(self, k: dict) -> None:
        c = _kline_candle(k)
        if self.candles and self.candles[-1]["t"] == c["t"]:
            self.candles[-1] = c
        else:
            self.candles.append(c)
            del self.candles[:-HISTORY]

    async def run(self) -> None:
        import websockets

        backoff = 1.0
        topic = f"kline.{_IV.get(self.interval, '1')}.{self.symbol}"
        while True:
            try:
                async with websockets.connect(self._ws_url) as ws:
                    await ws.send(json.dumps({"op": "subscribe", "args": [topic]}))
                    log.info("bybit strategy bars: %s %s", self.symbol, self.interval)
                    backoff = 1.0
                    ping = asyncio.create_task(_ping(ws))
                    try:
                        async for raw in ws:
                            msg = json.loads(raw)
                            if msg.get("topic", "").startswith("kline."):
                                for k in msg.get("data", []):
                                    self._ingest(k)
                    finally:
                        ping.cancel()
            except asyncio.CancelledError:
                raise
            except Exception as e:  # noqa: BLE001
                log.warning("bybit bar stream dropped (%r); reconnecting", e)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30.0)
