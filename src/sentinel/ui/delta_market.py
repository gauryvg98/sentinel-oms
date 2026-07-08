"""Delta Exchange India public market data — chart candles + best bid/ask +
strategy bars, so a Delta venue prices its peg against the SAME book its orders
land in (no cross-venue basis). Mirrors the MarketData / BarFeed interface the
terminal and runner already use; only the wire format differs (Delta's
/v2/history/candles REST + one public WS with v2/ticker / candlestick_<res>
channels — resolutions are Sentinel-style strings, timestamps in MICROseconds
on the socket and SECONDS on REST).

Prices here stay in quote units (USD per base) exactly like the other venues —
contract sizing is an ORDER concern and lives in the broker adapter, never in
market data.
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

log = logging.getLogger("sentinel.delta.market")

REST_BASE = "https://cdn-ind.testnet.deltaex.org"
WS_PUBLIC = "wss://socket-ind.testnet.deltaex.org"

# Sentinel interval -> Delta resolution (same strings) and bar length, needed
# because /v2/history/candles wants an explicit [start, end] second range.
_RES = {"1m": "1m", "3m": "3m", "5m": "5m", "15m": "15m", "30m": "30m",
        "1h": "1h", "2h": "2h", "4h": "4h", "1d": "1d", "1w": "1w"}
_SECONDS = {"1m": 60, "3m": 180, "5m": 300, "15m": 900, "30m": 1800,
            "1h": 3600, "2h": 7200, "4h": 14400, "1d": 86400, "1w": 604800}
VALID_INTERVALS = ("1m", "5m", "15m", "1h", "4h", "1d")


async def _fetch_candles(rest: str, symbol: str, interval: str,
                         limit: int) -> list[dict]:
    end = int(time.time())
    start = end - limit * _SECONDS.get(interval, 60)
    async with httpx.AsyncClient(base_url=rest, timeout=10) as http:
        resp = await http.get("/v2/history/candles", params={
            "resolution": _RES.get(interval, "1m"), "symbol": symbol,
            "start": start, "end": end})
        resp.raise_for_status()
    rows = resp.json().get("result") or []      # {time,open,high,low,close}, time in s
    out = [{"t": int(r["time"]), "o": float(r["open"]), "h": float(r["high"]),
            "l": float(r["low"]), "c": float(r["close"])} for r in rows]
    out.sort(key=lambda c: c["t"])              # Delta serves newest-first; normalize
    return out[-limit:]


def _ws_candle(msg: dict) -> dict:
    """One candlestick_<res> frame -> chart candle. candle_start_time is in
    MICROseconds; charts key on seconds."""
    return {"t": int(msg["candle_start_time"]) // 1_000_000,
            "o": float(msg["open"]), "h": float(msg["high"]),
            "l": float(msg["low"]), "c": float(msg["close"])}


async def _enable_heartbeat(ws) -> None:
    # Server heartbeats prove the pipe is alive between sparse testnet ticks;
    # the websockets lib's protocol pings cover the client->server direction.
    await ws.send(json.dumps({"type": "enable_heartbeat"}))


class DeltaMarketData:
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
        self.candles = await _fetch_candles(self._rest, self.symbol,
                                            self.interval, HISTORY)
        if self.candles:
            self._price = Decimal(str(self.candles[-1]["c"]))
            self._price_ts = time.time()

    def _ingest(self, msg: dict) -> None:
        kind = msg.get("type", "")
        if kind == "v2/ticker":
            q = msg.get("quotes") or {}          # best bid/ask live under quotes
            if q.get("best_bid"):
                self._bid = Decimal(str(q["best_bid"]))
            if q.get("best_ask"):
                self._ask = Decimal(str(q["best_ask"]))
            if self._bid is not None and self._ask is not None:
                self._price = (self._bid + self._ask) / 2
                self._price_ts = self._book_ts = time.time()
        elif kind.startswith("candlestick_"):
            if msg.get("candle_start_time") is None:
                return                            # subscription ack, not a candle
            c = _ws_candle(msg)
            if self.candles and self.candles[-1]["t"] == c["t"]:
                self.candles[-1] = c
            else:
                self.candles.append(c)
                del self.candles[:-HISTORY]
            self._price = Decimal(str(msg["close"]))
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
            channels = [
                {"name": "v2/ticker", "symbols": [self.symbol]},
                {"name": f"candlestick_{_RES.get(interval, '1m')}",
                 "symbols": [self.symbol]},
            ]
            try:
                async with websockets.connect(self._ws_url) as ws:
                    self._ws = ws
                    await ws.send(json.dumps({"type": "subscribe",
                                              "payload": {"channels": channels}}))
                    await _enable_heartbeat(ws)
                    log.info("delta market stream: %s %s", self.symbol, interval)
                    backoff = 1.0
                    async for raw in ws:
                        if self.interval != interval:
                            break
                        self._ingest(json.loads(raw))
                        await self._bump()
            except asyncio.CancelledError:
                raise
            except Exception as e:  # noqa: BLE001
                if self.interval != interval:
                    continue
                log.warning("delta market stream dropped (%r); reconnecting", e)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30.0)


class DeltaBarFeed:
    """Closed OHLC bars at a FIXED interval — the strategy's clock.
    candlestick_<res> only."""

    def __init__(self, symbol: str, interval: str, *,
                 rest_base: str = REST_BASE, ws_url: str = WS_PUBLIC) -> None:
        self.symbol = symbol
        self.interval = interval
        self._rest = rest_base
        self._ws_url = ws_url
        self.candles: list[dict] = []

    async def load_history(self) -> None:
        self.candles = await _fetch_candles(self._rest, self.symbol,
                                            self.interval, HISTORY)

    def _ingest(self, msg: dict) -> None:
        c = _ws_candle(msg)
        if self.candles and self.candles[-1]["t"] == c["t"]:
            self.candles[-1] = c
        else:
            self.candles.append(c)
            del self.candles[:-HISTORY]

    async def run(self) -> None:
        import websockets

        backoff = 1.0
        channel = f"candlestick_{_RES.get(self.interval, '1m')}"
        while True:
            try:
                async with websockets.connect(self._ws_url) as ws:
                    await ws.send(json.dumps({"type": "subscribe", "payload": {
                        "channels": [{"name": channel,
                                      "symbols": [self.symbol]}]}}))
                    await _enable_heartbeat(ws)
                    log.info("delta strategy bars: %s %s", self.symbol,
                             self.interval)
                    backoff = 1.0
                    async for raw in ws:
                        msg = json.loads(raw)
                        if (msg.get("type", "").startswith("candlestick_")
                                and msg.get("candle_start_time") is not None):
                            self._ingest(msg)
            except asyncio.CancelledError:
                raise
            except Exception as e:  # noqa: BLE001
                log.warning("delta bar stream dropped (%r); reconnecting", e)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30.0)
