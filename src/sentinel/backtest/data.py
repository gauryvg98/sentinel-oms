"""Historical bars from Binance's PUBLIC mainnet API — real production data, no
auth, no account. (Live trading uses testnet; backtest data is the real thing.)
Paginated 1000/req and cached to disk so a re-run doesn't refetch."""

from __future__ import annotations

import json
import time
from pathlib import Path

import httpx

MAINNET = "https://api.binance.com"
CACHE = Path.home() / ".sentinel" / "backtest_cache"

_UNIT_MS = {"m": 60_000, "h": 3_600_000, "d": 86_400_000, "w": 604_800_000}


def _interval_ms(interval: str) -> int:
    return int(interval[:-1]) * _UNIT_MS[interval[-1]]


def load_klines(symbol: str, interval: str, days: int,
                *, refresh: bool = False) -> list[dict]:
    """Last `days` of {t,o,h,l,c,v} bars (t in seconds), cached under
    ~/.sentinel/backtest_cache. Pass refresh=True to force a refetch."""
    CACHE.mkdir(parents=True, exist_ok=True)
    cache_file = CACHE / f"{symbol}_{interval}_{days}d.json"
    if cache_file.exists() and not refresh:
        return json.loads(cache_file.read_text())
    end = int(time.time() * 1000)
    start = end - days * _UNIT_MS["d"]
    bars = _fetch(symbol, interval, start, end)
    cache_file.write_text(json.dumps(bars))
    return bars


def _fetch(symbol: str, interval: str, start_ms: int, end_ms: int) -> list[dict]:
    out: list[dict] = []
    step = _interval_ms(interval)
    with httpx.Client(base_url=MAINNET, timeout=30) as http:
        cursor = start_ms
        while cursor < end_ms:
            resp = http.get("/api/v3/klines", params={
                "symbol": symbol, "interval": interval,
                "startTime": cursor, "endTime": end_ms, "limit": 1000})
            resp.raise_for_status()
            rows = resp.json()
            if not rows:
                break
            for k in rows:
                out.append({"t": k[0] // 1000, "o": float(k[1]), "h": float(k[2]),
                            "l": float(k[3]), "c": float(k[4]), "v": float(k[5])})
            cursor = rows[-1][0] + step
            if len(rows) < 1000:
                break
    return out
