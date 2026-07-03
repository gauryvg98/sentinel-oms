"""Binance request signing: HMAC-SHA256 over the urlencoded query string.

Kept as pure functions so the signature path is unit-testable without any
network: sign(params, secret) must produce byte-identical output to what
Binance verifies, or every private call 401s.
"""

from __future__ import annotations

import hashlib
import hmac
import time
from decimal import Decimal
from urllib.parse import urlencode


def fmt_decimal(value: Decimal) -> str:
    """Plain decimal string — Binance rejects scientific notation."""
    return format(value.normalize(), "f")


def ws_auth_params(api_key: str, secret: str, *, timestamp_ms: int) -> dict:
    """Params for WebSocket-API signed requests (userDataStream.subscribe.signature).
    The signature payload is the urlencoded params SORTED ALPHABETICALLY —
    unlike REST, where we sign the string exactly as sent."""
    params = {"apiKey": api_key, "timestamp": timestamp_ms}
    payload = urlencode(sorted(params.items()))
    params["signature"] = hmac.new(
        secret.encode(), payload.encode(), hashlib.sha256
    ).hexdigest()
    return params


def signed_query(params: dict, secret: str, *, timestamp_ms: int) -> str:
    """Return the final query string: params + timestamp + signature.
    Order matters: the signature covers exactly the encoded string."""
    items = {k: v for k, v in params.items() if v is not None}
    items["timestamp"] = timestamp_ms
    qs = urlencode(items)
    signature = hmac.new(
        secret.encode(), qs.encode(), hashlib.sha256
    ).hexdigest()
    return f"{qs}&signature={signature}"


class ServerClock:
    """Tracks the broker's clock offset so signatures don't fail on skew
    (-1021). Sync once at start; re-sync when told to."""

    def __init__(self) -> None:
        self._offset_ms = 0

    def sync(self, server_time_ms: int) -> None:
        self._offset_ms = server_time_ms - int(time.time() * 1000)

    def now_ms(self) -> int:
        return int(time.time() * 1000) + self._offset_ms
