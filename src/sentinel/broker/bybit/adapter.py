"""BybitFuturesAdapter — the BrokerAdapter contract, live against Bybit V5
USDT perpetuals (category=linear). Same contract as the Binance adapters; a
different venue with a different API, chosen because its testnet is generally
open and auto-funds (no region wall).

Testnet-first (https://api-testnet.bybit.com — free keys + faucet, 24/7).

Bybit specifics vs Binance, isolated here so the OMS never sees them:
- Auth: HMAC-SHA256 over (timestamp + apiKey + recvWindow + payload); the
  payload is the query string (GET) or the raw JSON body (POST). Sent as
  X-BAPI-* headers, not a signature query param.
- Every REST response wraps {retCode, retMsg, result}; retCode 0 = ok. A
  transport/5xx failure is UNPROVABLE -> BrokerTimeout; a non-zero retCode is a
  conclusive refusal -> BrokerReject. "order not exists" (110001) on a lookup is
  the proof of absence -> None (Binance's -2013).
- Client order id is `orderLinkId`. The user stream is one WS with an auth
  handshake, then execution/order/wallet topics.
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import logging
import time
from decimal import Decimal
from typing import AsyncIterator
from urllib.parse import urlencode

import httpx

from sentinel.domain import Side

from ..adapter import (
    BrokerBalanceUpdate,
    BrokerCancelConfirmed,
    BrokerError,
    BrokerEvent,
    BrokerFill,
    BrokerOrderState,
    BrokerOrderView,
    BrokerReject,
    BrokerTimeout,
)

log = logging.getLogger("sentinel.bybit")

TESTNET_BASE = "https://api-testnet.bybit.com"
TESTNET_WS = "wss://stream-testnet.bybit.com/v5/private"
_CATEGORY = "linear"                          # USDT-margined perpetuals
_RECV_WINDOW = "5000"
_ABSENT_CODES = (110001,)                      # "order not exists" -> proven absent

_STATUS_MAP = {
    "New": BrokerOrderState.WORKING,
    "PartiallyFilled": BrokerOrderState.PARTIAL,
    "Filled": BrokerOrderState.FILLED,
    "Cancelled": BrokerOrderState.CANCELED,
    "Rejected": BrokerOrderState.REJECTED,
    "Deactivated": BrokerOrderState.CANCELED,
    "Untriggered": BrokerOrderState.WORKING,
    "Triggered": BrokerOrderState.WORKING,
}


def parse_bybit_events(msg: dict) -> list[BrokerEvent]:
    """Pure parser: one private-stream frame -> zero or more broker events (a
    Bybit frame batches multiple rows). Testable without a socket."""
    topic = msg.get("topic", "")
    rows = msg.get("data", []) or []
    out: list[BrokerEvent] = []
    if topic == "execution":
        for d in rows:
            if d.get("execType", "Trade") != "Trade":
                continue                       # funding/settlement rows aren't fills
            out.append(BrokerFill(
                client_order_id=d.get("orderLinkId", ""),
                exec_id=str(d["execId"]),
                qty=Decimal(d["execQty"]),
                price=Decimal(d["execPrice"]),
            ))
    elif topic == "order":
        for d in rows:
            if d.get("orderStatus") in ("Cancelled", "Deactivated"):
                out.append(BrokerCancelConfirmed(client_order_id=d.get("orderLinkId", "")))
    elif topic == "wallet":
        balances: dict[str, Decimal] = {}
        for acct in rows:
            for c in acct.get("coin", []):
                if c.get("walletBalance", "") != "":
                    balances[c["coin"]] = Decimal(c["walletBalance"])
        if balances:
            out.append(BrokerBalanceUpdate(balances=balances))
    return out


class BybitFuturesAdapter:
    def __init__(
        self,
        api_key: str,
        api_secret: str,
        *,
        symbols: tuple[str, ...],
        base_url: str = TESTNET_BASE,
        ws_url: str = TESTNET_WS,
        transport: httpx.AsyncBaseTransport | None = None,
        timeout_s: float = 5.0,
    ) -> None:
        if not symbols:
            raise ValueError("configure at least one traded symbol")
        self._key = api_key
        self._secret = api_secret
        self._symbols = symbols
        self._ws_url = ws_url
        self._http = httpx.AsyncClient(base_url=base_url, timeout=timeout_s,
                                       transport=transport)

    # ------------------------------------------------------------- plumbing

    def _now_ms(self) -> int:
        return int(time.time() * 1000)

    def _sign(self, ts: str, payload: str) -> str:
        origin = f"{ts}{self._key}{_RECV_WINDOW}{payload}"
        return hmac.new(self._secret.encode(), origin.encode(), hashlib.sha256).hexdigest()

    def _headers(self, ts: str, payload: str) -> dict:
        return {
            "X-BAPI-API-KEY": self._key,
            "X-BAPI-TIMESTAMP": ts,
            "X-BAPI-RECV-WINDOW": _RECV_WINDOW,
            "X-BAPI-SIGN": self._sign(ts, payload),
        }

    async def _get(self, path: str, params: dict) -> httpx.Response:
        ts = str(self._now_ms())
        qs = urlencode(params)
        try:
            return await self._http.get(f"{path}?{qs}", headers=self._headers(ts, qs))
        except (httpx.TimeoutException, httpx.TransportError) as e:
            raise BrokerTimeout(f"GET {path}: {e!r}") from e

    async def _post(self, path: str, params: dict) -> httpx.Response:
        ts = str(self._now_ms())
        body = json.dumps(params, separators=(",", ":"))
        headers = {**self._headers(ts, body), "Content-Type": "application/json"}
        try:
            return await self._http.post(path, content=body, headers=headers)
        except (httpx.TimeoutException, httpx.TransportError) as e:
            raise BrokerTimeout(f"POST {path}: {e!r}") from e

    @staticmethod
    def _unwrap(resp: httpx.Response) -> dict:
        """5xx -> unprovable timeout; non-zero retCode -> reject; else result."""
        if resp.status_code >= 500:
            raise BrokerTimeout(f"HTTP {resp.status_code}")
        body = resp.json()
        if body.get("retCode") not in (0, "0"):
            raise BrokerReject(f"[{body.get('retCode')}] {body.get('retMsg')}")
        return body.get("result") or {}

    # ------------------------------------------------------------- adapter

    async def submit(
        self,
        *,
        client_order_id: str,
        instrument: str,
        side: Side,
        qty: Decimal,
        limit_price: Decimal | None,
    ) -> str:
        params: dict = {
            "category": _CATEGORY,
            "symbol": instrument,
            "side": "Buy" if side is Side.BUY else "Sell",
            "qty": format(qty.normalize(), "f"),
            "orderLinkId": client_order_id,
        }
        if limit_price is not None:
            params.update(orderType="Limit", timeInForce="GTC",
                          price=format(limit_price.normalize(), "f"))
        else:
            params["orderType"] = "Market"
        result = self._unwrap(await self._post("/v5/order/create", params))
        return str(result.get("orderId", ""))

    async def cancel(self, client_order_id: str) -> None:
        symbol = await self._resolve_symbol(client_order_id)
        resp = await self._post("/v5/order/cancel",
                                {"category": _CATEGORY, "symbol": symbol,
                                 "orderLinkId": client_order_id})
        if resp.status_code >= 500:
            raise BrokerTimeout(f"cancel {client_order_id}: HTTP {resp.status_code}")
        body = resp.json()
        code = body.get("retCode")
        if code in (0, "0"):
            return
        if code in _ABSENT_CODES:               # already terminal or never existed
            raise BrokerTimeout(f"cancel {client_order_id}: [{code}], reconcile")
        raise BrokerError(f"cancel {client_order_id}: [{code}] {body.get('retMsg')}")

    async def query_order(self, client_order_id: str) -> BrokerOrderView | None:
        symbol = self._symbols[0] if len(self._symbols) == 1 else None
        for sym in ((symbol,) if symbol else self._symbols):
            view = await self._query_one(sym, client_order_id)
            if view is not None:
                return view
        return None

    async def _query_one(self, symbol: str, client_order_id: str
                         ) -> BrokerOrderView | None:
        order = None
        for path in ("/v5/order/realtime", "/v5/order/history"):   # open, then terminal
            resp = await self._get(path, {"category": _CATEGORY, "symbol": symbol,
                                          "orderLinkId": client_order_id})
            body = resp.json()
            if body.get("retCode") in _ABSENT_CODES:
                continue
            if body.get("retCode") not in (0, "0"):
                raise BrokerError(f"query {client_order_id}: {body.get('retMsg')}")
            rows = (body.get("result") or {}).get("list") or []
            if rows:
                order = rows[0]
                break
        if order is None:
            return None

        fills: tuple[BrokerFill, ...] = ()
        if Decimal(order.get("cumExecQty", "0")) > 0:
            ex = await self._get("/v5/execution/list",
                                 {"category": _CATEGORY, "symbol": symbol,
                                  "orderLinkId": client_order_id})
            rows = (self._unwrap(ex)).get("list") or []
            fills = tuple(
                BrokerFill(client_order_id=client_order_id, exec_id=str(e["execId"]),
                           qty=Decimal(e["execQty"]), price=Decimal(e["execPrice"]))
                for e in rows if e.get("execType", "Trade") == "Trade"
            )
        return BrokerOrderView(
            client_order_id=client_order_id,
            broker_order_id=str(order.get("orderId", "")),
            state=_STATUS_MAP.get(order.get("orderStatus", ""), BrokerOrderState.WORKING),
            filled_qty=Decimal(order.get("cumExecQty", "0")),
            fills=fills,
        )

    async def query_positions(self) -> dict[str, Decimal]:
        """USDT wallet balance for the equity display. The SIGNED position per
        instrument is tracked by our fills ledger (broker truth = stream +
        reconciliation), like the Binance adapters."""
        result = self._unwrap(await self._get("/v5/account/wallet-balance",
                                               {"accountType": "UNIFIED"}))
        out: dict[str, Decimal] = {}
        for acct in result.get("list", []):
            for c in acct.get("coin", []):
                bal = Decimal(c.get("walletBalance", "0") or "0")
                if bal != 0:
                    out[c["coin"]] = bal
        return out

    # --------------------------------------------------------- user stream

    async def events(self) -> AsyncIterator[BrokerEvent]:
        """At-least-once private stream: connect -> auth (HMAC over
        'GET/realtime{expires}') -> subscribe execution/order/wallet, ping every
        20s. Reconnect on drop; gaps -> reconciler, dupes -> ledger exec_id."""
        import websockets

        backoff = 1.0
        while True:
            try:
                async with websockets.connect(self._ws_url) as ws:
                    expires = self._now_ms() + 10_000
                    sig = hmac.new(self._secret.encode(),
                                   f"GET/realtime{expires}".encode(),
                                   hashlib.sha256).hexdigest()
                    await ws.send(json.dumps({"op": "auth",
                                              "args": [self._key, expires, sig]}))
                    await ws.send(json.dumps({"op": "subscribe",
                                              "args": ["execution", "order", "wallet"]}))
                    log.info("bybit user stream connected")
                    backoff = 1.0
                    ping = asyncio.create_task(self._ping(ws))
                    try:
                        async for raw in ws:
                            for ev in parse_bybit_events(json.loads(raw)):
                                yield ev
                    finally:
                        ping.cancel()
            except asyncio.CancelledError:
                raise
            except Exception as e:  # noqa: BLE001
                log.warning("bybit user stream dropped (%r); reconnecting", e)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30.0)

    async def _ping(self, ws) -> None:
        while True:
            await asyncio.sleep(20)             # Bybit drops idle sockets
            await ws.send(json.dumps({"op": "ping"}))

    # ------------------------------------------------------------- helpers

    async def _resolve_symbol(self, client_order_id: str) -> str:
        if len(self._symbols) == 1:
            return self._symbols[0]
        view = await self.query_order(client_order_id)
        if view is None:
            raise BrokerError(f"no symbol known for {client_order_id!r}")
        return self._symbols[0]

    async def aclose(self) -> None:
        await self._http.aclose()
