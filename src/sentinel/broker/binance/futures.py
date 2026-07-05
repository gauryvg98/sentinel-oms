"""BinanceFuturesAdapter — the BrokerAdapter contract, live against Binance
USDT-M perpetual futures. Same contract as the spot adapter; the difference is
the venue can hold a SIGNED (long OR short) position, which is what unlocks
regime-agnostic strategies.

Testnet-first (https://testnet.binancefuture.com — free keys, 24/7).

Design choices:
- ONE-WAY position mode (net signed position), set lazily on first submit so a
  SELL opens/increases a short exactly the way the signed target model wants.
- Leverage is set once (default 1x — no leverage until a strategy asks for it).
- The user data stream uses the classic listenKey flow (POST /fapi/v1/listenKey
  -> connect wss/<key> -> PUT keepalive), which — unlike spot testnet — is live.

Contract semantics are identical to spot (timeout != rejection; absence needs
code -2013; exec_id dedup + reconciliation cover the at-least-once stream).
"""

from __future__ import annotations

import asyncio
import json
import logging
from decimal import Decimal
from typing import AsyncIterator

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
from .signing import ServerClock, fmt_decimal, signed_query

log = logging.getLogger("sentinel.binance.futures")

TESTNET_BASE = "https://testnet.binancefuture.com"
TESTNET_WS = "wss://fstream.binancefuture.com"

_ABSENT_CODE = -2013           # "Order does not exist" — the ONLY proof of absence

_STATUS_MAP = {
    "NEW": BrokerOrderState.WORKING,
    "PARTIALLY_FILLED": BrokerOrderState.PARTIAL,
    "FILLED": BrokerOrderState.FILLED,
    "CANCELED": BrokerOrderState.CANCELED,
    "EXPIRED": BrokerOrderState.CANCELED,
    "REJECTED": BrokerOrderState.REJECTED,
}


def parse_futures_event(payload: dict) -> BrokerEvent | None:
    """Pure parser for USDT-M user-data messages -> broker events. Testable
    without a socket."""
    e = payload.get("e")

    if e == "ACCOUNT_UPDATE":
        # Balances pushed after every change; a["B"] = [{a: asset, wb: wallet}].
        balances = {b["a"]: Decimal(b["wb"]) for b in payload.get("a", {}).get("B", [])}
        return BrokerBalanceUpdate(balances=balances) if balances else None

    if e != "ORDER_TRADE_UPDATE":
        return None
    o = payload.get("o", {})
    x = o.get("x")
    if x == "TRADE":
        return BrokerFill(
            client_order_id=o["c"],
            exec_id=str(o["t"]),          # trade id
            qty=Decimal(o["l"]),          # last filled qty
            price=Decimal(o["L"]),        # last filled price
        )
    if x in ("CANCELED", "EXPIRED"):
        return BrokerCancelConfirmed(client_order_id=o["c"])
    return None


class BinanceFuturesAdapter:
    def __init__(
        self,
        api_key: str,
        api_secret: str,
        *,
        symbols: tuple[str, ...],
        leverage: int = 1,
        base_url: str = TESTNET_BASE,
        ws_base: str = TESTNET_WS,
        transport: httpx.AsyncBaseTransport | None = None,
        timeout_s: float = 5.0,
    ) -> None:
        if not symbols:
            raise ValueError("configure at least one traded symbol")
        self._secret = api_secret
        self._symbols = symbols
        self._leverage = leverage
        self._ws_base = ws_base
        self._clock = ServerClock()
        self._synced = False
        self._configured = False
        self._symbol_of: dict[str, str] = {}   # client_order_id -> symbol
        self._http = httpx.AsyncClient(
            base_url=base_url,
            headers={"X-MBX-APIKEY": api_key},
            timeout=timeout_s,
            transport=transport,
        )

    # ------------------------------------------------------------- plumbing

    async def _sync_clock(self) -> None:
        if self._synced:
            return
        resp = await self._http.get("/fapi/v1/time")
        resp.raise_for_status()
        self._clock.sync(resp.json()["serverTime"])
        self._synced = True

    async def _signed(self, method: str, path: str, params: dict) -> httpx.Response:
        await self._sync_clock()
        qs = signed_query(params, self._secret, timestamp_ms=self._clock.now_ms())
        try:
            return await self._http.request(method, f"{path}?{qs}")
        except (httpx.TimeoutException, httpx.TransportError) as e:
            raise BrokerTimeout(f"{method} {path}: {e!r}") from e

    async def _keyed(self, method: str, path: str) -> httpx.Response:
        """API-key-only call (no signature) — the listenKey (USER_STREAM) flow."""
        try:
            return await self._http.request(method, path)
        except (httpx.TimeoutException, httpx.TransportError) as e:
            raise BrokerTimeout(f"{method} {path}: {e!r}") from e

    @staticmethod
    def _error_code(resp: httpx.Response) -> tuple[int | None, str]:
        try:
            body = resp.json()
            return body.get("code"), body.get("msg", resp.text)
        except json.JSONDecodeError:
            return None, resp.text

    async def _ensure_configured(self) -> None:
        """One-time, best-effort: net (one-way) position mode + leverage. A
        failure here is logged, not fatal — trading shouldn't be blocked by a
        'no need to change' response (-4046/-4059)."""
        if self._configured:
            return
        self._configured = True
        try:
            await self._signed("POST", "/fapi/v1/positionSide/dual",
                               {"dualSidePosition": "false"})
            for sym in self._symbols:
                await self._signed("POST", "/fapi/v1/leverage",
                                   {"symbol": sym, "leverage": self._leverage})
        except Exception as e:  # noqa: BLE001
            log.warning("futures configure failed (continuing): %r", e)

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
        await self._ensure_configured()
        params: dict = {
            "symbol": instrument,
            "side": side.value,               # BUY opens/adds long or covers short;
            "quantity": fmt_decimal(qty),     # SELL opens/adds short or trims long
            "newClientOrderId": client_order_id,
        }
        if limit_price is not None:
            params.update(type="LIMIT", timeInForce="GTC",
                          price=fmt_decimal(limit_price))
        else:
            params["type"] = "MARKET"

        self._symbol_of[client_order_id] = instrument
        resp = await self._signed("POST", "/fapi/v1/order", params)
        if resp.status_code >= 500:
            raise BrokerTimeout(f"submit {client_order_id}: HTTP {resp.status_code}")
        if resp.status_code >= 400:
            code, msg = self._error_code(resp)
            raise BrokerReject(f"[{code}] {msg}")
        return str(resp.json()["orderId"])

    async def cancel(self, client_order_id: str) -> None:
        symbol = await self._resolve_symbol(client_order_id)
        resp = await self._signed(
            "DELETE", "/fapi/v1/order",
            {"symbol": symbol, "origClientOrderId": client_order_id})
        if resp.status_code >= 500:
            raise BrokerTimeout(f"cancel {client_order_id}: HTTP {resp.status_code}")
        if resp.status_code >= 400:
            code, _ = self._error_code(resp)
            if code == -2011:                 # unknown order: terminal or absent
                raise BrokerTimeout(
                    f"cancel {client_order_id}: -2011, reconcile to resolve")
            raise BrokerError(f"cancel {client_order_id}: {resp.text}")

    async def query_order(self, client_order_id: str) -> BrokerOrderView | None:
        symbol = self._symbol_of.get(client_order_id)
        for sym in ((symbol,) if symbol else self._symbols):
            view = await self._query_one(sym, client_order_id)
            if view is not None:
                self._symbol_of[client_order_id] = sym
                return view
        return None

    async def _query_one(self, symbol: str, client_order_id: str
                         ) -> BrokerOrderView | None:
        resp = await self._signed(
            "GET", "/fapi/v1/order",
            {"symbol": symbol, "origClientOrderId": client_order_id})
        if resp.status_code >= 400:
            code, msg = self._error_code(resp)
            if code == _ABSENT_CODE:
                return None
            raise BrokerError(f"query {client_order_id}@{symbol}: [{code}] {msg}")
        body = resp.json()

        fills: tuple[BrokerFill, ...] = ()
        if Decimal(body["executedQty"]) > 0:
            trades = await self._signed(
                "GET", "/fapi/v1/userTrades",
                {"symbol": symbol, "orderId": body["orderId"]})
            if trades.status_code >= 400:
                raise BrokerError(f"userTrades {client_order_id}: {trades.text}")
            fills = tuple(
                BrokerFill(client_order_id=client_order_id, exec_id=str(t["id"]),
                           qty=Decimal(t["qty"]), price=Decimal(t["price"]))
                for t in trades.json()
            )

        return BrokerOrderView(
            client_order_id=client_order_id,
            broker_order_id=str(body["orderId"]),
            state=_STATUS_MAP[body["status"]],
            filled_qty=Decimal(body["executedQty"]),
            fills=fills,
        )

    async def query_positions(self) -> dict[str, Decimal]:
        """USDT-M wallet balance for the equity display. The SIGNED position per
        instrument is tracked by our own fills ledger (broker truth is the
        stream + reconciliation), so this is display-only, like spot."""
        resp = await self._signed("GET", "/fapi/v2/balance", {})
        if resp.status_code >= 400:
            raise BrokerError(f"balance: {resp.text}")
        return {b["asset"]: Decimal(b["balance"])
                for b in resp.json() if Decimal(b["balance"]) != 0}

    # --------------------------------------------------------- user stream

    async def events(self) -> AsyncIterator[BrokerEvent]:
        """At-least-once stream via listenKey: POST to open, connect
        wss/<key>, PUT to keep alive; reconnect (fresh key) on any drop. Gaps
        are the reconciler's job, duplicates the ledger's (exec_id dedup)."""
        import websockets

        backoff = 1.0
        while True:
            try:
                key_resp = await self._keyed("POST", "/fapi/v1/listenKey")
                key_resp.raise_for_status()
                listen_key = key_resp.json()["listenKey"]
                async with websockets.connect(f"{self._ws_base}/ws/{listen_key}") as ws:
                    log.info("futures user stream connected")
                    backoff = 1.0
                    keepalive = asyncio.create_task(self._keepalive())
                    try:
                        async for raw in ws:
                            event = parse_futures_event(json.loads(raw))
                            if event is not None:
                                yield event
                    finally:
                        keepalive.cancel()
            except asyncio.CancelledError:
                raise
            except Exception as e:  # noqa: BLE001
                log.warning("futures user stream dropped (%r); reconnecting", e)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30.0)

    async def _keepalive(self) -> None:
        while True:
            await asyncio.sleep(1800)          # listenKey lives 60m; refresh at 30m
            try:
                await self._keyed("PUT", "/fapi/v1/listenKey")
            except Exception as e:  # noqa: BLE001
                log.warning("listenKey keepalive failed: %r", e)
                return

    # ------------------------------------------------------------- helpers

    async def _resolve_symbol(self, client_order_id: str) -> str:
        symbol = self._symbol_of.get(client_order_id)
        if symbol:
            return symbol
        view = await self.query_order(client_order_id)
        if view is None:
            raise BrokerError(f"no symbol known for {client_order_id!r}")
        return self._symbol_of[client_order_id]

    async def aclose(self) -> None:
        await self._http.aclose()
