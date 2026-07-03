"""BinanceSpotAdapter — the BrokerAdapter contract, live against Binance Spot.

Testnet-first (https://testnet.binance.vision — free keys, 24/7 market).

Semantics enforced here (the contract's teeth):
- submit(): network timeout or 5xx means the outcome is UNPROVABLE ->
  BrokerTimeout, never an invented id, never a retry. A 4xx with a Binance
  error code is a conclusive refusal -> BrokerReject.
- query_order(): ONLY code -2013 ("Order does not exist") means conclusively
  absent -> None. Any transport failure raises — absence must be proven,
  not inferred from an error.
- events(): the user data stream is at-least-once across reconnects; the
  ledger's exec_id dedup absorbs duplicates, reconciliation backfills gaps.

Binance scopes order lookups by symbol; our contract looks up by client
order id alone. The adapter keeps a client_order_id -> symbol map (populated
on submit) and falls back to scanning its configured symbol universe — a
real-broker finding to feed back into the adapter contract later.
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

log = logging.getLogger("sentinel.binance")

TESTNET_BASE = "https://testnet.binance.vision"
# The WebSocket API endpoint: user-data events arrive on THIS connection
# after userDataStream.subscribe.signature. (The REST listenKey flow —
# POST /api/v3/userDataStream — is dead: 410 Gone, found live 2026-07.)
TESTNET_WS = "wss://ws-api.testnet.binance.vision/ws-api/v3"

_ABSENT_CODE = -2013           # "Order does not exist" — the ONLY proof of absence

_STATUS_MAP = {
    "NEW": BrokerOrderState.WORKING,
    "PARTIALLY_FILLED": BrokerOrderState.PARTIAL,
    "FILLED": BrokerOrderState.FILLED,
    "CANCELED": BrokerOrderState.CANCELED,
    "EXPIRED": BrokerOrderState.CANCELED,
    "EXPIRED_IN_MATCH": BrokerOrderState.CANCELED,
    "REJECTED": BrokerOrderState.REJECTED,
    # Cancel not yet confirmed — the order is still live at the exchange.
    "PENDING_CANCEL": BrokerOrderState.WORKING,
}


def parse_user_event(payload: dict) -> BrokerEvent | None:
    """Pure parser for user-data-stream messages -> broker events.
    Unit-testable without a socket."""
    if payload.get("e") != "executionReport":
        return None
    exec_type = payload.get("x")
    if exec_type == "TRADE":
        return BrokerFill(
            client_order_id=payload["c"],
            exec_id=str(payload["t"]),
            qty=Decimal(payload["l"]),      # last executed quantity
            price=Decimal(payload["L"]),    # last executed price
        )
    if exec_type == "CANCELED":
        # On cancels Binance moves the original client id to 'C'; 'c' holds
        # the cancel request's own id.
        original = payload.get("C") or payload.get("c")
        return BrokerCancelConfirmed(client_order_id=original)
    return None


class BinanceSpotAdapter:
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
        self._secret = api_secret
        self._symbols = symbols
        self._ws_url = ws_url
        self._clock = ServerClock()
        self._synced = False
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
        resp = await self._http.get("/api/v3/time")
        resp.raise_for_status()
        self._clock.sync(resp.json()["serverTime"])
        self._synced = True

    async def _signed(self, method: str, path: str, params: dict) -> httpx.Response:
        await self._sync_clock()
        qs = signed_query(params, self._secret, timestamp_ms=self._clock.now_ms())
        try:
            return await self._http.request(method, f"{path}?{qs}")
        except (httpx.TimeoutException, httpx.TransportError) as e:
            # Outcome unprovable: the request may or may not have landed.
            raise BrokerTimeout(f"{method} {path}: {e!r}") from e

    @staticmethod
    def _error_code(resp: httpx.Response) -> tuple[int | None, str]:
        try:
            body = resp.json()
            return body.get("code"), body.get("msg", resp.text)
        except json.JSONDecodeError:
            return None, resp.text

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
            "symbol": instrument,
            "side": side.value,
            "quantity": fmt_decimal(qty),
            "newClientOrderId": client_order_id,
        }
        if limit_price is not None:
            params.update(
                type="LIMIT", timeInForce="GTC", price=fmt_decimal(limit_price)
            )
        else:
            params["type"] = "MARKET"

        self._symbol_of[client_order_id] = instrument
        resp = await self._signed("POST", "/api/v3/order", params)
        if resp.status_code >= 500:
            # Exchange-side failure AFTER the request may have been accepted.
            raise BrokerTimeout(f"submit {client_order_id}: HTTP {resp.status_code}")
        if resp.status_code >= 400:
            code, msg = self._error_code(resp)
            raise BrokerReject(f"[{code}] {msg}")
        return str(resp.json()["orderId"])

    async def cancel(self, client_order_id: str) -> None:
        symbol = await self._resolve_symbol(client_order_id)
        resp = await self._signed(
            "DELETE",
            "/api/v3/order",
            {"symbol": symbol, "origClientOrderId": client_order_id},
        )
        if resp.status_code >= 500:
            raise BrokerTimeout(f"cancel {client_order_id}: HTTP {resp.status_code}")
        if resp.status_code >= 400:
            code, _ = self._error_code(resp)
            if code == -2011:
                # "Unknown order sent": already terminal OR never existed —
                # either way the outcome is not provable from this response.
                raise BrokerTimeout(
                    f"cancel {client_order_id}: -2011, reconcile to resolve"
                )
            raise BrokerError(f"cancel {client_order_id}: {resp.text}")

    async def query_order(self, client_order_id: str) -> BrokerOrderView | None:
        symbol = self._symbol_of.get(client_order_id)
        candidates = (symbol,) if symbol else self._symbols
        for sym in candidates:
            view = await self._query_one(sym, client_order_id)
            if view is not None:
                self._symbol_of[client_order_id] = sym
                return view
        return None  # conclusively absent in every configured symbol

    async def _query_one(self, symbol: str, client_order_id: str
                         ) -> BrokerOrderView | None:
        resp = await self._signed(
            "GET",
            "/api/v3/order",
            {"symbol": symbol, "origClientOrderId": client_order_id},
        )
        if resp.status_code >= 400:
            code, msg = self._error_code(resp)
            if code == _ABSENT_CODE:
                return None                    # proven absent for THIS symbol
            raise BrokerError(f"query {client_order_id}@{symbol}: [{code}] {msg}")
        body = resp.json()

        fills: tuple[BrokerFill, ...] = ()
        if Decimal(body["executedQty"]) > 0:
            trades = await self._signed(
                "GET",
                "/api/v3/myTrades",
                {"symbol": symbol, "orderId": body["orderId"]},
            )
            if trades.status_code >= 400:
                raise BrokerError(f"myTrades {client_order_id}: {trades.text}")
            fills = tuple(
                BrokerFill(
                    client_order_id=client_order_id,
                    exec_id=str(t["id"]),
                    qty=Decimal(t["qty"]),
                    price=Decimal(t["price"]),
                )
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
        """Spot has balances, not positions: report non-zero asset balances
        (free + locked). Position truth per instrument still derives from
        our own fills ledger."""
        resp = await self._signed("GET", "/api/v3/account", {})
        if resp.status_code >= 400:
            raise BrokerError(f"account: {resp.text}")
        out: dict[str, Decimal] = {}
        for b in resp.json()["balances"]:
            total = Decimal(b["free"]) + Decimal(b["locked"])
            if total != 0:
                out[b["asset"]] = total
        return out

    # --------------------------------------------------------- user stream

    async def events(self) -> AsyncIterator[BrokerEvent]:
        """At-least-once event stream with reconnect, via the WebSocket API:
        connect -> userDataStream.subscribe.signature (HMAC) -> events arrive
        on the SAME connection as {"subscriptionId", "event": {...}} frames.
        Gaps across reconnects are the reconciler's job (query_order
        backfills); duplicates are the ledger's job (exec_id dedup)."""
        import websockets

        from .signing import ws_auth_params

        backoff = 1.0
        while True:
            try:
                await self._sync_clock()
                async with websockets.connect(self._ws_url) as ws:
                    await ws.send(json.dumps({
                        "id": "sentinel-subscribe",
                        "method": "userDataStream.subscribe.signature",
                        "params": ws_auth_params(
                            self._http.headers["X-MBX-APIKEY"], self._secret,
                            timestamp_ms=self._clock.now_ms(),
                        ),
                    }))
                    ack = json.loads(await ws.recv())
                    if ack.get("status") != 200:
                        raise BrokerError(f"user stream subscribe failed: {ack}")
                    log.info("user stream subscribed")
                    backoff = 1.0
                    async for raw in ws:
                        frame = json.loads(raw)
                        payload = frame.get("event")
                        if payload is None:
                            continue  # request responses, session notices
                        event = parse_user_event(payload)
                        if event is not None:
                            yield event
            except asyncio.CancelledError:
                raise
            except Exception as e:  # noqa: BLE001 — reconnect on anything
                log.warning("user stream dropped (%r); reconnecting", e)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30.0)

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
