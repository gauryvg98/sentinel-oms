"""DeltaFuturesAdapter — the BrokerAdapter contract, live against Delta
Exchange India perpetuals. Same contract as the Binance/Bybit adapters; a
different venue with a different API, added because it settles real INR-margin
crypto derivatives for Indian accounts (testnet first, real money eventually).

Testnet-first (REST https://cdn-ind.testnet.deltaex.org, WS
wss://socket-ind.testnet.deltaex.org — free demo keys). Production India is
https://api.india.delta.exchange / wss://socket.india.delta.exchange, selected
via constructor args (SENTINEL_DELTA_REST / SENTINEL_DELTA_WS in the runner).

Delta specifics vs Binance/Bybit, isolated here so the OMS never sees them:

- CONTRACT SIZING. Delta order/position/fill sizes are INTEGER CONTRACT
  COUNTS; each product carries a `contract_value` (e.g. 0.001 BTC/contract).
  Sentinel deals exclusively in decimal BASE quantity, so this adapter converts
  qty -> contracts on submit and contracts -> qty on every fill, view and
  position it emits. The conversion must be EXACT: a submit qty that is not an
  integer multiple of contract_value is refused locally (BrokerReject) rather
  than silently rounded — rounding here is the classic way to 100x a position.
  The spec fetcher (ui.instruments.fetch_delta_spec) publishes lot_step =
  contract_value so guards and sizing keep everything on that grid upstream.

- Auth: headers `api-key`, `timestamp` (epoch SECONDS) and `signature` =
  HMAC-SHA256 hexdigest of the secret over method + timestamp + path +
  queryString + body (queryString includes its leading '?'; Delta rejects
  signatures older than 5s, so they are minted per request).

- Every REST response wraps {success, result|error}. A transport/5xx failure
  is UNPROVABLE -> BrokerTimeout; success=false is a conclusive refusal ->
  BrokerReject (submit) / BrokerError (queries). Absence proof: the
  by-client-oid lookup 404s ("not_found"-family codes, mirrored in
  _ABSENT_CODES) — but Delta 404s that endpoint for TERMINAL orders too, so
  query_order additionally sweeps /v2/orders/history before returning None
  (None tells the reconciler "safe to resolve terminal", R1.6 — it must never
  mean "the network hiccuped").

- Client order id is `client_order_id` (max 32 chars — Sentinel's ULID-style
  keys fit). The user stream is one WS with a `key-auth` handshake (signature
  over "GET" + timestamp + "/live"), then orders / v2/user_trades / margins
  channels.
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
    BrokerPosition,
    BrokerReject,
    BrokerTimeout,
)

log = logging.getLogger("sentinel.delta")

TESTNET_BASE = "https://cdn-ind.testnet.deltaex.org"
TESTNET_WS = "wss://socket-ind.testnet.deltaex.org"
PROD_BASE = "https://api.india.delta.exchange"
PROD_WS = "wss://socket.india.delta.exchange"

# Error codes that PROVE the order is not known at the queried endpoint —
# Delta's analog of Binance -2013 / Bybit 110001. The by-client-oid endpoint
# also serves plain HTTP 404 (sometimes with a non-JSON body), handled in
# _error_code. Anything else on a lookup RAISES; None is reserved for proof.
_ABSENT_CODES = ("not_found", "order_not_found", "open_order_not_found")

# Delta order states: open (resting), pending (stop not yet triggered),
# closed (fully filled), cancelled. "open" refines to PARTIAL by filled qty.
_STATUS_MAP = {
    "open": BrokerOrderState.WORKING,
    "pending": BrokerOrderState.WORKING,
    "closed": BrokerOrderState.FILLED,
    "cancelled": BrokerOrderState.CANCELED,
}


def parse_delta_events(msg: dict, contract_values: dict[str, Decimal]
                       ) -> list[BrokerEvent]:
    """Pure parser: one private-stream frame -> zero or more broker events.
    Testable without a socket (like parse_bybit_events).

    `contract_values` maps symbol -> contract_value so fill sizes (integer
    contracts on the wire) come out in Sentinel's decimal base units. A fill on
    a symbol we have no contract_value for is SKIPPED rather than guessed —
    emitting a wrong-sized fill corrupts the ledger, while a skipped one is
    repaired by reconciliation (exec_id dedup makes the backfill exactly-once).
    """
    kind = msg.get("type", "")
    out: list[BrokerEvent] = []
    if kind == "v2/user_trades":
        cv = contract_values.get(msg.get("symbol", ""))
        if cv is None:
            log.warning("delta fill on unknown symbol %r skipped (reconcile "
                        "will backfill)", msg.get("symbol"))
            return out
        out.append(BrokerFill(
            client_order_id=msg.get("client_order_id") or "",
            exec_id=str(msg["fill_id"]),
            qty=Decimal(str(msg["size"])) * cv,
            price=Decimal(str(msg["price"])),
        ))
    elif kind == "orders":
        if msg.get("state") == "cancelled":
            out.append(BrokerCancelConfirmed(
                client_order_id=msg.get("client_order_id") or ""))
    elif kind == "margins":
        sym = msg.get("asset_symbol", "")
        bal = msg.get("balance", "")
        if sym and bal != "" and bal is not None:
            out.append(BrokerBalanceUpdate(balances={sym: Decimal(str(bal))}))
    return out


class DeltaFuturesAdapter:
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
        # symbol -> product dict and product_id -> product dict, fetched once
        # from /v2/products. contract_value/product_id are NEVER hardcoded.
        self._products: dict[str, dict] = {}
        self._products_by_id: dict[int, dict] = {}

    # ------------------------------------------------------------- plumbing

    def _signature(self, method: str, ts: str, path: str, qs: str,
                   body: str) -> str:
        """Delta prehash: method + timestamp + path + queryString + body.
        `qs` carries its leading '?' (or is empty) — that is what the server
        hashes. Signatures expire after 5s, so mint one per request."""
        origin = f"{method}{ts}{path}{qs}{body}"
        return hmac.new(self._secret.encode(), origin.encode(),
                        hashlib.sha256).hexdigest()

    def _headers(self, method: str, path: str, qs: str, body: str) -> dict:
        ts = str(int(time.time()))               # epoch SECONDS, not ms
        return {
            "api-key": self._key,
            "timestamp": ts,
            "signature": self._signature(method, ts, path, qs, body),
            "User-Agent": "sentinel-oms",        # Delta rejects blank UAs
            "Content-Type": "application/json",
        }

    async def _request(self, method: str, path: str, *,
                       params: dict | None = None,
                       json_body: dict | None = None) -> httpx.Response:
        """Signed request. Transport failures are UNPROVABLE -> BrokerTimeout
        (never a reject, never a silent retry — R1.3 has no resubmit path)."""
        qs = ""
        if params:
            qs = "?" + urlencode(params)
        body = ""
        if json_body is not None:
            body = json.dumps(json_body, separators=(",", ":"))
        headers = self._headers(method, path, qs, body)
        try:
            return await self._http.request(
                method, f"{path}{qs}", content=body or None, headers=headers)
        except (httpx.TimeoutException, httpx.TransportError) as e:
            raise BrokerTimeout(f"{method} {path}: {e!r}") from e

    @staticmethod
    def _error_code(resp: httpx.Response) -> str | None:
        """None when the response is a success envelope; else the error code.
        Delta's failure envelope is {"success": false, "error": {"code": ...}};
        the by-client-oid endpoint can also 404 with a NON-JSON body, which we
        normalize to "not_found" so absence handling has one shape."""
        try:
            body = resp.json()
        except ValueError:
            return "not_found" if resp.status_code == 404 else \
                f"http_{resp.status_code}"
        if body.get("success", False):
            return None
        err = body.get("error")
        if isinstance(err, dict):
            return str(err.get("code", "unknown"))
        return str(err) if err else f"http_{resp.status_code}"

    def _unwrap(self, resp: httpx.Response) -> dict | list:
        """5xx -> unprovable timeout; success=false -> reject; else result."""
        if resp.status_code >= 500:
            raise BrokerTimeout(f"HTTP {resp.status_code}")
        code = self._error_code(resp)
        if code is not None:
            raise BrokerReject(f"[{code}]")
        return resp.json().get("result") or {}

    # ------------------------------------------------------ contract sizing

    async def _ensure_products(self) -> None:
        """Lazy one-shot load of /v2/products (public, unsigned): symbol ->
        {id, contract_value, ...}. Everything qty<->contracts flows through
        this map — no contract size is ever assumed."""
        if self._products:
            return
        try:
            resp = await self._http.get("/v2/products",
                                        params={"page_size": "500"})
        except (httpx.TimeoutException, httpx.TransportError) as e:
            raise BrokerTimeout(f"GET /v2/products: {e!r}") from e
        if resp.status_code >= 500:
            raise BrokerTimeout(f"GET /v2/products: HTTP {resp.status_code}")
        rows = resp.json().get("result") or []
        for p in rows:
            if p.get("symbol") and p.get("contract_value") is not None:
                self._products[p["symbol"]] = p
                if p.get("id") is not None:
                    self._products_by_id[int(p["id"])] = p
        if not self._products:
            raise BrokerError("Delta /v2/products returned no products")

    def _product(self, symbol: str) -> dict:
        p = self._products.get(symbol)
        if p is None:
            raise BrokerError(f"{symbol!r} not listed on Delta")
        return p

    def _contract_value(self, symbol: str) -> Decimal:
        return Decimal(str(self._product(symbol)["contract_value"]))

    def _to_contracts(self, symbol: str, qty: Decimal) -> int:
        """Base qty -> integer contract count, EXACTLY. Off-grid quantities are
        refused, not rounded: a silent round here changes real position size —
        the #1 way to be 100x wrong. lot_step=contract_value upstream means a
        well-formed order never trips this."""
        cv = self._contract_value(symbol)
        n = qty / cv
        if n != n.to_integral_value():
            raise BrokerReject(
                f"qty {qty} is not a multiple of {symbol} contract_value {cv}")
        return int(n)

    def _to_qty(self, symbol: str, contracts: Decimal | int | str) -> Decimal:
        """Integer contracts (possibly signed) -> decimal base qty."""
        return Decimal(str(contracts)) * self._contract_value(symbol)

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
        await self._ensure_products()
        params: dict = {
            "product_symbol": instrument,
            "size": self._to_contracts(instrument, qty),
            "side": "buy" if side is Side.BUY else "sell",
            "client_order_id": client_order_id,
        }
        if limit_price is not None:
            params.update(order_type="limit_order", time_in_force="gtc",
                          limit_price=format(limit_price.normalize(), "f"))
        else:
            params["order_type"] = "market_order"
        result = self._unwrap(await self._request("POST", "/v2/orders",
                                                  json_body=params))
        return str(result.get("id", ""))

    async def cancel(self, client_order_id: str) -> None:
        """DELETE /v2/orders needs the exchange id + product_id, so resolve the
        order first. Absence at cancel time is NOT success — the order may have
        just filled — so it maps to BrokerTimeout and the reconciler decides."""
        order = await self._find_order(client_order_id)
        if order is None:                       # already terminal or never existed
            raise BrokerTimeout(f"cancel {client_order_id}: not found, reconcile")
        resp = await self._request("DELETE", "/v2/orders", json_body={
            "id": order.get("id"), "product_id": order.get("product_id"),
            "client_order_id": client_order_id})
        if resp.status_code >= 500:
            raise BrokerTimeout(f"cancel {client_order_id}: HTTP {resp.status_code}")
        code = self._error_code(resp)
        if code is None:
            return
        if code in _ABSENT_CODES:               # raced terminal between lookup+delete
            raise BrokerTimeout(f"cancel {client_order_id}: [{code}], reconcile")
        raise BrokerError(f"cancel {client_order_id}: [{code}]")

    async def query_order(self, client_order_id: str) -> BrokerOrderView | None:
        order = await self._find_order(client_order_id)
        if order is None:
            return None
        await self._ensure_products()
        symbol = order.get("product_symbol") or ""
        if not symbol and order.get("product_id") is not None:
            p = self._products_by_id.get(int(order["product_id"]))
            symbol = p["symbol"] if p else ""
        size = Decimal(str(order.get("size", "0")))
        unfilled = Decimal(str(order.get("unfilled_size", "0")))
        filled_contracts = size - unfilled

        fills: tuple[BrokerFill, ...] = ()
        if filled_contracts > 0:
            fills = await self._fills_for(client_order_id, order, symbol)

        state = _STATUS_MAP.get(order.get("state", ""), BrokerOrderState.WORKING)
        if state is BrokerOrderState.WORKING and filled_contracts > 0:
            state = BrokerOrderState.PARTIAL
        return BrokerOrderView(
            client_order_id=client_order_id,
            broker_order_id=str(order.get("id", "")),
            state=state,
            filled_qty=self._to_qty(symbol, filled_contracts),
            fills=fills,
        )

    async def _find_order(self, client_order_id: str) -> dict | None:
        """The raw order dict, or None ONLY on proven absence. Two sweeps:

        1. GET /v2/orders/client_order_id/{id} — but Delta 404s this for
           TERMINAL (filled/cancelled) orders too, so a 404 here proves nothing
           terminal-vs-never-existed on its own.
        2. GET /v2/orders/history (cancelled + closed) — client_order_id is
           passed as a filter AND re-checked client-side, so a server that
           ignores the param still can't hand us the wrong order.

        Absent from both = conclusively absent. Any transport/5xx/parse failure
        RAISES — the reconciler resolves terminal on None, so None must never
        stand in for "couldn't reach Delta" (R1.6)."""
        resp = await self._request(
            "GET", f"/v2/orders/client_order_id/{client_order_id}")
        if resp.status_code >= 500:
            raise BrokerTimeout(f"query {client_order_id}: HTTP {resp.status_code}")
        code = self._error_code(resp)
        if code is None:
            result = resp.json().get("result") or {}
            # Defensive: some list-shaped deployments wrap the order in a list.
            if isinstance(result, list):
                result = next((r for r in result
                               if r.get("client_order_id") == client_order_id),
                              None)
            if result:
                return result
        elif code not in _ABSENT_CODES:
            raise BrokerError(f"query {client_order_id}: [{code}]")

        resp = await self._request("GET", "/v2/orders/history", params={
            "client_order_id": client_order_id, "page_size": "100"})
        if resp.status_code >= 500:
            raise BrokerTimeout(f"query {client_order_id}: HTTP {resp.status_code}")
        code = self._error_code(resp)
        if code is not None:
            if code in _ABSENT_CODES:
                return None
            raise BrokerError(f"query {client_order_id}: [{code}]")
        rows = resp.json().get("result") or []
        for row in rows:
            if row.get("client_order_id") == client_order_id:
                return row
        return None

    async def _fills_for(self, client_order_id: str, order: dict,
                         symbol: str) -> tuple[BrokerFill, ...]:
        """Executions for one order from /v2/fills. Filtered by order id
        server-side (param) AND client-side (belt and braces — an ignored
        filter must not attribute someone else's fills to this order). Fill ids
        are Delta's stable exec ids; the ledger dedups on them (R1.7)."""
        resp = await self._request("GET", "/v2/fills", params={
            "order_id": str(order.get("id", "")), "page_size": "100"})
        rows = self._unwrap(resp)
        if not isinstance(rows, list):
            rows = []
        oid = str(order.get("id", ""))
        return tuple(
            BrokerFill(
                client_order_id=client_order_id,
                exec_id=str(f["id"]),
                qty=self._to_qty(symbol, f["size"]),
                price=Decimal(str(f["price"])),
            )
            for f in rows if str(f.get("order_id", "")) == oid
        )

    async def query_positions(self) -> dict[str, Decimal]:
        """Wallet balances for the equity display (asset -> balance). The
        SIGNED position per instrument is tracked by our fills ledger, like the
        other adapters; exchange positions come from open_positions()."""
        rows = self._unwrap(await self._request("GET", "/v2/wallet/balances"))
        if not isinstance(rows, list):
            rows = []
        out: dict[str, Decimal] = {}
        for r in rows:
            bal = Decimal(str(r.get("balance", "0") or "0"))
            if bal != 0 and r.get("asset_symbol"):
                out[r["asset_symbol"]] = bal
        return out

    async def open_positions(self) -> dict[str, BrokerPosition]:
        """Actual open positions from /v2/positions/margined — sizes arrive as
        SIGNED integer contracts (negative = short) and leave here as signed
        base qty. Source of truth for POSITION reconciliation."""
        await self._ensure_products()
        rows = self._unwrap(await self._request("GET", "/v2/positions/margined"))
        if not isinstance(rows, list):
            rows = []
        out: dict[str, BrokerPosition] = {}
        for p in rows:
            contracts = Decimal(str(p.get("size", "0") or "0"))
            if contracts == 0:
                continue
            symbol = p.get("product_symbol") or ""
            if not symbol and p.get("product_id") is not None:
                prod = self._products_by_id.get(int(p["product_id"]))
                symbol = prod["symbol"] if prod else ""
            if not symbol:
                continue
            liq = p.get("liquidation_price")
            mark = p.get("mark_price")
            out[symbol] = BrokerPosition(
                qty=self._to_qty(symbol, contracts),
                entry_price=Decimal(str(p.get("entry_price", "0") or "0")),
                liq_price=Decimal(str(liq)) if liq not in (None, "") else None,
                mark_price=Decimal(str(mark)) if mark not in (None, "") else None,
            )
        return out

    # --------------------------------------------------------- user stream

    async def events(self) -> AsyncIterator[BrokerEvent]:
        """At-least-once private stream: connect -> key-auth (HMAC over
        'GET{timestamp}/live') -> subscribe orders / v2/user_trades / margins,
        server heartbeats enabled. Reconnect on drop; gaps are repaired by the
        stale-order sweeper upstream (no replay here), dupes are absorbed by
        the ledger's exec_id dedup (R1.7)."""
        import websockets

        backoff = 1.0
        while True:
            try:
                await self._ensure_products()   # cv map before any fill frame
                async with websockets.connect(self._ws_url) as ws:
                    ts = str(int(time.time()))
                    sig = hmac.new(self._secret.encode(),
                                   f"GET{ts}/live".encode(),
                                   hashlib.sha256).hexdigest()
                    await ws.send(json.dumps({"type": "key-auth", "payload": {
                        "api-key": self._key, "signature": sig,
                        "timestamp": ts}}))
                    await ws.send(json.dumps({"type": "subscribe", "payload": {
                        "channels": [
                            {"name": "orders", "symbols": ["all"]},
                            {"name": "v2/user_trades", "symbols": ["all"]},
                            {"name": "margins"},
                        ]}}))
                    # Server-side heartbeats keep idle connections provably
                    # alive; the websockets lib's protocol pings cover the
                    # client->server direction.
                    await ws.send(json.dumps({"type": "enable_heartbeat"}))
                    log.info("delta user stream connected")
                    backoff = 1.0
                    cv = {s: self._contract_value(s) for s in self._products}
                    async for raw in ws:
                        for ev in parse_delta_events(json.loads(raw), cv):
                            yield ev
            except asyncio.CancelledError:
                raise
            except Exception as e:  # noqa: BLE001
                log.warning("delta user stream dropped (%r); reconnecting", e)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30.0)

    async def aclose(self) -> None:
        await self._http.aclose()
