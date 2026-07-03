"""BinanceSpotAdapter — the BrokerAdapter contract mapped to Binance Spot.

FOUNDATION ONLY: the mapping below is the integration spec; methods raise
NotImplementedError until the integration lands. Testnet first
(https://testnet.binance.vision — free keys, 24/7 market).

Contract mapping
================
submit()
    POST /api/v3/order with newClientOrderId = our client_order_id.
    - HTTP 200 with orderId            -> return str(orderId)
    - -2010 (insufficient balance) etc -> BrokerReject(reason)
    - network timeout / 5xx / no body  -> BrokerTimeout  (NEVER invent an id;
      NEVER retry here — the reconciler owns recovery)

query_order()   (the reconciliation primitive)
    GET /api/v3/order?symbol=..&origClientOrderId=<client_order_id>
    - found  -> BrokerOrderView(state=map(status), filled=executedQty,
                fills via GET /api/v3/myTrades?orderId=..)
    - -2013 "Order does not exist" -> None (conclusively absent) — ONLY this
      exact code means absent; any transport error must raise, not return None
    status map: NEW->WORKING, PARTIALLY_FILLED->PARTIAL, FILLED->FILLED,
    CANCELED/EXPIRED->CANCELED, REJECTED->REJECTED

cancel()
    DELETE /api/v3/order?origClientOrderId=..
    - -2011 "Unknown order sent" on a live order -> treat as timeout-class:
      reconcile, don't assume
    - confirmation arrives via the user stream, not the HTTP response

events()
    User data stream (listenKey via POST /api/v3/userDataStream, keepalive
    every ~30min, ws wss://stream.testnet.binance.vision/ws/<listenKey>).
    executionReport: executionType TRADE -> BrokerFill(exec_id=str(tradeId
    field 't'), qty='l', price='L'); CANCELED -> BrokerCancelConfirmed.
    Delivery is at-least-once across reconnects — exactly what the ledger's
    exec_id dedup expects. On reconnect: resync via query_order backfill.

query_positions()
    Spot has balances, not positions: GET /api/v3/account -> map free+locked
    per asset to net quantity vs the quote currency.

Operational notes for the real build
    - clock skew: sign with serverTime offset (GET /api/v3/time), re-sync on
      -1021 errors
    - rate limits: respect X-MBX-USED-WEIGHT headers; surface as a metric
    - all requests HMAC-SHA256 signed with the secret; keys NEVER logged
"""

from __future__ import annotations

from decimal import Decimal
from typing import AsyncIterator

from sentinel.domain import Side

from ..adapter import BrokerEvent, BrokerOrderView

TESTNET_BASE = "https://testnet.binance.vision"
TESTNET_WS = "wss://stream.testnet.binance.vision"


class BinanceSpotAdapter:
    def __init__(
        self,
        api_key: str,
        api_secret: str,
        *,
        base_url: str = TESTNET_BASE,
        ws_url: str = TESTNET_WS,
    ) -> None:
        self._api_key = api_key
        self._api_secret = api_secret
        self._base_url = base_url
        self._ws_url = ws_url

    async def submit(
        self,
        *,
        client_order_id: str,
        instrument: str,
        side: Side,
        qty: Decimal,
        limit_price: Decimal | None,
    ) -> str:
        raise NotImplementedError("binance integration: next milestone")

    async def cancel(self, client_order_id: str) -> None:
        raise NotImplementedError("binance integration: next milestone")

    async def query_order(self, client_order_id: str) -> BrokerOrderView | None:
        raise NotImplementedError("binance integration: next milestone")

    async def query_positions(self) -> dict[str, Decimal]:
        raise NotImplementedError("binance integration: next milestone")

    def events(self) -> AsyncIterator[BrokerEvent]:
        raise NotImplementedError("binance integration: next milestone")
