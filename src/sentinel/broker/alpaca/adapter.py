"""AlpacaPaperAdapter — the BrokerAdapter contract mapped to Alpaca paper.

FOUNDATION ONLY: mapping documented, methods raise until the integration
milestone. Paper base: https://paper-api.alpaca.markets (free account keys).
US equities + options (paper); market hours apply.

Contract mapping
================
submit()
    POST /v2/orders with client_order_id = ours.
    - 200 with id             -> return id
    - 403/422 (rejects)       -> BrokerReject(reason from body)
    - timeout / 5xx           -> BrokerTimeout (no retry here; recon owns it)

query_order()   (the reconciliation primitive)
    GET /v2/orders:by_client_order_id?client_order_id=..
    - found -> BrokerOrderView (status map below; filled_qty from filled_qty;
      per-fill detail is NOT itemized by this endpoint — synthesize a single
      backfill exec as f"{id}-agg-{filled_qty}" or pull activities:
      GET /v2/account/activities/FILL for true per-exec ids)
    - 404 -> None (conclusively absent)
    status map: new/accepted->WORKING, partially_filled->PARTIAL,
    filled->FILLED, canceled/expired/done_for_day->CANCELED,
    rejected->REJECTED, pending_cancel->WORKING (cancel unconfirmed!)

cancel()
    DELETE /v2/orders/{id} — 204 means REQUEST accepted; the terminal cancel
    arrives via the trade_updates stream.

events()
    wss://paper-api.alpaca.markets/stream, subscribe trade_updates.
    fill/partial_fill -> BrokerFill(exec_id=event execution_id); canceled ->
    BrokerCancelConfirmed. At-least-once across reconnects (ledger dedups).

query_positions()
    GET /v2/positions -> qty per symbol (options symbology: OCC 21-char).
"""

from __future__ import annotations

from decimal import Decimal
from typing import AsyncIterator

from sentinel.domain import Side

from ..adapter import BrokerEvent, BrokerOrderView

PAPER_BASE = "https://paper-api.alpaca.markets"


class AlpacaPaperAdapter:
    def __init__(self, api_key: str, api_secret: str, *,
                 base_url: str = PAPER_BASE) -> None:
        self._api_key = api_key
        self._api_secret = api_secret
        self._base_url = base_url

    async def submit(
        self, *, client_order_id: str, instrument: str, side: Side,
        qty: Decimal, limit_price: Decimal | None,
    ) -> str:
        raise NotImplementedError("alpaca integration: later milestone")

    async def cancel(self, client_order_id: str) -> None:
        raise NotImplementedError("alpaca integration: later milestone")

    async def query_order(self, client_order_id: str) -> BrokerOrderView | None:
        raise NotImplementedError("alpaca integration: later milestone")

    async def query_positions(self) -> dict[str, Decimal]:
        raise NotImplementedError("alpaca integration: later milestone")

    def events(self) -> AsyncIterator[BrokerEvent]:
        raise NotImplementedError("alpaca integration: later milestone")
