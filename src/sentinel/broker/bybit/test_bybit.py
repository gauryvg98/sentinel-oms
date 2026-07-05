"""Bybit V5 adapter unit tests — httpx.MockTransport, zero network.

Proves the contract semantics on Bybit's shape: retCode 0 vs reject, 5xx =
timeout, "order not exists" (110001) = absence, the X-BAPI-* signature headers,
and execution/order/wallet stream parsing.
"""

from __future__ import annotations

from decimal import Decimal
from urllib.parse import parse_qs, urlparse

import httpx
import pytest

from sentinel.broker import (
    BrokerBalanceUpdate,
    BrokerCancelConfirmed,
    BrokerFill,
    BrokerOrderState,
    BrokerReject,
    BrokerTimeout,
)
from sentinel.broker.bybit import BybitFuturesAdapter, parse_bybit_events
from sentinel.domain import Side

KEY, SECRET = "test-key", "test-secret"


def adapter(handler) -> BybitFuturesAdapter:
    return BybitFuturesAdapter(KEY, SECRET, symbols=("BTCUSDT",),
                               transport=httpx.MockTransport(handler))


def ok(result, code=0):
    return httpx.Response(200, json={"retCode": code, "retMsg": "OK", "result": result})


# --------------------------------------------------------------- submit

async def test_submit_short_limit_maps_params_signs_and_returns_id():
    seen = {}

    def handler(request):
        seen["path"] = urlparse(str(request.url)).path
        seen["body"] = request.content.decode()
        seen["hdr"] = dict(request.headers)
        return ok({"orderId": "abc123", "orderLinkId": "K1"})

    a = adapter(handler)
    oid = await a.submit(client_order_id="K1", instrument="BTCUSDT", side=Side.SELL,
                         qty=Decimal("0.01"), limit_price=Decimal("60000"))
    assert oid == "abc123" and seen["path"] == "/v5/order/create"
    assert '"side":"Sell"' in seen["body"] and '"orderType":"Limit"' in seen["body"]
    assert '"orderLinkId":"K1"' in seen["body"] and '"price":"60000"' in seen["body"]
    assert seen["hdr"]["x-bapi-api-key"] == KEY and "x-bapi-sign" in seen["hdr"]


async def test_submit_reject_and_5xx_timeout():
    with pytest.raises(BrokerReject, match="110007"):
        await adapter(lambda r: httpx.Response(200, json={
            "retCode": 110007, "retMsg": "insufficient balance"})).submit(
            client_order_id="K1", instrument="BTCUSDT", side=Side.SELL,
            qty=Decimal("1"), limit_price=None)

    with pytest.raises(BrokerTimeout):
        await adapter(lambda r: httpx.Response(502)).submit(
            client_order_id="K1", instrument="BTCUSDT", side=Side.SELL,
            qty=Decimal("1"), limit_price=None)


# --------------------------------------------------------------- query

async def test_query_absent_when_not_in_realtime_or_history():
    def handler(request):
        return httpx.Response(200, json={"retCode": 110001, "retMsg": "order not exists"})

    assert await adapter(handler).query_order("nope") is None


async def test_query_reports_short_fill_from_execution():
    def handler(request):
        path = urlparse(str(request.url)).path
        if path == "/v5/order/realtime":
            return ok({"list": [{"orderId": "o1", "orderStatus": "Filled",
                                 "cumExecQty": "0.01"}]})
        if path == "/v5/execution/list":
            return ok({"list": [{"execId": "e9", "execQty": "0.01",
                                 "execPrice": "60000", "execType": "Trade"}]})
        return ok({"list": []})

    v = await adapter(handler).query_order("K1")
    assert v.state is BrokerOrderState.FILLED and v.filled_qty == Decimal("0.01")
    assert v.fills[0].exec_id == "e9" and v.fills[0].price == Decimal("60000")


# --------------------------------------------------------------- event parsing

def test_parse_execution_fill():
    evs = parse_bybit_events({"topic": "execution", "data": [
        {"orderLinkId": "K1", "execId": "7", "execQty": "0.01", "execPrice": "60000",
         "execType": "Trade"}]})
    assert len(evs) == 1 and isinstance(evs[0], BrokerFill)
    assert evs[0].client_order_id == "K1" and evs[0].qty == Decimal("0.01")


def test_parse_skips_non_trade_executions():
    evs = parse_bybit_events({"topic": "execution", "data": [
        {"orderLinkId": "K1", "execId": "8", "execQty": "0.01", "execPrice": "1",
         "execType": "Funding"}]})
    assert evs == []


def test_parse_order_cancel_and_wallet():
    c = parse_bybit_events({"topic": "order", "data": [
        {"orderLinkId": "K1", "orderStatus": "Cancelled"}]})
    assert isinstance(c[0], BrokerCancelConfirmed) and c[0].client_order_id == "K1"

    w = parse_bybit_events({"topic": "wallet", "data": [
        {"coin": [{"coin": "USDT", "walletBalance": "9500.5"}]}]})
    assert isinstance(w[0], BrokerBalanceUpdate)
    assert w[0].balances["USDT"] == Decimal("9500.5")


def test_parse_ignores_other_topics():
    assert parse_bybit_events({"topic": "greeks", "data": []}) == []
    assert parse_bybit_events({"op": "pong", "success": True}) == []
