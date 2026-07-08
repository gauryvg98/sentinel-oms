"""Binance FUTURES adapter unit tests — httpx.MockTransport, zero network.

Same contract teeth as spot (timeout != rejection, absence needs -2013), plus
the futures-specific bits: /fapi paths, one-way position mode + leverage set on
first submit, and ORDER_TRADE_UPDATE / ACCOUNT_UPDATE event parsing.
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
from sentinel.broker.binance import BinanceFuturesAdapter, parse_futures_event
from sentinel.domain import Side

KEY, SECRET = "test-key", "test-secret"
TIME = {"serverTime": 1_700_000_000_000}


def adapter(handler) -> BinanceFuturesAdapter:
    return BinanceFuturesAdapter(
        KEY, SECRET, symbols=("BTCUSDT",),
        transport=httpx.MockTransport(handler))


def route(request):
    url = urlparse(str(request.url))
    return request.method, url.path, {k: v[0] for k, v in parse_qs(url.query).items()}


def ok(body):
    return httpx.Response(200, json=body)


def err(status, code, msg="err"):
    return httpx.Response(status, json={"code": code, "msg": msg})


# --------------------------------------------------------------- submit

async def test_submit_sets_oneway_mode_and_leverage_then_places_limit():
    seen = {"leverage": None, "dual": None, "order": None}

    def handler(request):
        method, path, params = route(request)
        if path == "/fapi/v1/time":
            return ok(TIME)
        if path == "/fapi/v1/positionSide/dual":
            seen["dual"] = params.get("dualSidePosition"); return ok({"code": 200})
        if path == "/fapi/v1/leverage":
            seen["leverage"] = params.get("leverage"); return ok({"leverage": 1})
        assert (method, path) == ("POST", "/fapi/v1/order")
        seen["order"] = params
        return ok({"orderId": 99, "status": "NEW"})

    a = adapter(handler)
    bid = await a.submit(client_order_id="K1", instrument="BTCUSDT", side=Side.SELL,
                         qty=Decimal("0.01"), limit_price=Decimal("60000"))
    assert bid == "99"
    assert seen["dual"] == "false" and seen["leverage"] == "1"   # one-way, 1x
    assert seen["order"]["side"] == "SELL"                        # SELL opens a short
    assert seen["order"]["type"] == "LIMIT" and seen["order"]["price"] == "60000"
    assert "signature" in seen["order"]


async def test_submit_stop_price_maps_to_reduce_only_stop_market():
    """The hard-stop backstop on the wire: stop_price set + limit None ->
    type=STOP_MARKET with stopPrice and reduceOnly=true, and NO limit price."""
    seen = {}

    def handler(request):
        method, path, params = route(request)
        if path == "/fapi/v1/time":
            return ok(TIME)
        if path in ("/fapi/v1/positionSide/dual", "/fapi/v1/leverage"):
            return ok({})
        assert (method, path) == ("POST", "/fapi/v1/order")
        seen["order"] = params
        return ok({"orderId": 7, "status": "NEW"})

    bid = await adapter(handler).submit(
        client_order_id="BS1", instrument="BTCUSDT", side=Side.SELL,
        qty=Decimal("0.078"), limit_price=None, stop_price=Decimal("59000.5"))
    assert bid == "7"
    o = seen["order"]
    assert o["type"] == "STOP_MARKET"
    assert o["stopPrice"] == "59000.5"
    assert o["reduceOnly"] == "true"                  # can only shrink the position
    assert o["quantity"] == "0.078" and o["side"] == "SELL"
    assert "price" not in o and "timeInForce" not in o


async def test_open_positions_carries_liq_and_mark():
    """positionRisk -> BrokerPosition with signed qty, entry, liquidation and
    mark. A zero liquidationPrice (cross, no liq) becomes None; flat rows are
    dropped."""
    def handler(request):
        _, path, _ = route(request)
        if path == "/fapi/v1/time":
            return ok(TIME)
        assert path == "/fapi/v2/positionRisk"
        return ok([
            {"symbol": "BTCUSDT", "positionAmt": "-0.078", "entryPrice": "63600",
             "markPrice": "63590.9", "liquidationPrice": "126684.14"},
            {"symbol": "SOLUSDC", "positionAmt": "0", "entryPrice": "0",
             "markPrice": "81.4", "liquidationPrice": "0"},          # flat -> dropped
        ])

    a = adapter(handler)
    pos = await a.open_positions()
    assert set(pos) == {"BTCUSDT"}                    # flat row excluded
    btc = pos["BTCUSDT"]
    assert btc.qty == Decimal("-0.078") and btc.entry_price == Decimal("63600")
    assert btc.liq_price == Decimal("126684.14")
    assert btc.mark_price == Decimal("63590.9")


async def test_open_positions_zero_liq_becomes_none():
    def handler(request):
        _, path, _ = route(request)
        if path == "/fapi/v1/time":
            return ok(TIME)
        return ok([{"symbol": "BTCUSDT", "positionAmt": "1", "entryPrice": "100",
                    "markPrice": "101", "liquidationPrice": "0"}])
    pos = await adapter(handler).open_positions()
    assert pos["BTCUSDT"].liq_price is None            # 0 = no liq, not price 0
    assert pos["BTCUSDT"].mark_price == Decimal("101")


async def test_submit_4xx_is_reject_5xx_is_timeout():
    def h4(request):
        _, path, _ = route(request)
        if path == "/fapi/v1/time":
            return ok(TIME)
        if path in ("/fapi/v1/positionSide/dual", "/fapi/v1/leverage"):
            return ok({})
        return err(400, -2019, "margin is insufficient")

    with pytest.raises(BrokerReject, match="-2019"):
        await adapter(h4).submit(client_order_id="K1", instrument="BTCUSDT",
                                 side=Side.SELL, qty=Decimal("1"), limit_price=None)

    def h5(request):
        _, path, _ = route(request)
        if path == "/fapi/v1/time":
            return ok(TIME)
        if path in ("/fapi/v1/positionSide/dual", "/fapi/v1/leverage"):
            return ok({})
        return httpx.Response(503)

    with pytest.raises(BrokerTimeout):
        await adapter(h5).submit(client_order_id="K1", instrument="BTCUSDT",
                                 side=Side.SELL, qty=Decimal("1"), limit_price=None)


# --------------------------------------------------------------- query

async def test_query_order_absent_is_none():
    def handler(request):
        _, path, _ = route(request)
        if path == "/fapi/v1/time":
            return ok(TIME)
        return err(400, -2013, "Order does not exist")

    assert await adapter(handler).query_order("nope") is None


async def test_query_order_reports_short_fill():
    def handler(request):
        _, path, _ = route(request)
        if path == "/fapi/v1/time":
            return ok(TIME)
        if path == "/fapi/v1/order":
            return ok({"orderId": 5, "status": "FILLED", "executedQty": "0.01"})
        if path == "/fapi/v1/userTrades":
            return ok([{"id": 7, "qty": "0.01", "price": "60000"}])
        return err(400, 0)

    v = await adapter(handler).query_order("K1")
    assert v.state is BrokerOrderState.FILLED and v.filled_qty == Decimal("0.01")
    assert v.fills[0].exec_id == "7"


# --------------------------------------------------------------- event parsing

def test_parse_order_trade_update_fill():
    ev = parse_futures_event({"e": "ORDER_TRADE_UPDATE", "o": {
        "c": "K1", "x": "TRADE", "t": 42, "l": "0.01", "L": "60000", "S": "SELL"}})
    assert isinstance(ev, BrokerFill)
    assert ev.client_order_id == "K1" and ev.exec_id == "42"
    assert ev.qty == Decimal("0.01") and ev.price == Decimal("60000")


def test_parse_order_trade_update_cancel():
    ev = parse_futures_event({"e": "ORDER_TRADE_UPDATE",
                              "o": {"c": "K1", "x": "CANCELED"}})
    assert isinstance(ev, BrokerCancelConfirmed) and ev.client_order_id == "K1"


def test_parse_stp_expired_in_match_is_a_cancel():
    # Self-trade prevention expiry — terminal, no residual. Must resolve like a
    # cancel, not crash recovery with an unmapped status (KeyError halted boot).
    from sentinel.broker.binance.futures import _STATUS_MAP
    from sentinel.broker.adapter import BrokerOrderState
    assert _STATUS_MAP["EXPIRED_IN_MATCH"] is BrokerOrderState.CANCELED
    ev = parse_futures_event({"e": "ORDER_TRADE_UPDATE",
                              "o": {"c": "K1", "x": "EXPIRED_IN_MATCH"}})
    assert isinstance(ev, BrokerCancelConfirmed) and ev.client_order_id == "K1"


def test_parse_account_update_balances():
    ev = parse_futures_event({"e": "ACCOUNT_UPDATE", "a": {
        "B": [{"a": "USDT", "wb": "9500.5"}]}})
    assert isinstance(ev, BrokerBalanceUpdate)
    assert ev.balances["USDT"] == Decimal("9500.5")


def test_parse_ignores_other_events():
    assert parse_futures_event({"e": "listenKeyExpired"}) is None
    assert parse_futures_event({"e": "ORDER_TRADE_UPDATE",
                                "o": {"c": "K1", "x": "NEW"}}) is None
