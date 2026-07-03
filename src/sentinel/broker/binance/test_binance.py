"""Binance adapter unit tests — httpx.MockTransport, zero network.

What these prove: the CONTRACT semantics, which is where broker adapters go
wrong — timeout is never rejection, absence requires code -2013, signatures
are byte-exact, decimals never go scientific.
"""

from __future__ import annotations

import hashlib
import hmac as hmac_lib
import json
from decimal import Decimal
from urllib.parse import parse_qs, urlparse

import httpx
import pytest

from sentinel.broker import (
    BrokerCancelConfirmed,
    BrokerError,
    BrokerFill,
    BrokerOrderState,
    BrokerReject,
    BrokerTimeout,
)
from sentinel.broker.binance import BinanceSpotAdapter, parse_user_event
from sentinel.broker.binance.signing import fmt_decimal, signed_query
from sentinel.domain import Side

KEY, SECRET = "test-key", "test-secret"


def adapter(handler) -> BinanceSpotAdapter:
    return BinanceSpotAdapter(
        KEY, SECRET, symbols=("BTCUSDT", "ETHUSDT"),
        transport=httpx.MockTransport(handler),
    )


def route(request: httpx.Request) -> tuple[str, str, dict]:
    url = urlparse(str(request.url))
    return request.method, url.path, {k: v[0] for k, v in parse_qs(url.query).items()}


def ok(body) -> httpx.Response:
    return httpx.Response(200, json=body)


def err(status: int, code: int, msg: str = "err") -> httpx.Response:
    return httpx.Response(status, json={"code": code, "msg": msg})


TIME = {"serverTime": 1_700_000_000_000}


# ------------------------------------------------------------------ signing


def test_signature_is_byte_exact():
    qs = signed_query({"symbol": "BTCUSDT", "side": "BUY"}, SECRET,
                      timestamp_ms=1_700_000_000_000)
    base, _, sig = qs.partition("&signature=")
    expected = hmac_lib.new(SECRET.encode(), base.encode(),
                            hashlib.sha256).hexdigest()
    assert sig == expected
    assert "timestamp=1700000000000" in base


def test_none_params_are_omitted_from_signature():
    qs = signed_query({"a": "1", "b": None}, SECRET, timestamp_ms=1)
    assert "b=" not in qs


def test_decimals_never_go_scientific():
    assert fmt_decimal(Decimal("0.00000500")) == "0.000005"
    assert fmt_decimal(Decimal("1E-7")) == "0.0000001"
    assert fmt_decimal(Decimal("100.10")) == "100.1"


# ------------------------------------------------------------------- submit


async def test_submit_limit_maps_params_and_returns_order_id():
    seen = {}

    def handler(request):
        method, path, params = route(request)
        if path == "/api/v3/time":
            return ok(TIME)
        assert (method, path) == ("POST", "/api/v3/order")
        seen.update(params)
        return ok({"orderId": 4321, "status": "NEW"})

    a = adapter(handler)
    broker_id = await a.submit(
        client_order_id="K1", instrument="BTCUSDT", side=Side.BUY,
        qty=Decimal("0.001"), limit_price=Decimal("43000.5"),
    )
    assert broker_id == "4321"
    assert seen["type"] == "LIMIT" and seen["timeInForce"] == "GTC"
    assert seen["price"] == "43000.5" and seen["quantity"] == "0.001"
    assert seen["newClientOrderId"] == "K1"
    assert "signature" in seen


async def test_submit_market_when_no_limit_price():
    def handler(request):
        _, path, params = route(request)
        if path == "/api/v3/time":
            return ok(TIME)
        assert params["type"] == "MARKET" and "price" not in params
        assert request.headers["X-MBX-APIKEY"] == KEY
        return ok({"orderId": 1})

    await adapter(handler).submit(
        client_order_id="K1", instrument="BTCUSDT", side=Side.SELL,
        qty=Decimal("0.001"), limit_price=None,
    )


async def test_submit_4xx_is_conclusive_reject():
    def handler(request):
        _, path, _ = route(request)
        return ok(TIME) if path == "/api/v3/time" else err(400, -2010,
                                                           "insufficient balance")

    with pytest.raises(BrokerReject, match="-2010"):
        await adapter(handler).submit(
            client_order_id="K1", instrument="BTCUSDT", side=Side.BUY,
            qty=Decimal("1"), limit_price=None,
        )


async def test_submit_5xx_is_unprovable_timeout():
    def handler(request):
        _, path, _ = route(request)
        return ok(TIME) if path == "/api/v3/time" else httpx.Response(502)

    with pytest.raises(BrokerTimeout):
        await adapter(handler).submit(
            client_order_id="K1", instrument="BTCUSDT", side=Side.BUY,
            qty=Decimal("1"), limit_price=None,
        )


async def test_submit_transport_timeout_is_broker_timeout():
    def handler(request):
        _, path, _ = route(request)
        if path == "/api/v3/time":
            return ok(TIME)
        raise httpx.ConnectTimeout("boom", request=request)

    with pytest.raises(BrokerTimeout):
        await adapter(handler).submit(
            client_order_id="K1", instrument="BTCUSDT", side=Side.BUY,
            qty=Decimal("1"), limit_price=None,
        )


# -------------------------------------------------------------------- query


async def test_query_absent_only_on_2013():
    def handler(request):
        _, path, _ = route(request)
        if path == "/api/v3/time":
            return ok(TIME)
        return err(400, -2013, "Order does not exist")

    # -2013 in every configured symbol -> conclusively absent -> None
    assert await adapter(handler).query_order("GHOST") is None


async def test_query_transport_error_never_means_absent():
    def handler(request):
        _, path, _ = route(request)
        if path == "/api/v3/time":
            return ok(TIME)
        return httpx.Response(500, text="exchange sad")

    with pytest.raises(BrokerError):
        await adapter(handler).query_order("K1")


async def test_query_found_maps_status_and_backfills_trades():
    def handler(request):
        _, path, params = route(request)
        if path == "/api/v3/time":
            return ok(TIME)
        if path == "/api/v3/order":
            if params["symbol"] != "ETHUSDT":       # symbol scan: miss then hit
                return err(400, -2013, "no")
            return ok({"orderId": 99, "status": "PARTIALLY_FILLED",
                       "executedQty": "0.5"})
        if path == "/api/v3/myTrades":
            assert params["orderId"] == "99"
            return ok([
                {"id": 7001, "qty": "0.3", "price": "2000.0"},
                {"id": 7002, "qty": "0.2", "price": "2001.0"},
            ])
        raise AssertionError(path)

    view = await adapter(handler).query_order("K1")
    assert view.state is BrokerOrderState.PARTIAL
    assert view.broker_order_id == "99"
    assert view.filled_qty == Decimal("0.5")
    assert [f.exec_id for f in view.fills] == ["7001", "7002"]
    assert all(f.client_order_id == "K1" for f in view.fills)


async def test_pending_cancel_maps_to_working():
    """A pending cancel is NOT canceled: the order is still live and can
    still fill. Mapping it to CANCELED would fake conclusiveness."""
    def handler(request):
        _, path, _ = route(request)
        if path == "/api/v3/time":
            return ok(TIME)
        return ok({"orderId": 1, "status": "PENDING_CANCEL", "executedQty": "0"})

    view = await adapter(handler).query_order("K1")
    assert view.state is BrokerOrderState.WORKING


# ------------------------------------------------------------------- cancel


async def test_cancel_unknown_order_demands_reconciliation():
    def handler(request):
        method, path, _ = route(request)
        if path == "/api/v3/time":
            return ok(TIME)
        if method == "GET":   # symbol resolution path
            return ok({"orderId": 1, "status": "NEW", "executedQty": "0"})
        return err(400, -2011, "Unknown order sent")

    with pytest.raises(BrokerTimeout, match="reconcile"):
        await adapter(handler).cancel("K1")


# ------------------------------------------------------------- user stream


def test_parse_trade_event():
    event = parse_user_event({
        "e": "executionReport", "x": "TRADE", "c": "K1",
        "t": 8801, "l": "0.001", "L": "43000.5",
    })
    assert isinstance(event, BrokerFill)
    assert event.exec_id == "8801" and event.client_order_id == "K1"
    assert event.qty == Decimal("0.001") and event.price == Decimal("43000.5")


def test_parse_cancel_uses_original_client_id():
    event = parse_user_event({
        "e": "executionReport", "x": "CANCELED",
        "c": "cancel-req-id", "C": "K1",
    })
    assert isinstance(event, BrokerCancelConfirmed)
    assert event.client_order_id == "K1"


def test_parse_ignores_everything_else():
    assert parse_user_event({"e": "outboundAccountPosition"}) is None
    assert parse_user_event({"e": "executionReport", "x": "NEW"}) is None


def test_ws_auth_signature_is_computed_over_sorted_params():
    from sentinel.broker.binance.signing import ws_auth_params

    params = ws_auth_params(KEY, SECRET, timestamp_ms=1_700_000_000_000)
    payload = f"apiKey={KEY}&timestamp=1700000000000"  # alphabetical order
    expected = hmac_lib.new(SECRET.encode(), payload.encode(),
                            hashlib.sha256).hexdigest()
    assert params["signature"] == expected
    assert set(params) == {"apiKey", "timestamp", "signature"}
