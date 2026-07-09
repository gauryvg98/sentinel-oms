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


def adapter(handler, *, leverage: int = 1) -> BinanceFuturesAdapter:
    return BinanceFuturesAdapter(
        KEY, SECRET, symbols=("BTCUSDT",), leverage=leverage,
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


# ------------------------------------------------- leverage brackets (-2027)

# Real-ish BTCUSDT bracket ladder: ascending tiers, each capping the position
# NOTIONAL allowed at (and below) its initialLeverage ceiling.
_BRACKETS = [
    {"bracket": 1, "initialLeverage": 125, "notionalCap": 50000},
    {"bracket": 2, "initialLeverage": 100, "notionalCap": 25000},
    {"bracket": 3, "initialLeverage": 50, "notionalCap": 250000},
    {"bracket": 4, "initialLeverage": 20, "notionalCap": 1000000},
    {"bracket": 5, "initialLeverage": 10, "notionalCap": 5000000},
]


def _bracket_handler(seen=None):
    def handler(request):
        method, path, params = route(request)
        if path == "/fapi/v1/time":
            return ok(TIME)
        assert (method, path) == ("GET", "/fapi/v1/leverageBracket")
        if seen is not None:
            seen.append(params.get("symbol"))
        return ok([{"symbol": "BTCUSDT", "brackets": _BRACKETS}])
    return handler


async def test_max_notional_selects_lowest_tier_covering_leverage():
    # At 100x, the binding tier is the LOWEST-tier bracket whose initialLeverage
    # >= 100 -> the 100x tier, cap 25000.
    a = adapter(_bracket_handler(), leverage=100)
    assert await a.max_notional("BTCUSDT") == Decimal("25000")


async def test_max_notional_at_lower_leverage_gets_higher_cap():
    # At 50x the binding tier is initialLeverage 50 -> cap 250000 (more notional
    # allowed at lower leverage).
    a = adapter(_bracket_handler(), leverage=50)
    assert await a.max_notional("BTCUSDT") == Decimal("250000")


async def test_max_notional_caches_after_first_fetch():
    seen: list = []
    a = adapter(_bracket_handler(seen), leverage=100)
    assert await a.max_notional("BTCUSDT") == Decimal("25000")
    assert await a.max_notional("BTCUSDT") == Decimal("25000")
    assert seen.count("BTCUSDT") == 1              # fetched once, then cached


async def test_max_notional_fail_open_returns_none_and_is_retried():
    # A fetch failure returns None (no clamp, existing behavior) and is NOT
    # cached as known — the next call retries and can succeed.
    calls = {"n": 0}

    def handler(request):
        _, path, _ = route(request)
        if path == "/fapi/v1/time":
            return ok(TIME)
        calls["n"] += 1
        if calls["n"] == 1:
            return err(400, -1000, "boom")         # first call fails
        return ok([{"symbol": "BTCUSDT", "brackets": _BRACKETS}])

    a = adapter(handler, leverage=100)
    assert await a.max_notional("BTCUSDT") is None  # fail-open
    assert await a.max_notional("BTCUSDT") == Decimal("25000")  # retried, cached


# ------------------------------------------------ availableBalance (-2019)

# /fapi/v2/balance shape: per-asset wallet balance AND availableBalance (the
# exchange's REAL free margin, already net of margin/orders/unrealized loss).
_BALANCE = [
    {"asset": "USDT", "balance": "9800.0", "availableBalance": "4200.5"},
    {"asset": "USDC", "balance": "500.0", "availableBalance": "500.0"},
]


async def test_available_balance_parses_per_asset():
    a = adapter(lambda r: ok(TIME) if urlparse(str(r.url)).path == "/fapi/v1/time"
                else ok(_BALANCE))
    assert await a.available_balance("USDT") == Decimal("4200.5")
    # A second asset from the SAME payload resolves without a refetch.
    assert await a.available_balance("USDC") == Decimal("500.0")


async def test_available_balance_ttl_cache_second_call_within_ttl_no_refetch(monkeypatch):
    import sentinel.broker.binance.futures as fut

    clock = {"t": 1000.0}
    monkeypatch.setattr(fut.time, "monotonic", lambda: clock["t"])

    calls = {"n": 0}

    def handler(request):
        path = urlparse(str(request.url)).path
        if path == "/fapi/v1/time":
            return ok(TIME)
        assert path == "/fapi/v2/balance"
        calls["n"] += 1
        return ok(_BALANCE)

    a = adapter(handler)
    assert await a.available_balance("USDT") == Decimal("4200.5")
    # Within the TTL: served from cache, NO second /fapi/v2/balance hit.
    clock["t"] += fut._AVAIL_TTL_S - 0.1
    assert await a.available_balance("USDT") == Decimal("4200.5")
    assert calls["n"] == 1
    # Past the TTL: the memo expired -> exactly one refetch.
    clock["t"] += 1.0
    assert await a.available_balance("USDT") == Decimal("4200.5")
    assert calls["n"] == 2


async def test_available_balance_fail_open_returns_none_not_cached():
    calls = {"n": 0}

    def handler(request):
        path = urlparse(str(request.url)).path
        if path == "/fapi/v1/time":
            return ok(TIME)
        calls["n"] += 1
        if calls["n"] == 1:
            return err(400, -1000, "boom")      # first call fails
        return ok(_BALANCE)

    a = adapter(handler)
    assert await a.available_balance("USDT") is None          # fail-open, no clamp
    assert await a.available_balance("USDT") == Decimal("4200.5")  # retried, works


async def test_available_balance_unknown_asset_is_none():
    a = adapter(lambda r: ok(TIME) if urlparse(str(r.url)).path == "/fapi/v1/time"
                else ok(_BALANCE))
    assert await a.available_balance("DOGE") is None          # not in the payload


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
