"""Delta Exchange India adapter unit tests — httpx.MockTransport, zero network.

Proves the contract semantics on Delta's shape: success/error envelopes, 5xx =
timeout, the "not found" family = absence ONLY after the terminal-history sweep,
the api-key/timestamp/signature headers, orders / v2/user_trades / margins
stream parsing, and — the venue's sharp edge — the exact integer-contract <->
decimal-base-qty conversion in both directions.
"""

from __future__ import annotations

import hashlib
import hmac
from decimal import Decimal
from urllib.parse import urlparse

import httpx
import pytest

from sentinel.broker import (
    BrokerBalanceUpdate,
    BrokerCancelConfirmed,
    BrokerError,
    BrokerFill,
    BrokerOrderState,
    BrokerReject,
    BrokerTimeout,
)
from sentinel.broker.delta import DeltaFuturesAdapter, parse_delta_events
from sentinel.domain import Side

KEY, SECRET = "test-key", "test-secret"

# BTCUSD @ 0.001 BTC/contract — the mapping every size in these tests exercises.
PRODUCTS = {"success": True, "result": [
    {"id": 27, "symbol": "BTCUSD", "contract_value": "0.001",
     "tick_size": "0.5", "state": "live"},
]}
CV = {"BTCUSD": Decimal("0.001")}


def adapter(handler) -> DeltaFuturesAdapter:
    return DeltaFuturesAdapter(KEY, SECRET, symbols=("BTCUSD",),
                               transport=httpx.MockTransport(handler))


def ok(result):
    return httpx.Response(200, json={"success": True, "result": result})


def err(code, status=400):
    return httpx.Response(status, json={"success": False,
                                        "error": {"code": code}})


def route(request, table):
    """Dispatch a mock request by path; /v2/products is always served so the
    lazy contract_value cache can load."""
    path = urlparse(str(request.url)).path
    if path == "/v2/products":
        return httpx.Response(200, json=PRODUCTS)
    handler = table.get(path)
    assert handler is not None, f"unexpected request: {request.method} {path}"
    return handler(request)


# --------------------------------------------------------------- signing

def test_signature_vector():
    """Known-answer test for the prehash method+timestamp+path+query+body —
    a wrong concatenation order would still 'work' locally but fail live."""
    a = DeltaFuturesAdapter(KEY, SECRET, symbols=("BTCUSD",))
    sig = a._signature("POST", "1700000000", "/v2/orders", "", '{"size":10}')
    assert sig == "433041d0ff445a0521b1def008cc339c960f02fc06c44200eadc4f2158beff57"


# --------------------------------------------------------------- submit

async def test_submit_short_limit_maps_contracts_signs_and_returns_id():
    seen = {}

    def handler(request):
        def orders(req):
            seen["body"] = req.content.decode()
            seen["hdr"] = dict(req.headers)
            return ok({"id": 12345, "client_order_id": "K1"})
        return route(request, {"/v2/orders": orders})

    a = adapter(handler)
    oid = await a.submit(client_order_id="K1", instrument="BTCUSD",
                         side=Side.SELL, qty=Decimal("0.010"),
                         limit_price=Decimal("60000"))
    assert oid == "12345"
    # 0.010 BTC at 0.001 BTC/contract = EXACTLY 10 contracts on the wire.
    assert '"size":10' in seen["body"] and '"side":"sell"' in seen["body"]
    assert '"order_type":"limit_order"' in seen["body"]
    assert '"limit_price":"60000"' in seen["body"]
    assert '"client_order_id":"K1"' in seen["body"]
    assert '"product_symbol":"BTCUSD"' in seen["body"]
    # Headers carry the key + a signature over POST{ts}/v2/orders{body}.
    assert seen["hdr"]["api-key"] == KEY
    expect = hmac.new(SECRET.encode(),
                      f"POST{seen['hdr']['timestamp']}/v2/orders"
                      f"{seen['body']}".encode(),
                      hashlib.sha256).hexdigest()
    assert seen["hdr"]["signature"] == expect


async def test_submit_reject_and_5xx_timeout():
    with pytest.raises(BrokerReject, match="insufficient_margin"):
        await adapter(lambda r: route(r, {
            "/v2/orders": lambda req: err("insufficient_margin")})).submit(
            client_order_id="K1", instrument="BTCUSD", side=Side.SELL,
            qty=Decimal("0.001"), limit_price=None)

    with pytest.raises(BrokerTimeout):
        await adapter(lambda r: route(r, {
            "/v2/orders": lambda req: httpx.Response(502)})).submit(
            client_order_id="K1", instrument="BTCUSD", side=Side.SELL,
            qty=Decimal("0.001"), limit_price=None)


async def test_submit_off_grid_qty_refused_locally_never_hits_the_wire():
    """0.0015 BTC is 1.5 contracts — rounding EITHER way silently changes real
    position size, so the adapter must refuse before the wire."""
    hits = []

    def handler(request):
        hits.append(urlparse(str(request.url)).path)
        return route(request, {})

    with pytest.raises(BrokerReject, match="contract_value"):
        await adapter(handler).submit(
            client_order_id="K1", instrument="BTCUSD", side=Side.BUY,
            qty=Decimal("0.0015"), limit_price=None)
    assert hits == ["/v2/products"]              # only the spec cache loaded


# -------------------------------------------------------------- leverage

async def test_submit_sets_leverage_once_per_symbol_before_the_order():
    """First submit on a symbol POSTs the per-product leverage endpoint (before
    the order) with the string body Delta expects; the second submit on the
    same symbol must NOT repeat it — one attempt per symbol, cached."""
    paths, lev_bodies = [], []

    def handler(request):
        p = urlparse(str(request.url)).path
        if p != "/v2/products":
            paths.append(p)

        def leverage(req):
            lev_bodies.append(req.content.decode())
            return ok({"leverage": "25", "product_id": 27})
        return route(request, {
            "/v2/products/27/orders/leverage": leverage,
            "/v2/orders": lambda r: ok({"id": 1, "client_order_id": "K"}),
        })

    a = DeltaFuturesAdapter(KEY, SECRET, symbols=("BTCUSD",), leverage=25,
                            transport=httpx.MockTransport(handler))
    await a.submit(client_order_id="K1", instrument="BTCUSD", side=Side.BUY,
                   qty=Decimal("0.001"), limit_price=None)
    await a.submit(client_order_id="K2", instrument="BTCUSD", side=Side.BUY,
                   qty=Decimal("0.001"), limit_price=None)
    assert paths == ["/v2/products/27/orders/leverage",
                     "/v2/orders", "/v2/orders"]
    assert lev_bodies == ['{"leverage":"25"}']


async def test_submit_survives_leverage_failure_and_never_retries_it():
    """A failing leverage call is best-effort: the order still goes out, and
    the symbol is marked done so the submit path never re-blocks on it."""
    lev_hits = []

    def handler(request):
        def leverage(req):
            lev_hits.append(1)
            return httpx.Response(500)          # transport-shaped failure
        return route(request, {
            "/v2/products/27/orders/leverage": leverage,
            "/v2/orders": lambda r: ok({"id": 9, "client_order_id": "K"}),
        })

    a = DeltaFuturesAdapter(KEY, SECRET, symbols=("BTCUSD",), leverage=25,
                            transport=httpx.MockTransport(handler))
    oid = await a.submit(client_order_id="K1", instrument="BTCUSD",
                         side=Side.BUY, qty=Decimal("0.001"), limit_price=None)
    assert oid == "9"                           # order unblocked
    await a.submit(client_order_id="K2", instrument="BTCUSD", side=Side.SELL,
                   qty=Decimal("0.001"), limit_price=None)
    assert lev_hits == [1]                      # one attempt, no retry


async def test_submit_without_leverage_never_touches_the_endpoint():
    """leverage=None (the default) leaves the account's product default alone
    — route() asserts on any unexpected path, so a stray call would fail."""
    a = adapter(lambda r: route(r, {
        "/v2/orders": lambda req: ok({"id": 3, "client_order_id": "K1"})}))
    assert await a.submit(client_order_id="K1", instrument="BTCUSD",
                          side=Side.BUY, qty=Decimal("0.001"),
                          limit_price=None) == "3"


# --------------------------------------------------------------- query

async def test_query_absent_only_after_open_and_history_both_miss():
    paths = []

    def handler(request):
        paths.append(urlparse(str(request.url)).path)
        return route(request, {
            "/v2/orders/client_order_id/nope": lambda r: err("not_found", 404),
            "/v2/orders/history": lambda r: ok([]),
        })

    assert await adapter(handler).query_order("nope") is None
    # Proof of absence required BOTH sweeps — the by-client-oid endpoint 404s
    # for terminal orders too, so it alone proves nothing.
    assert "/v2/orders/history" in paths


async def test_query_tolerates_non_json_404_body():
    def handler(request):
        return route(request, {
            "/v2/orders/client_order_id/nope":
                lambda r: httpx.Response(404, text="Not Found"),
            "/v2/orders/history": lambda r: ok([]),
        })

    assert await adapter(handler).query_order("nope") is None


async def test_query_5xx_raises_never_returns_none():
    """A network-shaped failure is NOT absence: None would let the reconciler
    resolve the order terminal on a hiccup (R1.6)."""
    def open_404(request):
        return route(request, {
            "/v2/orders/client_order_id/K1": lambda r: err("not_found", 404),
            "/v2/orders/history": lambda r: httpx.Response(500),
        })

    with pytest.raises(BrokerTimeout):
        await adapter(open_404).query_order("K1")

    def open_500(request):
        return route(request, {
            "/v2/orders/client_order_id/K1": lambda r: httpx.Response(503),
        })

    with pytest.raises(BrokerTimeout):
        await adapter(open_500).query_order("K1")


async def test_query_unexpected_error_code_raises():
    def handler(request):
        return route(request, {
            "/v2/orders/client_order_id/K1":
                lambda r: err("unauthorized", 401),
        })

    with pytest.raises(BrokerError):
        await adapter(handler).query_order("K1")


async def test_query_reports_fill_with_base_qty_and_stable_exec_ids():
    def handler(request):
        return route(request, {
            "/v2/orders/client_order_id/K1": lambda r: ok(
                {"id": 77, "client_order_id": "K1", "product_id": 27,
                 "product_symbol": "BTCUSD", "state": "closed",
                 "size": 10, "unfilled_size": 0}),
            # A foreign fill (order 78) rides along: the client-side order_id
            # filter must drop it even if the server ignored our query param.
            "/v2/fills": lambda r: ok([
                {"id": 9, "order_id": 77, "size": 10, "price": "60000"},
                {"id": 10, "order_id": 78, "size": 5, "price": "1"},
            ]),
        })

    v = await adapter(handler).query_order("K1")
    assert v.state is BrokerOrderState.FILLED
    assert v.broker_order_id == "77"
    assert v.filled_qty == Decimal("0.010")          # 10 contracts * 0.001
    assert len(v.fills) == 1
    assert v.fills[0].exec_id == "9" and v.fills[0].qty == Decimal("0.010")
    assert v.fills[0].price == Decimal("60000")


async def test_query_finds_terminal_order_in_history_sweep():
    def handler(request):
        return route(request, {
            "/v2/orders/client_order_id/K1": lambda r: err("not_found", 404),
            "/v2/orders/history": lambda r: ok([
                {"id": 5, "client_order_id": "other", "state": "closed",
                 "product_symbol": "BTCUSD", "size": 1, "unfilled_size": 0},
                {"id": 6, "client_order_id": "K1", "state": "cancelled",
                 "product_symbol": "BTCUSD", "size": 10, "unfilled_size": 10},
            ]),
        })

    v = await adapter(handler).query_order("K1")
    assert v.state is BrokerOrderState.CANCELED and v.broker_order_id == "6"
    assert v.filled_qty == Decimal("0")


async def test_query_partial_open_order_maps_to_partial():
    def handler(request):
        return route(request, {
            "/v2/orders/client_order_id/K1": lambda r: ok(
                {"id": 77, "client_order_id": "K1", "product_symbol": "BTCUSD",
                 "state": "open", "size": 10, "unfilled_size": 6}),
            "/v2/fills": lambda r: ok([
                {"id": 9, "order_id": 77, "size": 4, "price": "60000"}]),
        })

    v = await adapter(handler).query_order("K1")
    assert v.state is BrokerOrderState.PARTIAL
    assert v.filled_qty == Decimal("0.004")


# ------------------------------------------------- positions & balances

async def test_open_positions_converts_signed_contracts_to_base_qty():
    def handler(request):
        return route(request, {
            "/v2/positions/margined": lambda r: ok([
                {"product_id": 27, "product_symbol": "BTCUSD", "size": -5,
                 "entry_price": "60000", "liquidation_price": "65000"},
                {"product_id": 99, "product_symbol": "GONE", "size": 0,
                 "entry_price": "1"},                       # flat -> skipped
            ]),
        })

    pos = await adapter(handler).open_positions()
    assert set(pos) == {"BTCUSD"}
    assert pos["BTCUSD"].qty == Decimal("-0.005")           # short, signed
    assert pos["BTCUSD"].entry_price == Decimal("60000")
    assert pos["BTCUSD"].liq_price == Decimal("65000")


async def test_query_positions_returns_nonzero_wallet_balances():
    def handler(request):
        return route(request, {
            "/v2/wallet/balances": lambda r: ok([
                {"asset_symbol": "USDT", "balance": "9500.5"},
                {"asset_symbol": "BTC", "balance": "0"},
            ]),
        })

    bal = await adapter(handler).query_positions()
    assert bal == {"USDT": Decimal("9500.5")}


# --------------------------------------------------------- event parsing

def test_parse_user_trade_fill_converts_contracts_to_base_qty():
    evs = parse_delta_events(
        {"type": "v2/user_trades", "symbol": "BTCUSD", "fill_id": "f-1",
         "order_id": 77, "client_order_id": "K1", "side": "buy",
         "size": 2, "price": "60000"}, CV)
    assert len(evs) == 1 and isinstance(evs[0], BrokerFill)
    assert evs[0].client_order_id == "K1" and evs[0].exec_id == "f-1"
    assert evs[0].qty == Decimal("0.002")                   # 2 * 0.001
    assert evs[0].price == Decimal("60000")


def test_parse_skips_fill_on_unknown_symbol_rather_than_guessing_size():
    evs = parse_delta_events(
        {"type": "v2/user_trades", "symbol": "MYSTERY", "fill_id": "f-2",
         "size": 2, "price": "1"}, CV)
    assert evs == []


def test_parse_order_cancel_and_margins():
    c = parse_delta_events({"type": "orders", "client_order_id": "K1",
                            "state": "cancelled", "action": "delete"}, CV)
    assert isinstance(c[0], BrokerCancelConfirmed)
    assert c[0].client_order_id == "K1"

    still_open = parse_delta_events({"type": "orders", "client_order_id": "K1",
                                     "state": "open"}, CV)
    assert still_open == []

    m = parse_delta_events({"type": "margins", "asset_symbol": "USDT",
                            "balance": "9500.5",
                            "available_balance": "9000"}, CV)
    assert isinstance(m[0], BrokerBalanceUpdate)
    assert m[0].balances["USDT"] == Decimal("9500.5")


def test_parse_ignores_other_frames():
    assert parse_delta_events({"type": "v2/ticker", "symbol": "BTCUSD"}, CV) == []
    assert parse_delta_events({"type": "heartbeat", "ts": 1}, CV) == []
    assert parse_delta_events({"type": "subscriptions", "channels": []}, CV) == []


# ------------------------------------------------- contract conversion

async def test_contract_conversion_round_trips_both_directions():
    a = adapter(lambda r: route(r, {}))
    await a._ensure_products()
    assert a._to_contracts("BTCUSD", Decimal("0.010")) == 10
    assert a._to_qty("BTCUSD", 10) == Decimal("0.010")
    assert a._to_qty("BTCUSD", -5) == Decimal("-0.005")     # signed positions
    with pytest.raises(BrokerReject):
        a._to_contracts("BTCUSD", Decimal("0.0015"))        # 1.5 contracts
