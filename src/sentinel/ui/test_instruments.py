"""InstrumentSpec parser + quantization tests — captured exchange payloads,
zero network. Proves BTCUSDT and ETHUSDT get their OWN rules (no shared magic
numbers) and that quantization/eligibility honor them."""

from __future__ import annotations

from decimal import Decimal

import httpx

from sentinel.ui.instruments import (
    InstrumentSpec,
    fetch_binance_spec,
    fetch_bybit_spec,
    fetch_delta_spec,
    parse_binance_spec,
    parse_bybit_spec,
    parse_delta_spec,
)

# ---- captured payloads (trimmed to the filters we read) --------------------

BINANCE_FUT_BTC = {
    "symbol": "BTCUSDT",
    "marginAsset": "USDT",
    "filters": [
        {"filterType": "PRICE_FILTER", "tickSize": "0.10"},
        {"filterType": "LOT_SIZE", "stepSize": "0.001", "minQty": "0.001"},
        {"filterType": "MIN_NOTIONAL", "notional": "100"},
    ],
}
BINANCE_FUT_SOLUSDC = {
    "symbol": "SOLUSDC",
    "marginAsset": "USDC",
    "filters": [
        {"filterType": "PRICE_FILTER", "tickSize": "0.010"},
        {"filterType": "LOT_SIZE", "stepSize": "0.01", "minQty": "0.01"},
        {"filterType": "MIN_NOTIONAL", "notional": "5"},
    ],
}
BINANCE_SPOT_ETH = {
    "symbol": "ETHUSDT",
    "quoteAsset": "USDT",
    "filters": [
        {"filterType": "PRICE_FILTER", "tickSize": "0.01"},
        {"filterType": "LOT_SIZE", "stepSize": "0.0001", "minQty": "0.0001"},
        {"filterType": "NOTIONAL", "minNotional": "5"},
    ],
}
BYBIT_SOL = {
    "symbol": "SOLUSDT",
    "settleCoin": "USDT",
    "quoteCoin": "USDT",
    "lotSizeFilter": {"qtyStep": "0.1", "minOrderQty": "0.1",
                      "minNotionalValue": "5"},
    "priceFilter": {"tickSize": "0.010"},
}
DELTA_BTC = {
    "id": 27,
    "symbol": "BTCUSD",
    "contract_value": "0.001",
    "tick_size": "0.5",
    "settling_asset": {"symbol": "USDT"},
    "quoting_asset": {"symbol": "USD"},
    "state": "live",
}


# ---- parsing ---------------------------------------------------------------

def test_binance_futures_filters():
    s = parse_binance_spec(BINANCE_FUT_BTC)
    assert s.lot_step == Decimal("0.001") and s.price_tick == Decimal("0.10")
    assert s.min_qty == Decimal("0.001") and s.min_notional == Decimal("100")


def test_binance_spot_uses_notional_minNotional_shape():
    s = parse_binance_spec(BINANCE_SPOT_ETH)
    assert s.lot_step == Decimal("0.0001") and s.price_tick == Decimal("0.01")
    assert s.min_notional == Decimal("5")


def test_bybit_linear_filters():
    s = parse_bybit_spec(BYBIT_SOL)
    assert s.lot_step == Decimal("0.1") and s.price_tick == Decimal("0.010")
    assert s.min_qty == Decimal("0.1") and s.min_notional == Decimal("5")


def test_delta_contract_value_becomes_the_qty_grid():
    # Delta sizes orders in integer contracts of contract_value each, so the
    # base-qty grid (and the min) must be exactly contract_value — that is what
    # keeps sizing in base units from ever producing a fractional contract.
    s = parse_delta_spec(DELTA_BTC)
    assert s.lot_step == Decimal("0.001") and s.min_qty == Decimal("0.001")
    assert s.price_tick == Decimal("0.5")
    assert s.quote_asset == "USDT"                    # settling, not quoting
    assert s.round_qty(Decimal("0.00318")) == Decimal("0.003")


def test_two_symbols_get_distinct_rules():
    btc = parse_binance_spec(BINANCE_FUT_BTC)
    eth = parse_binance_spec(BINANCE_SPOT_ETH)
    assert btc.lot_step != eth.lot_step and btc.price_tick != eth.price_tick


def test_settlement_asset_parsed_per_symbol():
    # A USDC perp must carry USDC as its settlement asset (Binance marginAsset),
    # a USDT perp USDT — this is what confines sizing to the right margin pool.
    assert parse_binance_spec(BINANCE_FUT_BTC).quote_asset == "USDT"
    assert parse_binance_spec(BINANCE_FUT_SOLUSDC).quote_asset == "USDC"
    assert parse_binance_spec(BINANCE_SPOT_ETH).quote_asset == "USDT"   # spot: quoteAsset
    assert parse_bybit_spec(BYBIT_SOL).quote_asset == "USDT"


# ---- quantization / eligibility -------------------------------------------

def test_round_qty_floors_to_lot_step():
    s = parse_binance_spec(BINANCE_FUT_BTC)          # 0.001 step
    assert s.round_qty(Decimal("0.00318")) == Decimal("0.003")


def test_round_price_stays_maker_side():
    s = parse_binance_spec(BINANCE_FUT_BTC)          # 0.10 tick
    assert s.round_price(Decimal("62778.55"), "BUY") == Decimal("62778.50")
    assert s.round_price(Decimal("62778.51"), "SELL") == Decimal("62778.60")


def test_round_snaps_to_non_power_of_ten_steps():
    # A symbol whose tick is 0.5 and lot is 2.5 — Decimal.quantize would
    # mangle these; _snap must land on real multiples.
    s = InstrumentSpec("XYZ", lot_step=Decimal("2.5"), price_tick=Decimal("0.5"),
                       min_qty=Decimal("2.5"), min_notional=Decimal("0"))
    assert s.round_qty(Decimal("11")) == Decimal("10.0")        # 4 * 2.5
    assert s.round_price(Decimal("100.3"), "BUY") == Decimal("100.0")
    assert s.round_price(Decimal("100.1"), "SELL") == Decimal("100.5")


def test_tradeable_enforces_min_qty_and_notional():
    s = parse_binance_spec(BINANCE_FUT_BTC)          # min 0.001 qty / 100 notional
    assert not s.tradeable(Decimal("0.0005"), Decimal("62800"))   # under min qty
    assert not s.tradeable(Decimal("0.001"), Decimal("50"))       # under notional
    assert s.tradeable(Decimal("0.002"), Decimal("62800"))        # ok


# ---- fetchers (mock transport, zero network) -------------------------------

async def test_fetch_binance_spec_hits_exchangeinfo_and_parses():
    seen = {}

    def handler(request):
        seen["url"] = str(request.url)
        return httpx.Response(200, json={"symbols": [BINANCE_FUT_BTC]})

    s = await fetch_binance_spec("https://demo-fapi.binance.com",
                                 "/fapi/v1/exchangeInfo", "BTCUSDT",
                                 transport=httpx.MockTransport(handler))
    assert s.symbol == "BTCUSDT" and s.lot_step == Decimal("0.001")
    assert "symbol=BTCUSDT" in seen["url"] and "exchangeInfo" in seen["url"]


async def test_fetch_binance_spec_finds_match_when_filter_is_ignored():
    # demo-fapi ignores ?symbol= and returns the whole universe (BTC first).
    # The fetcher must pick the REQUESTED symbol, not symbols[0].
    def handler(request):
        return httpx.Response(200, json={"symbols": [BINANCE_FUT_BTC, BINANCE_SPOT_ETH]})

    s = await fetch_binance_spec("https://demo-fapi.binance.com",
                                 "/fapi/v1/exchangeInfo", "ETHUSDT",
                                 transport=httpx.MockTransport(handler))
    assert s.symbol == "ETHUSDT" and s.lot_step == Decimal("0.0001")   # not BTC's


async def test_fetch_binance_spec_raises_for_unlisted_symbol():
    def handler(request):
        return httpx.Response(200, json={"symbols": [BINANCE_FUT_BTC]})
    import pytest
    with pytest.raises(ValueError):
        await fetch_binance_spec("https://x", "/p", "NOPEUSDT",
                                 transport=httpx.MockTransport(handler))


async def test_fetch_bybit_spec_parses_linear_instrument():
    def handler(request):
        return httpx.Response(200, json={"result": {"list": [BYBIT_SOL]}})

    s = await fetch_bybit_spec("https://api-testnet.bybit.com", "SOLUSDT",
                               transport=httpx.MockTransport(handler))
    assert s.symbol == "SOLUSDT" and s.lot_step == Decimal("0.1")


async def test_fetch_delta_spec_parses_product_and_rejects_unlisted():
    seen = {}

    def handler(request):
        seen["url"] = str(request.url)
        return httpx.Response(200, json={"success": True, "result": DELTA_BTC})

    s = await fetch_delta_spec("https://cdn-ind.testnet.deltaex.org", "BTCUSD",
                               transport=httpx.MockTransport(handler))
    assert s.symbol == "BTCUSD" and s.lot_step == Decimal("0.001")
    assert seen["url"].endswith("/v2/products/BTCUSD")

    def missing(request):
        return httpx.Response(404, json={"success": False,
                                         "error": {"code": "not_found"}})
    import pytest
    with pytest.raises(ValueError):
        await fetch_delta_spec("https://x", "NOPEUSD",
                               transport=httpx.MockTransport(missing))
