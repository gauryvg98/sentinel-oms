"""Per-symbol trading rules, fetched from the exchange — NEVER hardcoded.

Every venue publishes, per instrument, the quantum an order must snap to: the
quantity step (LOT_SIZE), the price tick (PRICE_FILTER), the minimum order size
and the minimum notional. Submitting anything off-grid is rejected (-1111 on
Binance, 10001 on Bybit). We fetch these once when an instrument is added and
carry them on an `InstrumentSpec`, so the same code trades BTCUSDT, ETHUSDT or
any listed symbol without a single magic number.

The parsers are pure (dict -> spec) so they unit-test with captured payloads and
zero network; the async fetchers just wrap an HTTP GET around them.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import ROUND_DOWN, ROUND_UP, Decimal

import httpx


def _snap(value: Decimal, step: Decimal, rounding: str) -> Decimal:
    """Round `value` to the nearest MULTIPLE of `step` (divide, round the
    integer count, multiply back). Correct for any step size — Decimal.quantize
    only handles decimal-place counts, so it breaks on steps like 0.5 or 2.5."""
    if step <= 0:
        return value
    return (value / step).quantize(Decimal(1), rounding=rounding) * step


@dataclass(frozen=True)
class InstrumentSpec:
    """The exchange's trading rules for ONE symbol. All quantization goes
    through here; nothing downstream hardcodes a step or a tick."""

    symbol: str
    lot_step: Decimal        # qty must be a multiple of this
    price_tick: Decimal      # price must be a multiple of this
    min_qty: Decimal         # smallest allowed order qty
    min_notional: Decimal    # smallest allowed price*qty

    def round_qty(self, qty: Decimal) -> Decimal:
        """Floor a quantity onto the lot grid (never round up past a target).
        Snaps to a MULTIPLE of lot_step — not to its decimal places — so a step
        like 0.5 or 2.5 works, not just powers of ten (Decimal.quantize would
        silently round 0.10 to the 0.01 grid)."""
        return _snap(qty, self.lot_step, ROUND_DOWN)

    def round_price(self, price: Decimal, side: str) -> Decimal:
        """Snap a price onto the tick grid, staying maker-side: a BUY rounds
        DOWN (<= bid), a SELL rounds UP (>= ask)."""
        rounding = ROUND_DOWN if side.upper() == "BUY" else ROUND_UP
        return _snap(price, self.price_tick, rounding)

    def tradeable(self, qty: Decimal, price: Decimal) -> bool:
        """Would the exchange accept this order? Checks min qty AND min notional
        so we reject locally instead of eating a broker error on the wire."""
        return qty >= self.min_qty and (qty * price) >= self.min_notional


# ------------------------------------------------------------------- parsers

def parse_binance_spec(info: dict) -> InstrumentSpec:
    """One `symbols[]` entry from Binance /exchangeInfo (spot OR USDT-M futures
    share the filter shape). MIN_NOTIONAL is 'NOTIONAL' on spot, 'MIN_NOTIONAL'
    on futures, and the amount key differs — handle both."""
    filters = {f["filterType"]: f for f in info.get("filters", [])}
    lot = filters.get("LOT_SIZE", {})
    price = filters.get("PRICE_FILTER", {})
    notional = filters.get("MIN_NOTIONAL") or filters.get("NOTIONAL") or {}
    amt = (notional.get("minNotional") or notional.get("notional") or "0")
    return InstrumentSpec(
        symbol=info["symbol"],
        lot_step=Decimal(lot.get("stepSize", "0.00000001")),
        price_tick=Decimal(price.get("tickSize", "0.00000001")),
        min_qty=Decimal(lot.get("minQty", "0")),
        min_notional=Decimal(amt),
    )


def parse_bybit_spec(item: dict) -> InstrumentSpec:
    """One `result.list[]` entry from Bybit /v5/market/instruments-info."""
    lot = item.get("lotSizeFilter", {})
    price = item.get("priceFilter", {})
    return InstrumentSpec(
        symbol=item["symbol"],
        lot_step=Decimal(lot.get("qtyStep", "0.00000001")),
        price_tick=Decimal(price.get("tickSize", "0.00000001")),
        min_qty=Decimal(lot.get("minOrderQty", "0")),
        min_notional=Decimal(lot.get("minNotionalValue", "0")),
    )


# ------------------------------------------------------------------- fetchers

async def fetch_binance_spec(rest_base: str, path: str, symbol: str,
                             *, transport=None) -> InstrumentSpec:
    """GET /exchangeInfo for a single symbol and parse its rules. `path` is
    /fapi/v1/exchangeInfo (futures) or /api/v3/exchangeInfo (spot)."""
    async with httpx.AsyncClient(base_url=rest_base, timeout=10,
                                 transport=transport) as http:
        resp = await http.get(path, params={"symbol": symbol})
        resp.raise_for_status()
    # Some endpoints (Binance demo-fapi) IGNORE the ?symbol= filter and return
    # the entire universe, so we must FIND the match — never trust symbols[0]
    # (that silently gave every instrument BTCUSDT's rules -> -1111 rejects).
    syms = resp.json().get("symbols", [])
    match = next((s for s in syms if s.get("symbol") == symbol), None)
    if match is None:
        raise ValueError(f"{symbol} not listed on {rest_base}")
    return parse_binance_spec(match)


async def fetch_bybit_spec(rest_base: str, symbol: str, *,
                           transport=None) -> InstrumentSpec:
    async with httpx.AsyncClient(base_url=rest_base, timeout=10,
                                 transport=transport) as http:
        resp = await http.get("/v5/market/instruments-info",
                              params={"category": "linear", "symbol": symbol})
        resp.raise_for_status()
    lst = resp.json().get("result", {}).get("list", [])
    if not lst:
        raise ValueError(f"{symbol} not listed on Bybit linear")
    return parse_bybit_spec(lst[0])
