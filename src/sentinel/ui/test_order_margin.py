"""_order_margin — the 'blocked' figure: margin a resting limit entry ties up
(remaining notional / leverage). Binance streams the order (x=NEW) but never the
reserved margin, so we derive it. Pure, no I/O."""

from __future__ import annotations

from decimal import Decimal

from sentinel.ui.server import _order_margin

L = Decimal("3")


def _entry(qty, filled, price):
    return {"qty": Decimal(qty), "filled": Decimal(filled),
            "limit_price": Decimal(price) if price is not None else None}


def test_unfilled_order_reserves_full_notional_over_leverage():
    # 0.5 BTC resting @ 60000, 3x -> 30000 notional / 3 = 10000 blocked
    assert _order_margin(_entry("0.5", "0", "60000"), L) == Decimal("10000")


def test_partial_fill_only_blocks_the_remainder():
    # 0.5 ordered, 0.2 filled -> 0.3 remaining @ 60000 / 3 = 6000
    assert _order_margin(_entry("0.5", "0.2", "60000"), L) == Decimal("6000")


def test_no_order_or_no_limit_price_blocks_nothing():
    assert _order_margin(None, L) == Decimal(0)
    assert _order_margin(_entry("1", "0", None), L) == Decimal(0)   # market/no peg


def test_fully_filled_blocks_nothing():
    assert _order_margin(_entry("0.5", "0.5", "60000"), L) == Decimal(0)


def test_leverage_of_one_blocks_full_notional():
    assert _order_margin(_entry("2", "0", "100"), Decimal("1")) == Decimal("200")
