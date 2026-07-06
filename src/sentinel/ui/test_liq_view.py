"""Bot._liq_view — the live distance-to-liquidation derived off the CURRENT mark
(not a fetch). The liq price is anchored; this recomputes the % distance every
mark tick, so it must track the mark and vanish when flat. Bot.__new__ bypasses
__init__ — the view only reads self.liq_price + the mark arg."""

from __future__ import annotations

from decimal import Decimal

from sentinel.ui.server import Bot


def _bot(liq):
    b = Bot.__new__(Bot)
    b.liq_price = liq
    return b


def test_distance_tracks_the_streamed_mark():
    b = _bot(Decimal("32826"))            # long, liq far below
    v = b._liq_view(Decimal("62958"))
    assert v["liq"] == "32826" and v["mark"] == "62958"
    assert v["dist_pct"] == "47.9"        # (62958-32826)/62958*100

    # mark falls toward the liq price -> distance shrinks live, no refetch
    assert b._liq_view(Decimal("40000"))["dist_pct"] == "17.9"
    assert b._liq_view(Decimal("33000"))["dist_pct"] == "0.5"


def test_none_when_flat_or_no_mark():
    assert _bot(None)._liq_view(Decimal("60000")) is None      # no position
    assert _bot(Decimal("30000"))._liq_view(None) is None      # no mark yet
    assert _bot(Decimal("30000"))._liq_view(Decimal("0")) is None
