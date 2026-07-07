"""P&L from the fills ledger — average-cost method.

Everything derives from the same deduplicated fills that drive positions
(INV-2 discipline): no separate P&L bookkeeping to drift out of sync. Walk
fills in ledger order per instrument; increases re-average the open cost,
reductions realize (price - avg_cost) * qty against the open side. Works
symmetrically for shorts.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from decimal import Decimal

import asyncpg

from .feed import MarkFeed


@dataclass(slots=True)
class InstrumentPnl:
    instrument: str
    position: Decimal
    avg_cost: Decimal | None       # of the open position; None when flat
    realized: Decimal
    unrealized: Decimal | None     # None when no mark available
    mark: Decimal | None


# Memo of the cost-basis walk (position / avg_cost / realized per instrument).
# This walk derives ONLY from fills, and used to re-fetch and re-reduce the
# ENTIRE fills table on every card()/snapshot — i.e. once per tick PER bot,
# 100+ full scans/sec of a growing table — which saturated the DB and the
# connection pool and starved the write path (event-apply). The basis changes
# only when a fill is appended, so we key the memo on the store's monotonic
# `fills_version`: recompute the walk only when it changes, then overlay live
# marks (the fast-moving part) with no DB touch. Callers that pass no version
# (tests, the one-shot reconcile phantom-close) always recompute — exact prior
# behaviour, no caching, no staleness.
_basis_cache: dict = {"version": None, "basis": {}}
_basis_lock = asyncio.Lock()


async def _compute_basis(pool: asyncpg.Pool) -> dict[str, dict]:
    rows = await pool.fetch(
        """
        SELECT f.exec_id, f.qty, f.price, o.instrument, o.side
        FROM fills f JOIN orders o ON o.order_id = f.order_id
        ORDER BY f.event_seq
        """
    )
    state: dict[str, dict] = {}
    for r in rows:
        s = state.setdefault(
            r["instrument"],
            {"pos": Decimal(0), "avg": Decimal(0), "realized": Decimal(0)},
        )
        signed = r["qty"] if r["side"] == "BUY" else -r["qty"]
        pos, avg = s["pos"], s["avg"]

        if pos == 0 or (pos > 0) == (signed > 0):
            # Opening or increasing: re-average the open cost.
            total = abs(pos) + abs(signed)
            s["avg"] = (avg * abs(pos) + r["price"] * abs(signed)) / total
            s["pos"] = pos + signed
        else:
            # Reducing (or flipping through zero): realize against avg cost.
            closing = min(abs(signed), abs(pos))
            direction = Decimal(1) if pos > 0 else Decimal(-1)
            s["realized"] += (r["price"] - avg) * closing * direction
            s["pos"] = pos + signed
            leftover = abs(signed) - closing
            if leftover > 0:                 # flipped: remainder opens anew
                s["avg"] = r["price"]
            elif s["pos"] == 0:
                s["avg"] = Decimal(0)
    return state


async def compute_pnl(
    pool: asyncpg.Pool, marks: MarkFeed | None = None, *, version: object = None
) -> dict[str, InstrumentPnl]:
    """Per-instrument P&L. `version` (the store's monotonic fills_version) gates
    the cost-basis memo: same version -> reuse the walk, overlay live marks with
    no DB. Pass None to always recompute (tests / one-shot callers)."""
    if version is None:
        basis = await _compute_basis(pool)               # uncached: exact walk
    elif _basis_cache["version"] == version:
        basis = _basis_cache["basis"]                    # fills unchanged: reuse
    else:
        async with _basis_lock:                          # one scan, not a herd
            if _basis_cache["version"] != version:
                _basis_cache["basis"] = await _compute_basis(pool)
                _basis_cache["version"] = version
            basis = _basis_cache["basis"]

    # The overlay is READ-ONLY over `basis` (never mutates it), so the memo is
    # safe to share across concurrent callers. Unrealized is recomputed here
    # from the live mark on every call — it moves every tick; the basis doesn't.
    out: dict[str, InstrumentPnl] = {}
    for instrument, s in basis.items():
        mark = marks.latest(instrument) if marks else None
        unrealized = None
        if mark is not None and s["pos"] != 0:
            unrealized = (mark.price - s["avg"]) * s["pos"]
        out[instrument] = InstrumentPnl(
            instrument=instrument,
            position=s["pos"],
            avg_cost=s["avg"] if s["pos"] != 0 else None,
            realized=s["realized"],
            unrealized=unrealized,
            mark=mark.price if mark else None,
        )
    return out
