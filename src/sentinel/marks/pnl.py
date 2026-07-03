"""P&L from the fills ledger — average-cost method.

Everything derives from the same deduplicated fills that drive positions
(INV-2 discipline): no separate P&L bookkeeping to drift out of sync. Walk
fills in ledger order per instrument; increases re-average the open cost,
reductions realize (price - avg_cost) * qty against the open side. Works
symmetrically for shorts.
"""

from __future__ import annotations

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


async def compute_pnl(
    pool: asyncpg.Pool, marks: MarkFeed | None = None
) -> dict[str, InstrumentPnl]:
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

    out: dict[str, InstrumentPnl] = {}
    for instrument, s in state.items():
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
