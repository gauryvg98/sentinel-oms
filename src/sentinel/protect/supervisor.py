"""Protective-exit supervisor — independent authority that keeps positions
covered (R1.13).

The core computation is stateless and DB-derived:

    uncovered(instrument) = |position| - quantity already committed to live exits

If uncovered > 0, place a PROTECTIVE_EXIT intent for exactly that much. That
single rule makes the supervisor:
- idempotent: rerunning places nothing when coverage exists (the math, not a
  flag, prevents duplicates — same pattern as the R1.4 hold);
- crash-safe: a restart recomputes from durable state and converges;
- independent: exits are exempt from entry guards, so protection flows even
  while the instrument is held or entries are failing.

Pricing policy for protective exits is strategy territory and stays out of
the OMS: exits are placed as market-style intents (limit_price=None) here;
a price provider can be injected where the strategy layer owns that decision.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from uuid import uuid4

from sentinel.domain import Authority, EconomicOrderIntent, Side
from sentinel.ledger import LedgerStore, StoredOrder
from sentinel.oms import NothingToExit, OrderEngine


@dataclass(slots=True)
class ProtectionReport:
    placed: list[StoredOrder] = field(default_factory=list)
    already_covered: list[str] = field(default_factory=list)


class ProtectiveExitSupervisor:
    def __init__(self, store: LedgerStore, engine: OrderEngine) -> None:
        self._store = store
        self._engine = engine

    async def ensure_protection(self) -> ProtectionReport:
        """Bring every nonzero position to full exit coverage. Safe to call
        at any time, any number of times, including immediately after
        startup recovery (the R1.14 'protection restored' step)."""
        report = ProtectionReport()
        for instrument, position in (await self._store.load_positions()).items():
            committed = await self._store.open_exit_remaining(instrument)
            uncovered = abs(position) - committed
            if uncovered <= 0:
                report.already_covered.append(instrument)
                continue
            side = Side.SELL if position > 0 else Side.BUY
            intent = EconomicOrderIntent(
                intent_id=uuid4(),
                idempotency_key=f"PROT-{instrument}-{uuid4().hex[:12]}",
                instrument=instrument,
                side=side,
                qty=Decimal(uncovered),
                limit_price=None,
                authority=Authority.PROTECTIVE_EXIT,
                trace_id=uuid4(),
            )
            try:
                placed = await self._engine.place(intent)
                report.placed.append(placed)
                await self._store.record_decision(
                    intent.trace_id, instrument, "protection",
                    "PROTECTION_ARMED",
                    {"key": placed.core.client_order_id,
                     "qty": str(placed.core.qty), "side": side.value},
                )
            except NothingToExit:
                # Raced a fill that closed the position between the read and
                # the placement — coverage is moot; the next pass re-derives.
                report.already_covered.append(instrument)
        return report
