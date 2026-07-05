"""Exposure guards — the rules that refuse or clamp intents before submission.

All guards read DURABLE state (the ledger), never in-memory belief: they give
the same answers after a restart as before it, which is what makes them part
of the integrity model rather than best-effort hygiene.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import replace
from decimal import Decimal

from sentinel.domain import Authority, EconomicOrderIntent, Side
from sentinel.ledger import LedgerStore

from .errors import (
    DuplicateEntryBlocked,
    InstrumentHeld,
    NothingToExit,
    PositionLimitReached,
)

# The signed exposure cap can be a single value (one-symbol deployments) or a
# per-instrument resolver (multi-bot: BTC caps at 0.05, ETH at 1.0, ...).
MaxPosition = "Decimal | Callable[[str], Decimal | None] | None"


class ExposureGuards:
    def __init__(self, store: LedgerStore, *,
                 max_position=None) -> None:
        self._store = store
        # Signed exposure cap (base units). None on spot (budget bounds size,
        # and you can't short anyway); futures sets a hard cap so a bad strategy
        # can never open more than the authorized |position|, in EITHER direction.
        # A callable is resolved per instrument so each bot has its OWN cap.
        self._max = max_position

    def _cap(self, instrument: str) -> Decimal | None:
        return self._max(instrument) if callable(self._max) else self._max

    async def check_entry(self, intent: EconomicOrderIntent) -> EconomicOrderIntent:
        """Entries are refused while the instrument holds unprovable state
        (R1.4) or an entry is already live (R1.9). Exits are exempt from both:
        protection must keep working precisely when entries are blocked (R1.13).
        With a position cap set, an open is also CLAMPED so |resulting position|
        never exceeds the maximum (never over-expose — the perps never-over-exit).
        """
        if await self._store.has_unresolved(intent.instrument):
            raise InstrumentHeld(
                f"{intent.instrument}: unresolved UNKNOWN/RECONCILING order"
            )
        if await self._store.has_open_entry(intent.instrument):
            raise DuplicateEntryBlocked(
                f"{intent.instrument}: a live ENTRY order already exists"
            )
        cap = self._cap(intent.instrument)
        if cap is None:
            return intent
        position = await self._store.get_position(intent.instrument)
        signed = intent.qty if intent.side is Side.BUY else -intent.qty
        if abs(position + signed) <= cap:
            return intent
        headroom = cap - abs(position)                # room to grow |exposure|
        if headroom <= 0:
            raise PositionLimitReached(
                f"{intent.instrument}: position {position} at cap {cap}"
            )
        return replace(intent, qty=min(intent.qty, Decimal(headroom)))

    async def clamp_exit(self, intent: EconomicOrderIntent) -> EconomicOrderIntent:
        """R1.10: an exit may never target more than the reconciled position
        minus what live exits already claim. Clamped at submission time —
        requested quantity is preserved in the command audit row."""
        position = await self._store.get_position(intent.instrument)
        committed = await self._store.open_exit_remaining(intent.instrument)
        exitable = abs(position) - committed
        if exitable <= 0:
            raise NothingToExit(
                f"{intent.instrument}: position {position}, "
                f"already committed to exits {committed}"
            )
        allowed = min(intent.qty, Decimal(exitable))
        return intent if allowed == intent.qty else replace(intent, qty=allowed)

    async def apply(self, intent: EconomicOrderIntent) -> EconomicOrderIntent:
        if intent.authority is Authority.ENTRY:
            return await self.check_entry(intent)
        return await self.clamp_exit(intent)
