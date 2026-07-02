"""Exposure guards — the rules that refuse or clamp intents before submission.

All guards read DURABLE state (the ledger), never in-memory belief: they give
the same answers after a restart as before it, which is what makes them part
of the integrity model rather than best-effort hygiene.
"""

from __future__ import annotations

from dataclasses import replace
from decimal import Decimal

from sentinel.domain import Authority, EconomicOrderIntent
from sentinel.ledger import LedgerStore

from .errors import DuplicateEntryBlocked, InstrumentHeld, NothingToExit


class ExposureGuards:
    def __init__(self, store: LedgerStore) -> None:
        self._store = store

    async def check_entry(self, intent: EconomicOrderIntent) -> None:
        """Entries are refused while the instrument holds unprovable state
        (R1.4) or an entry is already live (R1.9). Exits are exempt from both:
        protection must keep working precisely when entries are blocked (R1.13).
        """
        if await self._store.has_unresolved(intent.instrument):
            raise InstrumentHeld(
                f"{intent.instrument}: unresolved UNKNOWN/RECONCILING order"
            )
        if await self._store.has_open_entry(intent.instrument):
            raise DuplicateEntryBlocked(
                f"{intent.instrument}: a live ENTRY order already exists"
            )

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
            await self.check_entry(intent)
            return intent
        return await self.clamp_exit(intent)
