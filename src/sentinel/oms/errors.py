"""OMS-layer refusals. These are not failures — they are the system saying
'no' for a reason it can prove. Every one maps to an integrity requirement."""

from __future__ import annotations


class PlacementBlocked(Exception):
    """Base: the command was understood and refused."""


class InstrumentHeld(PlacementBlocked):
    """R1.4: an order on this instrument is UNKNOWN/RECONCILING. New entries
    are blocked until reconciliation restores provable state."""


class DuplicateEntryBlocked(PlacementBlocked):
    """R1.9: a live ENTRY order already exists for this instrument — a second
    entry would duplicate economic exposure."""


class NothingToExit(PlacementBlocked):
    """R1.10: no reconciled position remains for a protective exit to close
    (position minus already-committed exit quantity is zero or negative)."""


class PositionLimitReached(PlacementBlocked):
    """The signed exposure cap (futures) is already reached — an open that would
    push |position| past the authorized maximum is refused. The perps analogue
    of never-over-exit: never over-EXPOSE."""
