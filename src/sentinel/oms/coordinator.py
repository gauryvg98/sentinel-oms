"""Per-instrument writer coordination — the in-process half of single-writer.

One asyncio.Lock per instrument: every state-changing path (place, cancel,
broker-event application) runs under it, so two tasks can never interleave on
the same instrument. The cross-process half is the pg advisory lock the ledger
takes inside each write transaction (INV-3, defense in depth).
"""

from __future__ import annotations

import asyncio
from collections import defaultdict


class WriterCoordinator:
    def __init__(self) -> None:
        self._locks: defaultdict[str, asyncio.Lock] = defaultdict(asyncio.Lock)

    def lock(self, instrument: str) -> asyncio.Lock:
        return self._locks[instrument]
