"""In-memory ring of recent log records, for the UI's logs page.

A logging.Handler that keeps the last N formatted records so the terminal can
show the operational log (stream drops, reconciler notes, HALTs) without shipping
a file or a second transport. Attached to the "sentinel" logger, so it captures
every sentinel.* record that already flows to stdout — nothing else changes."""

from __future__ import annotations

import logging
from collections import deque


class LogRing(logging.Handler):
    """Keeps the most recent `capacity` log records as plain dicts (newest last).
    Bounded — old records drop off the front. Read-only via tail()."""

    def __init__(self, capacity: int = 400) -> None:
        super().__init__()
        self._records: deque[dict] = deque(maxlen=capacity)
        self._seq = 0

    def emit(self, record: logging.LogRecord) -> None:
        # A monotonic seq lets the UI de-dupe / detect gaps; record.created is the
        # epoch timestamp. format() applies our formatter (message only).
        self._seq += 1
        try:
            msg = self.format(record)
        except Exception:  # noqa: BLE001 — a bad format string must never break logging
            msg = record.getMessage()
        self._records.append({
            "seq": self._seq,
            "t": record.created,
            "level": record.levelname,
            "logger": record.name,
            "msg": msg,
        })

    def tail(self, n: int = 200) -> list[dict]:
        """The last `n` records, oldest first."""
        recs = list(self._records)
        return recs[-n:]


def install(capacity: int = 400, logger_name: str = "sentinel") -> LogRing:
    """Attach a LogRing to `logger_name` (default the whole sentinel.* tree) and
    return it. Idempotent-ish: safe to call once at boot."""
    ring = LogRing(capacity)
    ring.setFormatter(logging.Formatter("%(message)s"))
    lg = logging.getLogger(logger_name)
    lg.addHandler(ring)
    if lg.level == logging.NOTSET:
        lg.setLevel(logging.INFO)
    return ring
