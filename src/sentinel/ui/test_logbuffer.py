"""LogRing — bounded in-memory capture of recent log records for the logs page."""

from __future__ import annotations

import logging

from sentinel.ui.logbuffer import LogRing, install


def _emit(lg, ring, level, msg):
    ring.handle(lg.makeRecord(lg.name, level, __file__, 1, msg, None, None))


def test_captures_records_newest_last_with_fields():
    ring = LogRing(capacity=10)
    ring.setFormatter(logging.Formatter("%(message)s"))
    lg = logging.getLogger("sentinel.test")
    _emit(lg, ring, logging.INFO, "market stream: BTCUSDT")
    _emit(lg, ring, logging.WARNING, "market stream dropped")
    tail = ring.tail()
    assert [r["msg"] for r in tail] == ["market stream: BTCUSDT", "market stream dropped"]
    assert tail[-1]["level"] == "WARNING" and tail[-1]["logger"] == "sentinel.test"
    assert tail[0]["seq"] < tail[1]["seq"] and tail[0]["t"] > 0


def test_ring_is_bounded_dropping_oldest():
    ring = LogRing(capacity=3)
    ring.setFormatter(logging.Formatter("%(message)s"))
    lg = logging.getLogger("sentinel.test")
    for i in range(6):
        _emit(lg, ring, logging.INFO, f"line {i}")
    msgs = [r["msg"] for r in ring.tail()]
    assert msgs == ["line 3", "line 4", "line 5"]      # only the last 3 survive


def test_install_captures_sentinel_tree_records():
    ring = install(capacity=50, logger_name="sentinel.captured")
    logging.getLogger("sentinel.captured.child").warning("boom %d", 7)
    msgs = [r["msg"] for r in ring.tail()]
    assert "boom 7" in msgs
