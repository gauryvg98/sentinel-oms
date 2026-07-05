"""LedgerStore — the transactional boundary between the pure domain and Postgres.

Disciplines enforced here:
- Event append + projection update commit atomically, or not at all.
- Fill dedup happens INSIDE the transaction, before anything else: a duplicate
  execution id makes the entire application a no-op (R1.2 / R1.7).
- Every write transaction takes a pg advisory lock on the instrument — the
  cross-process half of single-writer ownership (R2 / INV-3).
- Projections carry an optimistic guard (last_event_seq) as a backstop: even
  if two writers somehow ran, the second write fails loudly instead of
  silently interleaving.
- rebuild_projections() re-derives orders/fills/positions by folding the pure
  domain ``transition()`` over the event log: recovery IS the domain function.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from decimal import Decimal
from enum import Enum
from typing import Any
from uuid import UUID, uuid4

import asyncpg

from sentinel.domain import (
    BrokerAcked,
    BrokerRejected,
    CancelConfirmed,
    CancelRequested,
    EconomicOrderIntent,
    FillReceived,
    OrderCore,
    OrderEvent,
    OrderState,
    ReconcileResolved,
    ReconcileStarted,
    SubmissionStarted,
    SubmissionTimedOut,
    transition,
)


class LedgerError(Exception):
    pass


class ConcurrencyViolation(LedgerError):
    """Optimistic guard tripped: the projection moved under us. With correct
    single-writer ownership this must never happen — treat as a halt signal."""


class FillOutcome(str, Enum):
    APPLIED = "APPLIED"
    DUPLICATE = "DUPLICATE"


# ---------------------------------------------------------------- event serde

_EVENT_KINDS: dict[type, str] = {
    SubmissionStarted: "SUBMISSION_STARTED",
    BrokerAcked: "BROKER_ACKED",
    BrokerRejected: "BROKER_REJECTED",
    SubmissionTimedOut: "SUBMISSION_TIMED_OUT",
    FillReceived: "FILL_APPLIED",
    CancelRequested: "CANCEL_REQUESTED",
    CancelConfirmed: "CANCEL_CONFIRMED",
    ReconcileStarted: "RECONCILE_STARTED",
    ReconcileResolved: "RECONCILE_RESOLVED",
}
INTENT_PERSISTED = "INTENT_PERSISTED"


def _encode_event(event: OrderEvent) -> tuple[str, dict[str, Any]]:
    kind = _EVENT_KINDS[type(event)]
    match event:
        case BrokerAcked(broker_order_id=b):
            payload = {"broker_order_id": b}
        case BrokerRejected(reason=r):
            payload = {"reason": r}
        case FillReceived(exec_id=e, qty=q, price=p):
            payload = {"exec_id": e, "qty": str(q), "price": str(p)}
        case ReconcileStarted(cause=c):
            payload = {"cause": c}
        case ReconcileResolved(resolved_state=s, broker_order_id=b, filled_qty=f):
            payload = {
                "resolved_state": s.value,
                "broker_order_id": b,
                "filled_qty": str(f),
            }
        case _:
            payload = {}
    return kind, payload


def _decode_event(kind: str, payload: dict[str, Any]) -> OrderEvent:
    match kind:
        case "SUBMISSION_STARTED":
            return SubmissionStarted()
        case "BROKER_ACKED":
            return BrokerAcked(broker_order_id=payload["broker_order_id"])
        case "BROKER_REJECTED":
            return BrokerRejected(reason=payload["reason"])
        case "SUBMISSION_TIMED_OUT":
            return SubmissionTimedOut()
        case "FILL_APPLIED":
            return FillReceived(
                exec_id=payload["exec_id"],
                qty=Decimal(payload["qty"]),
                price=Decimal(payload["price"]),
            )
        case "CANCEL_REQUESTED":
            return CancelRequested()
        case "CANCEL_CONFIRMED":
            return CancelConfirmed()
        case "RECONCILE_STARTED":
            return ReconcileStarted(cause=payload["cause"])
        case "RECONCILE_RESOLVED":
            return ReconcileResolved(
                resolved_state=OrderState(payload["resolved_state"]),
                broker_order_id=payload["broker_order_id"],
                filled_qty=Decimal(payload["filled_qty"]),
            )
    raise LedgerError(f"unknown event kind {kind}")


def _advisory_key(instrument: str) -> int:
    # Stable 64-bit signed key for pg_advisory_xact_lock.
    import hashlib

    h = hashlib.sha256(instrument.encode()).digest()
    return int.from_bytes(h[:8], "big", signed=True)


@dataclass(frozen=True, slots=True)
class StoredOrder:
    core: OrderCore
    side: str
    authority: str
    last_event_seq: int


def _row_to_stored(row: asyncpg.Record) -> StoredOrder:
    return StoredOrder(
        core=OrderCore(
            order_id=str(row["order_id"]),
            client_order_id=row["client_order_id"],
            instrument=row["instrument"],
            qty=row["qty"],
            filled_qty=row["filled_qty"],
            state=OrderState(row["state"]),
            broker_order_id=row["broker_order_id"],
        ),
        side=row["side"],
        authority=row["authority"],
        last_event_seq=row["last_event_seq"],
    )


class LedgerStore:
    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool

    # ------------------------------------------------------------- commands

    async def record_command(
        self, command_id: UUID, trace_id: UUID, kind: str, payload: dict[str, Any]
    ) -> bool:
        """Durable command dedup (R1.2). False = duplicate, do nothing."""
        row = await self._pool.fetchrow(
            """
            INSERT INTO commands (command_id, trace_id, kind, payload)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (command_id) DO NOTHING
            RETURNING seq
            """,
            command_id,
            trace_id,
            kind,
            json.dumps(payload, default=str),
        )
        return row is not None

    # ---------------------------------------------------------------- orders

    async def create_order(self, intent: EconomicOrderIntent) -> StoredOrder:
        """Persist durable intent as a CREATED order (R1.1), idempotently:
        replaying the same intent returns the existing order untouched."""
        client_order_id = intent.idempotency_key
        order_id = uuid4()
        async with self._pool.acquire() as conn, conn.transaction():
            await conn.execute(
                "SELECT pg_advisory_xact_lock($1)", _advisory_key(intent.instrument)
            )
            existing = await conn.fetchrow(
                "SELECT * FROM orders WHERE client_order_id = $1", client_order_id
            )
            if existing is not None:
                return _row_to_stored(existing)
            seq = await conn.fetchval(
                """
                INSERT INTO events (event_id, trace_id, order_id, kind,
                                    from_state, to_state, payload)
                VALUES ($1, $2, $3, $4, NULL, $5, $6)
                RETURNING seq
                """,
                uuid4(),
                intent.trace_id,
                order_id,
                INTENT_PERSISTED,
                OrderState.CREATED.value,
                json.dumps(
                    {
                        "client_order_id": client_order_id,
                        "instrument": intent.instrument,
                        "side": intent.side.value,
                        "qty": str(intent.qty),
                        "authority": intent.authority.value,
                        "limit_price": str(intent.limit_price)
                        if intent.limit_price is not None else None,
                    }
                ),
            )
            row = await conn.fetchrow(
                """
                INSERT INTO orders (order_id, client_order_id, instrument, side,
                                    qty, filled_qty, state, authority,
                                    last_event_seq, limit_price)
                VALUES ($1, $2, $3, $4, $5, 0, $6, $7, $8, $9)
                RETURNING *
                """,
                order_id,
                client_order_id,
                intent.instrument,
                intent.side.value,
                intent.qty,
                OrderState.CREATED.value,
                intent.authority.value,
                seq,
                intent.limit_price,
            )
            return _row_to_stored(row)

    async def load_order(self, client_order_id: str) -> StoredOrder | None:
        row = await self._pool.fetchrow(
            "SELECT * FROM orders WHERE client_order_id = $1", client_order_id
        )
        return None if row is None else _row_to_stored(row)

    async def load_nonterminal_orders(self) -> list[StoredOrder]:
        rows = await self._pool.fetch(
            "SELECT * FROM orders WHERE state NOT IN ('FILLED','CANCELED','REJECTED') "
            "ORDER BY last_event_seq"
        )
        return [_row_to_stored(r) for r in rows]

    # ------------------------------------------------------------ transitions

    async def apply_event(
        self, stored: StoredOrder, event: OrderEvent, trace_id: UUID
    ) -> StoredOrder | FillOutcome:
        """Run the pure transition, then commit event + projections atomically.

        For FillReceived, dedup on exec_id gates the ENTIRE application:
        a duplicate returns FillOutcome.DUPLICATE and writes nothing.
        """
        core = stored.core
        new_core = transition(core, event)  # raises on anything illegal
        kind, payload = _encode_event(event)

        async with self._pool.acquire() as conn, conn.transaction():
            await conn.execute(
                "SELECT pg_advisory_xact_lock($1)", _advisory_key(core.instrument)
            )

            if isinstance(event, FillReceived):
                inserted = await conn.fetchval(
                    """
                    INSERT INTO fills (exec_id, order_id, qty, price, event_seq)
                    VALUES ($1, $2, $3, $4, 0)
                    ON CONFLICT (exec_id) DO NOTHING
                    RETURNING exec_id
                    """,
                    event.exec_id,
                    UUID(core.order_id),
                    event.qty,
                    event.price,
                )
                if inserted is None:
                    return FillOutcome.DUPLICATE  # txn commits; nothing changed

            seq = await conn.fetchval(
                """
                INSERT INTO events (event_id, trace_id, order_id, kind,
                                    from_state, to_state, payload)
                VALUES ($1, $2, $3, $4, $5, $6, $7)
                RETURNING seq
                """,
                uuid4(),
                trace_id,
                UUID(core.order_id),
                kind,
                core.state.value,
                new_core.state.value,
                json.dumps(payload),
            )

            if isinstance(event, FillReceived):
                await conn.execute(
                    "UPDATE fills SET event_seq = $1 WHERE exec_id = $2",
                    seq,
                    event.exec_id,
                )
                signed = event.qty if stored.side == "BUY" else -event.qty
                await conn.execute(
                    """
                    INSERT INTO positions (instrument, qty, last_event_seq)
                    VALUES ($1, $2, $3)
                    ON CONFLICT (instrument) DO UPDATE
                    SET qty = positions.qty + EXCLUDED.qty,
                        last_event_seq = EXCLUDED.last_event_seq,
                        updated_at = now()
                    """,
                    core.instrument,
                    signed,
                    seq,
                )

            updated = await conn.execute(
                """
                UPDATE orders
                SET state = $1, filled_qty = $2, broker_order_id = $3,
                    last_event_seq = $4, updated_at = now()
                WHERE order_id = $5 AND last_event_seq = $6
                """,
                new_core.state.value,
                new_core.filled_qty,
                new_core.broker_order_id,
                seq,
                UUID(core.order_id),
                stored.last_event_seq,
            )
            if updated != "UPDATE 1":
                # Backstop for INV-3: single-writer means this cannot happen;
                # if it does, fail the transaction loudly rather than interleave.
                raise ConcurrencyViolation(
                    f"projection for {core.client_order_id} moved underneath us"
                )

        return StoredOrder(
            core=new_core,
            side=stored.side,
            authority=stored.authority,
            last_event_seq=seq,
        )

    # ---------------------------------------------------------- decision log

    async def record_decision(
        self,
        trace_id: UUID,
        instrument: str,
        actor: str,
        decision: str,
        detail: dict[str, Any],
    ) -> None:
        await self._pool.execute(
            "INSERT INTO decision_log (trace_id, instrument, actor, decision, detail) "
            "VALUES ($1, $2, $3, $4, $5)",
            trace_id,
            instrument,
            actor,
            decision,
            json.dumps(detail, default=str),
        )

    async def recent_decisions(self, limit: int = 50) -> list[dict[str, Any]]:
        rows = await self._pool.fetch(
            "SELECT seq, trace_id, instrument, actor, decision, detail, occurred_at "
            "FROM decision_log ORDER BY seq DESC LIMIT $1",
            limit,
        )
        return [
            {
                "seq": r["seq"],
                "trace_id": str(r["trace_id"]),
                "instrument": r["instrument"],
                "actor": r["actor"],
                "decision": r["decision"],
                "detail": json.loads(r["detail"]),
                "at": r["occurred_at"].isoformat(),
            }
            for r in rows
        ]

    # ------------------------------------------------------------ UI reads

    async def recent_orders(self, limit: int = 30) -> list[dict[str, Any]]:
        rows = await self._pool.fetch(
            # Order by last_event_seq (the authoritative per-order sequence),
            # NOT updated_at: updated_at has DEFAULT now() and is reset for
            # every row by rebuild_projections()'s TRUNCATE+re-insert, so after
            # a recovery it collapses to ~one instant and this LIMIT window
            # would return an arbitrary slice — silently dropping live working
            # orders from the panel. last_event_seq is reproduced from the log.
            "SELECT client_order_id, instrument, side, qty, filled_qty, state, "
            "broker_order_id, authority, limit_price, updated_at FROM orders "
            "ORDER BY last_event_seq DESC LIMIT $1",
            limit,
        )
        return [
            {
                "key": r["client_order_id"],
                "instrument": r["instrument"],
                "side": r["side"],
                "qty": str(r["qty"]),
                "filled": str(r["filled_qty"]),
                "state": r["state"],
                "broker_id": r["broker_order_id"],
                "authority": r["authority"],
                "limit_price": format(r["limit_price"].normalize(), "f")
                if r["limit_price"] is not None else None,
            }
            for r in rows
        ]

    async def open_entry(self, instrument: str) -> dict[str, Any] | None:
        """The live (non-terminal) ENTRY order on an instrument, if any, with
        its resting price and progress. Peg-to-touch execution uses this to
        decide whether the working limit needs re-pricing or is already filling.
        There is at most one (the R1.9 duplicate-entry guard)."""
        r = await self._pool.fetchrow(
            "SELECT client_order_id, side, qty, filled_qty, state, limit_price "
            "FROM orders WHERE instrument = $1 AND authority = 'ENTRY' "
            "AND state NOT IN ('FILLED','CANCELED','REJECTED') "
            "ORDER BY last_event_seq DESC LIMIT 1",
            instrument,
        )
        if r is None:
            return None
        return {
            "key": r["client_order_id"],
            "side": r["side"],
            "qty": r["qty"],
            "filled": r["filled_qty"],
            "state": r["state"],
            "limit_price": r["limit_price"],
        }

    async def recent_events(self, limit: int = 40) -> list[dict[str, Any]]:
        rows = await self._pool.fetch(
            "SELECT e.seq, e.trace_id, e.kind, e.from_state, e.to_state, "
            "o.client_order_id FROM events e "
            "LEFT JOIN orders o ON o.order_id = e.order_id "
            "ORDER BY e.seq DESC LIMIT $1",
            limit,
        )
        return [
            {
                "seq": r["seq"],
                "trace": str(r["trace_id"])[:8],
                "kind": r["kind"],
                "from": r["from_state"],
                "to": r["to_state"],
                "order": r["client_order_id"],
            }
            for r in rows
        ]

    async def order_reject_reason(self, client_order_id: str) -> str | None:
        """The broker's stated reason for a rejected order — for honest UI."""
        return await self._pool.fetchval(
            """
            SELECT e.payload->>'reason' FROM events e
            JOIN orders o ON o.order_id = e.order_id
            WHERE o.client_order_id = $1 AND e.kind = 'BROKER_REJECTED'
            ORDER BY e.seq DESC LIMIT 1
            """,
            client_order_id,
        )

    async def recent_fills(self, instrument: str, limit: int = 200,
                           since: int | None = None
                           ) -> list[dict[str, Any]]:
        """Fills with side + REAL market time — the chart's trade markers.

        The time comes from the append-only event log (the FILL_APPLIED
        event's occurred_at), NOT the fills projection's own occurred_at.
        The projection is TRUNCATEd and re-inserted on every
        rebuild_projections(), so its occurred_at defaults to now() and every
        marker would stack on the latest candle. The event log is never
        truncated, so its timestamp is the true moment the fill was processed.
        Joined via fills.event_seq -> events.seq. `since` (unix seconds) bounds
        the set to the visible window so a long history doesn't ship hundreds
        of off-screen fills every frame."""
        base = """
            SELECT f.exec_id, f.qty, f.price, e.occurred_at, o.side
            FROM fills f
            JOIN events e ON e.seq = f.event_seq AND e.kind = 'FILL_APPLIED'
            JOIN orders o ON o.order_id = f.order_id
            WHERE o.instrument = $1
        """
        if since is not None:
            from datetime import datetime, timezone
            rows = await self._pool.fetch(
                base + " AND e.occurred_at >= $2 ORDER BY e.occurred_at DESC LIMIT $3",
                instrument,
                datetime.fromtimestamp(since, tz=timezone.utc),
                limit,
            )
        else:
            rows = await self._pool.fetch(
                base + " ORDER BY e.occurred_at DESC LIMIT $2",
                instrument,
                limit,
            )
        return [
            {
                "t": int(r["occurred_at"].timestamp()),
                "side": r["side"],
                "qty": format(r["qty"].normalize(), "f"),
                "price": format(r["price"].normalize(), "f"),
            }
            for r in rows
        ]

    # ------------------------------------------------------------ guards API

    async def has_unresolved(self, instrument: str) -> bool:
        """True if any order on the instrument is UNKNOWN or RECONCILING —
        the R1.4 instrument hold is DERIVED from durable state, so it
        survives restarts for free."""
        return await self._pool.fetchval(
            "SELECT EXISTS (SELECT 1 FROM orders WHERE instrument = $1 "
            "AND state IN ('UNKNOWN','RECONCILING'))",
            instrument,
        )

    async def has_open_entry(self, instrument: str) -> bool:
        """True if a non-terminal ENTRY order exists (R1.9 duplicate guard)."""
        return await self._pool.fetchval(
            "SELECT EXISTS (SELECT 1 FROM orders WHERE instrument = $1 "
            "AND authority = 'ENTRY' "
            "AND state NOT IN ('FILLED','CANCELED','REJECTED'))",
            instrument,
        )

    async def open_exit_remaining(self, instrument: str) -> Decimal:
        """Unfilled quantity already committed to live protective exits —
        counted against the position so stacked exits can't over-exit (R1.10)."""
        val = await self._pool.fetchval(
            "SELECT COALESCE(SUM(qty - filled_qty), 0) FROM orders "
            "WHERE instrument = $1 AND authority = 'PROTECTIVE_EXIT' "
            "AND state NOT IN ('FILLED','CANCELED','REJECTED')",
            instrument,
        )
        return val

    # ------------------------------------------------------------ positions

    async def get_position(self, instrument: str) -> Decimal:
        val = await self._pool.fetchval(
            "SELECT qty FROM positions WHERE instrument = $1", instrument
        )
        return val if val is not None else Decimal(0)

    async def load_positions(self) -> dict[str, Decimal]:
        """All nonzero positions — the protective-exit supervisor's worklist."""
        rows = await self._pool.fetch(
            "SELECT instrument, qty FROM positions WHERE qty != 0"
        )
        return {r["instrument"]: r["qty"] for r in rows}

    # -------------------------------------------------------------- rebuild

    async def rebuild_projections(self) -> int:
        """R1.11: re-derive orders/fills/positions from the event log by
        folding the pure domain transition. The ledger wins; projections are
        disposable. Returns the number of events replayed."""
        async with self._pool.acquire() as conn, conn.transaction():
            rows = await conn.fetch("SELECT * FROM events ORDER BY seq")

            orders: dict[str, dict[str, Any]] = {}   # order_id -> row data
            fills: dict[str, dict[str, Any]] = {}    # exec_id -> row data
            positions: dict[str, Decimal] = {}
            last_seq_by_instrument: dict[str, int] = {}

            for r in rows:
                oid = str(r["order_id"])
                payload = json.loads(r["payload"])
                if r["kind"] == INTENT_PERSISTED:
                    orders[oid] = {
                        "core": OrderCore(
                            order_id=oid,
                            client_order_id=payload["client_order_id"],
                            instrument=payload["instrument"],
                            qty=Decimal(payload["qty"]),
                            filled_qty=Decimal(0),
                            state=OrderState.CREATED,
                            broker_order_id=None,
                        ),
                        "side": payload["side"],
                        "authority": payload["authority"],
                        "limit_price": payload.get("limit_price"),
                        "seq": r["seq"],
                    }
                    continue

                entry = orders[oid]
                event = _decode_event(r["kind"], payload)
                if isinstance(event, FillReceived) and event.exec_id in fills:
                    continue  # dedup holds during replay too
                entry["core"] = transition(entry["core"], event)
                entry["seq"] = r["seq"]
                if isinstance(event, FillReceived):
                    fills[event.exec_id] = {
                        "order_id": oid,
                        "qty": event.qty,
                        "price": event.price,
                        "seq": r["seq"],
                    }
                    instrument = entry["core"].instrument
                    signed = event.qty if entry["side"] == "BUY" else -event.qty
                    positions[instrument] = positions.get(instrument, Decimal(0)) + signed
                    last_seq_by_instrument[instrument] = r["seq"]

            await conn.execute("TRUNCATE orders, fills, positions")
            for oid, e in orders.items():
                c: OrderCore = e["core"]
                await conn.execute(
                    """
                    INSERT INTO orders (order_id, client_order_id, instrument, side,
                                        qty, filled_qty, state, broker_order_id,
                                        authority, last_event_seq, limit_price)
                    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
                    """,
                    UUID(oid),
                    c.client_order_id,
                    c.instrument,
                    e["side"],
                    c.qty,
                    c.filled_qty,
                    c.state.value,
                    c.broker_order_id,
                    e["authority"],
                    e["seq"],
                    Decimal(e["limit_price"]) if e["limit_price"] else None,
                )
            for exec_id, f in fills.items():
                await conn.execute(
                    """
                    INSERT INTO fills (exec_id, order_id, qty, price, event_seq)
                    VALUES ($1,$2,$3,$4,$5)
                    """,
                    exec_id,
                    UUID(f["order_id"]),
                    f["qty"],
                    f["price"],
                    f["seq"],
                )
            for instrument, qty in positions.items():
                await conn.execute(
                    """
                    INSERT INTO positions (instrument, qty, last_event_seq)
                    VALUES ($1,$2,$3)
                    """,
                    instrument,
                    qty,
                    last_seq_by_instrument[instrument],
                )
            return len(rows)
