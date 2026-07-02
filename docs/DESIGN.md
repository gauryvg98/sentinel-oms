# Sentinel OMS — Concrete Design

Companion to [REQUIREMENTS.md](REQUIREMENTS.md). This document fixes the schema,
module boundaries, invariants, and failure-window analysis the implementation is
built against.

---

## 1. Design stance

1. **Write-ahead intent.** Intent is committed to PostgreSQL before any broker
   call. Every crash window then degrades to "record without exposure" (trivially
   recoverable), never "exposure without record" (unrecoverable).
2. **One recovery mechanism.** Timeout, lost ack, crash-before-submit,
   crash-after-submit, late fill — all funnel into a single procedure:
   *reconcile by client order ID against the broker*. Found → adopt broker state.
   Conclusively absent → intent is unexposed; policy decides.
3. **Functional core, imperative shell.** State machine + transition rules are
   pure functions (no I/O). Postgres, broker, and queues live at the edges.
4. **Event-sourced truth.** Append-only ledger of commands and lifecycle events;
   `orders`/`positions` are rebuildable projections.
5. **Exposure moves only on broker facts.** Fills are deduplicated on the
   broker's execution ID; positions are derived from accepted fills.

## 2. Module layout

```
src/sentinel/
  domain/        # PURE: states, transitions, commands, events, product config
    states.py            # OrderState enum + ALLOWED transition map
    transition.py        # transition(state, event) -> (state', [emitted])
    intent.py            # EconomicOrderIntent (frozen dataclass)
    events.py            # lifecycle event types (frozen dataclasses)
    product.py           # ProductDefinition: settlement/exercise/expiry flags
  ledger/        # PostgreSQL persistence
    schema/              # migrations (SQL)
    store.py             # append_event, record_command, transactional unit
    projections.py       # orders/positions/risk projectors + rebuild()
    checkpoints.py
  oms/
    gateway.py           # validate -> persist intent -> enqueue (idempotent ack)
    writer.py            # per-instrument single-writer task
    coordinator.py       # asyncio.Lock registry + pg advisory locks
    guards.py            # duplicate-entry block, never-over-exit clamp
  broker/
    adapter.py           # BrokerAdapter protocol (submit/cancel/replace/query/events)
    sim/
      broker.py          # deterministic simulator
      script.py          # failure scripting DSL (per-order behaviors)
      clock.py           # virtual clock (no wall time in tests)
  recon/
    reconciler.py        # resolve UNKNOWN, disagreement, restart convergence
  protect/
    supervisor.py        # independent protective-exit authority
  replay/
    access.py            # ReplayDataAccess: read(instrument, as_of=T)
    clock.py             # ReplayClock
  runtime/
    supervisor.py        # task tree, restart policy, fail-loud
    queues.py            # bounded queues
    shutdown.py          # drain -> checkpoint -> cancel
    audit.py             # trace ids, sequence numbers, structured events
```

## 3. Schema (PostgreSQL)

```sql
-- Every inbound instruction, immutable. command_id is the client idempotency key.
CREATE TABLE commands (
  seq         BIGSERIAL PRIMARY KEY,
  command_id  UUID        NOT NULL UNIQUE,        -- idempotency: retries collapse here
  trace_id    UUID        NOT NULL,
  kind        TEXT        NOT NULL,               -- PLACE | CANCEL | REPLACE
  payload     JSONB       NOT NULL,
  received_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Append-only lifecycle ledger. THE source of truth.
CREATE TABLE events (
  seq         BIGSERIAL PRIMARY KEY,              -- global monotonic sequence
  event_id    UUID        NOT NULL UNIQUE,
  trace_id    UUID        NOT NULL,
  order_id    UUID        NOT NULL,
  kind        TEXT        NOT NULL,  -- INTENT_PERSISTED | SUBMITTED | ACKED | UNKNOWN_ENTERED
                                     -- | RECONCILE_STARTED | RECONCILED | FILL_APPLIED
                                     -- | CANCEL_REQUESTED | CANCELED | REJECTED | ...
  from_state  TEXT,
  to_state    TEXT,
  payload     JSONB       NOT NULL,
  occurred_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX ix_events_order ON events(order_id, seq);

-- Projection: current order state. Rebuildable from events.
CREATE TABLE orders (
  order_id        UUID PRIMARY KEY,
  client_order_id TEXT NOT NULL UNIQUE,           -- reconciliation key at the broker
  instrument      TEXT NOT NULL,
  side            TEXT NOT NULL,                  -- BUY | SELL
  qty             NUMERIC NOT NULL CHECK (qty > 0),
  filled_qty      NUMERIC NOT NULL DEFAULT 0,
  avg_fill_price  NUMERIC,
  state           TEXT NOT NULL,
  broker_order_id TEXT,
  authority       TEXT NOT NULL,                  -- ENTRY | PROTECTIVE_EXIT
  last_event_seq  BIGINT NOT NULL,
  updated_at      TIMESTAMPTZ NOT NULL
);

-- Fill dedup: exposure can only move through this table, exactly once per exec.
CREATE TABLE fills (
  exec_id     TEXT PRIMARY KEY,                   -- broker execution id (dedup key)
  order_id    UUID NOT NULL REFERENCES orders(order_id),
  qty         NUMERIC NOT NULL,
  price       NUMERIC NOT NULL,
  event_seq   BIGINT NOT NULL,
  occurred_at TIMESTAMPTZ NOT NULL
);

-- Projection: net position per instrument. Derived from fills.
CREATE TABLE positions (
  instrument  TEXT PRIMARY KEY,
  qty         NUMERIC NOT NULL DEFAULT 0,
  avg_price   NUMERIC,
  last_event_seq BIGINT NOT NULL,
  updated_at  TIMESTAMPTZ NOT NULL
);

-- Authorized risk: what exposure was approved (duplicate-entry guard reads this).
CREATE TABLE authorized_risk (
  instrument  TEXT PRIMARY KEY,
  max_qty     NUMERIC NOT NULL,
  open_intent NUMERIC NOT NULL DEFAULT 0,         -- qty in non-terminal entry orders
  updated_at  TIMESTAMPTZ NOT NULL
);

CREATE TABLE checkpoints (
  name        TEXT PRIMARY KEY,                   -- e.g. 'projections'
  last_seq    BIGINT NOT NULL,
  updated_at  TIMESTAMPTZ NOT NULL
);
```

Writes are transactional: event append + projection update + (if applicable) fill
insert commit together, or not at all.

**Single-writer across processes:** `pg_advisory_xact_lock(hash(instrument))`
inside the write transaction; `asyncio.Lock` per instrument inside the process.

## 4. State machine

States: `CREATED, SUBMITTING, WORKING, PARTIAL, CANCEL_PENDING, UNKNOWN,
RECONCILING, FILLED, CANCELED, REJECTED` (last three terminal — but reopenable to
RECONCILING on late events; see [02-order-state-machine](diagrams/02-order-state-machine.svg)).

Transition rules are data (an allowed-transition map) checked by a pure
`transition()`; an illegal transition raises and is itself recorded as an audit
event. Notable rules:

- `SUBMITTING --timeout/no-id--> UNKNOWN` (never REJECTED, never resubmit)
- `UNKNOWN --> RECONCILING` is UNKNOWN's **only** exit
- `CANCEL_PENDING --fill--> PARTIAL/FILLED` (fill-during-cancel is normal, not an error)
- `FILLED/CANCELED --late-event--> RECONCILING` (presumed-terminal is reopenable)
- Replace is modeled as cancel + new order sized from **broker-confirmed**
  remaining qty, linked by trace_id

## 5. Crash-window analysis (R1.11)

| Crash point | On restart, ledger shows | Recovery |
|---|---|---|
| After intent commit, before submit | CREATED, no SUBMITTED | Reconcile by client_order_id: absent at broker → policy (submit or abandon); present → adopt |
| After submit, before ack recorded | SUBMITTED, no ACKED | Same reconcile: found → adopt state; absent → unexposed |
| After ack, before fill applied | ACKED/WORKING | Broker events replay / recon pulls executions; fills dedup on exec_id |
| Mid cancel-pending | CANCEL_REQUESTED | Recon: order state at broker decides CANCELED vs PARTIAL vs FILLED |
| During projection update | Event appended, projection stale | Rebuild projections from ledger past checkpoint |
| Any time | — | Startup sequence: rebuild projections → reconcile ALL non-terminal orders → re-arm protective exits → only then accept commands |

The startup sequence IS the compound-test (R1.14) backbone.

## 6. Broker simulator

`ScriptedBroker` implements `BrokerAdapter`. Behavior is scripted per
client_order_id with a small DSL:

```python
script = BrokerScript()
script.on_submit("K1",
    drop_ack=True,            # simulate timeout: accept but never ack
    accept=True)              # broker-side truth: order IS working
script.fill("K1", qty=1, price=4.20, at=Step(3))
script.fill("K1", qty=1, price=4.25, at=Step(4))
script.on_cancel("K1", delay_steps=2)      # cancel races the next fill
script.fill("K1", qty=1, price=4.30, at=Step(5))  # fill during cancel-pending
```

Time is a **virtual clock** advanced by the test (`await sim.step()`), so every
race is deterministic and reproducible. The simulator maintains true broker-side
state (orders, executions, positions) so the reconciler has something real to
converge against.

## 7. Invariants (what tests actually assert)

- **INV-1** No broker submission without a committed intent row.
- **INV-2** Position qty == sum of deduplicated fills. Exposure never moves
  otherwise.
- **INV-3** At most one writer per instrument (process-local and cross-process).
- **INV-4** While any order on an instrument is UNKNOWN/RECONCILING, new
  conflicting submissions on it are blocked.
- **INV-5** Exit qty ≤ reconciled position qty, clamped at submission time.
- **INV-6** `open_intent + position ≤ authorized max` (duplicate-entry guard).
- **INV-7** After restart + reconciliation, local state == broker state.
- **INV-8** Every accepted command/event is durable before its effects are
  visible (no lost events on shutdown).

## 8. Replay & product domain

- `ReplayDataAccess.read(instrument, as_of)` — the ONLY historical read path;
  `as_of > replay_clock.now()` raises by construction. No component holds a raw
  DB handle to historical data.
- Contract-lifetime gate: reads outside `[listed_at, delisted_at]` raise.
- `ProductDefinition(frozen)`: `settlement=CASH|PHYSICAL`,
  `exercise=EUROPEAN|AMERICAN`, `expiry_rules`, `multiplier`. Index-option vs
  ETF-option behavior differences are configuration read by the domain, never
  `if symbol == ...` branches.
- Determinism: replay path takes clock + RNG (if ever needed) as injected
  dependencies; same script → identical event sequence, asserted by test.

## 9. Build order

1. `domain/` — states, transition map, intent/events (pure, fully unit-tested)
2. `ledger/` — schema, migrations, append+project transaction, rebuild
3. `broker/sim/` — scripted simulator + virtual clock
4. `oms/` — gateway, writer, coordinator, guards
5. `recon/` — the single recovery mechanism
6. `protect/` — independent exit supervisor
7. Scenario tests R1.1–R1.13, then the compound R1.14
8. `replay/` + product definitions + their acceptance tests
9. Docker Compose, pinned deps, one-command suite, CI
```
