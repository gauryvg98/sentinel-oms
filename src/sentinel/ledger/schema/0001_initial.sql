-- Sentinel ledger: append-only truth + rebuildable projections.
-- The safety lives in the constraints, not in careful code:
--   commands.command_id UNIQUE  -> idempotency (R1.2)
--   fills.exec_id PRIMARY KEY   -> exactly-once exposure (R1.5/R1.7)
--   events append-only          -> audit + recovery spine (R1.11)

CREATE TABLE commands (
    seq         BIGSERIAL PRIMARY KEY,
    command_id  UUID        NOT NULL UNIQUE,
    trace_id    UUID        NOT NULL,
    kind        TEXT        NOT NULL,
    payload     JSONB       NOT NULL,
    received_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE events (
    seq         BIGSERIAL PRIMARY KEY,
    event_id    UUID        NOT NULL UNIQUE,
    trace_id    UUID        NOT NULL,
    order_id    UUID        NOT NULL,
    kind        TEXT        NOT NULL,
    from_state  TEXT,
    to_state    TEXT,
    payload     JSONB       NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX ix_events_order ON events (order_id, seq);

-- Projection: current order state. Rebuildable from events; on disagreement
-- the ledger wins.
CREATE TABLE orders (
    order_id        UUID PRIMARY KEY,
    client_order_id TEXT NOT NULL UNIQUE,
    instrument      TEXT NOT NULL,
    side            TEXT NOT NULL CHECK (side IN ('BUY', 'SELL')),
    qty             NUMERIC NOT NULL CHECK (qty > 0),
    filled_qty      NUMERIC NOT NULL DEFAULT 0 CHECK (filled_qty >= 0),
    state           TEXT NOT NULL,
    broker_order_id TEXT,
    authority       TEXT NOT NULL CHECK (authority IN ('ENTRY', 'PROTECTIVE_EXIT')),
    last_event_seq  BIGINT NOT NULL,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK (filled_qty <= qty)
);
CREATE INDEX ix_orders_instrument_state ON orders (instrument, state);

-- Exposure can only move through this table, exactly once per execution.
CREATE TABLE fills (
    exec_id     TEXT PRIMARY KEY,
    order_id    UUID NOT NULL REFERENCES orders (order_id),
    qty         NUMERIC NOT NULL CHECK (qty > 0),
    price       NUMERIC NOT NULL,
    event_seq   BIGINT NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX ix_fills_order ON fills (order_id);

-- Projection: net position per instrument, derived from fills.
CREATE TABLE positions (
    instrument     TEXT PRIMARY KEY,
    qty            NUMERIC NOT NULL DEFAULT 0,
    last_event_seq BIGINT NOT NULL,
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE checkpoints (
    name       TEXT PRIMARY KEY,
    last_seq   BIGINT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
