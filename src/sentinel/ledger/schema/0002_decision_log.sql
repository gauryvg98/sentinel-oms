-- Decision log: why the system did (or refused to do) something.
-- Order lifecycle events answer "what happened"; decisions answer "why" —
-- including the trades that DIDN'T happen (blocked entries, clamped exits),
-- which lifecycle events structurally cannot capture.
CREATE TABLE decision_log (
    seq         BIGSERIAL PRIMARY KEY,
    trace_id    UUID        NOT NULL,
    instrument  TEXT        NOT NULL,
    actor       TEXT        NOT NULL,   -- gateway | guards | reconciler | protection | strategy
    decision    TEXT        NOT NULL,   -- ENTRY_PLACED | ENTRY_BLOCKED | EXIT_CLAMPED | ...
    detail      JSONB       NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX ix_decision_log_time ON decision_log (seq DESC);
