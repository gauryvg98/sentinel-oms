# Sentinel OMS — Requirements

An execution-integrity-first Order Management System for automated trading.
Python 3.11 / asyncio / PostgreSQL. Broker-agnostic by construction: all broker
behavior lives behind an adapter boundary, with a deterministic simulator as the
first adapter.

The organising principle: **the broker is the source of truth, and the system must
never manufacture economic exposure it cannot prove it authorized.** Every
requirement below serves that.

---

## R1 — Execution-integrity scenarios (functional core)

Each scenario must be enforced by the design AND demonstrated by at least one
automated test.

| # | Scenario | Requirement |
|---|----------|-------------|
| R1.1 | Durable intent | Economic order intent is persisted (with a client-generated idempotency key) **before** broker submission. A crash between persist and submit is recoverable; the intent is the authority for everything that follows. |
| R1.2 | Idempotency | Duplicate commands — retries, reconnects, restarts, replays — can never create duplicate economic exposure. Dedup is durable (survives restart), not in-memory. |
| R1.3 | Timeout ≠ rejection | A submission timeout with no broker order ID moves the order to UNKNOWN. A timeout is **never** treated as a rejection and **never** triggers blind resubmission. |
| R1.4 | UNKNOWN locking | While an order is UNKNOWN, conflicting submissions on that instrument/account are blocked until reconciliation resolves true broker state. |
| R1.5 | Partial fills | Filled, remaining, and average-price accounting is exact across any sequence of partial fills. |
| R1.6 | Fill during cancel-pending | A fill arriving while a cancel is in flight is accepted and accounted; the cancel then applies only to what conclusively remains. |
| R1.7 | Late fill | A fill arriving after the order was presumed terminal is detected, reconciled, and applied — never dropped, never double-applied. |
| R1.8 | Cancel/replace | Replacement orders are sized only from **conclusively remaining** quantity (broker-confirmed), never from optimistic local state. |
| R1.9 | Duplicate-entry prevention | Two signals/commands targeting the same exposure resolve to one order. |
| R1.10 | Never-over-exit | Exit quantity can never exceed the position actually held. Exits are bounded by reconciled position, not requested position. |
| R1.11 | Crash & restart recovery | After a process kill at any point, restart rebuilds orders, fills, locks, and authorized risk from PostgreSQL, reconciles with the broker, and only then resumes activity. |
| R1.12 | Position disagreement | When local and broker state disagree, the broker wins; local state is corrected through an explicit reconciliation event (auditable), and activity resumes only after convergence. |
| R1.13 | Protective-exit continuity | Protective exits run under independent authority and remain operational when new entries are blocked or upstream components fail. Entry failure must never disable exits. |
| R1.14 | Compound scenario | One deterministic end-to-end test chains the above: multi-contract intent → submission timeout with no broker ID → broker accepted anyway → two partial fills → cancel request → third fill during cancel-pending → process crash → restart → local/broker disagreement → reconciliation to the true filled quantity — asserting no duplicate entry, correct remaining quantity, no over-exit, and protective-exit restoration. |

## R2 — Runtime (asyncio)

- **Task supervision**: a structured supervision tree; task failures are detected
  and surface loudly (restart or halt policy per task) — never silently swallowed.
- **Bounded queues + backpressure**: no unbounded buffering anywhere; producers
  feel pressure when consumers lag.
- **Cancellation propagation**: every task honors cancellation; cleanup runs in
  `finally`; no orphaned tasks.
- **Single-writer ownership**: exactly one writer per account/instrument —
  `asyncio.Lock` in-process plus a PostgreSQL advisory lock across processes.
- **Graceful shutdown**: stop intake → drain queues → checkpoint → cancel tasks.
  No accepted lifecycle event may be lost during shutdown.
- **No blocking code** on the event loop; all I/O is async.

## R3 — Persistence (PostgreSQL)

- **Immutable commands and lifecycle events** in an append-only event ledger;
  state transitions are events, never in-place mutations of history.
- **Transactional writes**: an event and its projection updates commit atomically.
- **Projections** (working orders, positions, authorized risk) are rebuildable
  from the ledger.
- **Checkpoints** record the last durable point for restart.
- **Restart reconstruction**: cold start rebuilds full state from ledger +
  checkpoints, then reconciles against the broker before resuming (R1.11).
- **Structured audit events** throughout: trace IDs and monotonic sequence
  numbers on every event.

## R4 — Replay & product domain (narrow, architectural)

- **Point-in-time data contract**: every read is as-of time T; future data is
  unreachable *by construction* (interface design), with tests proving denial.
- **Contract-lifetime controls**: an instrument is queryable only within its
  listed lifetime.
- **Deterministic replay**: repeated replay of the same inputs produces identical
  results — no wall-clock reads, no uncontrolled randomness in the replay path.
- **First-class product definitions**: settlement style, exercise style, and
  expiration behavior modeled as explicit product configuration (e.g. cash-settled
  European index options vs physically-delivered American equity options) — never
  scattered conditionals. Differences are covered by tests.
- **Ambiguity discipline**: unresolved domain questions are documented in
  `ASSUMPTIONS_AND_QUESTIONS.md`, never guessed silently.

Out of scope for the core OMS: options pricing/Greeks computation, strategy and
signal logic, full replay engine.

## R5 — Architecture boundaries (documented interfaces)

The system is a reusable foundation, not a demo. Documented interfaces for:

1. **BrokerAdapter** — submit/cancel/replace, order & position queries, event
   stream. The simulator lives strictly behind this boundary; a real broker
   adapter must be a drop-in.
2. **EconomicOrderIntent** — the durable, idempotent unit of authorization.
3. **OMS lifecycle** — the state machine and its transition rules.
4. **Reconciliation** — resolving UNKNOWN and local/broker disagreement.
5. **Event persistence** — ledger, projections, checkpoints.
6. **ReplayDataAccess** — the as-of-T read contract.
7. **ProductConfiguration** — product definitions and their behavioral flags.

## R6 — Quality & evidence

- At least one automated test per R1 scenario; fault-injection tests for timeout,
  lost-ack, crash, and disagreement paths; the R1.14 compound test runs as a
  single scenario.
- **Deterministic broker simulator**: scriptable acknowledgments, fills,
  executions, positions, and failures — every failure scenario reproducible.
- **Reproducible environment**: Docker Compose (Postgres + app), pinned
  dependencies, and one command — or a short documented sequence — to run
  migrations and the complete suite from a clean machine.
- Mainstream open-source dependencies only, with a license inventory.
- CI-runnable; the test suite is the acceptance evidence.

## Acceptance definition

The system meets this specification when the complete automated suite runs green
from the documented setup on a clean machine, every R1 scenario (including R1.14)
is demonstrated by at least one passing test, and the R4 evidence tests
(future-data rejection, lifetime controls, determinism, product-definition
coverage) pass.
