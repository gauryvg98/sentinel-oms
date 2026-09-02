# Sentinel OMS

**Live demo:** [sentinel-oms.fly.dev](https://sentinel-oms.fly.dev) &nbsp;·&nbsp; **Engineering write-up:** [sentinel-oms.fly.dev/engineering](https://sentinel-oms.fly.dev/engineering)

An **execution-integrity-first Order Management System** for automated trading.
Broker-agnostic by construction — all venue behaviour lives behind an adapter
boundary, with a deterministic, failure-scriptable simulator as the first
adapter.

> **v2 is being built alongside this**, in Rust and Go: the same integrity model,
> with the durability boundary moved off Postgres and onto a local append-only
> journal. It exists because the deployed Python took **ten seconds at the median
> to place an order** — it paid a database round trip per state transition and
> held the instrument lock across the venue call. Start at
> [docs/rewrite/ARCHITECTURE.md](docs/rewrite/ARCHITECTURE.md); the format both
> languages read is [docs/rewrite/JOURNAL-FORMAT.md](docs/rewrite/JOURNAL-FORMAT.md);
> deployment is [docs/rewrite/DEPLOY.md](docs/rewrite/DEPLOY.md).
>
> ```bash
> ./run-v2-tests.sh
> ```
>
> The Python below stays as the reference implementation until v2 has
> paper-traded a week.

Most trading-system failures aren't strategy failures — they're integrity failures:
a timeout treated as a rejection, a blind retry that doubles a position, a fill that
lands mid-cancel, a crash that forgets an order the broker still remembers. Sentinel
is built around one principle:

> **The broker is the source of truth, and the system must never manufacture
> economic exposure it cannot prove it authorized.**

— **Yashvardhan Gaur** · [github.com/gauryvg98](https://github.com/gauryvg98)

---

## What makes it interesting

- **UNKNOWN is a first-class state.** A submission timeout with no broker order ID
  never means "rejected" and never triggers a resubmit. The order goes UNKNOWN, the
  instrument locks, and a reconciler recovers the truth from the broker — by client
  order ID — before anything else may act.
- **Durable intent before submission.** Economic order intent is persisted with a
  client-generated idempotency key *before* the broker ever sees it. Crash anywhere;
  the intent is the recovery authority. Duplicates are structurally impossible, not
  merely unlikely.
- **Single-writer by construction.** One writer per instrument — `asyncio.Lock`
  in-process, PostgreSQL advisory lock across processes. Two tasks acting on the
  same position isn't discouraged; it's impossible.
- **Protective exits under independent authority.** The exit supervisor is an
  isolated task that keeps running when intake, entries, or upstream services fail.
  Entry failure can never disable protection.
- **Crash-and-restart is a tested path, not a hope.** Append-only event ledger,
  rebuildable projections, checkpoints; restart rebuilds orders/fills/locks/risk,
  reconciles against the broker, and only then resumes.
- **Point-in-time replay contract.** Every historical read is as-of time T; future
  data is unreachable *by interface design*, with tests proving denial. Product
  behavior (settlement style, exercise style, expiration) is first-class
  configuration, not scattered conditionals.

## Architecture

| Diagram | What it shows |
|---|---|
| [System architecture](docs/diagrams/01-system-architecture.svg) | Command intake → single-writer OMS core → event ledger, the broker adapter boundary, reconciliation, and the independent protective-exit supervisor |
| [Order state machine](docs/diagrams/02-order-state-machine.svg) | The authoritative lifecycle — UNKNOWN and RECONCILING as first-class states, cancel-pending races, late-fill reopening |
| [Compound failure sequence](docs/diagrams/03-compound-failure-sequence.svg) | The hardest end-to-end test: timeout + hidden acceptance + partial fills + fill-during-cancel + crash + restart + reconciliation |
| [Runtime & concurrency](docs/diagrams/04-runtime-concurrency.svg) | Supervision tree, bounded queues/backpressure, single-writer ownership, cancellation propagation, graceful shutdown |
| [Replay & product domain](docs/diagrams/05-replay-product-domain.svg) | The as-of-T data contract, contract-lifetime gates, deterministic replay, first-class product definitions |

Full specification: [docs/REQUIREMENTS.md](docs/REQUIREMENTS.md) — 14
execution-integrity scenarios (including the compound test), runtime and
persistence requirements, and the documented interface boundaries.

## Status — 124 tests green

```bash
./run-tests.sh        # clean machine -> venv, pinned deps, full suite (~8s)
```

- [x] Requirements specification + architecture diagrams
- [x] `domain/` — pure state machine, intents, events, products (44 tests)
- [x] `ledger/` — event log, projections, exec-id dedup, rebuild-from-log (9)
- [x] `broker/sim/` — deterministic failure-scriptable simulator (12)
- [x] `oms/` — gateway, single-writer engine, coordinator, exposure guards (13)
- [x] `recon/` — the single recovery mechanism + startup recovery (7)
- [x] `protect/` — independent exit supervisor, idempotent by arithmetic (7)
- [x] `scenarios/` — **acceptance suite R1.1–R1.14**, incl. the full compound
      crash/reconcile scenario as one deterministic test (14)
- [x] `replay/` — as-of-T data contract, lifetime gates, determinism (13)
- [x] `runtime/` — task supervision, fail-loud policies, graceful drain,
      assembled `SentinelApp` lifecycle (5)
- [x] CI (3.11/3.12/3.13) · pinned lockfile · Docker Compose · one-command suite
- [ ] `perf/` — load harness + live invariant auditor (next)

## Design lineage

The integrity patterns here — durable intent, deterministic idempotency,
source-of-truth reconciliation, single-writer ownership, checkpointed restart
recovery — are the same ones I've built and operated in production on a
multi-chain custody platform and documented in
[design-docs](https://github.com/gauryvg98/design-docs). Sentinel applies them to
the broker/OMS domain.

## License

MIT (code, when it lands) · diagrams and docs CC BY 4.0.
