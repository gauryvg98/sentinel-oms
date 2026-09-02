# Sentinel v2 — Rust core, Go edge

The Python system is correct and slow. This document says what replaces it and
why each choice follows from a measurement rather than a preference.

The integrity model does not change. The broker is still the source of truth,
UNKNOWN is still a first-class state, intent is still durable before submission,
and no path resubmits blind. What changes is **where durability lives** and
**what is allowed to sit on the critical path**.

---

## 1. What was actually wrong

Measured on the live deployment, 2026-08-26, after 34 days of uptime:

| | p50 | p95 | p99 | max |
|---|---|---|---|---|
| `place_ms` | 10,272 | 25,000 | 28,423 | 35,917 |
| `event_apply_ms` | 3,593 | 9,840 | 14,438 | 27,835 |

Ten seconds to place an order, on an account trading one symbol.

Four causes, and they feed each other:

**Postgres is on the critical path, once per state transition.** A placement is
`create_order` → `SubmissionStarted` → broker → `BrokerAcked`: three separate
transactions, each opening with `pg_advisory_xact_lock`, each a round trip to a
managed Postgres behind a proxy. A market order that fills in thirteen pieces
costs thirteen more.

**The instrument lock is held across the venue HTTP call.** Every inbound fill
for that symbol queues behind a network RTT. A convoy forms and never drains.

**The UI runs four full-table scans every three seconds**, including a
`fills JOIN orders GROUP BY` and a `SELECT 1 FROM events WHERE trace_id IS NULL`
over an append-only table that grows forever — against the same ten-connection
pool the hot path uses.

**The event loop starves.** Which is why the market socket dies with
`keepalive ping timeout` every one to five minutes, which opens a stream gap,
which fires the stale-order sweep (685 sweeps against 686 reconciliations),
which produces more database work, which starves the loop further.

`command_timeout=30.0` is why the tail lands at 35.9 seconds and not higher.

**And one separate bug:** the account halted on 2026-08-21 with
`AnotherWriterActive`. There was one machine. Fly recycled the long-lived lock
connection, `_reacquire()` opened a fresh session, `pg_try_advisory_lock`
returned false because the *old backend was still alive server-side*, and the
code read that as a rival writer. The rival was itself. A distributed lock was
being used to answer a question that was never distributed.

## 2. The shape that replaces it

```
                    ┌─────────────────────── one machine ───────────────────────┐
                    │                                                            │
   Binance ─────────┼──▶ sentineld (Rust)                                        │
   REST + WS        │      ├── venue adapter    signing, streams, decode         │
                    │      ├── engine           single writer, command ring      │
                    │      ├── domain           pure transition(), no clock      │
                    │      └── journal ─────────┐  append-only, group-commit     │
                    │                           │  fsync, flock = the writer     │
                    │                           │  proof                         │
                    │                           ▼                                │
                    │                     /data/journal                          │
                    │                           │                                │
                    │            ┌──────────────┴──────────────┐                 │
                    │            ▼                             ▼                 │
                    │      gatewayd (Go)                 projectord (Go)          │
                    │      tail + fan-out                tail + batch             │
                    │      HTTP + SSE                          │                  │
                    └───────────┬──────────────────────────────┼─────────────────┘
                                ▼                              ▼
                             browsers                       Supabase
```

**Rust holds everything that touches the venue or the money.** Go holds
everything that talks to a human or a database. Neither reaches into the
other's half; the journal is the only thing between them, and it is a file.

## 3. Rules

These are borrowed from `blink-markets/exchange`, where they are already load
bearing. They are repeated here because a rule that lives only in another repo
gets violated in this one.

**Money is fixed point.** `Price`, `Qty` and `Money` are `i64` at 1e8. There is
no `Decimal` and no `f64` anywhere below the gateway, and the one place two
fixed-point values are multiplied rounds toward zero and says so. Binance quotes
BTCUSDT in 0.1-dollar ticks and steps its size at 0.001 BTC; Delta sizes the
same instrument in contracts of 0.001. All of them fit exactly, and a float
would not survive a settlement sum.

**No wall clocks below the gateway.** Time enters the engine as
`Event::Ticked { nanos }` and in no other way. `SystemTime::now()` in the
engine, ledger, or domain is a bug, because it is exactly what makes replay stop
reproducing.

**No maps in an event payload.** Iteration order that varies is nondeterminism
that will not surface until the day it costs money. Maps as internal indices are
fine, but every iteration of one sorts before its order can reach an output.

**Numbers cross the wire as strings.** JavaScript has one numeric type and it is
a float64. A nanosecond timestamp loses its low digits the moment a browser
parses it as a JSON number.

**The journal is the fan-out.** It is an ordered log with an independent cursor
per consumer, which is the abstraction a message broker sells. Go tails it. No
process is introduced to sit in the middle and provide something that already
exists.

**Apply, then commit, then acknowledge.** Submit applies to memory and buffers;
`flush` makes the group durable. No order is acknowledged before its events are
durable. A crash between apply and fsync loses events that were never acked, and
memory dies with the process and is rebuilt from the log. The one unsafe state
is memory ahead of a log still being served from, so **a flush failure is fatal
and permanent** — the engine refuses all further work and the process must
restart.

**The venue is never called from the writer.** A command produces records and,
if it needs the venue, a request that is handed to an I/O worker *after* those
records are durable. That is the structural half of the fix: in the old system
the instrument lock was held across the venue's HTTP round trip, so an inbound
fill queued behind a network call. Here the two cannot queue behind each other,
because they are not in the same place.

**The writer proof is local.** One machine, one journal file, one `flock`. A
second process cannot open the journal for writing, which is a stronger
statement than a lock in a database that both processes have to be able to reach.
Every record carries the writer's `epoch`, so a log with two epochs interleaved
is detectable after the fact rather than merely improbable.

## 4. Why this is fast

Not because Rust is fast. Because the work moved.

| | Python today | Rust v2 |
|---|---|---|
| durability | 3–4 Postgres transactions per placement, over a proxy | one `fsync`, shared across every command queued in that instant |
| state read | `SELECT` per transition | a struct in a `HashMap` |
| lock held across venue I/O | yes | no — the venue call happens outside the writer, its result re-enters as a command |
| UI diagnostics | four table scans every 3s on the hot pool | derived from the tailed log in another process |
| fan-out | recompute-and-push per viewer | one ring, conflated per channel |

Group commit is the part that matters and the part that is counter-intuitive:
the engine drains everything queued, applies each to memory, and flushes the
whole group with one fsync. Cost per record falls as load rises.

Measured — `cargo run --release -p sentinel-journal --example group_commit`, on
an M-series laptop, 96-byte records:

| records per fsync | µs / record | records / s |
|---|---|---|
| 1 | 2,807.6 | 356 |
| 4 | 676.3 | 1,479 |
| 16 | 172.8 | 5,787 |
| 64 | 45.5 | 21,992 |
| 128 | 26.4 | 37,844 |

**The system gets faster under load instead of slower**, which is the property
the thing it replaces did not have.

Look at the first row. One record per fsync is 356 records a second — and the
Python system's ceiling was ~350 orders a second at every level of concurrency,
because it paid a durability barrier per state transition and the batch could
never form. That is not a coincidence; it is the same wall, and the only way
past it is to stop paying per record.

These are macOS/APFS numbers, where `sync_data` issues a full drive barrier.
Linux `fdatasync` on Fly's NVMe is one to two orders of magnitude cheaper, so
**this is a floor for a laptop, not a prediction for the deploy target.** Do not
size an order rate from it.

The target is not a benchmark number anyway. It is that `place_ms` p99 is
bounded by the venue RTT plus one fsync, and that nothing the UI does can move
it.

## 5. Fan-out, and the word "multicast"

Fly's network does not carry IP multicast, and the deployment target is "cheapest",
which means one machine. So the fan-out is a **shared-memory SPMC ring** in-box,
behind a `Transport` trait:

```rust
trait Transport {
    fn publish(&self, channel: Channel, frame: &[u8]) -> Result<()>;
    fn subscribe(&self, channel: Channel) -> Cursor;
}
```

`RingTransport` is what ships. `MulticastTransport` is a file that does not exist
yet and can be written without touching a caller, the day the deployment moves
to metal or an AWS TGW multicast domain. Naming the seam now is the whole point;
building the second implementation now would be building for a network we are
not on.

Channels are not treated alike, because they are not alike:

| Channel | Rate | Policy |
|---|---|---|
| `marks` | ~4/s, superseded instantly | **conflate** — keep the newest |
| `book` | one per change, superseded | **conflate** |
| `fills` | every print is history | **sequenced** — never dropped |
| `orders` | order lifecycle | **sequenced** |
| `account` | balances, halts | **sequenced** |

The channels carrying volume are exactly the ones that conflate. A burst costs no
events.

## 6. Supabase is the cold path

`projectord` tails the journal and writes Supabase in batches, with its own
cursor persisted next to the journal. It can be down for an hour without the
engine noticing, because nothing reads from it to make a decision. This is the
single most important property of the rewrite and the direct answer to "no
Postgres in the hot path": Postgres is not slow, it was just *load bearing*.

What lands there: the event log (for history and the write-up), fills, order
lifecycle, decisions, and daily P&L. What does not: anything the engine reads.

## 7. Layout

```
rust/crates/
  sentinel-types      Price, Qty, Money, ids, enums — the units
  sentinel-domain     the pure state machine, intents, products
  sentinel-journal    append-only log, group commit, tailer, torn-tail recovery
  sentinel-record     the journal's vocabulary and its byte layout
  sentinel-ledger     in-memory projections, positions, invariants
  sentinel-venue      the adapter trait + the deterministic simulator
  sentinel-risk       sizing and protective-exit geometry
  sentinel-engine     the single writer, guards, reconciler
  sentinel-delta      Delta India: signing, REST, the user stream
  sentineld           the binary: three threads and a control socket

go/
  cmd/gatewayd        HTTP + server-sent events, tails the journal
  cmd/projectord      journal → Supabase, batched
  cmd/adversary       checks the invariants from outside the process
  internal/fixed      the same fixed-point money, in Go
  internal/journal    Go reader for the same on-disk format
  internal/record     Go decoder for the same record layout
  internal/book       the gateway's read-only projection
  internal/fanout     the ring, the channel policies, the Transport seam
  internal/gateway    the HTTP surface
  internal/supabase   the cold path
```

## 8. Build order

Sequential. Each step lands green before the next starts.

| # | Step | Done when | |
|---|---|---|---|
| 0 | `types` | fixed-point arithmetic is exact across the whole contract range; no enum's zero value is valid | **done** |
| 1 | `domain` | every R1 transition rule has a test; illegal transitions are unrepresentable | **done** |
| 2 | `journal` | a crash mid-write leaves a readable log; a torn tail is truncated, not accepted; **Go and Rust read the same bytes** | **done** |
| 3 | `record` + `ledger` | the layout is pinned by a golden fixture; replaying the same log twice yields identical projections; positions equal the fill sum, always | **done** |
| 4 | `venue/sim` | every failure in R1 is scriptable and reproducible | **done** |
| 5 | `engine` | a rebuild equals the live book; the venue is never called from the writer; a flush failure is permanent | **done** |
| 6 | `risk` | a stop-out costs what was risked; sizing and the placed stop use one distance | **done** |
| 7 | `binance` + `delta` | the signature matches a published vector; quantities sit on the venue's grid or are refused; absence has exactly one proof; the stream reconnects | **done** |
| 8 | `gatewayd` | the terminal renders from the log alone; marks conflate under load and executions never drop | **done** |
| 9 | `projectord` | Supabase can be down for an hour and the engine does not notice | **done** |
| 10 | scenarios | R1.1–R1.14, including the compound test, green against the sim | **done** |
| 11 | cutover | runs on the deploy target, then paper-trades a week before it sees a key that can lose money | **in progress** |

Step 11 is the one that cannot be hurried, and `docs/rewrite/DEPLOY.md` says what
it consists of. Everything before it is green and checked from outside the
process by `cmd/adversary`.

## 9. What this does not do

It does not add a strategy, change the risk model, or touch the numbers in
`fly.toml`. Those are decisions that were made with money on the line and they
are out of scope for a rewrite whose entire claim is that behaviour is
preserved. The Python remains in `src/` as the reference implementation until
step 10 is green against it.
