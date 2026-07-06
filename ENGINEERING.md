# Sentinel — Engineering Design

End-to-end design of the OMS: how it's built, the core data model, the reconciliation
protocol, the multi-bot risk layer, and the live UI.

> **TL;DR.** Sentinel is an execution-integrity-first order management system for automated
> crypto trading (Python 3.11 / asyncio / PostgreSQL / FastAPI). The OMS is an **append-only
> event ledger** with a **pure state machine**; orders, positions and fills are **derived
> projections** you can rebuild from the log at any time. On restart or any divergence it
> **reconciles against broker truth and halts on genuine disagreement** — *"halt, don't
> absorb."* On top sits a fleet of independent per-symbol bots sharing one exchange account,
> sized by a **risk layer** that ties position size to where the stop is, and a
> **topic-pushed UI** where the browser never polls.

---

## 1. The organising principle

In automated trading the strategy is the glamorous part and the least important. What actually
loses money is an OMS that *thinks* it holds a position it doesn't — or doesn't hold one it
does. A request times out, a socket drops mid-fill, the exchange partially fills and cancels
the rest via self-trade prevention; now local belief and exchange reality have forked, and
every later decision compounds the error.

Sentinel is built so that fork **cannot go unnoticed**:

- the **exchange** is the source of truth for *exposure*,
- the **ledger** is the source of truth for *intent and history*,
- and any contradiction between them is a first-class, **halt-worthy** event.

> **"Halt, don't absorb."** A timeout is not a rejection. An unknown is not a zero. When we
> can't prove state, we stop trading and reconcile — we never invent exposure the exchange
> doesn't hold.

---

## 2. System overview

```
 ┌──────────────────────────┐        ┌───────────────────────────────────────────────┐
 │  Browser UI (FastAPI/WS)  │        │              SentinelApp · one per account     │
 │  lightweight-charts       │        │                                                │
 │  status · logs · risk     │  WS    │  CommandGateway (+ guards)   OrderEngine        │
 │  pushes: nothing          │◀──────▶│         │                        │             │
 │  receives: topics         │  push  │         ▼                        ▼             │
 └──────────────────────────┘        │  ┌───────────────────────────────────────┐     │
                                      │  │ Event ledger · append-only            │─────┼──▶ PostgreSQL
                                      │  │ events → orders · positions · fills   │◀────┼── (rebuildable)
                                      │  └───────────────────────────────────────┘     │
                                      │   Reconciler   Supervisor   Writer-lock         │
                                      │  ┌───────────────────────────────────────┐     │
                                      │  │ InstrumentManager · N independent bots │─────┼──▶ Venue adapter
                                      │  │ Bot(BTC) Bot(ETH) …  feed·bars·strat   │     │    Binance/Bybit
                                      │  └───────────────────────────────────────┘     │         │
                                      └───────────────────────────────────────────────┘         ▼
                                                                                            Exchange
                                                                                       (REST + user-data WS)
```

**Design principle that drives everything:** state changes are *events appended to an
immutable log*; everything else is derived and rebuildable, and any disagreement with the
exchange halts rather than corrupts.

| Package | Responsibility |
|---|---|
| `domain` | Pure order model + `transition(order, event)` — no I/O, no clock; the whole state machine, unit-testable before a broker exists |
| `ledger` | Append-only `events` and the projections (`orders` · `positions` · `fills`) folded from them |
| `oms` | `CommandGateway` + guards (R1.x invariants) + the single-writer coordinator that serialises writes per instrument |
| `recon` | Startup recovery and live reconciliation against broker truth — the halt-or-resolve decisions |
| `broker` | Exchange adapters behind one interface; timeouts, rejects and fills normalised to domain events |
| `risk` | Position sizing, stop/target geometry, leverage & per-pool margin — the money layer, separate from alpha |
| `ui` | FastAPI + a topic-based change signal that pushes one bot card, or the account, over the WS |

---

## 3. The event-sourced core

Every state change is an **event appended to an immutable log**. Orders, positions and fills
are not authoritative tables you mutate — they're **projections**, folded from the stream
through one pure function. Lose them and you rebuild by replay; the log is the truth.

```
Command → guards (R1.x) → append event → transition(order, event) → projections
                             (immutable)      (pure · total)     orders · positions · fills
                                    └────────────── rebuild by replay ──────────────┘
```

The heart of it is `transition(order, event) → order`: a total, side-effect-free function
encoding the entire legal state machine. Because it touches no clock, socket, or database,
the whole protocol is tested exhaustively before any real feed exists — and because it's the
*only* way state moves, a bad strategy can trade badly but can never corrupt the ledger.

---

## 4. The order state machine

The subtlety that separates a real OMS from a toy is what it does when it *doesn't know*.

```
 CREATED ─▶ SUBMITTING ─▶ WORKING ─▶ PARTIAL ─▶ FILLED
                │            │  ╲       │  ╲
   timeout ─────┤            │   ╲      │   ╲ fills first
                ▼            ▼    ▼     ▼    ▼
             UNKNOWN    CANCEL_PENDING ─▶ CANCELED        REJECTED
                │            ▲  (also: unsolicited STP cancel from WORKING/PARTIAL)
                ▼            │
           RECONCILING ──────┴──▶ resolve → broker truth  (terminal states reopen only here)
```

- A submission that **times out** is not rejected — its outcome is unprovable, so it parks in
  `UNKNOWN`; only reconciliation may move it.
- A **fill can race ahead of the ack** (`SUBMITTING → PARTIAL/FILLED` is legal).
- An **unsolicited cancel** — the venue expiring a resting or partly-filled order via
  self-trade prevention — is accepted from `WORKING`/`PARTIAL`, keeping any fills.
- **Terminal states are reopenable only via reconciliation**, never by a normal event.

Each of these was, before it was modelled, a real production halt. They are now explicit,
legal, tested edges.

---

## 5. Reconciliation — halt, don't absorb

On restart the OMS rebuilds projections from the log, then walks every non-terminal order and
asks the broker what really happened. Missing fills are back-filled — idempotently, keyed by
exec-id, so replay is exactly-once. Then it compares booked quantity to broker truth:

```
 query broker ─▶ absent? ──(local fills)──▶ HALT
       │            │no
       ▼            ▼
   back-fill ─▶ booked vs broker
                 ├─ booked > broker ───────────────▶ HALT   (invented exposure)
                 ├─ booked < broker & retries left ─▶ retry  (feed lag; catches up)
                 ├─ booked < broker & exhausted ────▶ HALT   (genuine missing fills)
                 └─ equal ─────────────────────────▶ resolve → broker truth
```

The distinction that took real care: a **timeout** or a **lagging feed** means "try again"; a
genuine **disagreement about exposure** means "stop the account." A pure connectivity
`BrokerTimeout` retries indefinitely — halting while blind would only stop risk management
without making anything safer. Conflating transient with genuine either makes the system
brittle (halts on every network blip) or unsafe (trades through real divergence). Sentinel
separates them explicitly, and only genuine ledger-vs-broker divergence halts.

---

## 6. A fleet on one account

Tens of bots trade the same exchange account at once — each independent (its own feed, bars,
strategy, order terminal) but sharing one ledger, one user-data stream, and one margin pool.
Three things make that safe:

- **Single-writer per instrument.** A coordinator serialises writes so two bots can never
  interleave on the same symbol; a per-account write-lock stops a second process from
  corrupting the same ledger.
- **Exchange-fetched instrument rules.** Lot step, price tick, min-notional and the settlement
  asset are read from the venue when a symbol is added — *never hardcoded*. The same code
  trades BTC, a low-priced alt, or a USDC-margined perp without a magic number.
- **Per-settlement-asset margin pools, shared across bots.** Each bot sizes off its *slice* of
  the pool it actually draws on (USDC perps against the USDC balance, split across the bots on
  it) — so N bots can never each size against the whole account.

---

## 7. The risk layer

Sizing is the part most bots get wrong: they trade a flat notional and let a volatile symbol
blow through it. Sentinel ties size to **where the stop is**.

```
qty = risk_pct · equity_share · conviction / stop_distance     (hard-capped by max leverage)
```

The *same* stop distance feeds both the size and the protective stop-loss — so size and SL can
never disagree. A strategy can supply its **own** stop geometry: the SMA-cross emits the
distance from price back to its slow moving average (a long is wrong once price falls back to
the trend line), floored so a near-cross entry can't hand the risk layer a zero-width stop.
Stop-loss / take-profit brackets are computed at a fixed reward:risk, drawn on the chart, and
enforced on every poll. Liquidation distance is derived live from the streamed mark, never a
fixed-interval fetch.

---

## 8. The live UI

FastAPI serves a single-page terminal; a **topic-based change signal** pushes only what
changed — one bot card, or the account — over one WebSocket. The browser **polls nothing**.
Bar/price ticks bump the symbol topic; order and fill events bump the account roll-up (wallet,
margin, blocked-by-orders) so it stays live during trading, not just on a heartbeat. A
client-side **liveness watchdog** reconnects a silently-dead socket instead of freezing.
Metrics and an event/system log stream over the same channel to a status page and a logs page.

---

## 9. Properties worth calling out

- **The core is pure.** No I/O, no clock in the domain; the entire state machine and every
  guard are unit-tested before a broker or feed exists (**315 tests**).
- **Rebuildable by construction.** Projections are a fold of the event log — drop them and
  replay. Continuously-checked invariants (`positions = fills`, `no_overfill`,
  `exits_bounded`, `audit_traced`) prove the fold stayed honest.
- **Fail toward safety, not brittleness.** Timeouts, unknown broker statuses,
  self-trade-prevention cancels and lagging fill feeds are all *legal, tested transitions* —
  each a real production halt before it was handled. Only genuine divergence halts now.
- **The browser polls nothing.** One WebSocket, topic-pushed, with a reconnect watchdog.

The strategy is replaceable. The integrity is the product.

---

*— Yashvardhan Gaur · [github.com/gauryvg98](https://github.com/gauryvg98)*
