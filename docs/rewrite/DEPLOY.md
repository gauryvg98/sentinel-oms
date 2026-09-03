# Deploying v2

One machine, one volume, three processes. The volume is the important part:
the journal is the system of record, and a container filesystem that vanishes on
restart would take it with them.

> The current Python deployment is untouched by any of this and stays halted.
> Nothing here redeploys it.

## The shape

```
fly machine
├── /data/journal            the volume — must survive a restart
│   ├── 0000000000000001.jrnl
│   ├── LOCK                 flocked by sentineld; the single-writer proof
│   ├── control.sock         how gatewayd asks sentineld to trade
│   └── cursors/projectord
│
├── sentineld    (Rust)  claims the journal, talks to the venue, decides
├── gatewayd     (Go)    tails the journal, serves the terminal on :8000
└── projectord   (Go)    tails the journal, batches into Supabase
```

`sentineld` is the only writer. `gatewayd` and `projectord` only read, and
neither can slow it down — they hold a cursor into a file, which is the whole
reason the journal replaced a database on the critical path.

## Configuration

Everything non-secret is environment; everything secret is a Fly secret.

| variable | | |
|---|---|---|
| `SENTINEL_JOURNAL` | `/data/journal` | must be on the volume |
| `SENTINEL_INSTRUMENTS` | `BTCUSDT` | or `SENTINEL_SYMBOLS`, which the Python deployment already sets |
| `SENTINEL_VENUE` | `futures`/`binance`, `delta`, or unset | **unset means the simulator**, which reaches no venue |
| `SENTINEL_BINANCE_ENV` | `production` or unset | **unset means Demo Trading**, which is what the Python deployment runs on |
| `SENTINEL_LEVERAGE` | e.g. `25` | optional; unset leaves the account's own |
| `SENTINEL_MAX_POSITION` | e.g. `0.05` | fat-finger cap on absolute position |
| `SENTINEL_GATEWAY_ADDR` | `:8000` | |
| `SUPABASE_TABLE` | `journal_records` | |

Binance USD-M futures is the day-one venue, because it is what the Python
deployment actually trades. The variable names above are the ones already set on
the Fly app — `SENTINEL_SYMBOLS`, `SENTINEL_VENUE=futures`,
`BINANCE_FUTURES_KEY`, `BINANCE_FUTURES_SECRET` — so a cutover changes the image
and not the configuration, and nobody has to re-enter a secret.

**The Python deployment runs on Binance Demo Trading, not production.**
`ui/__main__.py` defaults `FUT_REST` to `https://demo-fapi.binance.com` and
`fly.toml` never overrides it, so the account it trades is paper. `DEPLOY.md`
for v1 says "real capital"; the code disagrees, and the code is what runs.

Delta India is built and tested behind the same `Venue` trait;
`SENTINEL_VENUE=delta` with `DELTA_KEY` / `DELTA_SECRET` selects it, and nothing
above the adapter changes.

Secrets:

```bash
fly secrets set --app sentinel-v2 \
  BINANCE_API_KEY="…" \
  BINANCE_API_SECRET="…" \
  SENTINEL_ADMIN_TOKEN="$(openssl rand -hex 24)" \
  SUPABASE_URL="https://….supabase.co" \
  SUPABASE_SERVICE_KEY="…"
```

The keys are already on the Fly app under exactly these names, so
`fly secrets set` is only needed for a new app.

Restrict the Binance key to **futures trading only, no withdrawals**, and
IP-allowlist Fly's egress if you want belt and braces. And rotate the keys the
Python deployment is using: they were pasted into a chat.

### What running it against the account actually found

`cargo run --release -p sentinel-binance --example verify`, with the keys in the
environment, on 2026-08-27. Read-only — nothing in that tool can place, cancel
or modify an order.

```
ok   clock + exchangeInfo, offset 513ms
ok   BTCUSDT step 0.0001 minQty 0.0001 tick 0.1 minNotional 50
ok   mark stream, 6 frames decoded
ok   balances     BTC 0.01  USDC 4311.39401344  USDT 1482.148195
ok   positions    BTCUSDT 0.0759
ok   open orders: none
```

Two things came out of running it that reading the code would not have shown.

**`/fapi/v2/positionRisk` returns every listed symbol** — 735 rows on this
account, almost all flat — and some of those symbols are longer than the 16-byte
`Instrument`. The adapter now uses **v3**, which returns only what is actually
held, skips flat rows before parsing anything, and reports only instruments it
was opened for. A position in something we do not trade is somebody else's on a
shared account, and reconciling against it would halt us over a number we have
no fills for.

**The demo symbol grid is not the production grid.** Demo `BTCUSDT` steps at
0.0001; production steps at 0.001. Nothing is hardcoded, so both work — but a
size that is placeable on demo can be refused in production, which is worth
knowing before the cutover rather than during it.

### One thing to know before pointing this at production

Measured on 2026-08-27, from this laptop and from the Fly machine in `sin`:
`wss://fstream.binance.com` — the **production** futures stream — completes the
websocket handshake (HTTP 101) and then sends nothing at all, on every stream
tried. `wss://demo-fstream.binance.com` streams normally from both. Production
REST is fine; it is the stream specifically.

Which means a move to production needs the stream verified from the deploy
target *first*. Without it, fills arrive only through the stale sweep — correct,
but minutes late instead of milliseconds, and not something to discover while
holding a position.

`SENTINEL_ADMIN_TOKEN` unlocks the gateway's controls. **Leave it unset and the
gateway is read-only for everyone**, which is what a public URL wants — the safe
configuration is the one you get by not configuring anything.

## Supabase

One table, created once, by a human:

```bash
go run ./go/cmd/projectord schema
```

Printed rather than applied. A process that both creates tables and writes to
them is how a bad migration reaches production at three in the morning.

Nothing reads from Supabase to make a decision, so it can be down for an hour
and the engine will not notice. What it loses is history for a chart, and
history is a thing that can arrive late.

## Volume

```bash
fly volumes create sentinel_data --size 3 --region sin --app sentinel-v2
```

Three gigabytes is years of journal at this order rate. Segments roll at 64 MiB
and nothing prunes them yet — when that changes, it will be a separate process
that copies old segments off and truncates, never something the writer does
while trading.

## Boot order, and why it is that order

`sentineld` does four things, in this sequence, and refuses to continue if any
of them fails:

1. **Claim the journal.** One `flock`. A second process is refused here and
   never gets far enough to write anything.
2. **Rebuild the book** by folding the whole log. There is no window in which
   the process has opinions about a position it has not read from its own log.
3. **Check the invariants** on what it rebuilt. A book that does not hold them
   is not one to trade from, and refusing to start is louder than halting after
   the first order.
4. **Connect the venue**, then open the control socket — last, so nothing can
   ask it to trade until all of the above is true.

Connecting to Binance does four more things at boot, each because it would
otherwise fail at the first order:

- **Syncs the clock.** A machine a second slow gets `-1021` on every signed call
  and nothing else. The offset is tracked, and re-synced whenever `-1021`
  appears anyway.
- **Loads the symbol filters** from `/fapi/v1/exchangeInfo`. The step, the tick
  and the minimum notional are read, never hardcoded.
- **Sets one-way position mode.** In hedge mode a SELL opens a separate short
  leg instead of reducing the long, and every position number in this system
  assumes one signed position.
- **Sets leverage**, if asked. Best effort: getting it wrong shows up as a
  margin rejection on a real order, which is loud enough.

## Nothing opens until the venue has been asked

An empty book means "this process has booked nothing". It does not mean "the
account holds nothing". On a first boot, after a restore, or on a cutover from
another system the two differ, and only the venue knows how — so entries are
refused until a position report has arrived, with the refusal recorded as
`the venue has not yet reported the position`. Exits are not gated: refusing to
close is the more dangerous failure (R1.13).

The request goes out on the stale sweep, which is also the first thing the
ticker does rather than something that happens a minute in. Every other route
to a position report presumes an event *arrives* — a fill, or an execution for
an order we do not recognise. A position that predates the log produces no event
at all, so without the sweep asking, the account-level check ran only by
accident.

A venue that never answers therefore leaves the system unable to open anything,
which is the direction this should fail in.

## Taking over an account that is already holding something

A cutover is not a fresh account. The first time `sentineld` asks the venue what
it holds, it can get an answer its own log cannot explain — because the log is
empty and the position predates it.

That is an **inheritance**, not a divergence, and it is adopted rather than
halted on. The adoption is written as a real order whose lifecycle runs
`Created -> Reconciling -> Filled` and never passes through `Submitting`,
because nothing was submitted; claiming otherwise would put a submission in the
log that never happened. It is booked at the venue's own entry price, so the
cost basis and every P&L number after it are the account's real ones rather than
a guess from whatever the mark was at boot. Its client order id starts `ADOPT-`
and its trace id starts `adb7`, so it is recognisable at a glance.

The distinction that makes this safe has two halves, and both are needed.

**The instrument must be one the log has never heard of** — no position, no
fills, and no orders in any state. Not merely "a different number": once
anything has been booked here, the venue disagreeing with us is the real thing
and it halts the account.

**And it must be the first position report of the process.** Positions are
polled on every sweep, so from the second report onward a book that is merely
*behind* — a fill lost in a stream gap and not yet recovered — is
indistinguishable from an inheritance. Adopting then books the position twice:
once as an adoption, and again when the lookup recovers the fill it was always
going to find.

A machine that could never take over an open book would be no use for the one
job a cutover is; a machine that adopts whenever it is behind would double every
position it ever lost sight of.

## Switching a running deployment to a real venue

Two things have to be true before `SENTINEL_VENUE` is set, and neither is
enforced by the code because neither can be.

**Nothing else may be writing to the account.** The v1 Python reports
`halted: true` and places orders anyway — measured 26 Aug to 2 Sep, 565 -> 713
orders on a process that was never restarted. A halt flag is a claim about a
variable. Stop the machine:

```bash
fly scale count 0 --app <the-other-app>
```

**`fly machine stop` is not enough.** A stopped machine with services declared
is restarted by Fly — on a request, on a health check, on its own. Measured
2026-09-03: a machine stopped by hand was running again a minute later, and for
that minute two deployments were trading the same account. `fly scale count 0`
destroys the machine, which is the only form of "off" that stays off. The
volume, the app and its secrets survive; `fly scale count 1` brings it back.

No lock in this system can substitute for that step: the other writer is a
different process on a different host and never asks.

**The live journal must start empty.** The simulator's records describe orders
that never reached an exchange. Interleaving them with real ones leaves a log
that cannot be read back as an account of what happened, which is the one thing
the log exists to be. So the venue switch also moves `SENTINEL_JOURNAL` to a new
directory and leaves the old one on the volume as history — nothing is deleted,
and the first live record is sequence 1.

## A format bump needs a new journal directory

There is no in-place migration and there deliberately is not one: the whole
reason to bump is that a record from the old format cannot supply what the new
one needs, so reading it would mean guessing. A segment from another version is
refused instead.

So a format change is also a `SENTINEL_JOURNAL` change. The directory is named
for the format it holds — `/data/journal-fmt2` — which makes the next bump
obvious rather than archaeological, and leaves the old log on the volume to be
read with the build that wrote it.

Nothing is lost that the venue can still be asked about: an inherited position
is re-adopted on the next boot, at the venue's own entry price.

## Operating

**A halt is not a crash.** `/health` returns 200 with `"halted": true`. That is
deliberate: a check that failed on a halt would make Fly restart a process whose
whole point is that it has stopped, and the restart would not clear a real
divergence — it would hide it behind a crash loop. That mistake is why the
previous deployment looked healthy for five days while refusing to trade.

```bash
fly logs --app sentinel-v2                   # what it is doing
curl "$URL/health"                           # up, and halted or not
curl "$URL/api/state" | jq                   # the whole picture
```

To stop or resume trading:

```bash
curl -X POST "$URL/api/command" -H "X-Sentinel-Token: $TOKEN" \
  -d '{"op":"halt","note":"looking at something"}'
curl -X POST "$URL/api/command" -H "X-Sentinel-Token: $TOKEN" \
  -d '{"op":"resume","note":"understood"}'
```

To check the account from outside the engine — which is the only check worth
much, because an in-process assertion shares the engine's assumptions:

```bash
fly ssh console --app sentinel-v2 -C "/app/adversary /data/journal"
```

It exits non-zero on a breach, so it can run on a schedule.

## Never scale past one

```toml
auto_stop_machines = false
auto_start_machines = false
min_machines_running = 1
```

A second machine would have its own volume and its own journal, so it would not
be a second writer on *this* log — it would be a second account, trading the
same venue, with neither one aware of the other. The `flock` cannot help with
that. Do not scale this.

## Cutover

The order matters, and none of it is reversible in a hurry:

1. **Simulator, on the deploy target.** `SENTINEL_VENUE` unset. Proves the three
   processes agree about a file on Fly's filesystem, which is not the same
   filesystem the tests ran on.
2. **Binance Demo Trading, a week.** `SENTINEL_VENUE=binance` and nothing else:
   real API, real stream drops, real reconnects, real `-1021`s if the clock
   drifts — and no money. Watch the adversary and the records-per-fsync figure.
3. **Binance production, at the smallest size the symbol allows.**
   `SENTINEL_VENUE=binance` *and* `SENTINEL_BINANCE_ENV=production`, both,
   deliberately. `SENTINEL_MAX_POSITION` set low enough that a mistake is
   affordable.
4. **Retire the Python** only once the new deployment has held a position
   through a restart, a stream drop and a reconciliation — and been checked from
   outside afterwards.

Note that Binance Demo Trading (`demo-fapi.binance.com`, keys from
demo.binance.com) is **not** the older `testnet.binancefuture.com`. They are
separate systems with separate keys, and using one's key against the other
produces a 401 that says nothing about which.

The Python stays in `src/` throughout, halted, as the reference implementation.
