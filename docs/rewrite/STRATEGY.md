# The strategy layer

The OMS deliberately does not contain a strategy. `strategyd` is a separate
process that reads the journal, decides, and posts orders through the control
socket — the same door a human uses, so it faces exactly the guards a human
does. A bad strategy trades badly; it cannot corrupt state.

It is outside the writer on purpose. Alpha is the part of a trading system most
likely to be wrong, and it has no business sharing a process with the thing that
owns the order log.

## Target position, not signal edges

Each closed bar the strategy declares a **stance** — long, flat, or short — and
the runner reconciles the actual position to it. It does not act on crossings.

That distinction is what makes it crash-safe. On restart there is no "did I
already act on that cross?" to answer: rebuild the book, ask the strategy what
it wants now, and move toward it. A position that was already open when the
process started is accounted for by construction rather than by memory.

Warming up is a fourth answer — **no opinion** — and it is not the same as flat.
The runner does nothing at all, so a restart never flattens a live position
during the twenty bars it takes the slow average to fill.

## The rule

Fast average above slow means hold long. Below means flat, or short if
`SENTINEL_STRATEGY_SHORT=true`, which makes it stop-and-reverse and always in
the market. That is the whole rule. The point of this system is execution
integrity, not alpha.

The stop distance is anchored on the slow average — the trend line a long is
wrong once price falls back to — then floored and capped into a tight band of
price. Floored, because a near-cross entry would otherwise hand the risk layer a
stop of almost nothing and size itself straight into the leverage cap. Capped,
because a trend that has run far from its average would otherwise produce a
3-5% stop, and trend-following cuts losses short.

## Bars

v2 has 1-second marks and no bar history, so `strategyd` aggregates its own. A
bar closes when a mark arrives belonging to a later interval, and its close is
the last mark actually seen. An interval in which nothing arrived is **skipped,
not invented** — a bar this never saw is not a bar whose price it may guess at.

Boundaries come from the mark's own journal timestamp, never from a clock read
here. That is what makes a replay produce exactly the bars that were produced
live; an aggregator that read the wall clock would give a different answer every
run and quietly make every backtest a fiction.

The cost is warm-up: at 1m bars with a slow window of 20, a cold start has no
opinion for twenty minutes.

## Reconciling

One order per bar, and never one that crosses zero. A flip from long to short is
two trades wearing one number — it closes a position and opens an opposite one,
under different authority and different guards — so a crossing reconcile
flattens first and opens on the next bar. That costs a bar and buys an
unambiguous audit trail.

Orders that reduce exposure are sent as `authority=exit`, which routes them
through `evaluate_exit`: bounded by the reconciled position, and allowed while
halted. Everything else is an entry. A difference smaller than the venue's step
is left alone, since it cannot be traded and re-deciding it every bar would be a
stream of rejections.

A failed placement is logged and not retried. The next bar reconciles from
whatever the position actually is, so a refused or lost order corrects itself
rather than leaving this process reasoning from a fiction.

## Configuration

| variable | default | meaning |
|---|---|---|
| `SENTINEL_STRATEGY` | unset | set at all, `strategyd` starts; unset, it never launches |
| `SENTINEL_STRATEGY_LIVE` | unset | **trading is opt-in by this exact word `true`.** Anything else logs the orders it would have placed and places none |
| `SENTINEL_STRATEGY_INSTRUMENT` | `SENTINEL_INSTRUMENTS` | one instrument; a comma-separated list is refused rather than silently trading the first |
| `SENTINEL_STRATEGY_FAST` / `_SLOW` | 5 / 20 | the two windows, in bars |
| `SENTINEL_STRATEGY_INTERVAL` | `1m` | bar length |
| `SENTINEL_STRATEGY_QTY` | `0.002` | position size when not flat |
| `SENTINEL_STRATEGY_STEP` | `0.001` | the venue's lot step |
| `SENTINEL_STRATEGY_SHORT` | unset | `true` makes it stop-and-reverse |
| `SENTINEL_STRATEGY_STOP_FLOOR` / `_STOP_CAP` | 0.005 / 0.01 | the stop band, as a fraction of price |

Dry run is the default because a process that starts trading the moment it is
deployed, because someone forgot a flag, is not a safe default. The flag says
go, rather than saying stop.

On BTCUSDC the size floor is real: the venue steps at 0.001 with a minNotional
of 100, so at ~77,000 a 0.001 order is worth ~77 and is refused. 0.002 is the
smallest thing that venue will accept.
