# Strategy v3 — Sequential Two-Leg Hedging on 5-Minute Binary Markets

**Status:** IMPLEMENTED as a library, DELIBERATELY NOT WIRED to a live venue.
Code at `src/sentinel/strategy/binary/`, 73 tests. There is no Phantom adapter and
no order submission path — see §7 for why that ordering is the whole point.

**Scope reminder,** inherited from `strategy-v2-design.md`: Sentinel's product is
*execution integrity*, not alpha. This document is deliberately honest about a
strategy that looks like free money and mostly isn't.

---

## 1. The strategy as described

On Phantom's 5-minute BTC up/down markets, during high-volatility swings near
support/resistance, buy **both** sides at odds better than 1:1 — for example UP at
1:3 and DOWN at 1:2. Equal size on both sides means one leg always pays, so if the
two legs together cost less than the payout, the round is profitable *whichever way
BTC goes*. Observed manually many times, often ~100% on capital deployed.

The arithmetic of the claim is correct. Normalize odds into **price space** — cost
per $1 of payout, which is also the implied probability:

| Odds | Price | |
|---|---|---|
| 1:1 | 0.500 | |
| 1:2 | 0.333 | |
| 1:3 | 0.250 | |
| 1:5 | 0.167 | |

Let `S = p_up + p_down`. Buying `q` of each side costs `q·S` and returns `q`
regardless of outcome, so:

> **`S < 1` ⟺ guaranteed profit, of `(1−S)/S` on capital deployed.**

Both worked examples clear it comfortably: 0.25 + 0.333 = **0.583** (+71%), and
0.5 + 0.167 = **0.667** (+50%). The "100% per trade" corresponds to `S ≈ 0.5`.

Everything in this package is expressed in price space and compared against that
one scalar. Odds conversion happens only at the edges (`quotes.price_from_odds`).

## 2. The distinction that changes everything

The two legs are **not available at the same instant**. That single fact moves the
strategy out of arbitrage and into something else entirely.

At any *one* instant a market maker quotes `p_up + p_down = 1 + v` with `v > 0`.
That overround **is** its revenue; a venue quoting `S < 1` simultaneously is paying
people to take both sides. So simultaneous two-sided buying is not available, not
because the venue is careful, but because it would be the venue's own loss.

Legging in over time is a different trade. Filled on one side at `p1`, hedging at
time `t2` costs `p_other(t2) = (1 + v) − p_entry(t2)`. A lock needs
`p1 + p_other(t2) < 1`, so:

> **`p_entry(t2) > p1 + v`**
>
> The leg already held must have **risen in price** before a profitable hedge is
> even offered. A binary's price is its implied probability — so the hedge only
> becomes available once the first leg is *winning*.

This is `legger.entry_price_must_reach()`, and it is the honest headline of the
strategy. It is confirmed by the worked examples themselves:

| | entry | later hedge | implied `p_up` then | move in leg 1 |
|---|---|---|---|---|
| Example 1 | UP @ 0.250 | DOWN @ 0.333 | ≈ 0.67 | **+0.42** |
| Example 2 | UP @ ~0.50 | DOWN @ 0.167 | ≈ 0.83 | **+0.33** |

In both, leg one had already won big by the time the hedge appeared.

So this is not arbitrage. It is a **take-profit rule**: enter directionally, and
when the position moves your way, buy the other side to lock the gain instead of
selling. That is a legitimate structure. It is not a free one.

## 3. Why it feels like printing (and genuinely should)

The lock triggers on a **touch**, not on the final outcome. For a driftless random
walk, the probability of touching a level before expiry is roughly *twice* the
probability of finishing beyond it. So most rounds really do lock. The win rate is
real and it is high.

The losses live entirely in the rounds where price never came back — held to expiry
naked, losing ~100% of the leg. Those don't feel like "the strategy," they feel
like one bad trade you got stuck in. That asymmetry in how the two outcomes are
*remembered* is the reason this needs measurement rather than recollection.

Payoff shape: **high win rate, negative skew.** Twenty or thirty manual wins is the
expected observation under *both* hypotheses (real edge, and no edge), so it does
not discriminate between them. That is why §5 exists.

## 4. What the reshaping is worth: exactly minus the vig

The price of a binary is a martingale under fair pricing. Buying at `p1` and
hedging at a barrier `B` is a stopping-time policy on that martingale — equivalent
to "sell at `B` if touched, else hold to settlement." By optional stopping:

> **E[profit] = −P(lock) · v** per contract, or **−P(lock)·v / p1** per $1 staked.

All the reshaping in the world moves probability mass around without creating any.
What survives is the spread paid on the hedge leg. Two consequences worth stating:

- **The barrier is not worth optimizing.** Hedge early → lock a little, often.
  Hedge late (the `S = 0.583` case) → lock a lot, rarely. Both pay the same
  expected vig. There is no placement that beats the spread.
- **Position sizing cannot fix it.** Kelly on a negative-EV distribution is zero.

`sizing.fair_ev()` is this bound. The implementation is cross-checked against a
completely independent route — building the full outcome distribution and taking
its expectation — and the two agree to 1e-9 across a parameter sweep
(`test_a_fair_market_yields_exactly_minus_the_vig_on_the_hedge`). If that test ever
fails, the payoff model has invented edge the market did not offer.

## 5. The decisive test — and it is a real one

Everything above assumes the venue prices *fairly*. The claim here is that it does
not, and that claim is entirely reasonable: a market maker quoting 5-minute binaries
during volatility spikes has real, well-known failure modes.

Conditioning terminal probability on whether the barrier was touched gives
`p1 = L·B + (1−L)·w`, where `w` is the win rate among unhedged rounds. Since
`w ≥ 0`:

> **`L ≤ p1 / B`** — a hard ceiling on the lock rate, for *any* barrier, *any*
> entry, *any* signal. It follows from prices being a martingale, not from data.

That makes the hypothesis falsifiable:

| observation | reading |
|---|---|
| lock rate **≤ ceiling** | a fair bet reshaped; the vig is the whole story |
| lock rate **> ceiling** | the venue is genuinely mispricing — **edge is real** |

`measure.LegLedger.edge_vs_fair()` runs this as an exact one-sided binomial test
(no scipy — it belongs in the deterministic suite). "I've done this tons of times
and it works" becomes a p-value over a counted sample. Below 30 rounds it refuses
to render a verdict at all.

Where genuine edge would plausibly come from, all measurable:

1. **Quote lag.** The MM's implied probability trails spot during fast moves, so
   the entry leg is bought cheap relative to true probability. Most likely of the three.
2. **Asymmetric vig.** One side systematically underpriced.
3. **Bad convergence near expiry.** Implied probability should snap toward 0/1 in
   the final seconds; if it does so too slowly, the near-certain side is cheap.

`measure.OverroundMonitor` also watches for `S < 1` outright. That is genuine
simultaneous arbitrage — rare, but free — and costs nothing to keep looking for.

## 6. The $20 problem

Two constraints bind, independent of whether the edge is real.

**Granularity.** A $20 bankroll with a $1 venue minimum has 20 units of resolution.
A risk-correct size of 3% ($0.60) is *inexpressible* — the smallest legal bet is 5%.
`BankrollPolicy.stake()` returns **0** rather than rounding up, because rounding up
past the risk limit is how small accounts die.

**Ruin.** With negative skew, a positive expected return is not sufficient.
`sizing.risk_of_ruin()` computes it by exact forward DP over a discretized bankroll
grid (deterministic, so it lives in the test suite). A strategy locking 80% of
rounds for +30% and losing the stake 20% of the time has positive EV — and at 40%
of bankroll per round, ruins a $20 account with ~50% probability inside 200 rounds.
At 288 rounds/day on 5-minute markets, that is *hours*.

## 6a. Calibration against a real fill

A live locked position, 2:51 before expiry, BTC having fallen from $79,760 to the
$79,674.85 target:

| leg | cost | pays | implied price |
|---|---|---|---|
| Down | $4.62 | $15.26 | 0.3028 |
| Up | $4.59 | $18.22 | 0.2519 |

Total cost $9.21, floor **+$6.05** (+65.7%), ceiling **+$9.01** (+97.8%). Genuinely
locked — the mechanism works exactly as claimed, and the ~100% figure is the
ceiling branch.

It also confirms §2's structure rather than contradicting it. Down was bought at
0.303 while price was *above* target (Down the underdog); the fall then took Down
to ~0.82 and made Up cheap enough to hedge at 0.252. The hedge became available
because leg one won.

**Two corrections came out of this data:**

1. **Payouts were unequal**, because equal *dollars* were staked at unequal
   prices. §1's equal-size assumption is the exception in practice, not the rule.
   `position.py` models the general case, where a lock is defined by the FLOOR
   (`min(payout) - cost`), not by `S < 1`.
2. **The hedge was oversized.** `optimal_hedge_cost` puts the floor-maximizing
   spend at $3.84, not $4.59 — equalizing both branches at $15.26 for a
   *guaranteed* $6.80 on $8.46 (**80.3%**, no variance) versus a $6.05 floor on
   $9.21. The surplus bought extra payout on the UP branch, the 15% side, so it
   lost expected value ($6.49) as well as certainty. **Equalize payouts, not
   dollars.**

**Measurement note.** The UI's "Up 15% / Down 85%" sums to exactly 100%, so the
displayed probabilities are normalized with the spread stripped out. True cost per
$1 of payout is visible only in fills, which means the vig — an input to every EV
calculation here — must be measured from executions, not read off the screen. It
is defaulted to 0.06 as a placeholder throughout.

At this geometry the fair-market ceiling is a **~34-38% lock rate** (vig-dependent).
Above that, the edge is real.

## 7. What was deliberately not built

**No Phantom adapter, and no live order path.** Not an omission — the ordering is
the argument. The one number that decides whether this is worth trading (the lock
rate versus its fair ceiling) is measurable from *logged quotes alone*, at zero
risk, before any capital is committed. Building execution first would invert that.

Also absent, and honestly so:

- **The entry signal.** `HedgeLegger.on_book()` takes the desired side as an
  argument. "Swings near key support/resistance" is a real intuition, but it is a
  separate claim needing separate evidence, and fusing it into the leg arithmetic
  would make both untestable. The lock mechanics are provably right or wrong; the
  signal is not, and they should not share a fate.
- **Leg risk.** Submitting a hedge that partially fills leaves a naked remainder.
  The OMS already has the machinery for this (UNKNOWN state, reconciliation,
  single-writer) and a live adapter must route through it — a partial lock is an
  incident to unwind, not a position to keep.
- **Tie/void handling.** Equal size means any resolution picking exactly one side
  pays `q`, so ties are harmless to a lock. A *void* that refunds stakes returns
  cost instead of payout, costing the fees. It matters for naked legs; venue rules
  should be confirmed before live trading.

## 8. Honest summary

The arithmetic in the original claim is correct: `S < 1` is real profit, and the
observed prices genuinely clear it. What the claim omits is that the two legs are
sampled at different times, and that the second price is only *offered* once the
first leg is winning. That turns a stated arbitrage into a take-profit rule whose
expected value, under fair pricing, is exactly minus the vig — with a high win rate
that is bought and paid for by a fat left tail.

That is not a verdict that the edge is absent. Market-maker mispricing on short-dated
binaries is a plausible, specific, and *testable* hypothesis, and §5 is a real test
of it with a real chance of passing. It just has to be measured on counted rounds
rather than remembered ones, and the measurement is free.

**Run the measurement first. If the lock rate clears its ceiling, the edge is real
and the sizing layer is already here to trade it safely.**
