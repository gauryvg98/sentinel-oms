# Building a bot on Phantom's BTC up/down markets — what it actually takes

Scoping document. Sources are the deployed programs and a public independent research
dossier ([`chainstacklabs/world-xyz-research`](https://github.com/chainstacklabs/world-xyz-research)),
which reverses World from mainnet and publishes reproduction commands for every claim.
**Everything below must be re-verified against mainnet before it carries money** — the
dossier is unaffiliated research, addresses were verified 2026-07-10, and all four
programs are upgradeable.

---

## 1. Phantom is not the venue

Phantom is a front-end. The BTC up/down markets are **World** (world.xyz), a Solana
protocol, and the stack decomposes into four independent pieces:

| Piece | Address | What it is |
|---|---|---|
| **prediCt** | `prediCtPZCttYMvm2W3PtxmMxLmT1dtN7riU6Cxh6tM` | World's market program. Holds collateral, mints/burns outcome tokens. |
| **JanusFI** | `JanusXpm3gsW3c9ErNoUgHppL8dGLvZKB7uekkJEYFP` | Market maker. Shares prediCt's upgrade key — World's in-house MM. |
| **BisonFI** | `2DNbzPochEcyCcWMbL4d9S3u9QqQEj5bbe6cSZFvKsbh` | Second market maker, independently controlled. |
| **DFlow** | `DF1ow4tspfHX9JwWJsAb9epbkA8hmpSEAtxXy1V27QBH` | RFQ router. Connects a wallet to the makers. Third-party. |

Collateral is **CASH** (`CASHx9KJUStyftLFWGvEVf59SGeG9sh5FfcnZMVPCASH`), a Token-2022
stablecoin issued by Bridge (Stripe), 6 decimals.

This matters because **Phantom having no public API is irrelevant**. We never touch
Phantom. Positions are ordinary Token-2022 balances in a wallet we control, and the
market program is permissionless.

## 2. The on-chain invariant — the thesis is now proven, not assumed

prediCt contains **no pricing math at all**. It is a pure conditional-token vault:

```
1 CASH  ⇔  1 UP + 1 DOWN          (split / merge — exact, reversible, fee-free)
at resolution:  winning token → 1 CASH,   losing token → 0
```

`split` and `merge` are **permissionless** — verified end-to-end on mainnet by the
dossier, value-conserving to the raw unit. Any wallet can mint a complete set for exactly
1 CASH and redeem one for exactly 1 CASH. No maker, no spread, no allowlist.

This is the anchor the whole strategy rests on, and it upgrades `S < 1` from a modelling
assumption to an **enforced on-chain invariant**: a complete set is worth exactly 1.00, so
acquiring both legs for less than 1.00 total is profit, guaranteed by the vault rather than
by a market maker's goodwill. `strategy/binary/` was written against exactly this
invariant; it turns out to be literally the protocol.

All price formation — and the spread — lives off-chain in the maker's quote.
**Measured spread: 2–3%**, embedded in the quote rather than charged separately. Our
tooling defaulted to 6%; that was pessimistic by roughly half.

## 3. The finding that changes trading today: `merge`

Once both legs are held, complete sets can be **merged back to CASH immediately** — no
maker, no spread, no waiting for the round to resolve.

Applied to the real locked position from the screenshot (15.26 DOWN tokens, 18.22 UP
tokens, $9.21 cost):

```
min(15.26, 18.22) = 15.26 complete sets  ->  merge  ->  15.26 CASH, instantly
leftover: 2.96 UP tokens (a free option on the 15% branch)
```

The $6.05 stops being a guarantee that pays in three minutes and becomes **cash in the
wallet now**, plus a free lottery ticket. On a $20 bankroll trading 5-minute rounds this
is the difference between one position at a time and continuous redeployment — capital
velocity, not a rounding error.

It also reframes §6a's advice. Equalizing payouts is not merely about maximizing the
floor; the equalized portion is exactly the *mergeable* portion. Skew is the part of the
position that stays stuck until resolution.

## 4. What is open, and what is gated

This is the whole feasibility question, and it splits cleanly.

**Open — no credentials, buildable now:**

| Need | How |
|---|---|
| Discover live rounds | `getProgramAccounts` on prediCt, Market accounts are 320 B, disc `dbbed53700e3c69a` |
| Round's UP/DOWN mints | Market account offsets 72 / 104 |
| Round window | start/end ms at offset ~256 |
| Our positions | Token-2022 balances of the outcome mints |
| Enter/exit a *pair* | `split` / `merge` — permissionless |
| **Historical price tape** | Decode maker CPI transactions: every fill through JanusFI/BisonFI is an on-chain tx whose amounts give the realized price |

**Gated — blocked on credentials:**

| Need | Status |
|---|---|
| Live two-sided quotes | DFlow `quote-api.dflow.net`, needs `x-api-key` (request form, 2–5 day approval) |
| **Buy or sell a single leg** | Same. `dev-quote-api.dflow.net` is keyless but returns `route_not_found` for World mints |
| World's market catalog | `api.world.xyz` → 403 behind Cloudflare, gated |

**Single-leg execution is the entire strategy** — buying one side, waiting, buying the
other. `split`/`merge` alone cannot express it: a split hands you both legs at exactly
1.00, which is precisely zero edge. So a **DFlow production API key is the hard
prerequisite for trading**, and its 2–5 day approval is the long pole.

## 5. The build, in dependency order

**Phase 0 — measurement. No credentials. Answers go/no-go at zero risk.**

The price tape can be reconstructed from chain alone: decode every JanusFI/BisonFI fill
and recover the price each trade paid. That yields the two-sided history the strategy
needs, without a quote endpoint.

1. `world/rpc.py` — RPC client, retry/backoff, `getProgramAccounts` + `getTransaction`.
2. `world/market.py` — Market account decoder; enumerate live and recent rounds.
3. `world/tape.py` — decode maker fills into `(round, side, price, size, ts)`.
4. Replay the tape through the existing `HedgeLegger` + `LegLedger`.
5. Run `edge_vs_fair()`. **This is the decision point.** Below the ceiling, stop — the
   venue is fair and the vig is the story. Above it, the edge is real and Phase 1 is
   justified.

Phase 0 also measures the two numbers currently guessed: the real spread, and how often a
hedge is actually offered.

**Phase 1 — execution. Needs the DFlow key.**

6. `world/quotes.py` — poll `/order`, normalize into `TwoSidedBook`.
7. `world/adapter.py` — implement the existing `BrokerAdapter`.
8. `world/keys.py` — signing. **The bot holds a dedicated hot wallet funded with only the
   working bankroll**, never a main wallet. Key material is configured by you and never
   leaves the machine.
9. Wire `HedgeLegger` → `BankrollPolicy` → adapter through the existing `CommandGateway`,
   so the strategy faces the same guards a human does.
10. `merge` sweeper — collapse complete sets to CASH the moment they exist.

**The OMS already fits this unusually well.** A submitted-but-unconfirmed Solana
transaction *is* the UNKNOWN state the system was built around, and a signed transaction's
signature is a natural idempotency key — the same signed tx cannot land twice, so
double-fill is structurally impossible rather than merely guarded against. Reconciliation
by `getSignatureStatuses` is exactly `query_order` by client order id.

**The impedance mismatch:** there are no resting orders, so `cancel` is meaningless — a
fill is atomic, it lands or it doesn't. Fail-closed, which is the good direction. The real
exposure is **leg risk**: entry fills, hedge transaction fails, and the round is naked.
That is an incident to unwind, not a position to keep, and it must route through the
protective-exit supervisor.

## 5a. Bitquery — the right tool for Phase 0, and not for Phase 1

[Bitquery](https://docs.bitquery.io/docs/blockchain/Solana/solana-instructions/) indexes
Solana and exposes it over GraphQL, with `Instruction.Program.Address` filtering on
arbitrary programs and an `InstructionBalanceUpdates` query returning
`Amount / PreBalance / PostBalance` per instruction. That is precisely the Phase 0 tape.

**Why it fits.** prediCt's IDL is withheld, so instruction *arguments* cannot be decoded —
and it does not matter. An outcome token pays exactly 1 CASH if it wins, so price is
recoverable from balance deltas alone:

```
price per $1 of payout  =  CASH delta / outcome-token delta
```

Pair the two movements inside each maker transaction and every trade's realized price
falls out. That replaces most of `world/rpc.py` and `world/tape.py` — no crawling
`getSignaturesForAddress` and hand-decoding thousands of transactions. Queries drafted in
[`scripts/bitquery/`](../scripts/bitquery/).

**Verify this first.** Both CASH and the outcome tokens are **Token-2022**, and Bitquery's
transfer docs do not state Token-2022 support. `Accounts[].Token.ProgramId` exists in the
schema, which is a good sign, but it is unconfirmed. `01-probe.graphql` settles it in one
call. If Token-2022 is not indexed the way we need, the tape must come from raw RPC and
this route is dead — so run the probe before building anything on it.

**Why it does not help Phase 1.** Bitquery is read-only: it cannot quote and cannot
execute, so the DFlow key gate is completely unchanged. Its rate limits also rule out a
live loop — 10 req/min free, 30 req/min on Personal ($49), against a strategy wanting
sub-second polling per live market. Live needs either Bitquery's gRPC CoreCast streams
(free for eligible use as of April 2026) or a dedicated RPC node.

**Division of labour:** Bitquery for history and measurement; own RPC + DFlow for live.

## 5b. Transaction cost — the constraint that "trade every round" runs into

Solana's floor is 5,000 lamports per signature, and a round costs about three
transactions (entry, hedge, merge). Failed transactions pay the base fee too.

| priority fee | tx/day | SOL/day | @ $250/SOL | needed just to break even on $20 |
|---|---|---|---|---|
| base only | 864 | 0.0043 | $1.08 | 5% / day |
| 10,000 lamports | 864 | 0.0130 | $3.24 | **16% / day** |
| 50,000 (congested) | 864 | 0.0475 | $11.88 | 59% / day |

Read this correctly. **Per round the fee is small** — roughly $0.011 against a $9 stake at
modest priority, about 0.1% of stake, versus ~$0.23 (2.5%) of maker spread. Fees are not
what kills a trade.

What the table actually shows is a **turnover** effect: recycling a $20 bankroll 288 times
a day means cumulative fees are large *relative to the bankroll* even while small relative
to volume. If the edge is real the bar clears easily — 2% on a $9 stake over 288 rounds is
~$52/day against $3.24 of fees. If the edge is marginal, frequency converts it to zero.

The design consequence is not "don't automate" — it is **be selective**. Trading 40
high-conviction rounds a day instead of 288 cuts the fee load by 86% while keeping the
setups with the most edge. `BankrollPolicy` already returns a zero stake when the risk-
correct size is inexpressible; round selection is the same discipline applied to frequency.
It also argues for growing the bankroll before raising the cadence, since fees are
per-transaction and utterly indifferent to position size.

## 6. Feasibility risks, in order of how likely they are to kill it

1. **Latency.** Quote → sign → land is realistically 1–3 s. If the maker reprices faster
   than that during the fast moves the strategy depends on, fills are adversely selected
   or fail. `predictionMarketSlippageBps` bounds the damage but not the miss rate.
   *Measurable in Phase 0 from tape timestamps.*
2. **No API key.** Approval is discretionary and the use case is a trading bot. If it is
   refused there is no legitimate execution path, and manual trading with bot-side
   analytics is the fallback.
3. **The edge is not there.** Phase 0's whole purpose. A 39% lock rate at a 2.5% spread is
   the bar for the screenshot's geometry.
4. **Size and cadence.** $20 against a 2–3% spread and a $1-ish minimum leaves very
   little room; see §6 of the strategy doc on granularity and ruin, and §5b on why fee
   turnover argues for selective rounds rather than every round.

## 7. Counterparty risk — worth reading before funding anything

These are properties of the venue, not opinions about it, and they are the reason to fund
a bot wallet with a working balance and nothing more:

- **Resolution is one key.** `determine_outcome` writes a 2-byte winner and carries no
  oracle account, price feed, or proof. Chainlink feeds that key off-chain; the chain
  cannot distinguish a Chainlink outcome from any other holder of the key. **No dispute
  window, no refund instruction.**
- **Positions are seizable by design.** Each market's token pair carries a Token-2022
  *permanent delegate* — the mechanism used to burn the losing side at settlement. The
  same power can move or destroy any position.
- **CASH is Bridge's.** The issuer holds freeze and clawback authority over every CASH
  account, independent of World.
- **All four programs are upgradeable.** None is immutable, and JanusFI shares prediCt's
  upgrade key — one key can rewrite both the market rules and the in-house maker's pricing.

Automating a wallet's markets may also sit outside Phantom's terms. We interact with World
and DFlow directly rather than with Phantom, which avoids the app entirely, but DFlow's
API terms govern the key.

## 8. What is needed from you

1. **Apply for the DFlow API key now** (`pond.dflow.net/build/api-key`) — 2–5 days, and
   nothing in Phase 1 starts without it.
2. **An RPC endpoint.** Public mainnet RPC will rate-limit `getProgramAccounts` hard; a
   Helius/QuickNode/Triton key makes Phase 0 pleasant and Phase 1 possible.
3. **A dedicated bot wallet**, funded with the working bankroll only. Never a main wallet.
4. **Confirm the round length.** The dossier documents ~15-minute BTC windows; the
   screenshot shows a 5-minute market. Both may exist; the tape will settle it.

Phase 0 needs only #2.
