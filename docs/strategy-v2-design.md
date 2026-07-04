# Strategy v2 — Regime-Gated Donchian Trend with Range Mean-Reversion Overlay

**Status:** WIRED IN. The strategy lives at `src/sentinel/strategy/regime.py`
(`RegimeTrendMR`), is exported from `strategy/__init__.py`, and is selectable at
runtime via `SENTINEL_STRATEGY=regime`. The runner feeds it OHLC bars
(`on_bar_ohlcv`). Tests: `strategy/test_regime.py`. This doc is the rationale.

**Scope reminder.** Sentinel's product is *execution integrity*, not alpha. This
document is deliberately honest: it proposes a strategy that is *structurally more
sophisticated* than a single SMA cross while staying (a) pure and deterministic,
(b) free of lookahead/repainting, (c) computable bar-by-bar from OHLCV, and (d)
fully unit-testable. It does **not** claim reliable edge on testnet. See §6.

---

## 0. Contract fit (what must not break)

The existing contract (`strategy/base.py`) is:

```python
def on_bar(self, close: Decimal) -> Decision      # PURE, deterministic
Decision(stance: Stance | None, detail: dict)      # LONG | FLAT | None(warming up)
```

The runner (`ui/strategy_runner.py`) does **target-position reconciliation**:
`LONG & flat -> ENTER`, `FLAT & long -> EXIT`, `None -> no-op` (warm-up never
flattens an existing position). It reads optional `fast_period`/`slow_period` off
the strategy for a chart overlay via `getattr(..., None)`.

**Two hard requirements this design honors:**

1. **The LONG/FLAT contract keeps working unchanged.** The proposal's `on_bar`
   still returns `Decision(Stance | None, detail)`. A runner that only understands
   LONG/FLAT works today with zero changes.
2. **Sized conviction is additive, not a breaking change.** We emit a target
   size *in `detail`* (`detail["target_weight"]`, a string-decimal in `[0, 1]`) —
   never by changing `Decision`'s required fields. A future sized runner can read
   it; today's runner ignores it. This mirrors how the current runner already
   reads `fast_period` opportunistically via `getattr`. See §3 for the migration
   path when `Decision` grows a first-class `target` field.

**Close-only vs OHLCV.** `on_bar` currently receives only `close`. Several of the
indicators below (Donchian, true range / ATR, ADX) are *defined on OHLC*. This is
a real design fork:

- **Minimal-change path (default in the skeleton):** synthesize a degenerate bar
  `high = low = close`. Donchian-on-close and a close-to-close volatility proxy are
  well-defined and honest — they just discard intrabar range. This keeps the exact
  current signature.
- **Preferred path (documented, `on_bar_ohlcv` provided):** add an OHLCV overload.
  The runner already has candles with `o/h/l/c` (see `market.candles` in the
  runner). Feeding true OHLC makes ATR/ADX/Donchian correct. **Recommendation:**
  extend the `Strategy` protocol with an optional `on_bar_ohlcv(bar) -> Decision`
  that `on_bar(close)` delegates to with a degenerate bar. This is a protocol
  addition, not a breaking change, and is the honest way to run these indicators.

---

## 1. The proposal, and why each piece should have edge

A single SMA cross has one economic bet (trend) and no awareness of *when trend
following works*. Trend following pays in trending regimes and bleeds (whipsaw) in
ranging ones. The core sophistication here is **regime awareness**: run different,
economically-opposite tactics depending on whether the market is trending or
ranging, and size by volatility.

### Components (a coherent combination, not a grab-bag)

**A. Donchian breakout — the trend engine.**
Go LONG when price makes a new N-bar high; exit when it makes a new M-bar low
(M < N, so exits are faster than entries — the classic asymmetric "turtle"
structure). *Economic rationale:* breakouts capture **momentum / underreaction** —
new highs attract trend-followers, stop-driven buying, and reflexive flows;
crypto in particular exhibits strong serial correlation of returns at daily-ish
horizons. Donchian is chosen over MACD/SMA because it reacts to *price levels the
market actually printed* (a real breakout), not to a smoothed average that lags and
whipsaws around a flat market. It is also parameter-light: two integer lookbacks.

**B. Regime gate — ADX (or a realized-vol-trend proxy).**
Only *arm* the breakout engine when the market is actually trending. ADX measures
trend strength (not direction) from directional movement. Gate: trend tactics
active when `ADX >= adx_trend` (e.g. 20–25). *Economic rationale:* breakout systems
have positive expectancy conditional on trend and negative expectancy in chop
(every breakout fails and reverses). The gate is the single highest-value addition
over plain SMA: it *turns the trend bet off when the edge isn't there*.

**C. Mean-reversion overlay — active ONLY in ranging regimes.**
When `ADX < adx_range` (market is ranging), switch tactic: fade extremes. Use a
z-score of price vs a rolling mean/stdev (equivalently, Bollinger position). Go
LONG when price is stretched *below* the mean (z <= -z_enter) and revert to FLAT as
it returns to the mean (z >= -z_exit, with z_exit < z_enter — asymmetric so we
don't chatter around the band). *Economic rationale:* in a range, order flow is
provided by market-makers and prices oscillate around a fair value; buying local
dips has positive expectancy *precisely when there is no trend*. Because we are
**long/flat only**, we only take the *long* leg of mean reversion (buy dips); we
never fade rallies short. This asymmetry is a real, acknowledged limitation (§6).

**D. Volatility-targeted conviction — the sizing layer.**
Rather than binary full-size LONG, target a *risk* budget: `target_weight =
clip(target_vol / realized_vol, 0, 1)`. When realized vol is high, hold less; when
low, hold more (up to 100%). *Economic rationale:* constant *dollar* exposure means
wildly varying *risk* exposure; vol-targeting equalizes risk-per-bet, which is the
single most robust, least-overfit improvement in the systematic-trading literature.
It is **not** a return-predicting parameter — it's a risk-normalizer. Emitted in
`detail["target_weight"]`; the LONG/FLAT decision is `LONG` iff `target_weight > 0`.

**Dead-band / hysteresis.** All three switches (breakout entry/exit, regime gate,
z-score entry/exit) use **asymmetric thresholds** so the strategy does not flip on
a single tick of noise around a boundary. This is the main whipsaw defense.

### Why this combination and not the others

- We *did not* stack MACD + RSI + SMA + Bollinger + Stochastic ("indicator soup").
  Every added indicator that isn't economically distinct is a free parameter to
  overfit. Donchian (trend) + ADX (regime) + z-score (range) + ATR (risk) are four
  *economically orthogonal* measurements: level breakout, trend strength,
  distance-from-mean, and volatility. There is little redundancy to curve-fit.
- Trend and mean-reversion are deliberately made **mutually exclusive by regime**,
  not blended. Blending them (weighted sum) creates a smooth surface begging to be
  optimized; a hard regime gate has one interpretable knob (the ADX threshold).

---

## 2. Signals, math, and O(1)-friendly streaming updates

All windows are fixed-length ring buffers (`collections.deque(maxlen=…)`), so
memory is bounded. Per-bar cost is O(window) in the naive form and O(1) with the
incremental accumulators noted below. Correctness first; the skeleton uses the
simple O(window) form and flags where O(1) is available.

Notation per bar `t`: `H_t, L_t, C_t` (high/low/close). In close-only mode
`H_t = L_t = C_t`.

### 2.1 Donchian channel (trend engine)
```
upper_N(t) = max(H_{t-N+1 .. t})          # excludes the CURRENT forming bar — see §4
lower_M(t) = min(L_{t-M+1 .. t})
```
- Entry arm: `C_t >= upper_N(t)`  (breakout to new N-bar high)
- Exit arm:  `C_t <= lower_M(t)`  (breaks the M-bar low)
- **Lookahead safety:** the channel is computed from bars **strictly up to and
  including the just-closed bar**; `on_bar` is only ever called on a *closed* bar
  (the runner feeds `candles[-2]`, never the forming `candles[-1]`). No future
  information enters. Because Donchian uses only realized extremes it **cannot
  repaint** — the value at bar `t` never changes after `t` closes.
- O(1) option: a monotonic-deque running-max/min keeps this O(1) amortized. The
  skeleton uses `max()/min()` over the deque (O(window), fine for small N).

### 2.2 Realized volatility & ATR (sizing + regime proxy)
Close-to-close realized vol (works in close-only mode):
```
r_t   = ln(C_t / C_{t-1})
sigma = stdev(r_{t-K+1 .. t})            # sample stdev over K returns
rv_ann_optional = sigma * sqrt(bars_per_year)   # only for display; not needed
```
ATR (needs OHLC; degenerates gracefully to |ΔC| in close-only mode):
```
TR_t = max(H_t - L_t, |H_t - C_{t-1}|, |L_t - C_{t-1}|)
ATR_t = Wilder_EMA(TR, period=P):  ATR_t = ATR_{t-1} + (TR_t - ATR_{t-1}) / P
```
- Wilder EMA is a **strictly O(1)** streaming update — one multiply-add per bar,
  no window. This is the preferred volatility measure when OHLC is available.

### 2.3 ADX (regime gate) — Wilder, streaming
```
upMove   = H_t - H_{t-1}
downMove = L_{t-1} - L_t
+DM = upMove   if (upMove > downMove and upMove > 0)   else 0
-DM = downMove if (downMove > upMove and downMove > 0) else 0
+DI = 100 * Wilder_EMA(+DM, P) / ATR_t
-DI = 100 * Wilder_EMA(-DM, P) / ATR_t
DX  = 100 * |+DI - -DI| / (+DI + -DI)
ADX = Wilder_EMA(DX, P)
```
- All Wilder EMAs are O(1) streaming. ADX ∈ [0, 100]; ~[0,20] ranging, ~[25+]
  trending. **Directionless** — it says *how much* trend, not *which way*; direction
  comes from Donchian. In close-only mode ADX is undefined (no H/L movement); the
  strategy then falls back to a **realized-vol-trend proxy**: sign-consistency of
  the last K returns (fraction of returns sharing the sign of their mean), which is
  a crude but honest close-only trend-strength measure.

### 2.4 Mean-reversion z-score (range overlay)
```
mu    = mean(C_{t-J+1 .. t})
sd    = stdev(C_{t-J+1 .. t})
z_t   = (C_t - mu) / sd                   # = Bollinger position in stdevs
```
- O(1) with running sum & sum-of-squares (Welford-style) accumulators; skeleton
  uses the simple windowed form. Guard `sd == 0` (flat window) -> z = 0.

### 2.5 Conviction / sizing
```
target_weight = clip(target_vol_per_bar / max(sigma, floor), 0, 1)
```
- `target_vol_per_bar` is the desired per-bar return stdev (a risk budget, e.g.
  1%–2%). `floor` avoids divide-by-zero and caps leverage in dead-flat markets.
  Output is clamped to `[0, 1]` because we are **long/flat, unlevered**.

---

## 3. Mapping to the target-position model

Per closed bar the strategy computes a **state machine** over regime, then a
desired stance and a target weight.

```
regime = TREND  if trend_strength >= adx_trend
         RANGE  if trend_strength <  adx_range      (adx_range <= adx_trend: dead-band)
         HOLD   otherwise (in the gap -> keep previous regime: hysteresis)

if regime == TREND:
    if not in_position and C_t >= upper_N:  want_long = True   # breakout entry
    if in_position     and C_t <= lower_M:  want_long = False  # channel exit
    (else: HOLD current stance — target-position, we don't re-chase)

if regime == RANGE:
    if not in_position and z_t <= -z_enter:  want_long = True   # buy the dip
    if in_position     and z_t >= -z_exit:   want_long = False  # revert to mean
```

**`in_position` is tracked internally as the strategy's own *intended* stance**,
NOT the broker position. The strategy is pure and cannot see the account; it
maintains a self-consistent target. The runner reconciles the *actual* broker
position to whatever stance we emit — exactly as today. (If the account is out of
sync, the runner's reconciliation still corrects it on the next bar; the strategy's
internal `in_position` only decides *its own* entry/exit logic, which is what a
target-position system wants.)

**The Decision each bar:**
```python
stance = Stance.LONG if want_long and target_weight > 0 else Stance.FLAT
Decision(stance, {
    "regime": regime,                    # "TREND" | "RANGE" | "HOLD"
    "trend_strength": "...",             # ADX or proxy, stringified
    "z": "...", "upper": "...", "lower": "...",
    "sigma": "...",
    "target_weight": "0.63",             # <-- sized conviction, string-decimal [0,1]
})
```

**Sized extension — three honest levels:**

1. **Today (no runner change):** stance is LONG/FLAT. `target_weight` rides in
   `detail`. Nothing sizes on it yet; it's observable/testable/plottable.
2. **Interim (runner reads detail):** a sized runner reads
   `detail["target_weight"]` and reconciles to `position ≈ weight * max_qty`,
   entering/trimming/exiting toward it. LONG/FLAT still derivable
   (`weight>0 == LONG`).
3. **First-class (protocol grows a `target`):** when `Decision` gains an optional
   `target: Decimal | None` (target *fraction* of max exposure), we populate it and
   keep `stance` as the derived LONG/FLAT view for legacy readers. **Recommended
   representation:** target as a *fraction of authorized max_qty* (a weight in
   [0,1]), NOT an absolute BTC qty — sizing policy (max exposure) belongs to the
   OMS/risk layer (`authorized_risk` in DESIGN.md), not the pure strategy. The
   strategy says "I want 63% of my risk budget deployed"; the OMS decides what 100%
   is. This keeps the strategy pure and keeps position limits in the guard layer
   where INV-6 lives.

**Reconciliation with sizing is still crash-safe:** the runner reconciles actual →
target every bar, so a missed fill or restart self-heals, exactly as the LONG/FLAT
version does today.

---

## 4. Warm-up and lookahead / repaint safety

**Warm-up.** Emit `Decision(None, {"warming_up": True, ...})` — the same sentinel
SMA uses — until *every* indicator has enough data:
```
need = max(N, M, K+1, J, P*<Wilder settle factor>)
```
Wilder EMAs technically produce a value after `P` bars but only *settle* after
~`3P`; we require a conservative warm count so the first live stance is trustworthy.
Because `None` is a no-op in the runner, a warm-up period **never flattens an
existing position** (documented contract in `base.py`). We warm indicators even
while paused (the runner feeds every closed bar regardless of `running`).

**No lookahead.** Three guarantees:
1. `on_bar` is only called on **closed** bars — the runner feeds `candles[-2]`,
   never the forming `candles[-1]`. The strategy never sees an unfinished bar.
2. Every indicator is a function of bars `≤ t` only. No centered windows, no
   `shift(-1)`, no "future max".
3. Determinism: no clock, no RNG, no I/O. Same bar sequence → identical stances
   (asserted by a determinism test, matching `test_sma.py::test_is_deterministic`).

**No repainting.** Donchian extremes, realized vol, ATR, ADX, and z-score are all
functions of *already-closed* bars; the value stamped at bar `t` is immutable once
`t` closes. There is no indicator here (e.g. no zig-zag, no non-causal filter) that
would revise a past signal.

---

## 5. Overfitting defense

**Parameter count and what each *means* (all economic, none cosmetic):**

| Param | Meaning | How to set WITHOUT curve-fitting |
|---|---|---|
| `N` (Donchian entry) | trend-capture horizon | pick from a horizon prior (e.g. 55 daily bars = classic turtle); do NOT grid-search |
| `M` (Donchian exit) | give-back tolerance | tie to N (e.g. M = N/2 ≈ 20); asymmetric exit is structural, not fit |
| `adx_trend` / `adx_range` | regime boundary + dead-band | standard ADX conventions (25 / 20); the dead-band width is the only judgment call |
| `K` (realized-vol window) | vol estimation horizon | statistical, not fit: long enough to estimate stdev (~20–30), short enough to adapt |
| `J` (z-score window) | range mean horizon | same order as K; can be shared with K to cut a parameter |
| `z_enter` / `z_exit` | how stretched before we fade | ~2.0 / 0.5 stdev — textbook Bollinger, asymmetric for hysteresis |
| `target_vol_per_bar` | risk budget | a *choice*, not an optimization — set by risk appetite, not backtest PnL |
| `P` (Wilder period) | ATR/ADX smoothing | Wilder's canonical 14 |

**Discipline:** every parameter is anchored to a **published convention or a
statistical requirement**, chosen *a priori*, then held fixed. We do **not**
grid-search PnL. The one genuine judgment call is the ADX dead-band; we'd sanity
it, not optimize it. Fewer knobs by design: `J = K` and `M = N/2` collapse two
params. Target count: ~6 free numbers, each interpretable.

**Walk-forward / backtest plan (replay-ready through the pure core):**
- The strategy is a pure function of a bar stream, so backtesting is: feed
  historical OHLCV bars into `on_bar_ohlcv`, collect stances/weights, simulate the
  same target-position reconciliation the live runner does (this is the value of
  the pure core — the replay path and the live path share identical logic).
- **Anchored walk-forward:** choose params on an early in-sample window using
  *priors only* (not optimization), then evaluate out-of-sample on later windows,
  rolling forward. Because we pick by convention, "in-sample" is mostly a sanity
  check that the conventions aren't pathological on this asset, not a fitting stage.
- **Robustness over peak PnL:** report performance across a *neighborhood* of each
  parameter (±1 step). A strategy whose edge evaporates when N goes 55→50 is
  overfit; we want a broad, flat plateau.
- **Cost-inclusive:** every backtest must charge realistic taker fees + slippage
  (§6). A strategy that only works at zero cost is a curve-fit to a fantasy.
- **Determinism test doubles as a replay-integrity test:** same script → same
  events, matching DESIGN.md §8.

**Smells I'm flagging honestly:**
- The mean-reversion overlay is the weakest-justified leg. "Buy dips in a range"
  has real microstructure basis but its *long-only* form (we can't fade rallies) is
  lopsided and may add more whipsaw than edge. I would **ship the trend+regime+vol
  engine first with the MR overlay OFF**, and only enable MR if walk-forward shows
  it helps out-of-sample — otherwise it's two extra parameters (`z_enter/z_exit`)
  buying noise.
- `z_enter=2.0` and ADX `25/20` are conventions, but they *are* still numbers that
  could be quietly tuned. Guard against that by freezing them in code as named
  constants with a comment citing the convention.

---

## 6. Honest expectations

**What is plausibly real vs noise.**
- The **regime gate + vol-targeting** are the parts most likely to be genuinely
  robust — they're risk-management, not return-prediction, and they're supported by
  a large, out-of-sample literature. Even these do not *create* alpha; they reduce
  the drag of trading trend in chop and equalize bet risk.
- The **Donchian trend edge** in crypto is *plausible* (crypto has shown momentum),
  but momentum edges decay as they get crowded and are highly regime-dependent. On
  testnet/paper with no real fills, any positive result is **not evidence of live
  edge** — it's a behavior check.
- The **mean-reversion overlay** is the most likely to be noise (see §5). Treat any
  MR-attributed PnL with maximum suspicion.

**Transaction-cost sensitivity.** This is the make-or-break. Trend/breakout systems
trade infrequently but at the *worst* moments (breakouts = fast markets = wide
spreads/slippage). Mean reversion trades *more often*, so it's far more
cost-sensitive; a 10 bps round-trip cost can erase a plausible MR edge entirely.
**Every backtest MUST include realistic taker fees (Binance spot ~10 bps) plus
slippage**, and we should report break-even cost (the cost at which PnL → 0). The
vol-targeting *helps* here by trimming turnover in high-vol regimes.

**Failure modes (named, so tests can target them):**
1. **Whipsaw in chop** — breakout enters, immediately reverses. Mitigated by the
   ADX gate (don't arm breakouts in chop) and asymmetric N/M. This is *the* trend
   killer; the whole regime gate exists for it.
2. **Trend exhaustion** — we buy the final new high before a reversal (bought the
   top). Donchian exits give some back before flattening; ATR-based sizing means we
   hold *less* into high-vol blowoff tops. Cannot be eliminated — trend following
   is structurally "buy high, sell higher, occasionally buy the top."
3. **Regime boundary flapping** — market sits at ADX≈22 and oscillates
   trend/range. The dead-band (`adx_range < adx_trend`) + regime hysteresis
   (`HOLD` keeps prior regime in the gap) is the defense.
4. **Vol-target instability in dead markets** — `sigma → 0` blows up the weight;
   the `floor` clamp and `[0,1]` clip bound it.
5. **Long-only asymmetry** — in downtrends we can only be FLAT, so we capture none
   of the down move and sit out. If shorting were allowed (see below) this halves.

**What changes if shorting were allowed.** Add a `SHORT` stance. Donchian:
new-M-bar-low arms SHORT symmetrically. Mean reversion: fade rallies short
(`z >= +z_enter → SHORT`). Vol-target sizes both sides. The exit guard's
`never-over-exit` clamp (INV-5, SELL clamped to position) would need a symmetric
buy-to-cover clamp. The strategy stays pure; only the stance enum and the runner's
reconciliation grow a third target. Everything in this doc is written so that
adding SHORT is *additive*, not a rewrite.

**Bottom line.** This is a structurally sound, low-parameter, regime-aware
trend+risk system. On testnet it will demonstrate *correct, honest, replayable
execution of a non-trivial strategy* — which is exactly Sentinel's point. It is
**not** represented as a money-maker, and the mean-reversion leg should be earned
by out-of-sample evidence before it's trusted.
