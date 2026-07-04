"""Strategy v2: regime-gated Donchian trend + range MR overlay + vol sizing.

Economic thesis (full rationale in docs/strategy-v2-design.md):
  * Donchian breakout  -> trend/momentum engine (buy new N-bar highs).
  * ADX regime gate    -> only arm the trend engine when actually trending;
                          the single highest-value addition over plain SMA.
  * z-score MR overlay -> in RANGE regimes only, buy dips (long-only leg).
  * Vol-targeted size  -> conviction = risk budget / realized vol, clipped
                          to [0, 1]; a risk-normalizer, NOT a return predictor.

Contract fit:
  * `on_bar(close: Decimal) -> Decision` still works (degenerate H=L=C bar).
  * Preferred path: `on_bar_ohlcv(Bar) -> Decision` for correct ATR/ADX/Donchian.
  * Sized conviction rides in `detail["target_weight"]` (string-decimal [0,1]);
    it never changes Decision's required fields, so today's LONG/FLAT runner is
    unaffected. Migration path to a first-class `target` field: see design doc §3.

Purity: no clock, no RNG, no I/O. Same bar stream -> identical stances.
Lookahead: only ever fed CLOSED bars (runner feeds candles[-2]); every indicator
is a function of bars <= t; Donchian/ATR/ADX/z-score cannot repaint.

NOTE ON MATH: indicators are implemented in the simple, obviously-correct windowed
form (O(window)). The design doc notes where an O(1) streaming form exists
(monotonic-deque max/min, Welford running moments); swap in when profiling asks.
Wilder EMAs (ATR/ADX) are already O(1) streaming below.
"""

from __future__ import annotations

import math
from collections import deque
from dataclasses import dataclass
from decimal import Decimal
from enum import Enum

from .base import Bar, Decision, Stance


class Regime(str, Enum):
    TREND = "TREND"
    RANGE = "RANGE"
    HOLD = "HOLD"     # in the ADX dead-band -> keep previous regime (hysteresis)


# --------------------------------------------------------------------------- #
# Parameters — every one anchored to a CONVENTION or a statistical need,       #
# chosen a priori, NOT grid-searched. See design doc §5.                       #
# --------------------------------------------------------------------------- #
@dataclass(frozen=True, slots=True)
class Params:
    donchian_entry: int = 55       # classic turtle trend-capture horizon
    donchian_exit: int = 20        # ~N/2: faster exit than entry (asymmetric)
    wilder_period: int = 14        # Wilder's canonical ATR/ADX period
    adx_trend: float = 25.0        # standard "trending" threshold
    adx_range: float = 20.0        # standard "ranging" threshold (dead-band < trend)
    vol_window: int = 20           # returns for realized-vol estimate (statistical)
    z_window: int = 20             # range mean/stdev horizon (shareable with vol_window)
    z_enter: float = 2.0           # textbook Bollinger stretch to fade
    z_exit: float = 0.5            # asymmetric revert target (hysteresis)
    target_vol_per_bar: float = 0.02   # per-bar return-stdev risk budget (a CHOICE)
    vol_floor: float = 1e-4        # guards divide-by-zero / caps size in dead markets
    enable_mean_reversion: bool = False  # OFF by default: earn it out-of-sample (§5)

    def __post_init__(self) -> None:
        if self.donchian_exit >= self.donchian_entry:
            raise ValueError("donchian_exit must be shorter than donchian_entry")
        if self.adx_range > self.adx_trend:
            raise ValueError("adx_range must be <= adx_trend (dead-band)")
        if self.z_exit >= self.z_enter:
            raise ValueError("z_exit must be < z_enter (hysteresis)")


# --------------------------------------------------------------------------- #
# Wilder EMA — strictly O(1) streaming smoother used by ATR and ADX.           #
# --------------------------------------------------------------------------- #
class _Wilder:
    """Wilder's smoothing: value += (x - value) / period. Seeds on the first
    `period` samples with a simple average, then streams O(1)."""

    def __init__(self, period: int) -> None:
        self._p = period
        self._seed: list[float] = []
        self.value: float | None = None

    def update(self, x: float) -> float | None:
        if self.value is None:
            self._seed.append(x)
            if len(self._seed) == self._p:
                self.value = sum(self._seed) / self._p
            return self.value
        self.value += (x - self.value) / self._p
        return self.value


# --------------------------------------------------------------------------- #
# The strategy                                                                 #
# --------------------------------------------------------------------------- #
class RegimeTrendMR:
    """Regime-gated Donchian trend with an optional range mean-reversion overlay
    and vol-targeted conviction. Matches the `Strategy` protocol: pure, on_bar."""

    def __init__(self, params: Params | None = None) -> None:
        self.p = params or Params()
        self.name = (
            f"regime-trend-mr(N={self.p.donchian_entry}/M={self.p.donchian_exit},"
            f"adx={self.p.adx_range}-{self.p.adx_trend}"
            f"{',mr' if self.p.enable_mean_reversion else ''})"
        )
        # UI chart overlay hooks (runner reads these via getattr; see snapshot()).
        self.fast_period = self.p.donchian_exit
        self.slow_period = self.p.donchian_entry

        # --- ring buffers -------------------------------------------------- #
        self._highs: deque[Decimal] = deque(maxlen=self.p.donchian_entry)
        self._lows: deque[Decimal] = deque(maxlen=self.p.donchian_entry)
        self._closes: deque[Decimal] = deque(maxlen=self.p.z_window)
        self._rets: deque[float] = deque(maxlen=self.p.vol_window)

        # --- Wilder streamers --------------------------------------------- #
        self._atr = _Wilder(self.p.wilder_period)
        self._pdm = _Wilder(self.p.wilder_period)   # +DM
        self._ndm = _Wilder(self.p.wilder_period)   # -DM
        self._adx = _Wilder(self.p.wilder_period)   # ADX = Wilder EMA of DX

        # --- carried state (prev bar for TR/DM; own intended stance) ------- #
        self._prev_high: Decimal | None = None
        self._prev_low: Decimal | None = None
        self._prev_close: Decimal | None = None
        self._bars_seen = 0
        self._regime = Regime.HOLD
        self._in_position = False          # the strategy's OWN intended stance,
        #                                    NOT the broker position (design doc §3)

    def view_spec(self) -> dict:
        """How the terminal renders this strategy: the panel shows the regime,
        trend strength, stretch and sizing conviction; the chart draws the
        Donchian channel it breaks out of."""
        return {
            "rows": [
                {"label": "Regime", "key": "regime", "kind": "regime"},
                {"label": "Trend (ADX)", "key": "trend_strength", "kind": "number"},
                {"label": "z-score", "key": "z", "kind": "signed"},
                {"label": "Target wt", "key": "target_weight", "kind": "weight"},
            ],
            "overlays": [
                {"kind": "band", "upper": "upper", "lower": "lower",
                 "label": f"Donchian {self.p.donchian_entry}/{self.p.donchian_exit}",
                 "color": "#6b7280"},
            ],
        }

    # ------------------------------------------------------------------ #
    # Protocol entry points                                              #
    # ------------------------------------------------------------------ #
    def on_bar(self, close: Decimal) -> Decision:
        """Close-only path: synthesize a degenerate H=L=C bar. Honest but
        discards intrabar range (ATR/ADX degrade to close-to-close moves)."""
        return self.on_bar_ohlcv(Bar(high=close, low=close, close=close))

    def on_bar_ohlcv(self, bar: Bar) -> Decision:
        """Preferred path: true OHLC -> correct ATR/ADX/Donchian. Pure &
        deterministic; must be called ONLY on closed bars."""
        self._bars_seen += 1
        self._ingest(bar)

        need = self._warmup_bars()
        if self._bars_seen < need:
            return Decision(None, {"warming_up": True,
                                   "have": self._bars_seen, "need": need})

        trend_strength = self._adx.value if self._adx.value is not None else 0.0
        self._regime = self._update_regime(trend_strength)

        z = self._zscore(bar.close)
        upper, lower = self._donchian()
        sigma = self._realized_vol()
        target_weight = self._conviction(sigma)

        want_long = self._decide_long(bar.close, upper, lower, z)
        self._in_position = want_long

        stance = Stance.LONG if (want_long and target_weight > 0.0) else Stance.FLAT
        return Decision(stance, {
            "regime": self._regime.value,
            "trend_strength": str(round(trend_strength, 2)),
            "z": str(round(z, 3)),
            "upper": str(upper), "lower": str(lower),
            "sigma": str(round(sigma, 6)),
            "target_weight": str(round(target_weight, 4)),
        })

    # ------------------------------------------------------------------ #
    # Ingest: update every accumulator from the new bar.                 #
    # ------------------------------------------------------------------ #
    def _ingest(self, bar: Bar) -> None:
        if self._prev_close is not None and self._prev_close > 0:
            self._rets.append(math.log(float(bar.close) / float(self._prev_close)))
            self._update_directional(bar)   # ATR/ADX need prev bar -> do before push

        self._highs.append(bar.high)
        self._lows.append(bar.low)
        self._closes.append(bar.close)
        self._prev_high, self._prev_low, self._prev_close = bar.high, bar.low, bar.close

    def _update_directional(self, bar: Bar) -> None:
        """Wilder true-range + directional movement -> ATR, +DI/-DI, DX, ADX.
        All O(1). Requires prev bar (guarded by caller)."""
        ph, pl, pc = float(self._prev_high), float(self._prev_low), float(self._prev_close)
        h, l = float(bar.high), float(bar.low)
        tr = max(h - l, abs(h - pc), abs(l - pc))
        atr = self._atr.update(tr)

        up_move, down_move = h - ph, pl - l
        pdm = up_move if (up_move > down_move and up_move > 0) else 0.0
        ndm = down_move if (down_move > up_move and down_move > 0) else 0.0
        s_pdm = self._pdm.update(pdm)
        s_ndm = self._ndm.update(ndm)

        if atr and atr > 0 and s_pdm is not None and s_ndm is not None:
            pdi = 100.0 * s_pdm / atr
            ndi = 100.0 * s_ndm / atr
            denom = pdi + ndi
            dx = 100.0 * abs(pdi - ndi) / denom if denom > 0 else 0.0
            self._adx.update(dx)

    # ------------------------------------------------------------------ #
    # Indicators (read-only views over current state).                   #
    # ------------------------------------------------------------------ #
    def _donchian(self) -> tuple[Decimal, Decimal]:
        """Upper = max high over the entry window; lower = min low over the exit
        window — BOTH computed EXCLUDING the current bar (the last element), so
        that `close_t >= upper` is a genuine "close above the prior N-bar high"
        breakout rather than the trivially-true `close <= its own bar's high`.
        Uses only closed bars <= t-1 -> cannot repaint. (O(window); monotonic
        deque gives O(1) — see design doc §2.1.)"""
        prior_highs = list(self._highs)[:-1]        # exclude current bar
        prior_lows = list(self._lows)[:-1]
        upper = max(prior_highs)
        lower = min(prior_lows[-self.p.donchian_exit:])
        return upper, lower

    def _realized_vol(self) -> float:
        """Sample stdev of log-returns over vol_window. 0 if <2 samples."""
        rets = list(self._rets)
        if len(rets) < 2:
            return 0.0
        mean = sum(rets) / len(rets)
        var = sum((r - mean) ** 2 for r in rets) / (len(rets) - 1)
        return math.sqrt(var)

    def _zscore(self, close: Decimal) -> float:
        """(close - mean)/stdev over z_window closes. 0 if flat/insufficient."""
        closes = [float(c) for c in self._closes]
        if len(closes) < 2:
            return 0.0
        mean = sum(closes) / len(closes)
        var = sum((c - mean) ** 2 for c in closes) / (len(closes) - 1)
        sd = math.sqrt(var)
        return 0.0 if sd == 0.0 else (float(close) - mean) / sd

    def _conviction(self, sigma: float) -> float:
        """Vol-targeted weight = clip(target_vol / max(sigma, floor), 0, 1).
        A risk-normalizer, not a return predictor. Long/flat & unlevered -> [0,1]."""
        w = self.p.target_vol_per_bar / max(sigma, self.p.vol_floor)
        return max(0.0, min(1.0, w))

    # ------------------------------------------------------------------ #
    # Regime + decision logic.                                           #
    # ------------------------------------------------------------------ #
    def _update_regime(self, trend_strength: float) -> Regime:
        """ADX dead-band with hysteresis: in the gap keep the previous regime."""
        if trend_strength >= self.p.adx_trend:
            return Regime.TREND
        if trend_strength < self.p.adx_range:
            return Regime.RANGE
        # In [adx_range, adx_trend): HOLD -> retain prior regime, don't flap.
        return self._regime if self._regime is not Regime.HOLD else Regime.RANGE

    def _decide_long(self, close: Decimal, upper: Decimal, lower: Decimal,
                     z: float) -> bool:
        """Target-position stance. TREND: Donchian breakout entry / channel exit.
        RANGE (if enabled): buy dips, revert to mean. Otherwise HOLD current
        stance — we do NOT re-chase mid-trend."""
        if self._regime is Regime.TREND:
            if not self._in_position and close >= upper:
                return True                        # breakout entry
            if self._in_position and close <= lower:
                return False                       # channel exit
            return self._in_position               # hold through the trend

        if self._regime is Regime.RANGE and self.p.enable_mean_reversion:
            if not self._in_position and z <= -self.p.z_enter:
                return True                        # buy the dip (long-only leg)
            if self._in_position and z >= -self.p.z_exit:
                return False                       # reverted to the mean
            return self._in_position

        # RANGE with MR disabled, or HOLD: keep whatever we currently intend.
        return self._in_position

    # ------------------------------------------------------------------ #
    def _warmup_bars(self) -> int:
        """Enough bars for EVERY indicator to be trustworthy. Wilder EMAs only
        SETTLE after ~3*period, so we require a conservative count -> the first
        live stance is honest. None stance during warm-up never flattens an
        existing position (base.py contract)."""
        wilder_settle = 3 * self.p.wilder_period
        return max(self.p.donchian_entry + 1,      # +1: channel excludes current bar
                   self.p.vol_window + 1,
                   self.p.z_window, wilder_settle)
