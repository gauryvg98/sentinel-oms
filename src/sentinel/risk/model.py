"""Risk-based position sizing — the money layer, separate from alpha.

The strategy says WHICH way and HOW convinced (stance + conviction). This says
HOW MUCH capital that's worth: size the position so a stop-out costs a fixed
fraction of equity, and never let notional exceed a leverage cap. Size is thus
tied to WHERE the stop is (volatility), not to a flat notional — the single
thing most naive bots get wrong.

    risk_amount = risk_pct · equity · conviction        (what we're willing to lose)
    stop_dist   = stop_atr_mult · ATR   (or fallback%)  (where the thesis breaks)
    qty         = risk_amount / stop_dist               (so a stop-out ≈ risk_amount)
    qty         = min(qty, equity · max_leverage / px)  (hard leverage cap)
    qty         = min(qty, margin_qty_cap)              (closed-loop margin clamp)
    qty         = min(qty, bracket_qty_cap)             (Binance leverage-bracket cap)
    qty         = min(qty, avail_qty_cap)               (real availableBalance clamp)

The margin_qty_cap line is what makes sizing CLOSED-loop against our OWN estimate
of the still-free equity share. avail_qty_cap tightens it to the exchange's REAL
availableBalance (net of every other bot's margin and unrealized loss) so we
never ask the exchange for more initial margin than truly exists (-2019 Margin is
insufficient).

Pure and unsigned — the runner applies the LONG/SHORT sign. No I/O, no clock, so
every rule is unit-testable before any feed or broker exists.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal


@dataclass(frozen=True)
class RiskParams:
    risk_pct: Decimal          # equity fraction risked per trade at full conviction
    max_leverage: Decimal      # cap: |notional| <= equity * max_leverage
    stop_atr_mult: Decimal     # stop distance = stop_atr_mult * ATR
    fallback_stop_pct: Decimal  # if ATR is unavailable: stop = fallback_stop_pct * price
    rr: Decimal = Decimal("2")  # reward:risk — take-profit distance = rr * stop.
    #                             rr <= 0 disables the fixed take-profit entirely:
    #                             a trend-follower then rides to its own signal
    #                             flip (opposite cross), protected only by the stop.
    trail: bool = False        # ratcheting trail: the stop follows the peak at
    #                            stop_dist and only ever TIGHTENS; no fixed TP
    #                            (the trail is the profit-taker — gains run).


def atr(candles: list[dict], period: int = 14) -> Decimal | None:
    """Average True Range over the last `period` closed bars (candles are
    {t,o,h,l,c}). None until there are enough bars — the caller then falls back
    to a fixed-% stop rather than sizing on a half-warm indicator."""
    if not candles or len(candles) < period + 1:
        return None
    window = candles[-period - 1:]
    trs = []
    prev_close = Decimal(str(window[0]["c"]))
    for c in window[1:]:
        high, low, close = (Decimal(str(c["h"])), Decimal(str(c["l"])),
                            Decimal(str(c["c"])))
        trs.append(max(high - low, abs(high - prev_close), abs(low - prev_close)))
        prev_close = close
    return sum(trs) / len(trs) if trs else None


def stop_distance(params: RiskParams, price: Decimal,
                  atr_value: Decimal | None) -> Decimal:
    """Price distance to the stop: ATR-scaled when the ATR is warm, else a fixed
    fraction of price."""
    if atr_value and atr_value > 0:
        return params.stop_atr_mult * atr_value
    return params.fallback_stop_pct * price


def brackets(entry: Decimal, is_long: bool, stop_dist: Decimal,
             rr: Decimal) -> tuple[Decimal, Decimal | None]:
    """Protective (stop_loss, take_profit) PRICES for a position entered at
    `entry`. The stop is `stop_dist` on the losing side; the target is `rr ×
    stop_dist` on the winning side. Long stops below / targets above; short is
    the mirror. When `rr <= 0` there is NO take-profit (returns None) — the
    thesis exits on its own signal, not at a fixed multiple. Pure."""
    take_dist = rr * stop_dist if rr > 0 else None
    if is_long:
        take = entry + take_dist if take_dist is not None else None
        return entry - stop_dist, take
    take = entry - take_dist if take_dist is not None else None
    return entry + stop_dist, take


def breached(is_long: bool, mark: Decimal, stop: Decimal,
             take: Decimal | None) -> str | None:
    """Which bracket the mark has hit, if any: 'STOP', 'TAKE', or None. For a
    long, price falling to/through the stop or rising to/through the take; short
    mirrors. A None `take` means take-profit is disabled — only the stop can
    fire, so a winner rides until the strategy itself flips."""
    if is_long:
        if mark <= stop:
            return "STOP"
        if take is not None and mark >= take:
            return "TAKE"
    else:
        if mark >= stop:
            return "STOP"
        if take is not None and mark <= take:
            return "TAKE"
    return None


def risk_sized_qty(params: RiskParams, *, equity: Decimal, price: Decimal,
                   stop_dist: Decimal | None,
                   conviction: Decimal = Decimal(1),
                   margin_qty_cap: Decimal | None = None,
                   bracket_qty_cap: Decimal | None = None,
                   avail_qty_cap: Decimal | None = None) -> Decimal:
    """Base-asset quantity so that hitting the stop (`stop_dist` price units away)
    loses about `risk_pct · conviction` of equity — capped by max leverage.
    Takes the stop distance directly so sizing and the SL use the SAME stop,
    whether it's ATR-derived or the strategy's own geometry. Unsigned.

    `margin_qty_cap` is the OPTIONAL closed-loop clamp: the most qty the caller's
    actually-free margin can carry (position + resting-order margin already
    deducted upstream). None keeps the function pure open-loop — same result as
    before the clamp existed — so the risk math stays testable in isolation.
    A zero/negative cap means no margin headroom: size to zero, never reject.

    `bracket_qty_cap` is the OPTIONAL Binance leverage-bracket clamp: the most
    qty whose notional stays under the exchange's per-symbol notionalCap for our
    configured leverage (the caller derives it as cap·SAFETY/price). Sizing past
    it is the -2027 rejection ('Exceeded the maximum allowable position at
    current leverage'), so this makes that structurally impossible. None leaves
    sizing unchanged — pure/backtest-safe, like margin_qty_cap.

    `avail_qty_cap` is the OPTIONAL real-availableBalance clamp: the most qty
    whose initial margin fits inside the settlement asset's actual exchange
    `availableBalance` × SAFETY (caller derives it as avail·SAFETY·leverage/price).
    Unlike margin_qty_cap — our per-bot ESTIMATE of the free equity share — this
    is the exchange's OWN net-of-everything figure, so an entry under it cannot be
    refused for margin (-2019). None leaves sizing unchanged, like the others."""
    if equity <= 0 or price <= 0 or stop_dist is None or stop_dist <= 0:
        return Decimal(0)
    conviction = max(Decimal(0), min(Decimal(1), conviction))
    qty = (params.risk_pct * equity * conviction) / stop_dist
    lev_cap_qty = (equity * params.max_leverage) / price   # never over-lever
    qty = min(qty, lev_cap_qty)
    if margin_qty_cap is not None:
        qty = min(qty, max(Decimal(0), margin_qty_cap))
    if bracket_qty_cap is not None:
        qty = min(qty, max(Decimal(0), bracket_qty_cap))
    if avail_qty_cap is not None:
        qty = min(qty, max(Decimal(0), avail_qty_cap))
    return qty


def trail_ratchet(is_long: bool, entry: Decimal, price: Decimal,
                  stop_dist: Decimal, hwm: Decimal | None,
                  prev_stop: Decimal | None) -> tuple[Decimal, Decimal]:
    """Ratcheting trailing stop. Track the position's best price (high-water
    mark for longs, low-water for shorts) and keep the stop `stop_dist` behind
    it — MONOTONIC: the stop may only tighten, never loosen, even if stop_dist
    widens later. Floor/ceiling at the entry-anchored initial stop, so it is
    never looser than the static bracket would have been. Returns
    (new_watermark, new_stop). Pure."""
    if is_long:
        wm = price if hwm is None else max(hwm, price)
        stop = max(entry - stop_dist, wm - stop_dist)
        if prev_stop is not None:
            stop = max(stop, prev_stop)          # ratchet: up only
    else:
        wm = price if hwm is None else min(hwm, price)
        stop = min(entry + stop_dist, wm + stop_dist)
        if prev_stop is not None:
            stop = min(stop, prev_stop)          # ratchet: down only
    return wm, stop
