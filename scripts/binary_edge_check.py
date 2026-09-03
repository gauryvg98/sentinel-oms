#!/usr/bin/env python3
"""What lock rate does a given trade geometry need before it is worth money?

    python scripts/binary_edge_check.py --entry 1:3 --hedge 1:2 --vig 0.06
    python scripts/binary_edge_check.py --entry 1:3 --hedge 1:2 --rounds 120 --locks 55

Given the two odds actually traded, prints the fair-market ceiling on the lock
rate. Measure your real lock rate over counted rounds and compare: above the
ceiling is evidence of genuine mispricing, below it means the vig is the story.

With --rounds/--locks it runs the exact binomial test and returns the verdict.
"""

from __future__ import annotations

import argparse
import sys
from decimal import Decimal
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from sentinel.strategy.binary import (BankrollPolicy, FeeModel, LegLedger,  # noqa: E402
                                      RoundRecord, fair_ev, fair_naked_win_rate,
                                      max_fair_lock_rate, payoffs_from_stats,
                                      price_from_odds, risk_of_ruin)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--entry", default="1:3", help="odds paid on the FIRST leg")
    ap.add_argument("--hedge", default="1:2", help="odds paid on the HEDGE leg")
    ap.add_argument("--vig", default="0.025",
                    help="overround (S - 1); World's makers embed a 2-3%% spread")
    ap.add_argument("--bankroll", default="20")
    ap.add_argument("--min-stake", default="1")
    ap.add_argument("--rounds", type=int, default=0)
    ap.add_argument("--locks", type=int, default=0)
    a = ap.parse_args()

    p1, p2, vig = price_from_odds(a.entry), price_from_odds(a.hedge), Decimal(a.vig)
    S = p1 + p2
    barrier = (Decimal(1) + vig) - p2
    ceiling = max_fair_lock_rate(p1, barrier)

    print(f"  entry {a.entry:>5} = {p1:.4f}      hedge {a.hedge:>5} = {p2:.4f}")
    print(f"  S = {S:.4f}   ->  {(1 - S) / S:+.1%} on capital deployed, once BOTH legs are on")
    print(f"  the entry leg must rally to {barrier:.4f} before that hedge is offered")
    print()
    print(f"  FAIR-MARKET CEILING ON LOCK RATE: {ceiling:.1%}")
    print(f"  -> above {ceiling:.1%} of entries reaching a lock = real edge")
    print(f"  -> at exactly the ceiling, EV = {fair_ev(ceiling, vig, p1):+.1%} per $1 staked")

    if a.rounds:
        lg = LegLedger()
        for i in range(a.rounds):
            locked = i < a.locks
            lg.record(RoundRecord(f"r{i}", p1, barrier, Decimal(100), locked,
                                  won=locked, hedge_price=p2 if locked else None,
                                  pnl=Decimal(0)))
        v = lg.edge_vs_fair()
        print()
        print(f"  measured: {v.locks}/{v.rounds} locked = {v.lock_rate:.1%}"
              f"   (ceiling {v.fair_ceiling:.1%}, excess {v.excess:+.1%})")
        print(f"  p-value : {v.p_value:.4f}")
        print(f"  VERDICT : {v.verdict}")

        w = max(Decimal(0), fair_naked_win_rate(p1, v.lock_rate, barrier))
        dist = payoffs_from_stats(lock_rate=v.lock_rate, naked_win_rate=w,
                                  entry_price=p1, hedge_price=p2)
        pol = BankrollPolicy(bankroll=Decimal(a.bankroll),
                             min_stake=Decimal(a.min_stake))
        stake = pol.stake(dist)
        print()
        print(f"  on ${a.bankroll} at the fair-market baseline: stake ${stake:.2f}"
              f"{'  (no edge -> sit out)' if stake == 0 else ''}")
        if stake > 0:
            ror = risk_of_ruin(dist, bankroll=Decimal(a.bankroll), stake=stake,
                               floor=Decimal(a.bankroll) * Decimal("0.3"),
                               rounds=288)
            print(f"  risk of ruin over one day (288 rounds): {ror:.1%}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
