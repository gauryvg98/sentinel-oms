"""Sentinel strategy layer — pure, deterministic target-position rules."""

from .base import Bar, Decision, Stance, Strategy
from .regime import Params, RegimeTrendMR
from .sma import SmaCross

__all__ = ["Bar", "Decision", "Params", "RegimeTrendMR", "SmaCross",
           "Stance", "Strategy"]
