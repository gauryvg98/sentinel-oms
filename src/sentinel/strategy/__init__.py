"""Sentinel strategy layer — pure, deterministic target-position rules."""

from .base import Decision, Stance, Strategy
from .sma import SmaCross

__all__ = ["Decision", "SmaCross", "Stance", "Strategy"]
