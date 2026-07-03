"""Sentinel strategy layer — pure, deterministic decision rules."""

from .base import Decision, Signal, Strategy
from .sma import SmaCross

__all__ = ["Decision", "Signal", "SmaCross", "Strategy"]
