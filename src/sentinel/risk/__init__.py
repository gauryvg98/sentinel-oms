"""Risk layer — position sizing and (later) protective exits, portfolio limits.

Separate from alpha (strategies): strategies propose direction + geometry, this
layer decides the money — size, leverage — and enforces protection.
"""

from .model import RiskParams, atr, risk_sized_qty, stop_distance

__all__ = ["RiskParams", "atr", "risk_sized_qty", "stop_distance"]
