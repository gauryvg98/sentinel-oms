"""Delta Exchange India perpetuals (testnet-first) adapter."""

from .adapter import (
    PROD_BASE,
    PROD_WS,
    TESTNET_BASE,
    TESTNET_WS,
    DeltaFuturesAdapter,
    parse_delta_events,
)

__all__ = [
    "DeltaFuturesAdapter",
    "parse_delta_events",
    "TESTNET_BASE",
    "TESTNET_WS",
    "PROD_BASE",
    "PROD_WS",
]
