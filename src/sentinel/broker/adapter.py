"""BrokerAdapter — the boundary a real broker integration must satisfy.

Everything the OMS knows about brokers is this interface. The simulator lives
strictly behind it; a Schwab/IBKR adapter is a drop-in replacement.

Semantics that shape the whole system:
- ``submit`` may raise BrokerTimeout, which carries NO broker order id. That
  is the UNKNOWN trigger (R1.3): the order may or may not exist at the broker.
- ``query_order`` looks up by CLIENT order id — the reconciliation primitive.
  Returning None means "conclusively absent": the broker never accepted it.
- The event stream delivers fills and cancel confirmations. Delivery is
  at-least-once and may be late or duplicated; the ledger's exec_id dedup is
  what makes that safe (R1.7).
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from enum import Enum
from typing import AsyncIterator, Protocol

from sentinel.domain import Side


class BrokerError(Exception):
    pass


class BrokerReject(BrokerError):
    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


class BrokerTimeout(BrokerError):
    """No response and no broker order id. NOT a rejection — the submission
    outcome is unprovable until reconciliation asks the broker."""


class BrokerOrderState(str, Enum):
    WORKING = "WORKING"
    PARTIAL = "PARTIAL"
    FILLED = "FILLED"
    CANCELED = "CANCELED"
    REJECTED = "REJECTED"


@dataclass(frozen=True, slots=True)
class BrokerFill:
    client_order_id: str
    exec_id: str
    qty: Decimal
    price: Decimal


@dataclass(frozen=True, slots=True)
class BrokerCancelConfirmed:
    client_order_id: str


@dataclass(frozen=True, slots=True)
class BrokerBalanceUpdate:
    """Account balances pushed by the broker after a trade. PARTIAL by nature
    — only changed assets are included — so consumers MERGE, never replace."""

    balances: dict[str, Decimal]


# Order-lifecycle events route to the OMS engine; account events (balances)
# are runtime state, routed straight to the app. See SentinelApp._event_apply.
OrderStreamEvent = BrokerFill | BrokerCancelConfirmed
BrokerEvent = OrderStreamEvent | BrokerBalanceUpdate


@dataclass(frozen=True, slots=True)
class BrokerPosition:
    """An actual open position on the exchange — signed base qty (negative =
    short) and the exchange's entry price. The source of truth for POSITION
    reconciliation (query_positions returns wallet BALANCES, not positions).

    liq_price / mark_price are the venue's own liquidation and mark (for the UI's
    liquidation display); optional because reconciliation doesn't need them and
    not every venue supplies them."""

    qty: Decimal
    entry_price: Decimal
    liq_price: Decimal | None = None
    mark_price: Decimal | None = None


@dataclass(frozen=True, slots=True)
class BrokerOrderView:
    """The broker's authoritative view, as returned by reconciliation queries.
    ``fills`` carries every execution so a recovering OMS can backfill missed
    ones — exec_id dedup makes re-applying them exactly-once."""

    client_order_id: str
    broker_order_id: str
    state: BrokerOrderState
    filled_qty: Decimal
    fills: tuple[BrokerFill, ...]


class BrokerAdapter(Protocol):
    async def submit(
        self,
        *,
        client_order_id: str,
        instrument: str,
        side: Side,
        qty: Decimal,
        limit_price: Decimal | None,
        stop_price: Decimal | None = None,
    ) -> str:
        """Returns broker_order_id. Raises BrokerReject or BrokerTimeout.

        ``stop_price`` set (with ``limit_price`` None) means a reduce-only
        stop-market order resting at the venue — the exchange-native hard-stop
        backstop. Venues without stop support raise BrokerReject."""
        ...

    async def cancel(self, client_order_id: str) -> None:
        """Requests cancellation; confirmation arrives on the event stream.
        Raises BrokerTimeout if the request outcome is unprovable."""
        ...

    async def query_order(self, client_order_id: str) -> BrokerOrderView | None:
        """Authoritative lookup by client order id. None = conclusively absent."""
        ...

    async def query_positions(self) -> dict[str, Decimal]:
        ...

    async def open_positions(self) -> dict[str, BrokerPosition]:
        """Actual open positions on the exchange, keyed by instrument. Default
        empty — venues without a position concept (spot) or a query need not
        implement it; POSITION reconciliation simply skips them."""
        return {}

    async def max_notional(self, symbol: str) -> Decimal | None:
        """Largest position notional this symbol may hold at our configured
        leverage — the Binance leverage-bracket cap (crossing it is the -2027
        rejection). None = unknown / not enforced (fail-open: no extra clamp).

        Optional, like open_positions: only leveraged futures venues implement
        it. Callers reach it via getattr(broker, "max_notional", None) so
        spot/sim/other adapters that don't define it simply skip the clamp."""
        return None

    async def available_balance(self, asset: str) -> Decimal | None:
        """The exchange's REAL free margin for `asset` (already net of all posted
        margin, resting-order margin and unrealized loss) — Binance's
        `availableBalance`. Clamping an entry so its initial margin fits under
        this makes -2019 'Margin is insufficient' structurally impossible. None =
        unknown / not enforced (fail-open: no availability clamp).

        Optional, like max_notional: only leveraged futures venues implement it.
        Callers reach it via getattr(broker, "available_balance", None) so
        spot/sim adapters that don't define it simply skip the clamp."""
        return None

    def events(self) -> AsyncIterator[BrokerEvent]:
        ...
