"""The failure-scripting DSL for the deterministic broker simulator.

A BrokerScript declares, per client order id, exactly how the broker will
misbehave — and WHEN, in virtual steps. Execution time and delivery time are
separate on purpose: that separation is what makes late fills (executed at
step 3, delivered at step 8) and duplicate deliveries expressible.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal


@dataclass(frozen=True, slots=True)
class SubmitBehavior:
    reject_reason: str | None = None
    timeout: bool = False            # raise BrokerTimeout to the caller...
    accept_on_timeout: bool = False  # ...while the broker-side truth ACCEPTS anyway
                                     # (the compound-scenario crux, R1.14)


@dataclass(frozen=True, slots=True)
class CancelBehavior:
    timeout: bool = False
    confirm_after_steps: int = 1     # confirmation delivered N step() calls later
                                     # — the window where fills can still land (R1.6)


@dataclass(frozen=True, slots=True)
class ScheduledFill:
    qty: Decimal
    price: Decimal
    execute_at: int                  # when the broker truth changes
    deliver_at: int | None = None    # when the OMS hears about it (None = same step)


@dataclass(slots=True)
class Redelivery:
    exec_id: str
    at_step: int


@dataclass(slots=True)
class BrokerScript:
    submits: dict[str, SubmitBehavior] = field(default_factory=dict)
    cancels: dict[str, CancelBehavior] = field(default_factory=dict)
    fills: dict[str, list[ScheduledFill]] = field(default_factory=dict)
    redeliveries: list[Redelivery] = field(default_factory=list)

    def on_submit(
        self,
        client_order_id: str,
        *,
        reject: str | None = None,
        timeout: bool = False,
        accept_on_timeout: bool = False,
    ) -> "BrokerScript":
        self.submits[client_order_id] = SubmitBehavior(
            reject_reason=reject, timeout=timeout, accept_on_timeout=accept_on_timeout
        )
        return self

    def on_cancel(
        self,
        client_order_id: str,
        *,
        timeout: bool = False,
        confirm_after_steps: int = 1,
    ) -> "BrokerScript":
        self.cancels[client_order_id] = CancelBehavior(
            timeout=timeout, confirm_after_steps=confirm_after_steps
        )
        return self

    def fill(
        self,
        client_order_id: str,
        *,
        qty: str,
        price: str,
        at_step: int,
        deliver_at_step: int | None = None,
    ) -> "BrokerScript":
        self.fills.setdefault(client_order_id, []).append(
            ScheduledFill(
                qty=Decimal(qty),
                price=Decimal(price),
                execute_at=at_step,
                deliver_at=deliver_at_step,
            )
        )
        return self

    def redeliver(self, exec_id: str, *, at_step: int) -> "BrokerScript":
        """Deliver an already-delivered execution again (reconnect replay)."""
        self.redeliveries.append(Redelivery(exec_id=exec_id, at_step=at_step))
        return self
