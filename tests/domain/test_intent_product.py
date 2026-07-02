"""Intent and product-definition invariants."""

from dataclasses import FrozenInstanceError
from decimal import Decimal
from uuid import uuid4

import pytest

from sentinel.domain import (
    Authority,
    EconomicOrderIntent,
    Exercise,
    ProductDefinition,
    Settlement,
    Side,
)


def make_intent(**overrides):
    defaults = dict(
        intent_id=uuid4(),
        idempotency_key="client-key-1",
        instrument="IDX-OPT-2026-07-17-C5000",
        side=Side.BUY,
        qty=Decimal("4"),
        limit_price=Decimal("4.20"),
        authority=Authority.ENTRY,
        trace_id=uuid4(),
    )
    defaults.update(overrides)
    return EconomicOrderIntent(**defaults)


def test_intent_is_immutable():
    intent = make_intent()
    with pytest.raises(FrozenInstanceError):
        intent.qty = Decimal("8")  # type: ignore[misc]


def test_intent_rejects_nonpositive_qty():
    with pytest.raises(ValueError):
        make_intent(qty=Decimal("0"))
    with pytest.raises(ValueError):
        make_intent(qty=Decimal("-1"))


def test_intent_rejects_nonpositive_limit():
    with pytest.raises(ValueError):
        make_intent(limit_price=Decimal("0"))


def test_intent_requires_idempotency_key():
    with pytest.raises(ValueError):
        make_intent(idempotency_key="")


def test_market_intent_allowed():
    assert make_intent(limit_price=None).limit_price is None


def test_exit_authority_is_first_class():
    """R1.13: protective exits carry their own authority through the system."""
    assert make_intent(authority=Authority.PROTECTIVE_EXIT).authority \
        is Authority.PROTECTIVE_EXIT


# ------------------------------------------------------------------ products


def cash_european_index(symbol="IDX", multiplier=100) -> ProductDefinition:
    return ProductDefinition(
        symbol=symbol,
        settlement=Settlement.CASH,
        exercise=Exercise.EUROPEAN,
        multiplier=multiplier,
    )


def physical_american_etf(symbol="ETF") -> ProductDefinition:
    return ProductDefinition(
        symbol=symbol,
        settlement=Settlement.PHYSICAL,
        exercise=Exercise.AMERICAN,
        multiplier=100,
    )


def test_european_cash_product_has_no_early_assignment_or_delivery_risk():
    p = cash_european_index()
    assert not p.early_assignment_possible
    assert not p.has_delivery_risk


def test_american_physical_product_carries_both_risks():
    """R4: the behavioral difference between product families is data the
    domain exposes — risk paths exist for one family and not the other."""
    p = physical_american_etf()
    assert p.early_assignment_possible
    assert p.has_delivery_risk


def test_mini_contract_is_configuration_not_special_case():
    mini = cash_european_index(symbol="IDX-MINI", multiplier=10)
    big = cash_european_index()
    assert mini.multiplier * 10 == big.multiplier * 1  # 1/10 notional, same semantics
    assert mini.settlement is big.settlement and mini.exercise is big.exercise


def test_product_definition_is_immutable_and_validated():
    p = cash_european_index()
    with pytest.raises(FrozenInstanceError):
        p.multiplier = 1  # type: ignore[misc]
    with pytest.raises(ValueError):
        ProductDefinition(
            symbol="X",
            settlement=Settlement.CASH,
            exercise=Exercise.EUROPEAN,
            multiplier=0,
        )
