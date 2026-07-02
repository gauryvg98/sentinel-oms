"""Replay acceptance evidence (SOW Section 4 / R4):
future-data rejection · lifetime gates · determinism · product differences."""

from decimal import Decimal

import pytest

from sentinel.domain import Exercise, ProductDefinition, Settlement
from sentinel.replay import (
    ContractLifetimeViolation,
    FutureDataDenied,
    ListedContract,
    ReplayClock,
    ReplayDataAccess,
    Tick,
)

CASH_EU_INDEX = ProductDefinition(
    symbol="IDX", settlement=Settlement.CASH, exercise=Exercise.EUROPEAN,
    multiplier=100,
)
MINI_INDEX = ProductDefinition(
    symbol="IDX-MINI", settlement=Settlement.CASH, exercise=Exercise.EUROPEAN,
    multiplier=10,
)
PHYS_AM_ETF = ProductDefinition(
    symbol="ETF", settlement=Settlement.PHYSICAL, exercise=Exercise.AMERICAN,
    multiplier=100,
)


def contract(symbol="IDX-C5000", product=CASH_EU_INDEX, listed=100, delisted=1000):
    return ListedContract(
        symbol=symbol, product=product, listed_at=listed, delisted_at=delisted
    )


def ticks(*pairs):
    return [Tick(ts=ts, price=Decimal(p)) for ts, p in pairs]


def rig(now=500):
    clock = ReplayClock(start=now)
    data = ReplayDataAccess(clock)
    data.load(contract(), ticks((150, "4.10"), (300, "4.20"), (450, "4.30"),
                                (600, "4.40")))
    return clock, data


# ---------------------------------------------------- future-data rejection


def test_read_beyond_clock_is_rejected():
    _, data = rig(now=500)
    with pytest.raises(FutureDataDenied):
        data.read("IDX-C5000", as_of=501)


def test_read_at_exactly_now_is_allowed():
    _, data = rig(now=500)
    assert [t.ts for t in data.read("IDX-C5000", as_of=500)] == [150, 300, 450]


def test_advancing_the_clock_reveals_data_incrementally():
    clock, data = rig(now=500)
    assert data.latest("IDX-C5000", as_of=500).price == Decimal("4.30")
    clock.advance_to(600)
    assert data.latest("IDX-C5000", as_of=600).price == Decimal("4.40")


def test_clock_never_goes_backward():
    clock, _ = rig(now=500)
    with pytest.raises(ValueError):
        clock.advance_to(499)


def test_no_api_shape_returns_future_ticks():
    """Even with as_of at the clock, ticks beyond as_of are structurally
    absent from every read result."""
    clock, data = rig(now=1000)                    # clock far ahead
    result = data.read("IDX-C5000", as_of=300)     # but we ask as-of 300
    assert [t.ts for t in result] == [150, 300]    # nothing newer leaks


# --------------------------------------------------------- lifetime gates


def test_contract_not_queryable_before_listing():
    clock, data = rig(now=500)
    with pytest.raises(ContractLifetimeViolation):
        data.read("IDX-C5000", as_of=99)           # listed at 100


def test_contract_not_queryable_after_delisting():
    clock, data = rig(now=5000)
    with pytest.raises(ContractLifetimeViolation):
        data.read("IDX-C5000", as_of=1001)         # delisted at 1000


def test_loading_ticks_outside_lifetime_is_refused():
    data = ReplayDataAccess(ReplayClock(start=0))
    with pytest.raises(ContractLifetimeViolation):
        data.load(contract(), ticks((99, "1.00")))  # before listing


# ------------------------------------------------------------ determinism


def test_repeated_replay_is_deterministic():
    """Two independent replays over the same data and clock walk produce
    identical observation sequences — nothing else exists in the path."""

    def run():
        clock, data = rig(now=100)
        observed = []
        for t in (150, 300, 450, 600):
            clock.advance_to(t)
            observed.append(data.latest("IDX-C5000", as_of=t))
        return observed

    assert run() == run()


# ---------------------------------------------- first-class product config


def test_index_vs_etf_differences_are_product_data():
    """Settlement and exercise-style differences live on the product
    definition — consuming code reads flags, never symbol names."""
    idx = contract(symbol="IDX-C5000", product=CASH_EU_INDEX)
    etf = contract(symbol="ETF-C500", product=PHYS_AM_ETF)

    assert not idx.product.early_assignment_possible   # European: never early
    assert not idx.product.has_delivery_risk           # cash settles
    assert etf.product.early_assignment_possible       # American: real risk path
    assert etf.product.has_delivery_risk               # physical delivery


def test_mini_contract_is_pure_configuration():
    mini = contract(symbol="MINI-C5000", product=MINI_INDEX)
    big = contract(symbol="IDX-C5000", product=CASH_EU_INDEX)
    assert mini.product.multiplier * 10 == big.product.multiplier
    assert mini.product.settlement is big.product.settlement
    assert mini.product.exercise is big.product.exercise


def test_replay_serves_multiple_contracts_independently():
    clock = ReplayClock(start=500)
    data = ReplayDataAccess(clock)
    data.load(contract(symbol="A", listed=100, delisted=400),
              ticks((150, "1.00")))
    data.load(contract(symbol="B", listed=100, delisted=1000),
              ticks((150, "2.00"), (450, "2.10")))

    assert data.latest("B", as_of=450).price == Decimal("2.10")
    with pytest.raises(ContractLifetimeViolation):
        data.read("A", as_of=450)                  # A died at 400
