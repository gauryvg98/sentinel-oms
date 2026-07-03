"""Protective-exit supervisor tests — coverage math, idempotency, independence
from entry authority, and post-recovery re-arming."""

from __future__ import annotations

from decimal import Decimal
from uuid import uuid4

import pytest


from sentinel.broker.sim import BrokerScript, ScriptedBroker
from sentinel.domain import Authority, EconomicOrderIntent, OrderState, Side
from sentinel.ledger import LedgerStore
from sentinel.oms import CommandGateway, InstrumentHeld, OrderEngine, WriterCoordinator
from sentinel.protect import ProtectiveExitSupervisor
from sentinel.recon import Reconciler


def intent(key="K1", qty="4", side=Side.BUY, authority=Authority.ENTRY,
           instrument="IDX-OPT"):
    return EconomicOrderIntent(
        intent_id=uuid4(),
        idempotency_key=key,
        instrument=instrument,
        side=side,
        qty=Decimal(qty),
        limit_price=Decimal("4.20"),
        authority=authority,
        trace_id=uuid4(),
    )


def rig(pool, script: BrokerScript | None = None):
    store = LedgerStore(pool)
    sim = ScriptedBroker(script or BrokerScript())
    coord = WriterCoordinator()
    engine = OrderEngine(store, sim, coord)
    gateway = CommandGateway(store, engine)
    recon = Reconciler(store, sim, coord)
    protect = ProtectiveExitSupervisor(store, engine)
    return store, sim, engine, gateway, recon, protect


async def pump(sim, engine, steps=1):
    for _ in range(steps):
        sim.step()
        for event in sim.take_events():
            await engine.on_broker_event(event)


async def build_position(pool, script=None, qty="3"):
    script = script or BrokerScript()
    script.fill("K1", qty=qty, price="4.20", at_step=1)
    parts = rig(pool, script)
    _, sim, engine, gateway, _, _ = parts
    await gateway.place(uuid4(), intent(key="K1", qty=qty))
    await pump(sim, engine)
    return parts


# ------------------------------------------------------------- coverage math


async def test_uncovered_position_gets_full_exit(pool):
    store, _, _, _, _, protect = await build_position(pool, qty="3")
    report = await protect.ensure_protection()

    assert len(report.placed) == 1
    exit_order = report.placed[0]
    assert exit_order.authority == "PROTECTIVE_EXIT"
    assert exit_order.core.qty == 3
    assert exit_order.core.state is OrderState.WORKING


async def test_rerun_places_nothing_when_covered(pool):
    """Idempotency by math: coverage exists -> zero new orders, no flags."""
    _, _, _, _, _, protect = await build_position(pool)
    await protect.ensure_protection()
    second = await protect.ensure_protection()
    assert second.placed == []
    assert second.already_covered == ["IDX-OPT"]


async def test_partial_coverage_is_topped_up_exactly(pool):
    store, sim, engine, gateway, _, protect = await build_position(pool, qty="3")
    await gateway.place(          # manual exit covering 1 of 3
        uuid4(),
        intent(key="X1", qty="1", side=Side.SELL,
               authority=Authority.PROTECTIVE_EXIT),
    )
    report = await protect.ensure_protection()
    assert len(report.placed) == 1
    assert report.placed[0].core.qty == 2          # tops up to exactly 3


async def test_short_position_gets_buy_exit(pool):
    script = BrokerScript()
    script.fill("S1", qty="2", price="4.20", at_step=1)
    store, sim, engine, gateway, _, protect = rig(pool, script)
    await gateway.place(uuid4(), intent(key="S1", qty="2", side=Side.SELL))
    await pump(sim, engine)                        # position: -2

    report = await protect.ensure_protection()
    assert report.placed[0].side == "BUY"          # exits close, never extend
    assert report.placed[0].core.qty == 2


async def test_closed_position_needs_no_protection(pool):
    """Exit fills flatten the position; the next pass finds nothing to do."""
    script = BrokerScript()
    script.fill("K1", qty="2", price="4.20", at_step=1)
    store, sim, engine, gateway, _, protect = rig(pool, script)
    await gateway.place(uuid4(), intent(key="K1", qty="2"))
    await pump(sim, engine)

    report = await protect.ensure_protection()
    prot_key = report.placed[0].core.client_order_id
    # Script the protective exit filling completely: position 2 - 2 = 0.
    sim._script.fill(prot_key, qty="2", price="4.50", at_step=sim.current_step + 1)
    await pump(sim, engine)

    assert await store.get_position("IDX-OPT") == 0
    final = await protect.ensure_protection()
    assert final.placed == [] and final.already_covered == []


# -------------------------------------------------------------------- R1.13


async def test_protection_flows_while_instrument_is_held(pool):
    """The whole point of independent authority: an UNKNOWN entry holds the
    instrument, entries are refused — protection still arms."""
    script = BrokerScript()
    script.fill("K1", qty="3", price="4.20", at_step=1)
    script.on_submit("K2", timeout=True, accept_on_timeout=True)
    store, sim, engine, gateway, _, protect = rig(pool, script)

    await gateway.place(uuid4(), intent(key="K1", qty="3"))
    await pump(sim, engine)
    await gateway.place(uuid4(), intent(key="K2"))     # UNKNOWN -> held

    with pytest.raises(InstrumentHeld):
        await gateway.place(uuid4(), intent(key="K3"))  # entries: blocked

    report = await protect.ensure_protection()          # exits: flow
    assert len(report.placed) == 1
    assert report.placed[0].core.state is OrderState.WORKING


# ------------------------------------------------- post-recovery re-arming


async def test_recovery_then_rearm_restores_protection(pool):
    """The R1.14 tail: crash with a position and no live protection ->
    startup recovery converges the book -> ensure_protection re-arms
    exactly the exposed quantity."""
    script = BrokerScript()
    script.on_submit("K1", timeout=True, accept_on_timeout=True)
    script.fill("K1", qty="3", price="4.20", at_step=1)
    store, sim, engine, gateway, recon, protect = rig(pool, script)

    await gateway.place(uuid4(), intent(key="K1", qty="4"))   # -> UNKNOWN
    sim.step()
    sim.take_events()                                # fills 3, never delivered

    # Crash: corrupt projections, fresh objects, same DB + broker truth.
    await pool.execute("UPDATE positions SET qty = 0")
    store2 = LedgerStore(pool)
    coord2 = WriterCoordinator()
    engine2 = OrderEngine(store2, sim, coord2)
    recon2 = Reconciler(store2, sim, coord2)
    protect2 = ProtectiveExitSupervisor(store2, engine2)

    report = await recon2.startup_recovery()
    assert report.resolved_states["K1"] is OrderState.PARTIAL
    assert await store2.get_position("IDX-OPT") == 3

    rearm = await protect2.ensure_protection()
    assert len(rearm.placed) == 1
    assert rearm.placed[0].core.qty == 3             # exactly the exposure
    assert rearm.placed[0].core.state is OrderState.WORKING
