"""Simulator behavior tests — every failure mode the scenarios depend on,
proven reproducible and deterministic."""

from decimal import Decimal

import pytest

from sentinel.broker import (
    BrokerCancelConfirmed,
    BrokerFill,
    BrokerOrderState,
    BrokerReject,
    BrokerTimeout,
)
from sentinel.broker.sim import BrokerScript, ScriptedBroker
from sentinel.domain import Side


async def submit(sim: ScriptedBroker, key="K1", qty="4"):
    return await sim.submit(
        client_order_id=key,
        instrument="IDX-OPT",
        side=Side.BUY,
        qty=Decimal(qty),
        limit_price=Decimal("4.20"),
    )


# ---------------------------------------------------------------- happy path


async def test_submit_ack_fill_to_completion():
    script = BrokerScript()
    script.fill("K1", qty="2", price="4.20", at_step=1)
    script.fill("K1", qty="2", price="4.25", at_step=2)
    sim = ScriptedBroker(script)

    broker_id = await submit(sim)
    assert broker_id == "SIM-1"

    sim.step()
    sim.step()
    events = sim.take_events()
    assert [type(e) for e in events] == [BrokerFill, BrokerFill]
    assert events[0].exec_id == "K1-E1" and events[1].exec_id == "K1-E2"

    view = await sim.query_order("K1")
    assert view.state is BrokerOrderState.FILLED and view.filled_qty == 4


async def test_stop_order_rests_working_and_cancels_normally():
    """A stop-market submit (stop_price set, no limit) just RESTS: WORKING
    until a scripted fill (the trigger firing) or a cancel — which is all the
    hard-stop backstop lifecycle needs from the sim."""
    sim = ScriptedBroker(BrokerScript())
    await sim.submit(client_order_id="BS1", instrument="IDX-OPT",
                     side=Side.SELL, qty=Decimal("4"), limit_price=None,
                     stop_price=Decimal("3.90"))
    view = await sim.query_order("BS1")
    assert view.state is BrokerOrderState.WORKING and view.filled_qty == 0
    assert sim._orders["BS1"].stop_price == Decimal("3.90")

    await sim.cancel("BS1")
    sim.step()
    assert (await sim.query_order("BS1")).state is BrokerOrderState.CANCELED
    assert [type(e) for e in sim.take_events()] == [BrokerCancelConfirmed]


async def test_reject_raises_and_registers_nothing():
    sim = ScriptedBroker(BrokerScript().on_submit("K1", reject="no permission"))
    with pytest.raises(BrokerReject):
        await submit(sim)
    assert await sim.query_order("K1") is None


# ------------------------------------------------------- the timeout family


async def test_timeout_not_accepted_is_conclusively_absent():
    sim = ScriptedBroker(BrokerScript().on_submit("K1", timeout=True))
    with pytest.raises(BrokerTimeout):
        await submit(sim)
    assert await sim.query_order("K1") is None  # reconciler: safe, unexposed


async def test_timeout_but_accepted_is_discoverable():
    """R1.14 crux: the caller gets nothing, the broker has a WORKING order.
    Only query_order (reconciliation) can discover it."""
    sim = ScriptedBroker(
        BrokerScript().on_submit("K1", timeout=True, accept_on_timeout=True)
    )
    with pytest.raises(BrokerTimeout):
        await submit(sim)

    view = await sim.query_order("K1")
    assert view is not None
    assert view.state is BrokerOrderState.WORKING
    assert view.broker_order_id == "SIM-1"  # the id we never received


async def test_resubmit_same_client_id_is_broker_idempotent():
    sim = ScriptedBroker(BrokerScript())
    first = await submit(sim)
    second = await submit(sim)
    assert first == second  # same client id -> same order, no duplicate


# ------------------------------------------------------- cancel-window races


async def test_fill_during_cancel_window():
    """Cancel requested at step 2, confirms at step 4; a fill lands at step 3.
    Broker truth: 3 filled, remainder canceled (R1.6)."""
    script = BrokerScript()
    script.fill("K1", qty="1", price="4.20", at_step=1)
    script.fill("K1", qty="1", price="4.22", at_step=2)
    script.fill("K1", qty="1", price="4.30", at_step=3)   # inside the window
    script.on_cancel("K1", confirm_after_steps=2)
    sim = ScriptedBroker(script)

    await submit(sim)
    sim.step()                      # fill 1
    sim.step()                      # fill 2
    await sim.cancel("K1")          # window opens (confirm due at step 4)
    sim.step()                      # fill 3 executes DURING the window
    sim.step()                      # cancel confirms

    view = await sim.query_order("K1")
    assert view.state is BrokerOrderState.CANCELED
    assert view.filled_qty == 3

    kinds = [type(e) for e in sim.take_events()]
    assert kinds == [BrokerFill, BrokerFill, BrokerFill, BrokerCancelConfirmed]


async def test_no_execution_after_cancel_confirmed():
    script = BrokerScript()
    script.on_cancel("K1", confirm_after_steps=1)
    script.fill("K1", qty="1", price="4.20", at_step=3)   # too late: canceled at 1
    sim = ScriptedBroker(script)

    await submit(sim)
    await sim.cancel("K1")
    for _ in range(4):
        sim.step()

    view = await sim.query_order("K1")
    assert view.state is BrokerOrderState.CANCELED and view.filled_qty == 0
    assert [type(e) for e in sim.take_events()] == [BrokerCancelConfirmed]


# ----------------------------------------------- late + duplicate deliveries


async def test_late_delivery_arrives_after_cancel_confirm():
    """Execution happens at step 1 but is DELIVERED at step 5 — after the
    cancel confirm at step 3. The OMS will see a fill for an order it believes
    CANCELED: the R1.7 reopening path."""
    script = BrokerScript()
    script.fill("K1", qty="1", price="4.20", at_step=1, deliver_at_step=5)
    script.on_cancel("K1", confirm_after_steps=1)
    sim = ScriptedBroker(script)

    await submit(sim)
    sim.step()                      # executes silently (delivery deferred)
    sim.step()
    await sim.cancel("K1")
    sim.step()                      # cancel confirms
    assert [type(e) for e in sim.take_events()] == [BrokerCancelConfirmed]

    sim.step()
    sim.step()                      # step 5: the late fill finally arrives
    late = sim.take_events()
    assert [type(e) for e in late] == [BrokerFill]
    # Broker truth had it all along:
    view = await sim.query_order("K1")
    assert view.filled_qty == 1


async def test_duplicate_redelivery_of_same_exec():
    script = BrokerScript()
    script.fill("K1", qty="2", price="4.20", at_step=1)
    script.redeliver("K1-E1", at_step=3)
    sim = ScriptedBroker(script)

    await submit(sim)
    for _ in range(3):
        sim.step()

    fills = [e for e in sim.take_events() if isinstance(e, BrokerFill)]
    assert len(fills) == 2                       # delivered twice...
    assert fills[0].exec_id == fills[1].exec_id  # ...same execution (ledger dedups)
    view = await sim.query_order("K1")
    assert view.filled_qty == 2                  # broker truth counted it ONCE


# ------------------------------------------------------------- broker truth


async def test_positions_reflect_broker_truth_side_signed():
    script = BrokerScript()
    script.fill("B", qty="3", price="4.20", at_step=1)
    script.fill("S", qty="1", price="4.20", at_step=1)
    sim = ScriptedBroker(script)

    await submit(sim, key="B", qty="3")
    await sim.submit(
        client_order_id="S",
        instrument="IDX-OPT",
        side=Side.SELL,
        qty=Decimal("1"),
        limit_price=None,
    )
    sim.step()
    assert await sim.query_positions() == {"IDX-OPT": Decimal("2")}


async def test_query_view_carries_full_fill_history_for_backfill():
    script = BrokerScript()
    script.fill("K1", qty="1", price="4.20", at_step=1)
    script.fill("K1", qty="2", price="4.25", at_step=2)
    sim = ScriptedBroker(script)
    await submit(sim)
    sim.step()
    sim.step()

    view = await sim.query_order("K1")
    assert [f.exec_id for f in view.fills] == ["K1-E1", "K1-E2"]
    assert sum(f.qty for f in view.fills) == view.filled_qty


# ------------------------------------------------------------- determinism


async def test_identical_scripts_produce_identical_sequences():
    def run_script():
        script = BrokerScript()
        script.on_submit("K1", timeout=True, accept_on_timeout=True)
        script.fill("K1", qty="1", price="4.20", at_step=2)
        script.fill("K1", qty="1", price="4.25", at_step=3, deliver_at_step=6)
        script.on_cancel("K1", confirm_after_steps=2)
        return ScriptedBroker(script)

    async def run(sim: ScriptedBroker) -> list:
        with pytest.raises(BrokerTimeout):
            await submit(sim)
        sim.step()
        sim.step()
        await sim.cancel("K1")
        sim.step()
        sim.step()
        sim.step()
        sim.step()
        return sim.take_events()

    a = await run(run_script())
    b = await run(run_script())
    assert a == b  # same script, same drive -> byte-identical event sequence
