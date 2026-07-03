"""Live smoke test against Binance Spot TESTNET.

Exercises every adapter path against the real exchange:
  1. signed connectivity (account balances)
  2. conclusive absence (ghost client id -> None)
  3. resting LIMIT order: submit -> WORKING -> cancel -> CANCELED
  4. MARKET order with the user data stream listening: real fill event
     arrives over the socket, then query backfills the same execution

Run:  ./.venv/bin/python scripts/smoke_binance.py
Keys: BINANCE_TESTNET_KEY / BINANCE_TESTNET_SECRET in .env (gitignored).
Testnet only — fake funds.
"""

from __future__ import annotations

import asyncio
import sys
import time
from decimal import ROUND_DOWN, Decimal
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from sentinel.broker import BrokerFill, BrokerOrderState
from sentinel.broker.binance import BinanceSpotAdapter
from sentinel.domain import Side

SYMBOL = "BTCUSDT"
TICK = Decimal("0.01")          # BTCUSDT price tick
STEP = Decimal("0.00001")       # BTCUSDT lot step
TARGET_NOTIONAL = Decimal("15") # comfortably above min notional


def load_env() -> tuple[str, str]:
    env = {}
    for line in (Path(__file__).parent.parent / ".env").read_text().splitlines():
        if "=" in line and not line.startswith("#"):
            k, _, v = line.partition("=")
            env[k.strip()] = v.strip()
    return env["BINANCE_TESTNET_KEY"], env["BINANCE_TESTNET_SECRET"]


def check(label: str, condition: bool, detail: str = "") -> None:
    mark = "✅" if condition else "❌"
    print(f"  {mark} {label}" + (f" — {detail}" if detail else ""))
    if not condition:
        raise SystemExit(f"SMOKE FAILED at: {label}")


async def main() -> None:
    key, secret = load_env()
    adapter = BinanceSpotAdapter(key, secret, symbols=(SYMBOL,))

    print("1) signed connectivity")
    balances = await adapter.query_positions()
    majors = {k: str(balances[k]) for k in ("BTC", "ETH", "USDT") if k in balances}
    check("account reachable, signature accepted", isinstance(balances, dict),
          f"{len(balances)} assets; majors: {majors}")

    print("2) conclusive absence")
    ghost = await adapter.query_order(f"GHOST-{int(time.time())}")
    check("unknown client id -> None (proven absent)", ghost is None)

    # current market price (public endpoint)
    resp = await adapter._http.get(f"/api/v3/ticker/price?symbol={SYMBOL}")
    market = Decimal(resp.json()["price"])
    print(f"   market: {SYMBOL} @ {market}")

    print("3) resting limit: submit -> WORKING -> cancel -> CANCELED")
    limit_key = f"SMOKE-L-{int(time.time())}"
    limit_price = (market * Decimal("0.8")).quantize(TICK, rounding=ROUND_DOWN)
    qty = (TARGET_NOTIONAL / limit_price).quantize(STEP, rounding=ROUND_DOWN)
    broker_id = await adapter.submit(
        client_order_id=limit_key, instrument=SYMBOL, side=Side.BUY,
        qty=qty, limit_price=limit_price,
    )
    check("submit acked with broker order id", bool(broker_id),
          f"id {broker_id}, {qty} @ {limit_price}")

    view = await adapter.query_order(limit_key)
    check("query by OUR client id finds it",
          view is not None and view.state is BrokerOrderState.WORKING,
          f"state {view.state.value}")

    await adapter.cancel(limit_key)
    for _ in range(10):
        view = await adapter.query_order(limit_key)
        if view.state is BrokerOrderState.CANCELED:
            break
        await asyncio.sleep(0.5)
    check("cancel confirmed", view.state is BrokerOrderState.CANCELED)

    print("4) market fill with the user stream listening")
    market_key = f"SMOKE-M-{int(time.time())}"
    mkt_qty = (TARGET_NOTIONAL / market).quantize(STEP, rounding=ROUND_DOWN)

    fills: list[BrokerFill] = []

    async def listen() -> None:
        async for event in adapter.events():
            if isinstance(event, BrokerFill) and event.client_order_id == market_key:
                fills.append(event)
                return

    listener = asyncio.create_task(listen())
    await asyncio.sleep(2.0)  # let the socket connect before the order fires

    await adapter.submit(
        client_order_id=market_key, instrument=SYMBOL, side=Side.BUY,
        qty=mkt_qty, limit_price=None,
    )
    try:
        await asyncio.wait_for(listener, timeout=15)
    except asyncio.TimeoutError:
        listener.cancel()
    check("fill event arrived over the user stream", len(fills) > 0,
          f"exec {fills[0].exec_id}: {fills[0].qty} @ {fills[0].price}"
          if fills else "no event in 15s")

    view = await adapter.query_order(market_key)
    check("query shows FILLED with per-exec backfill",
          view.state is BrokerOrderState.FILLED and len(view.fills) > 0,
          f"filled {view.filled_qty}, execs {[f.exec_id for f in view.fills]}")
    if fills:
        stream_ids = {f.exec_id for f in fills}
        query_ids = {f.exec_id for f in view.fills}
        check("stream exec ids match query exec ids (dedup-ready)",
              stream_ids <= query_ids, f"{stream_ids} ⊆ {query_ids}")

    await adapter.aclose()
    print("\nSMOKE PASSED — the adapter is live against Binance testnet.")


if __name__ == "__main__":
    asyncio.run(main())
