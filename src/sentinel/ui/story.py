"""Story Mode — guided, narrated demo scenarios.

Integrity systems can only be demoed by CONTRAST: the disaster that didn't
happen has no pixels. Each story therefore narrates two books side by side —
what a naive bot's book would look like after each act, versus Sentinel's.

Mechanics: a story runs on its own virgin instrument (fresh lane, fresh
marks), and each press of "next" EXECUTES the next act against the real
system — nothing is mocked; the narration points at real panels changing.
The app's background reconciler is gated during the UNKNOWN acts so the
drama is visible; safety is unaffected (UNKNOWN keeps the lane locked).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any, Awaitable, Callable
from uuid import uuid4

from sentinel.domain import Authority, EconomicOrderIntent, Side
from sentinel.oms import PlacementBlocked


@dataclass(frozen=True, slots=True)
class Act:
    title: str
    text: str                       # may reference {instrument}, {key}
    naive: str                      # the ghost book / commentary
    sentinel: str                   # our book / commentary
    highlight: str = ""             # panel id to glow: orders|log|ledger|invs|positions
    action: Callable[..., Awaitable[None]] | None = None


@dataclass(slots=True)
class StoryRun:
    name: str
    title: str
    acts: list[Act]
    ctx: dict[str, Any] = field(default_factory=dict)
    index: int = 0

    @property
    def done(self) -> bool:
        return self.index >= len(self.acts) - 1

    def view(self) -> dict:
        act = self.acts[self.index]
        fmt = lambda s: s.format(**{k: v for k, v in self.ctx.items()
                                    if isinstance(v, str)})
        return {
            "active": True,
            "name": self.name,
            "title": self.title,
            "step": self.index + 1,
            "total": len(self.acts),
            "act_title": fmt(act.title),
            "text": fmt(act.text),
            "naive": fmt(act.naive),
            "sentinel": fmt(act.sentinel),
            "highlight": act.highlight,
            "done": self.done,
        }


class StoryEngine:
    def __init__(self, driver) -> None:  # driver: ui.server.DemoDriver
        self._driver = driver
        self._run: StoryRun | None = None
        self._story_n = 0

    def snapshot(self) -> dict:
        if self._run is None:
            return {"active": False,
                    "available": [{"name": n, "title": s["title"]}
                                  for n, s in STORIES.items()]}
        return self._run.view()

    async def start(self, name: str) -> dict:
        story = STORIES[name]
        self._story_n += 1
        instrument = f"STORY-{self._story_n}"
        self._driver.marks.add_instrument(instrument, "4.20")
        self._run = StoryRun(
            name=name, title=story["title"], acts=story["acts"],
            ctx={"instrument": instrument, "key": f"S{self._story_n}"},
        )
        first = self._run.acts[0]
        if first.action:
            await first.action(self._driver, self._run.ctx)
        return self._run.view()

    async def next(self) -> dict:
        if self._run is None:
            return {"active": False}
        if self._run.done:
            return await self.exit()
        self._run.index += 1
        act = self._run.acts[self._run.index]
        if act.action:
            await act.action(self._driver, self._run.ctx)
        return self._run.view()

    async def exit(self) -> dict:
        # Never leave the reconciler gated behind.
        self._driver.app.recon_gate.set()
        self._run = None
        return self.snapshot()


# ---------------------------------------------------------------- act actions


def _intent(driver, ctx, key, qty, side=Side.BUY, authority=Authority.ENTRY):
    mark = driver.marks.latest(ctx["instrument"])
    return EconomicOrderIntent(
        intent_id=uuid4(), idempotency_key=key, instrument=ctx["instrument"],
        side=side, qty=Decimal(qty), limit_price=None, authority=authority,
        trace_id=uuid4(), quote_at_decision=mark.price if mark else None,
    )


async def _act_vanishing_submit(driver, ctx) -> None:
    """Hold the reconciler, then place an order whose ack the broker drops —
    while accepting it broker-side. The order parks in UNKNOWN and STAYS
    there for narration."""
    driver.app.recon_gate.clear()
    key = ctx["key"]
    driver.sim._script.on_submit(key, timeout=True, accept_on_timeout=True)
    await driver.app.gateway.place(uuid4(), _intent(driver, ctx, key, "4"))


async def _act_try_second_entry(driver, ctx) -> None:
    try:
        await driver.app.gateway.place(
            uuid4(), _intent(driver, ctx, f"{ctx['key']}-again", "4")
        )
    except PlacementBlocked:
        pass  # the point — and it's now in the decision log


async def _act_reconcile(driver, ctx) -> None:
    driver.app.recon_gate.set()  # release the held reconciler: truth time


async def _act_fill_full(driver, ctx) -> None:
    step = driver.sim.current_step
    mark = driver.marks.latest(ctx["instrument"])
    px = str(mark.price if mark else "4.20")
    driver.sim._script.fill(ctx["key"], qty="4", price=px, at_step=step + 1)


async def _act_protect(driver, ctx) -> None:
    await driver.app.protect.ensure_protection()


async def _act_place_and_fill(driver, ctx) -> None:
    key = ctx["key"]
    step = driver.sim.current_step
    driver.sim._script.fill(key, qty="2", price="4.20", at_step=step + 1)
    driver.sim._script.fill(key, qty="2", price="4.25", at_step=step + 2)
    await driver.app.gateway.place(uuid4(), _intent(driver, ctx, key, "4"))


async def _act_corrupt(driver, ctx) -> None:
    await driver.corrupt()


async def _act_recover(driver, ctx) -> None:
    await driver.recover()


# -------------------------------------------------------------------- stories


STORIES: dict[str, dict] = {
    "vanishing-ack": {
        "title": "The Vanishing Ack",
        "acts": [
            Act(
                title="A normal trade. Almost.",
                text="We are buying 4 contracts of {instrument}. Watch the "
                     "ORDERS panel and the DECISION LOG on the left.",
                naive="The naive bot is placing the same order.",
                sentinel="Intent will be persisted BEFORE the broker hears "
                         "anything — remember that.",
                highlight="orders",
            ),
            Act(
                title="The broker goes silent",
                text="Submitted... and nothing came back. No acknowledgment, "
                     "no order id. Did the order reach the broker? THERE IS "
                     "NO WAY TO KNOW YET. See {key}: state UNKNOWN, pulsing "
                     "red. (Secretly — the broker DID accept it.)",
                naive="Naive bot: 'timeout = failed'. It RESUBMITS. It now "
                      "owns 8 contracts and believes it owns 4. ⚠️",
                sentinel="UNKNOWN is a locked state — not an error message. "
                         "Position: unknown, and it KNOWS it doesn't know.",
                highlight="orders",
                action=_act_vanishing_submit,
            ),
            Act(
                title="The lock holds",
                text="We just tried to place ANOTHER entry on {instrument} — "
                     "look at the DECISION LOG: PLACE_BLOCKED / "
                     "InstrumentHeld. While anything on this instrument is "
                     "unprovable, no new exposure may be created here. This "
                     "refusal is exactly what prevented the naive bot's "
                     "double-buy.",
                naive="Naive bot has no such lock. Its second order went "
                      "through. It is now double-long and blind. ⚠️",
                sentinel="New entries on {instrument}: refused, with the "
                         "reason on the record.",
                highlight="log",
                action=_act_try_second_entry,
            ),
            Act(
                title="Ask the one who knows",
                text="Recovery is ONE mechanism: query the broker by OUR "
                     "client order id ({key}). The broker answers: 'yes, I "
                     "have it — it is WORKING'. Watch the LEDGER: UNKNOWN → "
                     "RECONCILING → WORKING. The order was ADOPTED — never "
                     "resubmitted. The ledger shows exactly ONE submission "
                     "for {key}, ever.",
                naive="Naive bot never asks. It still doesn't know it owns 8.",
                sentinel="Adopted the broker's truth. Zero duplicate exposure.",
                highlight="ledger",
                action=_act_reconcile,
            ),
            Act(
                title="The fills arrive",
                text="4 contracts fill — exactly the 4 that were authorized. "
                     "Check POSITIONS: {instrument} shows 4.",
                naive="Naive bot's fills arrive too: all 8. Its P&L screen "
                      "says 4. Its broker statement says 8. It will find out "
                      "at settlement. ⚠️",
                sentinel="Position 4. Book and broker agree to the contract.",
                highlight="positions",
                action=_act_fill_full,
            ),
            Act(
                title="Protection arms itself",
                text="The protective-exit supervisor — an INDEPENDENT "
                     "authority — noticed uncovered exposure and placed a "
                     "protective sell for exactly 4. See the 🛡 order.",
                naive="Naive bot has 8 naked contracts and no idea.",
                sentinel="4 held, 4 covered ✓",
                highlight="orders",
                action=_act_protect,
            ),
            Act(
                title="The receipts",
                text="Scroll the LEDGER: every transition of {key} is there — "
                     "sequenced, traced, permanent. That is what 'auditable' "
                     "means: not a log file, a reconstruction guarantee. "
                     "THE END — exit or run the other story.",
                naive="FINAL: naive bot — 8 contracts, thinks 4, naked. ⚠️",
                sentinel="FINAL: Sentinel — 4 contracts, knows 4, covered. ✓",
                highlight="ledger",
            ),
        ],
    },
    "the-crash": {
        "title": "Kill It Mid-Trade",
        "acts": [
            Act(
                title="Build something worth destroying",
                text="Buying 4 contracts of {instrument}; fills are landing "
                     "now. A position, a P&L, live orders — state that "
                     "matters.",
                naive="Same trade on the naive bot.",
                sentinel="Every command and every fill is an immutable ledger "
                         "event, not just a row that gets overwritten.",
                highlight="positions",
                action=_act_place_and_fill,
            ),
            Act(
                title="Now we destroy the database",
                text="We just VANDALIZED the projections: every position now "
                     "reads 99, live orders forgot their fills. This is what "
                     "a crash mid-write leaves behind. Look at the INV "
                     "strip: the first light is RED — the live auditor "
                     "caught the lie within a quarter second.",
                naive="Naive bot's database is its ONLY memory. Whatever "
                      "garbage is in there is now its reality: position 99. ⚠️",
                sentinel="The POSITIONS panel still shows the truth — it "
                         "reads from the fills ledger, not the corrupted "
                         "table. Lies detected AND contained.",
                highlight="invs",
                action=_act_corrupt,
            ),
            Act(
                title="Who do you trust?",
                text="Two sources cannot lie: the append-only event ledger "
                     "(every fill, sequenced) and the broker (who actually "
                     "holds the orders). Projections are DISPOSABLE. So we "
                     "dispose of them.",
                naive="Naive bot would need a human, a backup, and a very "
                      "bad weekend.",
                sentinel="Recovery = replay the ledger + reconcile with the "
                         "broker. No human, no backup, no guessing.",
                highlight="ledger",
            ),
            Act(
                title="Resurrection",
                text="Recovery ran: the ledger was replayed, every "
                     "non-terminal order reconciled against the broker, "
                     "protection re-armed. The INV strip is GREEN again. "
                     "Compare POSITIONS to before the crash: identical.",
                naive="Naive bot is still trading on position 99. Or it's "
                      "flat and doesn't know it. Nobody knows. ⚠️",
                sentinel="State destroyed → state reconstructed, verifiably, "
                         "in under a second. That's the property that lets "
                         "software touch money. ✓",
                highlight="invs",
                action=_act_recover,
            ),
        ],
    },
}
