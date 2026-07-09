"""Task supervision — structured, fail-loud, cancellation-clean (R2).

Rules:
- Every long-running task lives under the supervisor; there are no orphans.
- A task failure is RECORDED and surfaced (fail-loud): depending on policy it
  restarts the task or trips the halt event — it is never silently swallowed.
- Shutdown cancels children in reverse start order and AWAITS each one, so
  `finally` blocks (cleanup, checkpoints) always run.
"""

from __future__ import annotations

import asyncio
import logging
import os
from dataclasses import dataclass, field
from typing import Awaitable, Callable

log = logging.getLogger("sentinel.runtime")

# Restart backoff: an instantly-respawning restart=True task that fails on
# entry becomes a tight crash loop that starves the event loop. Back off
# exponentially per consecutive failure; a healthy run resets the streak.
_RESTART_BASE_S = 1.0
_RESTART_MAX_S = 30.0
_HEALTHY_RUN_S = 60.0    # ran this long before failing -> streak forgiven


@dataclass(slots=True)
class TaskFailure:
    name: str
    error: BaseException
    restarted: bool


@dataclass(slots=True)
class _Child:
    name: str
    factory: Callable[[], Awaitable[None]]
    restart: bool
    task: asyncio.Task | None = None
    started_at: float = 0.0
    fail_streak: int = 0


class TaskSupervisor:
    def __init__(self) -> None:
        self._children: list[_Child] = []
        self.failures: list[TaskFailure] = []
        self.halted = asyncio.Event()
        # Durable halt hook: set by the app to write the halt reason to the
        # ledger. self.halted is in-memory and the CRITICAL log line lives in
        # a short buffer — without this, a restart erases all evidence of WHAT
        # halted the account and WHEN.
        self.on_halt: Callable[[str], Awaitable[None]] | None = None

    def spawn(
        self,
        name: str,
        factory: Callable[[], Awaitable[None]],
        *,
        restart: bool = False,
    ) -> None:
        """restart=True: the task is respawned on failure (transient work).
        restart=False: a failure halts the supervisor (integrity-critical)."""
        child = _Child(name=name, factory=factory, restart=restart)
        self._children.append(child)
        self._start(child)

    def _start(self, child: _Child) -> None:
        child.started_at = asyncio.get_running_loop().time()
        child.task = asyncio.create_task(child.factory(), name=child.name)
        child.task.add_done_callback(lambda t, c=child: self._on_done(c, t))

    def _on_done(self, child: _Child, task: asyncio.Task) -> None:
        if task.cancelled():
            return
        error = task.exception()
        if error is None:
            return  # clean exit
        self.failures.append(
            TaskFailure(name=child.name, error=error, restarted=child.restart)
        )
        if child.restart and not self.halted.is_set():
            now = asyncio.get_running_loop().time()
            if now - child.started_at >= _HEALTHY_RUN_S:
                child.fail_streak = 0            # it ran fine for a while
            delay = min(_RESTART_BASE_S * (2 ** child.fail_streak),
                        _RESTART_MAX_S)
            child.fail_streak += 1
            log.error("task %s failed (%r); restarting in %.1fs",
                      child.name, error, delay)
            asyncio.get_running_loop().create_task(
                self._restart_later(child, delay))
        else:
            log.critical("task %s failed (%r); HALTING", child.name, error)
            self.halted.set()
            message = f"task {child.name} failed: {error!r}"
            self.alert(message)
            if self.on_halt is not None:
                asyncio.get_running_loop().create_task(
                    self._record_halt(message))

    async def _record_halt(self, message: str) -> None:
        """Run the on_halt hook, absorbing its failures: a halt-record
        failure must never cascade — the halt itself already stands."""
        try:
            await self.on_halt(message)  # type: ignore[misc]
        except Exception as e:  # noqa: BLE001
            log.warning("durable halt record failed (%r); "
                        "halt reason survives only in logs", e)

    async def _restart_later(self, child: _Child, delay: float) -> None:
        await asyncio.sleep(delay)
        # The world may have moved during the backoff: the child may have been
        # torn down (cancel()/shutdown() drop it from _children) or the account
        # halted — in either case, respawning would resurrect an orphan.
        if child in self._children and not self.halted.is_set():
            self._start(child)

    def alert(self, message: str) -> None:
        """Fire-and-forget operator notification when the account HALTS.
        Posts {'content': ...} (Discord/Slack-webhook compatible) to
        SENTINEL_ALERT_WEBHOOK if set; a halt should page someone, not wait
        to be noticed on the board. Delivery failure only logs — alerting
        must never take the process down with it."""
        url = os.environ.get("SENTINEL_ALERT_WEBHOOK")
        if not url:
            return

        async def _post() -> None:
            try:
                import httpx
                async with httpx.AsyncClient(timeout=10.0) as client:
                    await client.post(
                        url, json={"content": f"🛑 Sentinel OMS: {message}"})
            except Exception as e:  # noqa: BLE001
                log.warning("halt alert delivery failed (%r)", e)

        try:
            asyncio.get_running_loop().create_task(_post())
        except RuntimeError:      # no running loop (sync teardown path)
            log.warning("halt alert skipped: no running event loop")

    async def cancel(self, name: str) -> None:
        """Cancel and forget the tasks with this name — used to tear down ONE
        bot (its market/bars/strategy tasks) without touching the others. Awaits
        cleanup; a cancelled task records no failure and does not restart."""
        keep: list[_Child] = []
        for child in self._children:
            if child.name != name:
                keep.append(child)
                continue
            task = child.task
            if task is not None and not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
        self._children = keep

    async def shutdown(self) -> None:
        """Cancel children newest-first and await each: cleanup always runs,
        and no accepted work is abandoned mid-await."""
        for child in reversed(self._children):
            task = child.task
            if task is None or task.done():
                continue
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        self._children.clear()
