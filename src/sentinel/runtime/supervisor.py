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
from dataclasses import dataclass, field
from typing import Awaitable, Callable

log = logging.getLogger("sentinel.runtime")


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


class TaskSupervisor:
    def __init__(self) -> None:
        self._children: list[_Child] = []
        self.failures: list[TaskFailure] = []
        self.halted = asyncio.Event()

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
            log.error("task %s failed (%r); restarting", child.name, error)
            self._start(child)
        else:
            log.critical("task %s failed (%r); HALTING", child.name, error)
            self.halted.set()

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
