
from __future__ import annotations

from hfa.state import transition_state
from hfa_control.scheduler_loop import SchedulerLoop


class Scheduler:
    """Single-authority worker picker facade.

    scheduler.py must delegate dispatch to SchedulerLoop.run_cycle().
    """

    def __init__(self, scheduler_loop: SchedulerLoop | None = None) -> None:
        self._scheduler_loop = scheduler_loop

    async def tick(self, max_dispatches: int | None = None) -> int:
        if self._scheduler_loop is None:
            return 0
        return await self._scheduler_loop.run_cycle(max_dispatches=max_dispatches)

    def _pick_best_worker(self, candidates):
        usable = []
        for candidate in candidates:
            if not getattr(candidate, "schedulable", True):
                continue
            if not getattr(candidate, "worker_id", None):
                continue
            if getattr(candidate, "decision_score", None) is None:
                continue
            usable.append(candidate)
        if not usable:
            return None
        usable.sort(key=lambda c: c.decision_score.as_sort_key())
        return usable[0]
