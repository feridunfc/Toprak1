
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ScoringCandidate:
    tenant_id: str
    worker_id: str
    task_id: str
    vruntime: float
    inflight: int
    worker_load: int
    capacity: int


class SchedulerScoring:
    """
    Faz 4B:
    Unified scoring across fairness + load + capacity.

    Lower score is better.
    """

    @staticmethod
    def score(c: ScoringCandidate) -> tuple:
        # ordered tuple = deterministic scoring
        return (
            c.vruntime,           # fairness first
            c.inflight,           # tie-break fairness
            c.worker_load,        # efficiency
            -c.capacity,          # prefer higher capacity
            c.worker_id,          # deterministic
            c.task_id,
        )

    @staticmethod
    def choose_best(candidates: list[ScoringCandidate]) -> ScoringCandidate | None:
        if not candidates:
            return None
        return sorted(candidates, key=SchedulerScoring.score)[0]
