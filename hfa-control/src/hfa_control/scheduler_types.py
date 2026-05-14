from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class SchedulingDecisionScore:
    primary_score: int
    load_factor: float
    inflight: int
    capacity: int
    worker_group: str
    region: str
    worker_id: str

    def as_sort_key(self) -> tuple:
        return (
            self.primary_score,
            self.load_factor,
            self.inflight,
            -self.capacity,
            self.worker_group,
            self.region,
            self.worker_id,
        )