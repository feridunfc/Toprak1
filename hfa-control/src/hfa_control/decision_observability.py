
from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any


@dataclass(frozen=True)
class RejectedWorkerReason:
    worker_id: str
    reason: str
    detail: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class DecisionBreakdown:
    tenant_id: str
    task_id: str
    worker_id: str
    vruntime: float
    inflight: int
    worker_load: int
    capacity: int
    score: tuple


@dataclass(frozen=True)
class SchedulerDecisionTrace:
    decision_id: str
    stage: str
    selected_worker_id: str = ""
    selected_task_id: str = ""
    selected_tenant_id: str = ""
    selected_score: tuple | None = None
    selected_reason: str = ""
    candidate_count: int = 0
    compatible_worker_ids: list[str] = field(default_factory=list)
    rejected_workers: list[RejectedWorkerReason] = field(default_factory=list)
    scored_candidates: list[DecisionBreakdown] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "decision_id": self.decision_id,
            "stage": self.stage,
            "selected_worker_id": self.selected_worker_id,
            "selected_task_id": self.selected_task_id,
            "selected_tenant_id": self.selected_tenant_id,
            "selected_score": list(self.selected_score) if self.selected_score is not None else None,
            "selected_reason": self.selected_reason,
            "candidate_count": self.candidate_count,
            "compatible_worker_ids": list(self.compatible_worker_ids),
            "rejected_workers": [asdict(x) for x in self.rejected_workers],
            "scored_candidates": [asdict(x) | {"score": list(x.score)} for x in self.scored_candidates],
        }
