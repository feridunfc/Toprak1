from __future__ import annotations

from dataclasses import dataclass, field

from hfa_control.scheduler_capability_fair_dispatch import (
    SchedulerCapabilityFairDispatcher,
    TaskDispatchRequest,
)
from hfa_control.scheduler_capability_selector import (
    SchedulerCapabilitySelector,
    WorkerCandidate,
)
from hfa_control.scheduler_reservation_dispatch import SchedulerReservationDispatcher
from hfa_control.scheduler_scoring import SchedulerScoring, ScoringCandidate


@dataclass(frozen=True)
class ObservableRejectedWorker:
    worker_id: str
    reason: str


@dataclass(frozen=True)
class ObservableScoredCandidate:
    worker_id: str
    total: float


@dataclass(frozen=True)
class SchedulerDecisionTrace:
    decision_id: str
    stage: str
    selected_worker_id: str = ""
    selected_task_id: str = ""
    selected_tenant_id: str = ""
    selected_score: float | None = None
    selected_reason: str = ""
    candidate_count: int = 0
    compatible_worker_ids: list[str] = field(default_factory=list)
    rejected_workers: list[ObservableRejectedWorker] = field(default_factory=list)
    scored_candidates: list[ObservableScoredCandidate] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "decision_id": self.decision_id,
            "stage": self.stage,
            "selected_worker_id": self.selected_worker_id,
            "selected_task_id": self.selected_task_id,
            "selected_tenant_id": self.selected_tenant_id,
            "selected_score": self.selected_score,
            "selected_reason": self.selected_reason,
            "candidate_count": self.candidate_count,
            "compatible_worker_ids": self.compatible_worker_ids,
            "rejected_workers": [
                {"worker_id": w.worker_id, "reason": w.reason}
                for w in self.rejected_workers
            ],
            "scored_candidates": [
                {"worker_id": c.worker_id, "total": c.total}
                for c in self.scored_candidates
            ],
        }


@dataclass(frozen=True)
class ObservableTaskDispatchRequest(TaskDispatchRequest):
    pass


@dataclass(frozen=True)
class ObservableDispatchResult:
    ok: bool
    status: str
    tenant_id: str = ""
    task_id: str = ""
    worker_id: str = ""
    trace: SchedulerDecisionTrace | None = None


class ObservableSchedulerCapabilityFairDispatcher:
    """
    Backward-compatible observable wrapper around canonical dispatcher.
    """

    def __init__(self, reservation_dispatcher: SchedulerReservationDispatcher) -> None:
        self._reservation_dispatcher = reservation_dispatcher
        self._delegate = SchedulerCapabilityFairDispatcher(reservation_dispatcher)

    async def dispatch_task(
        self,
        *,
        request: ObservableTaskDispatchRequest,
        workers: list[WorkerCandidate],
        scheduler_epoch: str,
        reserved_at_ms: int | None = None,
    ) -> ObservableDispatchResult:
        selection = SchedulerCapabilitySelector.filter_workers(
            required_capabilities=request.required_capabilities,
            workers=workers,
        )

        rejected = [
            ObservableRejectedWorker(worker_id=worker_id, reason="capability_mismatch")
            for worker_id in selection.rejected_worker_ids
        ]

        if not selection.compatible:
            trace = SchedulerDecisionTrace(
                decision_id=f"{request.task_id}:{scheduler_epoch}",
                stage="reserve_dispatch",
                selected_reason="no_compatible_workers",
                selected_task_id=request.task_id,
                selected_tenant_id=request.tenant_id,
                candidate_count=0,
                rejected_workers=rejected,
                compatible_worker_ids=[],
                scored_candidates=[],
            )
            return ObservableDispatchResult(
                ok=False,
                status="no_compatible_workers",
                tenant_id=request.tenant_id,
                task_id=request.task_id,
                trace=trace,
            )

        scoring_candidates = [
            ScoringCandidate(
                tenant_id=request.tenant_id,
                worker_id=w.worker_id,
                task_id=request.task_id,
                vruntime=request.vruntime,
                inflight=request.inflight,
                worker_load=w.current_load,
                capacity=w.capacity,
            )
            for w in selection.compatible
        ]
        best = SchedulerScoring.choose_best(scoring_candidates)
        assert best is not None

        scored = [
            ObservableScoredCandidate(
                worker_id=c.worker_id,
                total=SchedulerScoring.score(c).total,
            )
            for c in scoring_candidates
        ]

        delegated = await self._delegate.dispatch_task(
            request=request,
            workers=workers,
            scheduler_epoch=scheduler_epoch,
            reserved_at_ms=reserved_at_ms,
        )

        trace = SchedulerDecisionTrace(
            decision_id=f"{request.task_id}:{scheduler_epoch}",
            stage="reserve_dispatch",
            selected_worker_id=best.worker_id,
            selected_task_id=request.task_id,
            selected_tenant_id=request.tenant_id,
            selected_score=SchedulerScoring.score(best).total,
            selected_reason=(
                "best_compatible_worker_by_score"
                if delegated.ok
                else delegated.status
            ),
            candidate_count=len(selection.compatible),
            compatible_worker_ids=[w.worker_id for w in selection.compatible],
            rejected_workers=rejected,
            scored_candidates=scored,
        )

        return ObservableDispatchResult(
            ok=delegated.ok,
            status=delegated.status,
            tenant_id=delegated.tenant_id,
            task_id=delegated.task_id,
            worker_id=delegated.worker_id,
            trace=trace,
        )


__all__ = [
    "ObservableSchedulerCapabilityFairDispatcher",
    "ObservableTaskDispatchRequest",
    "ObservableDispatchResult",
    "SchedulerDecisionTrace",
]