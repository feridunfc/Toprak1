
from __future__ import annotations

from dataclasses import dataclass

from hfa_control.scheduler_capability_selector import (
    SchedulerCapabilitySelector,
    WorkerCandidate,
)
from hfa_control.scheduler_reservation_dispatch import SchedulerReservationDispatcher
from hfa_control.scheduler_scoring import SchedulerScoring, ScoringCandidate


@dataclass(frozen=True)
class TaskDispatchRequest:
    tenant_id: str
    task_id: str
    required_capabilities: list[str]
    dispatch_payload: dict
    vruntime: float = 0.0
    inflight: int = 0


@dataclass(frozen=True)
class CapabilityFairDispatchDecision:
    ok: bool
    status: str
    tenant_id: str = ""
    task_id: str = ""
    worker_id: str = ""
    rejected_workers: list[str] | None = None


class SchedulerCapabilityFairDispatcher:
    """
    Faz 4C:
    Full scheduler-side path:
    1. capability filter workers
    2. unified score compatible worker candidates
    3. reserve + dispatch winner

    This makes worker-side capability rejection a fallback only.
    """

    def __init__(self, reservation_dispatcher: SchedulerReservationDispatcher) -> None:
        self._reservation_dispatcher = reservation_dispatcher

    async def dispatch_task(
        self,
        *,
        request: TaskDispatchRequest,
        workers: list[WorkerCandidate],
        scheduler_epoch: str,
        reserved_at_ms: int | None = None,
    ) -> CapabilityFairDispatchDecision:
        selection = SchedulerCapabilitySelector.filter_workers(
            required_capabilities=request.required_capabilities,
            workers=workers,
        )

        if not selection.compatible:
            return CapabilityFairDispatchDecision(
                ok=False,
                status="no_compatible_workers",
                tenant_id=request.tenant_id,
                task_id=request.task_id,
                rejected_workers=selection.rejected_worker_ids,
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

        dispatch_result = await self._reservation_dispatcher.reserve_and_dispatch(
            task_id=request.task_id,
            worker_id=best.worker_id,
            scheduler_epoch=scheduler_epoch,
            dispatch_payload=request.dispatch_payload,
            reserved_at_ms=reserved_at_ms,
        )

        return CapabilityFairDispatchDecision(
            ok=dispatch_result.ok,
            status=dispatch_result.status,
            tenant_id=request.tenant_id,
            task_id=request.task_id,
            worker_id=best.worker_id,
            rejected_workers=selection.rejected_worker_ids,
        )
