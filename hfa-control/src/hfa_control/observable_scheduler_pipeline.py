from __future__ import annotations

from dataclasses import dataclass

from hfa_control.decision_metrics import DecisionMetrics
from hfa_control.decision_trace_store import DecisionTraceStore
from hfa_control.observable_scheduler_capability_fair_dispatch import (
    ObservableSchedulerCapabilityFairDispatcher,
    ObservableTaskDispatchRequest,
)
from hfa_control.scheduler_capability_selector import WorkerCandidate


@dataclass(frozen=True)
class ObservablePipelineResult:
    ok: bool
    status: str
    trace_stream_id: str = ""
    worker_id: str = ""


class ObservableSchedulerPipeline:
    def __init__(
        self,
        dispatcher: ObservableSchedulerCapabilityFairDispatcher,
        trace_store: DecisionTraceStore,
        metrics: DecisionMetrics,
    ) -> None:
        self._dispatcher = dispatcher
        self._trace_store = trace_store
        self._metrics = metrics

    async def dispatch_with_observability(
        self,
        *,
        request: ObservableTaskDispatchRequest,
        workers: list[WorkerCandidate],
        scheduler_epoch: str,
        reserved_at_ms: int | None = None,
    ) -> ObservablePipelineResult:
        result = await self._dispatcher.dispatch_task(
            request=request,
            workers=workers,
            scheduler_epoch=scheduler_epoch,
            reserved_at_ms=reserved_at_ms,
        )

        if result.trace is not None:
            persisted = await self._trace_store.persist(result.trace.to_dict())
        else:
            persisted = None

        if result.ok:
            await self._metrics.incr("dispatch_success_total")
        else:
            await self._metrics.incr("dispatch_failure_total")
            if result.status == "no_compatible_workers":
                await self._metrics.incr("dispatch_no_compatible_workers_total")

        return ObservablePipelineResult(
            ok=result.ok,
            status=result.status,
            trace_stream_id=persisted.stream_id if persisted is not None else "",
            worker_id=result.worker_id,
        )


__all__ = ["ObservableSchedulerPipeline", "ObservablePipelineResult"]