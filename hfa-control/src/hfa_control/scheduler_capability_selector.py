
from __future__ import annotations

from dataclasses import dataclass

from hfa.dag.capabilities import TaskCapabilitySpec, WorkerCapabilitySpec
from hfa_control.capability_router import CapabilityRouter


@dataclass(frozen=True)
class WorkerCandidate:
    worker_id: str
    capabilities: list[str]
    worker_group: str = ""
    capacity: int = 0
    current_load: int = 0


@dataclass(frozen=True)
class CapabilitySelectionResult:
    compatible: list[WorkerCandidate]
    rejected_worker_ids: list[str]
    missing_by_worker: dict[str, list[str]]


class SchedulerCapabilitySelector:
    @staticmethod
    def filter_workers(
        *,
        required_capabilities: list[str] | None,
        workers: list[WorkerCandidate],
    ) -> CapabilitySelectionResult:
        task_spec = TaskCapabilitySpec(required_capabilities=required_capabilities or [])

        compatible: list[WorkerCandidate] = []
        rejected_worker_ids: list[str] = []
        missing_by_worker: dict[str, list[str]] = {}

        for worker in sorted(workers, key=lambda w: (w.worker_id, w.worker_group)):
            result = CapabilityRouter.matches(
                task_spec,
                WorkerCapabilitySpec(worker_id=worker.worker_id, capabilities=worker.capabilities),
            )
            if result.ok:
                compatible.append(worker)
            else:
                rejected_worker_ids.append(worker.worker_id)
                missing_by_worker[worker.worker_id] = result.missing_capabilities

        return CapabilitySelectionResult(
            compatible=compatible,
            rejected_worker_ids=rejected_worker_ids,
            missing_by_worker=missing_by_worker,
        )
