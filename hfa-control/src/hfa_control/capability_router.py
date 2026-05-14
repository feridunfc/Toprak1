
from __future__ import annotations

from dataclasses import dataclass

from hfa.dag.capabilities import TaskCapabilitySpec, WorkerCapabilitySpec


@dataclass(frozen=True)
class CapabilityMatchResult:
    ok: bool
    missing_capabilities: list[str]


class CapabilityRouter:
    @staticmethod
    def matches(task: TaskCapabilitySpec, worker: WorkerCapabilitySpec) -> CapabilityMatchResult:
        required = task.normalized()
        worker_caps = worker.normalized()
        missing = [cap for cap in required if cap not in worker_caps]
        return CapabilityMatchResult(ok=len(missing) == 0, missing_capabilities=missing)
