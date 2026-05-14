
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class TenantFairnessSnapshot:
    tenant_id: str
    vruntime: float
    weight: float = 1.0
    inflight: int = 0

    def normalized_runtime(self, runtime_ms: int) -> float:
        weight = self.weight if self.weight > 0 else 1.0
        return float(runtime_ms) / weight


def fairness_sort_key(
    snapshot: TenantFairnessSnapshot,
    *,
    worker_load: int = 0,
    capacity: int = 0,
    topology_rank: int = 0,
) -> tuple[float, int, int, str]:
    return (
        snapshot.vruntime,
        worker_load,
        snapshot.inflight,
        snapshot.tenant_id if capacity >= 0 or topology_rank >= 0 else snapshot.tenant_id,
    )
