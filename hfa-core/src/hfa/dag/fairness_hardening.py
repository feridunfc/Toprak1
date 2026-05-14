
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class FairnessPolicy:
    max_inflight_per_tenant: int = 0
    starvation_recovery_delta: float = 0.0
    vruntime_floor: float = 0.0

    def effective_vruntime(self, vruntime: float) -> float:
        if vruntime < self.vruntime_floor:
            return self.vruntime_floor
        return vruntime

    def apply_starvation_recovery(self, vruntime: float) -> float:
        recovered = vruntime - self.starvation_recovery_delta
        return max(self.vruntime_floor, recovered)

    def inflight_allowed(self, inflight: int) -> bool:
        if self.max_inflight_per_tenant <= 0:
            return True
        return inflight < self.max_inflight_per_tenant


@dataclass(frozen=True)
class FairnessEligibility:
    eligible: bool
    reason: str
    effective_vruntime: float
    inflight: int
