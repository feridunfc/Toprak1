
from __future__ import annotations

from dataclasses import dataclass

from hfa.dag.fairness_hardening import FairnessEligibility, FairnessPolicy
from hfa.dag.schema import DagRedisKey


@dataclass(frozen=True)
class FairnessRecoveryResult:
    tenant_id: str
    old_vruntime: float
    new_vruntime: float
    recovered: bool


class FairnessHardeningManager:
    def __init__(self, redis, policy: FairnessPolicy) -> None:
        self._redis = redis
        self._policy = policy

    async def get_vruntime(self, tenant_id: str) -> float:
        raw = await self._redis.get(DagRedisKey.tenant_vruntime(tenant_id))
        return float(raw) if raw is not None else 0.0

    async def get_inflight(self, tenant_id: str) -> int:
        raw = await self._redis.get(DagRedisKey.tenant_inflight(tenant_id))
        return int(raw) if raw is not None else 0

    async def evaluate_tenant(self, tenant_id: str) -> FairnessEligibility:
        vruntime = await self.get_vruntime(tenant_id)
        inflight = await self.get_inflight(tenant_id)
        effective = self._policy.effective_vruntime(vruntime)

        if not self._policy.inflight_allowed(inflight):
            return FairnessEligibility(
                eligible=False,
                reason="max_inflight_reached",
                effective_vruntime=effective,
                inflight=inflight,
            )

        return FairnessEligibility(
            eligible=True,
            reason="eligible",
            effective_vruntime=effective,
            inflight=inflight,
        )

    async def apply_starvation_recovery(self, tenant_id: str) -> FairnessRecoveryResult:
        old = await self.get_vruntime(tenant_id)
        new = self._policy.apply_starvation_recovery(old)
        recovered = new != old
        if recovered:
            await self._redis.set(DagRedisKey.tenant_vruntime(tenant_id), str(new))
        return FairnessRecoveryResult(
            tenant_id=tenant_id,
            old_vruntime=old,
            new_vruntime=new,
            recovered=recovered,
        )
