
from __future__ import annotations

from dataclasses import dataclass

from hfa.dag.fairness import TenantFairnessSnapshot
from hfa.dag.schema import DagRedisKey


@dataclass(frozen=True)
class FairnessUpdateResult:
    tenant_id: str
    old_vruntime: float
    new_vruntime: float
    delta: float


class FairnessManager:
    def __init__(self, redis) -> None:
        self._redis = redis

    async def get_snapshot(self, tenant_id: str, *, weight: float = 1.0) -> TenantFairnessSnapshot:
        raw_vruntime = await self._redis.get(DagRedisKey.tenant_vruntime(tenant_id))
        raw_inflight = await self._redis.get(DagRedisKey.tenant_inflight(tenant_id))

        vruntime = float(raw_vruntime) if raw_vruntime is not None else 0.0
        inflight = int(raw_inflight) if raw_inflight is not None else 0
        return TenantFairnessSnapshot(
            tenant_id=tenant_id,
            vruntime=vruntime,
            weight=weight,
            inflight=inflight,
        )

    async def add_runtime(self, tenant_id: str, *, runtime_ms: int, weight: float = 1.0) -> FairnessUpdateResult:
        snapshot = await self.get_snapshot(tenant_id, weight=weight)
        delta = snapshot.normalized_runtime(runtime_ms)
        new_value = snapshot.vruntime + delta
        await self._redis.set(DagRedisKey.tenant_vruntime(tenant_id), str(new_value))
        return FairnessUpdateResult(
            tenant_id=tenant_id,
            old_vruntime=snapshot.vruntime,
            new_vruntime=new_value,
            delta=delta,
        )

    async def set_inflight(self, tenant_id: str, inflight: int) -> None:
        await self._redis.set(DagRedisKey.tenant_inflight(tenant_id), str(inflight))

    async def choose_fairest_tenant(
        self,
        tenant_ids: list[str],
        *,
        weights: dict[str, float] | None = None,
    ) -> TenantFairnessSnapshot:
        weights = weights or {}
        snapshots = [
            await self.get_snapshot(tenant_id, weight=weights.get(tenant_id, 1.0))
            for tenant_id in tenant_ids
        ]
        snapshots.sort(key=lambda s: (s.vruntime, s.inflight, s.tenant_id))
        return snapshots[0]
