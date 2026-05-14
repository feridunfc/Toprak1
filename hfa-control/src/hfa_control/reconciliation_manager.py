
from __future__ import annotations

from dataclasses import dataclass

from hfa.dag.schema import DagRedisKey


@dataclass(frozen=True)
class ReconciliationReport:
    tenant_id: str
    expected_inflight: int
    actual_inflight: int
    corrected: bool


class ReconciliationManager:
    def __init__(self, redis) -> None:
        self._redis = redis

    async def actual_inflight(self, tenant_id: str) -> int:
        return int(await self._redis.zcard(DagRedisKey.task_running_zset(tenant_id)))

    async def expected_inflight(self, tenant_id: str) -> int:
        raw = await self._redis.get(DagRedisKey.tenant_inflight(tenant_id))
        return int(raw) if raw is not None else 0

    async def reconcile_tenant(self, tenant_id: str) -> ReconciliationReport:
        expected = await self.expected_inflight(tenant_id)
        actual = await self.actual_inflight(tenant_id)

        corrected = expected != actual
        if corrected:
            await self._redis.set(DagRedisKey.tenant_inflight(tenant_id), str(actual))

        return ReconciliationReport(
            tenant_id=tenant_id,
            expected_inflight=expected,
            actual_inflight=actual,
            corrected=corrected,
        )

    async def reconcile_many(self, tenant_ids: list[str]) -> list[ReconciliationReport]:
        return [await self.reconcile_tenant(tenant_id) for tenant_id in tenant_ids]
