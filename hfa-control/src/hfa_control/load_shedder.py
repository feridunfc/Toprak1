
from __future__ import annotations

from hfa.runtime.backpressure import BackpressurePolicy, LoadSheddingDecision


class LoadShedder:
    def __init__(self, redis, policy: BackpressurePolicy) -> None:
        self._redis = redis
        self._policy = policy

    @staticmethod
    def _tenant_ready_key(tenant_id: str) -> str:
        return f"hfa:dag:tenant:{tenant_id}:ready"

    @staticmethod
    def _tenant_running_key(tenant_id: str) -> str:
        return f"hfa:dag:tenant:{tenant_id}:running"

    async def tenant_pending(self, tenant_id: str) -> int:
        return int(await self._redis.zcard(self._tenant_ready_key(tenant_id)))

    async def tenant_running(self, tenant_id: str) -> int:
        return int(await self._redis.zcard(self._tenant_running_key(tenant_id)))

    async def global_pending(self, tenant_ids: list[str]) -> int:
        total = 0
        for tenant_id in tenant_ids:
            total += await self.tenant_pending(tenant_id)
        return total

    async def evaluate(
        self,
        *,
        tenant_id: str,
        known_tenant_ids: list[str] | None = None,
    ) -> LoadSheddingDecision:
        tenant_ids = known_tenant_ids or [tenant_id]
        tenant_pending = await self.tenant_pending(tenant_id)
        tenant_running = await self.tenant_running(tenant_id)
        global_pending = await self.global_pending(tenant_ids)

        if not self._policy.global_pending_allowed(global_pending):
            return LoadSheddingDecision(
                allowed=False,
                reason="global_backpressure",
                tenant_pending=tenant_pending,
                global_pending=global_pending,
                tenant_running=tenant_running,
            )

        if not self._policy.tenant_pending_allowed(tenant_pending):
            return LoadSheddingDecision(
                allowed=False,
                reason="tenant_rate_limited",
                tenant_pending=tenant_pending,
                global_pending=global_pending,
                tenant_running=tenant_running,
            )

        if not self._policy.tenant_running_allowed(tenant_running):
            return LoadSheddingDecision(
                allowed=False,
                reason="tenant_running_limit",
                tenant_pending=tenant_pending,
                global_pending=global_pending,
                tenant_running=tenant_running,
            )

        return LoadSheddingDecision(
            allowed=True,
            reason="allowed",
            tenant_pending=tenant_pending,
            global_pending=global_pending,
            tenant_running=tenant_running,
        )
