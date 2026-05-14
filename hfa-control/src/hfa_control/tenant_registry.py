from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from hfa.config.keys import RedisKey, RedisTTL

# Legacy module-level constants kept for any external importers.
# New code should use RedisKey.tenant_config() / RedisKey.tenant_inflight().
TENANT_CONFIG_KEY = "hfa:tenant:{tenant_id}:config"
TENANT_INFLIGHT_KEY = "hfa:tenant:{tenant_id}:inflight"


@dataclass
class TenantConfig:
    tenant_id: str
    weight: int = 1
    max_inflight_runs: Optional[int] = None
    max_runs_per_second: Optional[float] = None

    @classmethod
    def from_redis(cls, tenant_id: str, data: dict) -> "TenantConfig":
        try:
            weight = int(data.get("weight", 1))
        except (TypeError, ValueError):
            weight = 1
        weight = max(1, weight)

        max_inflight_runs = data.get("max_inflight_runs")
        if max_inflight_runs is not None:
            try:
                max_inflight_runs = int(max_inflight_runs)
            except (TypeError, ValueError):
                max_inflight_runs = None

        max_runs_per_second = data.get("max_runs_per_second")
        if max_runs_per_second is not None:
            try:
                max_runs_per_second = float(max_runs_per_second)
            except (TypeError, ValueError):
                max_runs_per_second = None

        return cls(
            tenant_id=tenant_id,
            weight=weight,
            max_inflight_runs=max_inflight_runs,
            max_runs_per_second=max_runs_per_second,
        )

    def to_redis_hash(self) -> dict[str, str]:
        data = {"weight": str(max(1, self.weight))}
        if self.max_inflight_runs is not None:
            data["max_inflight_runs"] = str(self.max_inflight_runs)
        if self.max_runs_per_second is not None:
            data["max_runs_per_second"] = str(self.max_runs_per_second)
        return data


class TenantRegistry:
    """
    Sprint 14A tenant config + inflight state primitives.

    All Redis keys use RedisKey builders; all TTLs use RedisTTL constants.
    """

    def __init__(self, redis):
        self._redis = redis

    async def get_config(self, tenant_id: str) -> TenantConfig:
        key = RedisKey.tenant_config(tenant_id)
        raw = await self._redis.hgetall(key)

        decoded: dict[str, str] = {}
        for k, v in raw.items():
            if isinstance(k, bytes):
                k = k.decode("utf-8")
            if isinstance(v, bytes):
                v = v.decode("utf-8")
            decoded[k] = v

        return TenantConfig.from_redis(tenant_id, decoded)

    async def update_weight(self, tenant_id: str, weight: int) -> TenantConfig:
        key = RedisKey.tenant_config(tenant_id)
        await self._redis.hset(key, "weight", str(max(1, weight)))
        return await self.get_config(tenant_id)

    async def update_limits(
        self,
        tenant_id: str,
        *,
        max_inflight_runs: Optional[int] = None,
        max_runs_per_second: Optional[float] = None,
    ) -> TenantConfig:
        key = RedisKey.tenant_config(tenant_id)

        if max_inflight_runs is not None:
            await self._redis.hset(key, "max_inflight_runs", str(max_inflight_runs))
        if max_runs_per_second is not None:
            await self._redis.hset(key, "max_runs_per_second", str(max_runs_per_second))

        return await self.get_config(tenant_id)

    async def get_inflight(self, tenant_id: str) -> int:
        key = RedisKey.tenant_inflight(tenant_id)
        value = await self._redis.get(key)
        if value is None:
            return 0
        try:
            return int(value)
        except (TypeError, ValueError):
            return 0

    async def increment_inflight(self, tenant_id: str) -> int:
        key = RedisKey.tenant_inflight(tenant_id)
        value = await self._redis.incr(key)
        await self._redis.expire(key, RedisTTL.TENANT_INFLIGHT)
        return int(value)

    async def decrement_inflight(self, tenant_id: str) -> int:
        key = RedisKey.tenant_inflight(tenant_id)
        value = await self._redis.get(key)

        if value is None:
            return 0

        try:
            current = int(value)
        except (TypeError, ValueError):
            current = 0

        new_value = max(0, current - 1)
        await self._redis.set(key, str(new_value))
        await self._redis.expire(key, RedisTTL.TENANT_INFLIGHT)
        return new_value
