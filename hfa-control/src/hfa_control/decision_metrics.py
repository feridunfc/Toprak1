
from __future__ import annotations

from dataclasses import dataclass

from hfa.dag.schema import DagRedisKey


@dataclass(frozen=True)
class DecisionMetricUpdate:
    metric_name: str
    new_value: int


class DecisionMetrics:
    def __init__(self, redis) -> None:
        self._redis = redis

    async def incr(self, metric_name: str, amount: int = 1) -> DecisionMetricUpdate:
        key = DagRedisKey.scheduler_metric_counter(metric_name)
        new_value = await self._redis.incrby(key, amount)
        return DecisionMetricUpdate(metric_name=metric_name, new_value=int(new_value))

    async def get(self, metric_name: str) -> int:
        raw = await self._redis.get(DagRedisKey.scheduler_metric_counter(metric_name))
        return int(raw) if raw is not None else 0
