
from __future__ import annotations

from dataclasses import dataclass

from hfa.dag.schema import DagRedisKey


@dataclass(frozen=True)
class WorkerStateSnapshot:
    worker_id: str
    capacity: int
    current_load: int
    heartbeat_at_ms: int
    freshness_remaining_ms: int

    @property
    def is_stale(self) -> bool:
        return self.heartbeat_at_ms <= 0 or self.freshness_remaining_ms <= 0


@dataclass(frozen=True)
class WorkerConsistencyDecision:
    worker_id: str
    eligible: bool
    reason: str


class WorkerStateConsistency:
    def __init__(self, redis) -> None:
        self._redis = redis

    async def read_worker_snapshot(
        self,
        worker_id: str,
        *,
        now_ms: int,
        stale_after_ms: int,
    ) -> WorkerStateSnapshot:
        raw_load = await self._redis.get(DagRedisKey.worker_load(worker_id))
        raw_capacity = await self._redis.get(DagRedisKey.worker_capacity(worker_id))
        raw_heartbeat = await self._redis.get(DagRedisKey.worker_heartbeat(worker_id))

        load = int(raw_load) if raw_load is not None else 0
        capacity = int(raw_capacity) if raw_capacity is not None else 0
        heartbeat_at = int(raw_heartbeat) if raw_heartbeat is not None else 0

        age = now_ms - heartbeat_at if heartbeat_at > 0 else stale_after_ms
        freshness_remaining = stale_after_ms - age

        return WorkerStateSnapshot(
            worker_id=worker_id,
            capacity=capacity,
            current_load=load,
            heartbeat_at_ms=heartbeat_at,
            freshness_remaining_ms=freshness_remaining,
        )

    async def evaluate_worker(
        self,
        worker_id: str,
        *,
        now_ms: int,
        stale_after_ms: int,
    ) -> WorkerConsistencyDecision:
        snap = await self.read_worker_snapshot(
            worker_id,
            now_ms=now_ms,
            stale_after_ms=stale_after_ms,
        )

        if snap.heartbeat_at_ms <= 0:
            return WorkerConsistencyDecision(worker_id=worker_id, eligible=False, reason="missing_heartbeat")

        if snap.freshness_remaining_ms <= 0:
            return WorkerConsistencyDecision(worker_id=worker_id, eligible=False, reason="stale_heartbeat")

        if snap.capacity > 0 and snap.current_load >= snap.capacity:
            return WorkerConsistencyDecision(worker_id=worker_id, eligible=False, reason="capacity_full")

        return WorkerConsistencyDecision(worker_id=worker_id, eligible=True, reason="eligible")

    async def filter_eligible_workers(
        self,
        worker_ids: list[str],
        *,
        now_ms: int,
        stale_after_ms: int,
    ) -> list[WorkerConsistencyDecision]:
        return [
            await self.evaluate_worker(
                worker_id,
                now_ms=now_ms,
                stale_after_ms=stale_after_ms,
            )
            for worker_id in worker_ids
        ]
