
from __future__ import annotations

from dataclasses import dataclass

from hfa.dag.schema import DagRedisKey

@dataclass(frozen=True)
class ReapedReservation:
    key: str
    task_id: str
    reason: str
    scheduler_id: str = ""

class ReservationReaper:
    def __init__(self, redis) -> None:
        self._redis = redis

    async def reap_orphaned_reservations(self) -> int:
        pattern = DagRedisKey.worker_reservation_pattern()
        cursor = 0
        reaped = 0
        while True:
            cursor, keys = await self._redis.scan(cursor=cursor, match=pattern, count=100)
            for key in keys:
                task_id = await self._redis.hget(key, "task_id")
                if not task_id:
                    await self._redis.delete(key)
                    reaped += 1
                    continue

                state = await self._redis.get(DagRedisKey.task_state(task_id))
                if state is None or state != "scheduled":
                    await self._redis.delete(key)
                    reaped += 1
            if cursor == 0:
                break
        return reaped

    async def list_orphaned_reservations(self) -> list[ReapedReservation]:
        pattern = DagRedisKey.worker_reservation_pattern()
        cursor = 0
        items: list[ReapedReservation] = []
        while True:
            cursor, keys = await self._redis.scan(cursor=cursor, match=pattern, count=100)
            for key in keys:
                task_id = await self._redis.hget(key, "task_id")
                scheduler_id = await self._redis.hget(key, "scheduler_id")
                if not task_id:
                    items.append(ReapedReservation(key=key, task_id="", reason="missing_task_id", scheduler_id=scheduler_id or ""))
                    continue
                state = await self._redis.get(DagRedisKey.task_state(task_id))
                if state is None:
                    items.append(ReapedReservation(key=key, task_id=task_id, reason="missing_task_state", scheduler_id=scheduler_id or ""))
                elif state != "scheduled":
                    items.append(ReapedReservation(key=key, task_id=task_id, reason=f"task_state_{state}", scheduler_id=scheduler_id or ""))
            if cursor == 0:
                break
        return items
