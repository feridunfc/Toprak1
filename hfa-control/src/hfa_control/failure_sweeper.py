from __future__ import annotations

import logging

from hfa.dag.schema import DagRedisKey

logger = logging.getLogger(__name__)


class FailureSweeper:
    """
    Lazy failure propagation:
    - failed parent task -> direct children become blocked_by_failure
    - idempotent
    """

    def __init__(self, redis) -> None:
        self._redis = redis

    async def sweep_failed_task(self, task_id: str) -> int:
        children_key = DagRedisKey.task_children(task_id)
        children = await self._redis.smembers(children_key)

        blocked = 0

        for child in children:
            state_key = DagRedisKey.task_state(child)
            state = await self._redis.get(state_key)

            if state in ("pending", "ready"):
                await self._redis.set(state_key, "blocked_by_failure")
                blocked += 1

        logger.info(
            "FailureSweeper: task=%s blocked_children=%s",
            task_id,
            blocked,
        )
        return blocked