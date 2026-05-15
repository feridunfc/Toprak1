from __future__ import annotations

import logging

from hfa.dag.schema import DagRedisKey
from hfa.events.append_service import AuthoritativeEventAppendError, AuthoritativeEventGate

logger = logging.getLogger(__name__)


class FailureSweeper:
    """
    Lazy failure propagation:
    - failed parent task -> direct children become blocked_by_failure
    - idempotent
    """

    def __init__(self, redis, event_store=None) -> None:
        self._redis = redis
        self._event_store = event_store

    async def sweep_failed_task(self, task_id: str) -> int:
        children_key = DagRedisKey.task_children(task_id)
        children = await self._redis.smembers(children_key)

        blocked = 0

        for child in children:
            state_key = DagRedisKey.task_state(child)
            state = await self._redis.get(state_key)

            if state in ("pending", "ready"):
                try:
                    if self._event_store is not None:
                        await AuthoritativeEventGate(self._event_store, enabled=True).append_before_authoritative_write(
                            run_id=str(child),
                            event_type="TASK_BLOCKED_BY_FAILURE",
                            worker_id=None,
                            details={
                                "task_id": str(child),
                                "parent_task_id": task_id,
                                "previous_state": state.decode("utf-8") if isinstance(state, bytes) else state,
                                "target_state": "blocked_by_failure",
                            },
                            authority="FailureSweeper.block_child",
                        )
                    # AUTHORITY_REVIEWED_PROJECTION_WRITE:
                    # Child task block marker is written only after TASK_BLOCKED_BY_FAILURE proof/event when available.
                    await self._redis.set(state_key, "blocked_by_failure")
                    blocked += 1
                except AuthoritativeEventAppendError as exc:
                    logger.error(
                        "FailureSweeper blocked_by_failure event append failed: parent=%s child=%s error=%s",
                        task_id,
                        child,
                        exc,
                    )

        logger.info(
            "FailureSweeper: task=%s blocked_children=%s",
            task_id,
            blocked,
        )
        return blocked