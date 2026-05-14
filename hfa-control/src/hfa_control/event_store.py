
from __future__ import annotations

import json
import logging
import time
from typing import Any, Optional

logger = logging.getLogger("hfa.event_store")


class EventStore:
    EVENT_TASK_ADMITTED = "TASK_ADMITTED"
    EVENT_TASK_SCHEDULED = "TASK_SCHEDULED"
    EVENT_TASK_CLAIMED = "TASK_CLAIMED"
    EVENT_TASK_COMPLETED = "TASK_COMPLETED"
    EVENT_TASK_FAILED = "TASK_FAILED"
    EVENT_TASK_REQUEUED = "TASK_REQUEUED"

    def __init__(self, redis_client) -> None:
        self._redis = redis_client

    @staticmethod
    def event_key(run_id: str) -> str:
        return f"hfa:events:{run_id}"

    async def append_event(
        self,
        run_id: str,
        event_type: str,
        worker_id: Optional[str] = None,
        details: Optional[dict[str, Any]] = None,
    ) -> bool:
        event_key = self.event_key(run_id)
        event_data: dict[str, Any] = {
            "event_type": event_type,
            "timestamp_ms": int(time.time() * 1000),
        }
        if worker_id:
            event_data["worker_id"] = worker_id
        if details:
            event_data["details"] = details

        try:
            await self._redis.rpush(event_key, json.dumps(event_data, ensure_ascii=False, sort_keys=True))
            return True
        except Exception as exc:
            logger.error(
                "EventStore append failed: run_id=%s event=%s error=%s",
                run_id,
                event_type,
                exc,
            )
            return False

    async def get_run_history(self, run_id: str) -> list[dict[str, Any]]:
        raw_events = await self._redis.lrange(self.event_key(run_id), 0, -1)
        out: list[dict[str, Any]] = []
        for item in raw_events:
            if isinstance(item, bytes):
                item = item.decode("utf-8")
            out.append(json.loads(item))
        return out
