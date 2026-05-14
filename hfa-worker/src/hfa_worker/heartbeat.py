"""
hfa-worker/src/hfa_worker/heartbeat.py
IRONCLAD Sprint 11 --- Worker Heartbeat Publisher with Drain Status
"""

from __future__ import annotations

import asyncio
import logging
import os
import time
from typing import Callable, List, Optional

from hfa.events.codec import serialize_event
from hfa.events.schema import WorkerHeartbeatEvent
from hfa.config.keys import RedisKey

logger = logging.getLogger(__name__)

HEARTBEAT_INTERVAL = float(os.environ.get("WORKER_HEARTBEAT_INTERVAL", "10"))
# Module-level constant kept for any external importers; uses canonical key.
HEARTBEAT_STREAM = RedisKey.stream_heartbeat()


class WorkerHeartbeatPublisher:
    def __init__(
        self,
        redis,
        worker_id: str,
        worker_group: str,
        region: str,
        shards: List[int],
        capacity: int,
        inflight_fn: Callable[[], int],
        is_draining_fn: Callable[[], bool],
        version: str = "",
        capabilities: Optional[List[str]] = None,
    ) -> None:
        self._redis = redis
        self._worker_id = worker_id
        self._worker_group = worker_group
        self._region = region
        self._shards = shards
        self._capacity = capacity
        self._inflight_fn = inflight_fn
        self._is_draining_fn = is_draining_fn
        self._version = version
        self._capabilities = capabilities or []
        self._task: Optional[asyncio.Task] = None
        self._stopped = False

    async def start(self) -> None:
        if self._task is not None:
            logger.warning("HeartbeatPublisher already started")
            return
        self._stopped = False
        loop = asyncio.get_running_loop()
        self._task = loop.create_task(self._loop(), name=f"heartbeat.{self._worker_id}")

    async def close(self) -> None:
        if self._task is None:
            return
        self._stopped = True
        self._task.cancel()
        try:
            await self._task
        except asyncio.CancelledError:
            pass
        self._task = None

    async def _loop(self) -> None:
        while not self._stopped:
            try:
                await self._publish()
            except Exception as exc:
                logger.error("Heartbeat publish error: %s", exc)
            try:
                await asyncio.sleep(HEARTBEAT_INTERVAL)
            except asyncio.CancelledError:
                break

    async def _publish(self) -> None:
        inflight = self._inflight_fn()
        is_draining = self._is_draining_fn()

        event = WorkerHeartbeatEvent(
            worker_id=self._worker_id,
            worker_group=self._worker_group,
            region=self._region,
            shards=self._shards,
            capacity=self._capacity,
            inflight=inflight,
            version=self._version,
            capabilities=self._capabilities,
            timestamp=time.time(),
        )

        fields = serialize_event(event)
        fields["is_draining"] = "1" if is_draining else "0"

        await self._redis.xadd(
            HEARTBEAT_STREAM,
            fields,
            maxlen=10_000,
            approximate=True,
        )
