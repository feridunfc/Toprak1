from __future__ import annotations

import logging
import os
import uuid
from typing import Any, Dict

from hfa_worker.consumer import WorkerConsumer
from hfa_worker.drain import DrainManager
from hfa_worker.executor import BaseExecutor
from hfa_worker.executor_factory import build_executor
from hfa_worker.heartbeat import WorkerHeartbeatPublisher

logger = logging.getLogger(__name__)


def _require_env(name: str) -> str:
    val = os.environ.get(name, "")
    if not val:
        raise RuntimeError(f"Required environment variable not set: {name}")
    return val


class WorkerService:
    def __init__(self, redis, config: Dict[str, Any]) -> None:
        self._redis = redis

        self._worker_id = str(config.get("worker_id") or f"worker-{uuid.uuid4().hex[:8]}")
        self._worker_group = str(config.get("worker_group") or "default-group")
        self._region = str(config.get("region") or "us-east-1")
        self._version = str(config.get("version") or "0.0.0")
        self._capabilities: list[str] = list(config.get("capabilities") or ["base"])
        self._shards: list[int] = list(config.get("shards") or [0])
        self._capacity = int(config.get("capacity") or 10)

        executor: BaseExecutor | None = config.get("executor")
        if executor is None:
            executor = build_executor(config)
            logger.info(
                "WorkerService executor built from factory: mode=%s worker=%s",
                config.get("executor_mode", "fake"),
                self._worker_id,
            )
        else:
            logger.info(
                "WorkerService using explicit executor: %s worker=%s",
                type(executor).__name__,
                self._worker_id,
            )

        self._consumer = WorkerConsumer(
            redis=redis,
            worker_id=self._worker_id,
            worker_group=self._worker_group,
            shards=self._shards,
            executor=executor,
        )

        self._heartbeat = WorkerHeartbeatPublisher(
            redis=redis,
            worker_id=self._worker_id,
            worker_group=self._worker_group,
            region=self._region,
            shards=self._shards,
            capacity=self._capacity,
            inflight_fn=lambda: self._consumer.inflight_count,
            is_draining_fn=lambda: self._consumer.is_draining,
            version=self._version,
            capabilities=self._capabilities,
        )

        self._drain_manager = DrainManager(
            redis=redis,
            worker_id=self._worker_id,
            worker_group=self._worker_group,
            shards=self._shards,
            consumer=self._consumer,
        )

    @property
    def worker_id(self) -> str:
        return self._worker_id

    async def start(self) -> None:
        logger.info(
            "WorkerService starting: worker_id=%s group=%s shards=%s capacity=%d",
            self._worker_id,
            self._worker_group,
            self._shards,
            self._capacity,
        )
        await self._consumer.start()
        await self._heartbeat.start()
        logger.info("WorkerService started: worker_id=%s", self._worker_id)

    async def graceful_shutdown(self, drain_timeout: float = 30.0) -> None:
        logger.info("WorkerService graceful_shutdown: worker_id=%s", self._worker_id)
        await self._drain_manager.start_drain(reason="SIGTERM", timeout=drain_timeout)
        await self._consumer.close()
        await self._heartbeat.close()
        logger.info("WorkerService shutdown complete: worker_id=%s", self._worker_id)
