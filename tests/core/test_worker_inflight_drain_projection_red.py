from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest

from hfa_worker.drain import DrainManager
from hfa_worker.heartbeat import HEARTBEAT_STREAM
from hfa_worker.main import WorkerService


class RedisSink:
    def __init__(self) -> None:
        self.xadds: list[tuple[str, dict]] = []

    async def xadd(self, stream, fields, **kwargs):
        self.xadds.append((str(stream), dict(fields)))
        return f"{len(self.xadds)}-0"


class ExecutorProbe:
    async def execute(self, event):
        return SimpleNamespace(
            status="done",
            payload={},
            cost_cents=0,
            tokens_used=0,
            error="",
        )


class InflightConsumerProbe:
    def __init__(self) -> None:
        self._task = None
        self._renewer_task = None
        self._draining = False
        self._inflight = 1
        self.stop_pulling_called = asyncio.Event()

    @property
    def inflight_count(self) -> int:
        return self._inflight

    @property
    def is_draining(self) -> bool:
        return self._draining

    async def prepare_consumer_groups(self) -> None:
        return None

    async def start(self) -> None:
        self._draining = False

    def stop_pulling(self) -> None:
        self._draining = True
        self.stop_pulling_called.set()

    def release_inflight(self) -> None:
        self._inflight = 0

    async def close(self) -> None:
        return None


def _heartbeat_draining_flags(redis: RedisSink) -> list[str]:
    flags: list[str] = []
    for stream, fields in redis.xadds:
        if stream != HEARTBEAT_STREAM:
            continue
        raw = fields.get("is_draining", fields.get(b"is_draining", ""))
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8", errors="replace")
        flags.append(str(raw))
    return flags


@pytest.mark.asyncio
async def test_inflight_drain_projects_unschedulable_before_waiting() -> None:
    redis = RedisSink()
    service = WorkerService(
        redis,
        {
            "production": False,
            "worker_id": "worker-inflight-drain-79-10",
            "worker_group": "group-inflight-drain-79-10",
            "region": "region-inflight-drain-79-10",
            "shards": [0],
            "capacity": 1,
            "executor": ExecutorProbe(),
        },
    )

    consumer = InflightConsumerProbe()
    service._consumer = consumer
    service._drain_manager = DrainManager(
        redis=redis,
        worker_id=service.worker_id,
        worker_group="group-inflight-drain-79-10",
        shards=[0],
        consumer=consumer,
    )

    await service.start()
    stop_task = asyncio.create_task(
        service.stop(drain_timeout=2.0)
    )
    try:
        await asyncio.wait_for(
            consumer.stop_pulling_called.wait(),
            timeout=0.2,
        )

        # Drain is still waiting for inflight work. Scheduler exclusion must
        # already be visible; it must not wait for inflight==0 or timeout.
        await asyncio.sleep(0.05)
        flags = _heartbeat_draining_flags(redis)
        assert flags and flags[0] == "0"
        assert flags[-1] == "1", (
            "WorkerService must publish is_draining=1 immediately after "
            "stop_pulling and before waiting for inflight tasks. Publishing "
            "only after DrainManager returns leaves a scheduling window."
        )
    finally:
        consumer.release_inflight()
        await asyncio.wait_for(stop_task, timeout=1.5)
