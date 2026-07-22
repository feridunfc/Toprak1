from __future__ import annotations

from types import SimpleNamespace

import pytest

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


class RestartableConsumerProbe:
    def __init__(self) -> None:
        self._task = None
        self._renewer_task = None
        self._pulling = True

    @property
    def inflight_count(self) -> int:
        return 0

    @property
    def is_draining(self) -> bool:
        return not self._pulling

    async def prepare_consumer_groups(self) -> None:
        return None

    async def start(self) -> None:
        self._pulling = True

    def stop_pulling(self) -> None:
        self._pulling = False

    async def close(self) -> None:
        self._pulling = False


class DrainProbe:
    def __init__(self, consumer: RestartableConsumerProbe) -> None:
        self._consumer = consumer

    async def start_drain(self, *, reason: str, timeout: float) -> None:
        self._consumer.stop_pulling()

    def reset(self) -> None:
        return None


def _flags(redis: RedisSink) -> list[str]:
    values: list[str] = []
    for stream, fields in redis.xadds:
        if stream != HEARTBEAT_STREAM:
            continue
        raw = fields.get("is_draining", fields.get(b"is_draining", ""))
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8", errors="replace")
        values.append(str(raw))
    return values


@pytest.mark.asyncio
async def test_restart_first_heartbeat_is_healthy_before_ready() -> None:
    redis = RedisSink()
    service = WorkerService(
        redis,
        {
            "production": False,
            "worker_id": "worker-restart-heartbeat-79-11",
            "worker_group": "group-restart-heartbeat-79-11",
            "region": "region-restart-heartbeat-79-11",
            "shards": [0],
            "capacity": 1,
            "executor": ExecutorProbe(),
        },
    )
    consumer = RestartableConsumerProbe()
    service._consumer = consumer
    service._drain_manager = DrainProbe(consumer)

    await service.start()
    await service.stop(drain_timeout=0)
    before_restart = len(_flags(redis))

    await service.start()
    try:
        restart_flags = _flags(redis)[before_restart:]
        assert restart_flags, "Restart did not publish its readiness heartbeat."
        assert restart_flags[0] == "0", (
            "WorkerService starts heartbeat before consumer.start(). After a "
            "previous drain, consumer.is_draining remains true, so restart "
            "must reset accepting-work state before the first heartbeat. A "
            "ready service must not remain projected DRAINING until the next "
            "periodic heartbeat."
        )
    finally:
        await service.close(drain_timeout=0)
