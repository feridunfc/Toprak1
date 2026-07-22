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


class FailingConsumerProbe:
    def __init__(self) -> None:
        self._task = None
        self._renewer_task = None

    @property
    def inflight_count(self) -> int:
        return 0

    @property
    def is_draining(self) -> bool:
        return False

    async def prepare_consumer_groups(self) -> None:
        return None

    async def start(self) -> None:
        raise RuntimeError("consumer-startup-failure-79-11")

    async def close(self) -> None:
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
async def test_post_registration_startup_failure_retracts_schedulability() -> None:
    redis = RedisSink()
    service = WorkerService(
        redis,
        {
            "production": False,
            "worker_id": "worker-startup-failure-79-11",
            "worker_group": "group-startup-failure-79-11",
            "region": "region-startup-failure-79-11",
            "shards": [0],
            "capacity": 1,
            "executor": ExecutorProbe(),
        },
    )
    service._consumer = FailingConsumerProbe()

    with pytest.raises(
        RuntimeError,
        match="consumer-startup-failure-79-11",
    ):
        await service.start()

    flags = _flags(redis)
    assert flags and flags[0] == "0"
    assert flags[-1] == "1", (
        "The first heartbeat registers the worker before consumer startup. "
        "If consumer startup then fails, rollback must publish an immediate "
        "unschedulable projection; otherwise a worker that never became ready "
        "remains HEALTHY until registry TTL expiry."
    )
