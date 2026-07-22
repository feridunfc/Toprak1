from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest

from hfa_worker.heartbeat import HEARTBEAT_STREAM
from hfa_worker.main import WorkerService


class HeartbeatRedisSink:
    def __init__(self) -> None:
        self.xadds: list[tuple[str, dict]] = []

    async def xadd(self, stream, fields, **kwargs):
        self.xadds.append((str(stream), dict(fields)))
        return f"{len(self.xadds)}-0"


class LegacyExecutorProbe:
    async def execute(self, event):
        return SimpleNamespace(
            status="done",
            payload={},
            cost_cents=0,
            tokens_used=0,
            error="",
        )


class ConsumerProbe:
    def __init__(self, *, fail_on_trigger: bool = False) -> None:
        self._task: asyncio.Task | None = None
        self._renewer_task = None
        self._draining = False
        self._trigger = asyncio.Event()
        self._fail_on_trigger = fail_on_trigger

    @property
    def inflight_count(self) -> int:
        return 0

    @property
    def is_draining(self) -> bool:
        return self._draining

    async def prepare_consumer_groups(self) -> None:
        return None

    async def _background(self) -> None:
        await self._trigger.wait()
        if self._fail_on_trigger:
            raise RuntimeError("consumer-fatal-79-9")

    async def start(self) -> None:
        self._draining = False
        if self._fail_on_trigger:
            self._task = asyncio.create_task(
                self._background(),
                name="consumer-fatal-red-79-9",
            )

    def stop_pulling(self) -> None:
        self._draining = True

    def trigger_failure(self) -> None:
        self._trigger.set()

    async def close(self) -> None:
        task = self._task
        self._task = None
        if task is not None and not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)


class DrainProbe:
    def __init__(self, consumer: ConsumerProbe) -> None:
        self._consumer = consumer

    async def start_drain(self, *, reason: str, timeout: float) -> None:
        self._consumer.stop_pulling()

    def reset(self) -> None:
        return None


def _draining_flags(redis: HeartbeatRedisSink) -> list[str]:
    flags: list[str] = []
    for stream, fields in redis.xadds:
        if stream != HEARTBEAT_STREAM:
            continue
        raw = (
            fields.get("is_draining")
            if "is_draining" in fields
            else fields.get(b"is_draining")
        )
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8", errors="replace")
        flags.append(str(raw or ""))
    return flags


def _build_service(
    redis: HeartbeatRedisSink,
    *,
    fatal_consumer: bool,
) -> tuple[WorkerService, ConsumerProbe]:
    service = WorkerService(
        redis,
        {
            "production": False,
            "worker_id": "worker-projection-79-9",
            "worker_group": "group-projection-79-9",
            "region": "region-projection-79-9",
            "shards": [0],
            "capacity": 1,
            "executor": LegacyExecutorProbe(),
        },
    )
    consumer = ConsumerProbe(fail_on_trigger=fatal_consumer)
    service._consumer = consumer
    service._drain_manager = DrainProbe(consumer)
    return service, consumer


@pytest.mark.asyncio
async def test_zero_inflight_stop_publishes_draining_before_heartbeat_close() -> None:
    redis = HeartbeatRedisSink()
    service, _consumer = _build_service(
        redis,
        fatal_consumer=False,
    )

    await service.start()
    await service.stop(drain_timeout=0)

    flags = _draining_flags(redis)
    assert flags and flags[0] == "0"
    assert flags[-1] == "1", (
        "WorkerService.stop() must publish an unschedulable/draining heartbeat "
        "after stop_pulling and before closing the heartbeat publisher. With "
        "zero inflight, waiting for the periodic loop is not sufficient."
    )


@pytest.mark.asyncio
async def test_fatal_consumer_failure_publishes_unschedulable_before_close() -> None:
    redis = HeartbeatRedisSink()
    service, consumer = _build_service(
        redis,
        fatal_consumer=True,
    )

    await service.start()
    try:
        consumer.trigger_failure()
        failure = await asyncio.wait_for(
            service.wait_for_failure(),
            timeout=0.5,
        )
        assert "consumer-fatal-79-9" in str(failure)

        cleanup = getattr(service, "_fatal_cleanup_task", None)
        if isinstance(cleanup, asyncio.Task):
            await asyncio.wait_for(
                asyncio.shield(cleanup),
                timeout=0.5,
            )

        flags = _draining_flags(redis)
        assert flags and flags[0] == "0"
        assert flags[-1] == "1", (
            "A fatal worker background failure must publish an immediate "
            "unschedulable/draining projection before heartbeat shutdown. "
            "Leaving the last projection HEALTHY until TTL expiry permits "
            "new reservations to a dead worker."
        )
    finally:
        await service.close(drain_timeout=0)
