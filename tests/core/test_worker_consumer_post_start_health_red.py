from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest
from redis.exceptions import ResponseError

from hfa_worker.consumer import WorkerConsumer


class ExecutorProbe:
    async def execute(self, event):
        return SimpleNamespace(
            status="done",
            payload={},
            cost_cents=0,
            tokens_used=0,
            error="",
        )


class NoGroupAfterStartupRedis:
    def __init__(self) -> None:
        self.xgroup_create_calls = 0
        self.xreadgroup_calls = 0

    async def xgroup_create(self, **kwargs):
        self.xgroup_create_calls += 1
        return True

    async def xpending_range(self, *args, **kwargs):
        return []

    async def xreadgroup(self, *args, **kwargs):
        self.xreadgroup_calls += 1
        raise ResponseError("NOGROUP No such key or consumer group")


def _fatal_task_failure(task: asyncio.Task | None) -> bool:
    if task is None or not task.done() or task.cancelled():
        return False
    try:
        return task.exception() is not None
    except asyncio.CancelledError:
        return False


@pytest.mark.asyncio
async def test_post_start_nogroup_recovers_or_exits_fatally() -> None:
    redis = NoGroupAfterStartupRedis()
    consumer = WorkerConsumer(
        redis=redis,
        worker_id="worker-nogroup-red-79-8",
        worker_group="group-nogroup-red-79-8",
        shards=[0],
        executor=ExecutorProbe(),
    )

    await consumer.start()
    try:
        await asyncio.sleep(0.35)
        task = getattr(consumer, "_task", None)

        recovered_group = redis.xgroup_create_calls >= 2
        fatal_exit = _fatal_task_failure(task)

        assert recovered_group or fatal_exit, (
            "Post-start NOGROUP is currently swallowed forever: the consumer "
            "does not recreate the group and does not exit for WorkerService "
            "fatal-health propagation."
        )
    finally:
        await consumer.close()


@pytest.mark.asyncio
async def test_consumer_close_invalidates_group_preparation_cache() -> None:
    redis = NoGroupAfterStartupRedis()
    consumer = WorkerConsumer(
        redis=redis,
        worker_id="worker-restart-red-79-8",
        worker_group="group-restart-red-79-8",
        shards=[0],
        executor=ExecutorProbe(),
    )

    await consumer.prepare_consumer_groups()
    assert getattr(consumer, "_groups_prepared", False) is True

    await consumer.close()

    assert getattr(consumer, "_groups_prepared", True) is False, (
        "A stopped WorkerConsumer must revalidate durable consumer groups on "
        "the next start instead of trusting a stale preparation cache."
    )
