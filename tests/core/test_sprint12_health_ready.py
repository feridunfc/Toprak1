"""
tests/core/test_sprint12_health_ready.py
IRONCLAD Sprint 12 — Worker lifecycle + health/readiness tests

Verifies:
  - WorkerConsumer.close() does not leak tasks (gather pattern)
  - WorkerConsumer.start() + close() round-trip is clean
  - WorkerService wires consumer/heartbeat/drain correctly
  - DrainManager.start_drain sets consumer.is_draining
  - consumer.is_draining reflects stop_pulling state
"""

from __future__ import annotations

import asyncio

import fakeredis.aioredis as faredis
import pytest

from hfa_worker.consumer import WorkerConsumer
from hfa_worker.drain import DrainManager
from hfa_worker.executor import FakeExecutor
from hfa_worker.main import WorkerService


# ---------------------------------------------------------------------------
# WorkerConsumer lifecycle
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_consumer_start_close_no_task_leak():
    redis = faredis.FakeRedis()
    consumer = WorkerConsumer(
        redis=redis,
        worker_id="w-lifecycle-1",
        worker_group="grp",
        shards=[0],
        executor=FakeExecutor(),
    )

    await consumer.start()
    assert consumer._task is not None
    assert consumer._renewer_task is not None

    await consumer.close()

    # Both tasks must be cleared
    assert consumer._task is None
    assert consumer._renewer_task is None


@pytest.mark.asyncio
async def test_consumer_close_is_idempotent():
    redis = faredis.FakeRedis()
    consumer = WorkerConsumer(
        redis=redis,
        worker_id="w-lifecycle-2",
        worker_group="grp",
        shards=[0],
        executor=FakeExecutor(),
    )

    await consumer.start()
    await consumer.close()
    # Second close should not raise
    await consumer.close()


@pytest.mark.asyncio
async def test_consumer_is_draining_after_stop_pulling():
    redis = faredis.FakeRedis()
    consumer = WorkerConsumer(
        redis=redis,
        worker_id="w-drain-flag",
        worker_group="grp",
        shards=[0],
        executor=FakeExecutor(),
    )

    assert consumer.is_draining is False
    consumer.stop_pulling()
    assert consumer.is_draining is True


@pytest.mark.asyncio
async def test_consumer_inflight_count_reflects_set():
    redis = faredis.FakeRedis()
    consumer = WorkerConsumer(
        redis=redis,
        worker_id="w-inflight",
        worker_group="grp",
        shards=[0],
        executor=FakeExecutor(),
    )

    assert consumer.inflight_count == 0
    consumer._inflight.add("run-1")
    consumer._inflight.add("run-2")
    assert consumer.inflight_count == 2
    consumer._inflight.discard("run-1")
    assert consumer.inflight_count == 1


# ---------------------------------------------------------------------------
# WorkerService lifecycle
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_worker_service_start_shutdown():
    redis = faredis.FakeRedis()
    svc = WorkerService(
        redis,
        {
            "worker_id": "ws-test-1",
            "worker_group": "grp",
            "shards": [0],
            "capacity": 5,
            "executor": FakeExecutor(),
        },
    )

    await svc.start()
    assert svc.worker_id == "ws-test-1"

    # graceful_shutdown should complete without hanging
    await asyncio.wait_for(
        svc.graceful_shutdown(drain_timeout=0.5),
        timeout=5.0,
    )


@pytest.mark.asyncio
async def test_worker_service_worker_id_generated_if_absent():
    redis = faredis.FakeRedis()
    svc = WorkerService(
        redis,
        {
            "executor": FakeExecutor(),
            "shards": [0],
        },
    )
    assert svc.worker_id.startswith("worker-")
    await svc.start()
    await asyncio.wait_for(svc.graceful_shutdown(drain_timeout=0.2), timeout=3.0)


# ---------------------------------------------------------------------------
# DrainManager integration with consumer
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_drain_manager_calls_stop_pulling():
    from unittest.mock import MagicMock

    redis = faredis.FakeRedis()
    consumer = MagicMock()
    consumer.inflight_count = 0
    consumer.stop_pulling = MagicMock()

    drain = DrainManager(
        redis=redis,
        worker_id="w1",
        worker_group="grp",
        shards=[],
        consumer=consumer,
    )
    await drain.start_drain(timeout=0.2)

    consumer.stop_pulling.assert_called_once()


@pytest.mark.asyncio
async def test_drain_manager_releases_owned_shards():
    redis = faredis.FakeRedis()
    from unittest.mock import MagicMock

    consumer = MagicMock()
    consumer.inflight_count = 0
    consumer.stop_pulling = MagicMock()

    shards = [0, 1]
    for s in shards:
        await redis.set(f"hfa:cp:shard:owner:{s}", "grp")

    drain = DrainManager(
        redis=redis,
        worker_id="w1",
        worker_group="grp",
        shards=shards,
        consumer=consumer,
    )
    await drain.start_drain(timeout=0.2)

    for s in shards:
        exists = await redis.exists(f"hfa:cp:shard:owner:{s}")
        assert exists == 0


@pytest.mark.asyncio
async def test_drain_manager_does_not_release_shards_owned_by_other():
    redis = faredis.FakeRedis()
    from unittest.mock import MagicMock

    consumer = MagicMock()
    consumer.inflight_count = 0
    consumer.stop_pulling = MagicMock()

    # Shard owned by a different group
    await redis.set("hfa:cp:shard:owner:0", "other-group")

    drain = DrainManager(
        redis=redis,
        worker_id="w1",
        worker_group="grp",  # does not own shard 0
        shards=[0],
        consumer=consumer,
    )
    await drain.start_drain(timeout=0.2)

    # Should not delete shard owned by other-group
    owner = await redis.get("hfa:cp:shard:owner:0")
    owner_str = owner.decode() if isinstance(owner, bytes) else owner
    assert owner_str == "other-group"
