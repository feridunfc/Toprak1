import asyncio

import fakeredis.aioredis as faredis
import pytest
from unittest.mock import MagicMock

from hfa_worker.drain import DrainManager


@pytest.mark.asyncio
async def test_drain_publishes_event_and_releases_shards():
    redis = faredis.FakeRedis()

    worker_id = "worker-1"
    worker_group = "group-a"
    shards = [0, 1, 2]
    control_stream = "hfa:stream:control"

    for shard in shards:
        await redis.set(f"hfa:cp:shard:owner:{shard}", worker_group)

    mock_consumer = MagicMock()
    mock_consumer.inflight_count = 0
    mock_consumer.stop_pulling = MagicMock()

    drain = DrainManager(
        redis=redis,
        worker_id=worker_id,
        worker_group=worker_group,
        shards=shards,
        consumer=mock_consumer,
        control_stream=control_stream,
    )

    await drain.start_drain(reason="test", timeout=1.0)

    events = await redis.xrange(control_stream)
    assert len(events) == 1
    data = events[0][1]
    assert data.get(b"event_type", b"").decode() == "WorkerDraining"
    assert data.get(b"worker_id", b"").decode() == worker_id
    assert data.get(b"reason", b"").decode() == "test"

    mock_consumer.stop_pulling.assert_called_once()

    for shard in shards:
        exists = await redis.exists(f"hfa:cp:shard:owner:{shard}")
        assert exists == 0


@pytest.mark.asyncio
async def test_drain_waits_for_inflight_tasks():
    redis = faredis.FakeRedis()
    worker_id = "worker-1"
    worker_group = "group-a"
    shards = [0]

    await redis.set("hfa:cp:shard:owner:0", worker_group)

    mock_consumer = MagicMock()
    mock_consumer.inflight_count = 2
    mock_consumer.stop_pulling = MagicMock()

    drain = DrainManager(
        redis=redis,
        worker_id=worker_id,
        worker_group=worker_group,
        shards=shards,
        consumer=mock_consumer,
    )

    drain_task = asyncio.create_task(drain.start_drain(timeout=0.5))
    await asyncio.sleep(0.1)
    mock_consumer.inflight_count = 0
    await drain_task

    exists = await redis.exists("hfa:cp:shard:owner:0")
    assert exists == 0
