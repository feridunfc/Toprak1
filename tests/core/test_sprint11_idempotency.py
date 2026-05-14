import time

import fakeredis.aioredis as faredis
import pytest

from hfa.events.codec import serialize_event
from hfa.events.schema import RunRequestedEvent
from hfa.runtime.state_store import StateStore
from hfa_worker.consumer import WorkerConsumer
from hfa_worker.executor import FakeExecutor
from hfa_worker.idempotency import IdempotencyGuard


@pytest.mark.asyncio
async def test_atomic_claim_prevents_concurrent_execution():
    redis = faredis.FakeRedis()
    run_id = "test-concurrent-123"
    tenant_id = "acme"
    shard = 0

    state_store = StateStore(redis)
    await state_store.create_run_meta(
        run_id,
        {
            "run_id": run_id,
            "tenant_id": tenant_id,
            "agent_type": "test",
            "reschedule_count": "0",
            "admitted_at": str(time.time()),
            "state": "scheduled",
        },
    )
    await redis.set(f"hfa:run:state:{run_id}", "scheduled", ex=86400)

    guard = IdempotencyGuard(redis)
    result1 = await guard.try_claim_and_mark_running(run_id, "worker-1", "group-a", shard)
    assert result1 is True

    result2 = await guard.try_claim_and_mark_running(run_id, "worker-2", "group-b", shard)
    assert result2 is False

    meta = await state_store.get_run_meta(run_id)
    assert meta.get("worker_id") == "worker-1"
    assert meta.get("worker_group") == "group-a"
    assert meta.get("state") == "running"


@pytest.mark.asyncio
async def test_duplicate_message_after_completion_is_skipped():
    redis = faredis.FakeRedis()

    worker_id = "worker-1"
    worker_group = "test-group"
    shard = 0
    run_id = "test-dup-123"
    tenant_id = "acme"
    stream = f"hfa:stream:runs:{shard}"

    consumer = WorkerConsumer(
        redis=redis,
        worker_id=worker_id,
        worker_group=worker_group,
        shards=[shard],
        executor=FakeExecutor(should_succeed=True),
    )

    event = RunRequestedEvent(
        run_id=run_id,
        tenant_id=tenant_id,
        agent_type="test",
        priority=5,
        payload={"input": "test"},
        idempotency_key=run_id,
    )

    state_store = StateStore(redis)
    await state_store.create_run_meta(
        run_id,
        {
            "run_id": run_id,
            "tenant_id": tenant_id,
            "agent_type": "test",
            "reschedule_count": "0",
            "admitted_at": str(time.time()),
            "state": "scheduled",
        },
    )
    await redis.set(f"hfa:run:state:{run_id}", "scheduled", ex=86400)
    await redis.xgroup_create(stream, "worker_consumers", id="0", mkstream=True)

    serialized = serialize_event(event)
    msg_id1 = await redis.xadd(stream, serialized)
    redis_data = {k.encode(): v.encode() for k, v in serialized.items()}
    await consumer._process_message(msg_id1, redis_data, stream, shard)

    state = await redis.get(f"hfa:run:state:{run_id}")
    assert state.decode() == "done"

    msg_id2 = await redis.xadd(stream, serialized)

    call_count = 0

    class TrackingExecutor(FakeExecutor):
        async def execute(self, run_event):
            nonlocal call_count
            call_count += 1
            return await super().execute(run_event)

    consumer._executor = TrackingExecutor(should_succeed=True)
    await consumer._process_message(msg_id2, redis_data, stream, shard)

    assert call_count == 0
    pending = await redis.xpending(stream, "worker_consumers")
    assert pending["pending"] == 0
