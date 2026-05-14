import time

import fakeredis.aioredis as faredis
import pytest

from hfa.events.codec import serialize_event
from hfa.events.schema import RunRequestedEvent
from hfa.runtime.state_store import StateStore
from hfa_worker.consumer import WorkerConsumer
from hfa_worker.executor import FakeExecutor


@pytest.mark.asyncio
async def test_worker_resumes_pending_messages_on_startup():
    redis = faredis.FakeRedis()

    worker_id = "worker-1"
    worker_group = "test-group"
    shard = 0
    run_id = "test-pending-123"
    tenant_id = "acme"
    stream = f"hfa:stream:runs:{shard}"
    consumer_group = "worker_consumers"

    await redis.xgroup_create(stream, consumer_group, id="0", mkstream=True)

    event = RunRequestedEvent(
        run_id=run_id,
        tenant_id=tenant_id,
        agent_type="test",
        priority=5,
        payload={"input": "test"},
        idempotency_key=run_id,
    )

    serialized = serialize_event(event)
    await redis.xadd(stream, serialized)

    # old worker reads but does not ACK
    await redis.xreadgroup(
        consumer_group,
        "worker-old",
        {stream: ">"},
        count=1,
    )

    pending = await redis.xpending(stream, consumer_group)
    assert pending["pending"] == 1

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

    consumer = WorkerConsumer(
        redis=redis,
        worker_id=worker_id,
        worker_group=worker_group,
        shards=[shard],
        executor=FakeExecutor(should_succeed=True),
        reclaim_idle_ms=0,
    )

    # IMPORTANT:
    # Do not call consumer.start() here.
    # That would enter the infinite consume loop and hang pytest teardown.
    await consumer._reclaim_pending_messages()

    state = await redis.get(f"hfa:run:state:{run_id}")
    assert state.decode() == "done"

    pending = await redis.xpending(stream, consumer_group)
    assert pending["pending"] == 0

    events = await redis.xrange("hfa:stream:results")
    assert len(events) == 1
