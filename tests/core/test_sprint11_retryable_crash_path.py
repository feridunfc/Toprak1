import time

import fakeredis.aioredis as faredis
import pytest

from hfa.events.codec import serialize_event
from hfa.events.schema import RunRequestedEvent
from hfa.runtime.state_store import StateStore
from hfa_worker.consumer import WorkerConsumer
from hfa_worker.executor import FakeExecutor
from hfa_worker.models import InfrastructureError


class CrashExecutor(FakeExecutor):
    async def execute(self, run_event):
        raise InfrastructureError("Redis connection lost")


@pytest.mark.asyncio
async def test_infrastructure_crash_does_not_ack():
    redis = faredis.FakeRedis()
    worker_id = "worker-1"
    worker_group = "test-group"
    shard = 0
    run_id = "test-crash-123"
    tenant_id = "acme"
    stream = f"hfa:stream:runs:{shard}"

    consumer = WorkerConsumer(
        redis=redis,
        worker_id=worker_id,
        worker_group=worker_group,
        shards=[shard],
        executor=CrashExecutor(),
    )

    event = RunRequestedEvent(
        run_id=run_id,
        tenant_id=tenant_id,
        agent_type="test",
        priority=5,
        payload={"input": "crash"},
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

    serialized = serialize_event(event)
    await redis.xadd(stream, serialized)
    await redis.xgroup_create(stream, "worker_consumers", id="0", mkstream=True)

    msgs = await redis.xreadgroup(
        "worker_consumers",
        worker_id,
        {stream: ">"},
        count=1,
    )

    _, entries = msgs[0]
    msg_id, redis_data = entries[0]

    await consumer._process_message(msg_id, redis_data, stream, shard)

    state = await redis.get(f"hfa:run:state:{run_id}")
    assert state.decode() == "running"

    result = await redis.get(f"hfa:run:result:{run_id}")
    assert result is None

    events = await redis.xrange("hfa:stream:results")
    assert len(events) == 0

    pending = await redis.xpending(stream, "worker_consumers")
    assert pending["pending"] == 1
