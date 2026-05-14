import json
import time

import fakeredis.aioredis as faredis
import pytest

from hfa.events.codec import serialize_event
from hfa.events.schema import RunRequestedEvent
from hfa.runtime.state_store import StateStore
from hfa_worker.consumer import WorkerConsumer
from hfa_worker.executor import FakeExecutor
from hfa_worker.models import ExecutionResult


class TerminalFailExecutor(FakeExecutor):
    async def execute(self, run_event):
        return ExecutionResult(
            status="failed",
            payload={},
            error="Business logic rejected input",
            cost_cents=10,
            tokens_used=5,
        )


@pytest.mark.asyncio
async def test_terminal_failure_sets_failed_state_and_acks():
    redis = faredis.FakeRedis()
    worker_id = "worker-1"
    worker_group = "test-group"
    shard = 0
    run_id = "test-fail-123"
    tenant_id = "acme"

    consumer = WorkerConsumer(
        redis=redis,
        worker_id=worker_id,
        worker_group=worker_group,
        shards=[shard],
        executor=TerminalFailExecutor(),
    )

    event = RunRequestedEvent(
        run_id=run_id,
        tenant_id=tenant_id,
        agent_type="test",
        priority=5,
        payload={"input": "bad"},
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

    stream = f"hfa:stream:runs:{shard}"
    serialized = serialize_event(event)
    msg_id = await redis.xadd(stream, serialized)
    await redis.xgroup_create(stream, "worker_consumers", id="0", mkstream=True)

    redis_data = {k.encode(): v.encode() for k, v in serialized.items()}
    await consumer._process_message(msg_id, redis_data, stream, shard)

    state = await redis.get(f"hfa:run:state:{run_id}")
    assert state.decode() == "failed"

    result_raw = await redis.get(f"hfa:run:result:{run_id}")
    result = json.loads(result_raw.decode() if isinstance(result_raw, bytes) else result_raw)
    assert result["status"] == "failed"
    assert "Business logic rejected" in result.get("error", "")

    events = await redis.xrange("hfa:stream:results")
    assert len(events) == 1
    data = events[0][1]
    assert data.get(b"event_type", b"").decode() == "RunFailed"

    pending = await redis.xpending(stream, "worker_consumers")
    assert pending["pending"] == 0
