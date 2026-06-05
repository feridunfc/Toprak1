from __future__ import annotations

import pytest

from hfa.config.keys import RedisKey
from hfa.events.codec import serialize_event
from hfa.events.schema import RunRequestedEvent
from hfa_worker.consumer import CONSUMER_GROUP, WorkerConsumer
from hfa_worker.fake_executor import FakeExecutor


class FakeRedis:
    def __init__(self) -> None:
        self.xack_calls: list[tuple[str, str, str]] = []
        self.xadd_calls: list[tuple[str, dict]] = []

    async def xack(self, stream: str, group: str, message_id: str) -> int:
        self.xack_calls.append((stream, group, message_id))
        return 1

    async def xadd(self, stream: str, fields: dict) -> str:
        self.xadd_calls.append((stream, fields))
        return "2-0"

    async def hget(self, *args, **kwargs):
        return None

    async def hset(self, *args, **kwargs):
        return 1

    async def hincrby(self, *args, **kwargs):
        return 0

    async def delete(self, *args, **kwargs):
        return 1


@pytest.mark.asyncio
async def test_worker_consumer_process_message_writes_result_completes_and_acks() -> None:
    redis = FakeRedis()
    executor = FakeExecutor()

    consumer = WorkerConsumer(
        redis=redis,
        worker_id="worker-test",
        worker_group="default",
        shards=[0],
        executor=executor,
    )

    calls = {
        "should_execute": [],
        "try_claim": [],
        "store_result": [],
        "transition_state": [],
        "mark_completed": [],
        "release_claim": [],
    }

    async def fake_should_execute(run_id: str) -> bool:
        calls["should_execute"].append(run_id)
        return True

    async def fake_try_claim_and_mark_running(
        run_id: str,
        worker_id: str,
        worker_group: str,
        shard: int,
    ) -> bool:
        calls["try_claim"].append((run_id, worker_id, worker_group, shard))
        return True

    async def fake_store_result(
        run_id: str,
        tenant_id: str,
        status: str,
        payload: dict,
        cost_cents: int,
        tokens_used: int,
        error: str | None = None,
    ) -> None:
        calls["store_result"].append(
            {
                "run_id": run_id,
                "tenant_id": tenant_id,
                "status": status,
                "payload": payload,
                "cost_cents": cost_cents,
                "tokens_used": tokens_used,
                "error": error,
            }
        )

    async def fake_transition_state(run_id: str, state: str) -> None:
        calls["transition_state"].append((run_id, state))

    async def fake_mark_completed(run_id: str) -> None:
        calls["mark_completed"].append(run_id)

    async def fake_release_claim(run_id: str) -> None:
        calls["release_claim"].append(run_id)

    consumer._guard.should_execute = fake_should_execute
    consumer._guard.try_claim_and_mark_running = fake_try_claim_and_mark_running
    consumer._state.store_result = fake_store_result
    consumer._state.transition_state = fake_transition_state
    consumer._state.mark_completed = fake_mark_completed
    consumer._state.release_claim = fake_release_claim

    event = RunRequestedEvent(
        run_id="run-canonical-1",
        tenant_id="tenant-a",
        agent_type="fake",
        payload={"prompt": "hello canonical worker"},
        idempotency_key="idem-1",
    )
    data = serialize_event(event)
    stream = RedisKey.stream_shard(0)

    await consumer._process_message(
        msg_id="1-0",
        data=data,
        stream=stream,
        shard=0,
    )

    assert calls["should_execute"] == ["run-canonical-1"]
    assert calls["try_claim"] == [
        ("run-canonical-1", "worker-test", "default", 0)
    ]

    assert len(calls["store_result"]) == 1
    stored = calls["store_result"][0]
    assert stored["run_id"] == "run-canonical-1"
    assert stored["tenant_id"] == "tenant-a"
    assert stored["status"] == "done"
    assert stored["payload"]["run_id"] == "run-canonical-1"
    assert stored["payload"]["result"] == "success"
    assert stored["payload"]["input"] == {"prompt": "hello canonical worker"}
    assert stored["error"] is None

    assert calls["transition_state"] == [("run-canonical-1", "done")]
    assert calls["mark_completed"] == ["run-canonical-1"]
    assert calls["release_claim"] == []

    assert redis.xadd_calls
    result_stream, result_event = redis.xadd_calls[0]
    assert result_stream == RedisKey.stream_results()
    assert result_event

    assert redis.xack_calls == [
        (stream, CONSUMER_GROUP, "1-0")
    ]

    assert consumer.inflight_count == 0


@pytest.mark.asyncio
async def test_worker_consumer_process_message_skips_terminal_run_and_acks() -> None:
    redis = FakeRedis()
    executor = FakeExecutor()

    consumer = WorkerConsumer(
        redis=redis,
        worker_id="worker-test",
        worker_group="default",
        shards=[0],
        executor=executor,
    )

    calls = {"try_claim": [], "store_result": []}

    async def fake_should_execute(run_id: str) -> bool:
        return False

    async def fake_try_claim_and_mark_running(*args, **kwargs) -> bool:
        calls["try_claim"].append(args)
        return True

    async def fake_store_result(*args, **kwargs) -> None:
        calls["store_result"].append(args)

    consumer._guard.should_execute = fake_should_execute
    consumer._guard.try_claim_and_mark_running = fake_try_claim_and_mark_running
    consumer._state.store_result = fake_store_result

    event = RunRequestedEvent(
        run_id="run-terminal",
        tenant_id="tenant-a",
        agent_type="fake",
        payload={"prompt": "skip"},
        idempotency_key="idem-terminal",
    )
    stream = RedisKey.stream_shard(0)

    await consumer._process_message(
        msg_id="1-1",
        data=serialize_event(event),
        stream=stream,
        shard=0,
    )

    assert calls["try_claim"] == []
    assert calls["store_result"] == []
    assert redis.xack_calls == [(stream, CONSUMER_GROUP, "1-1")]
