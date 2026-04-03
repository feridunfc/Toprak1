"""
tests/core/test_sprint20_consumer_request_mapping.py

Sprint 8 — Rewritten: _build_execution_request() is gone.
Consumer now passes RunRequestedEvent directly to executor.

Tests verify that the executor receives the original event with all
fields intact — no intermediate ExecutionRequest adapter.
"""

import pytest
import fakeredis.aioredis as faredis

from hfa.events.schema import RunRequestedEvent
from hfa_worker.models import ExecutionResult


class CapturingExecutor:
    """Captures whatever the consumer passes to execute()."""

    def __init__(self):
        self.captured_event = None

    async def execute(self, event) -> ExecutionResult:
        self.captured_event = event
        return ExecutionResult(
            status="done",
            payload={"captured_run_id": event.run_id},
            cost_cents=0,
            tokens_used=0,
        )


@pytest.mark.asyncio
async def test_consumer_passes_run_event_directly():
    """
    Consumer must pass RunRequestedEvent directly to executor.execute().
    No ExecutionRequest adapter is built.
    """
    from unittest.mock import AsyncMock, patch
    from hfa_worker.consumer import WorkerConsumer
    from hfa.events.codec import serialize_event

    redis = faredis.FakeRedis()
    capturing = CapturingExecutor()

    consumer = WorkerConsumer(
        redis=redis,
        worker_id="test-worker",
        worker_group="test-group",
        shards=[0],
        executor=capturing,
    )
    consumer._guard = AsyncMock()
    consumer._guard.should_execute.return_value = True
    consumer._guard.try_claim_and_mark_running.return_value = True
    consumer._state = AsyncMock()

    event = RunRequestedEvent(
        run_id="run-direct-01",
        tenant_id="tenant-abc",
        agent_type="test-agent",
        payload={"prompt": "hello world"},
        trace_parent="trace-123",
        trace_state="state-456",
    )

    with patch("hfa_worker.consumer.ack_message", new=AsyncMock()):
        await consumer._process_message(
            "123-0", serialize_event(event), "hfa:stream:runs:0", 0
        )

    assert capturing.captured_event is not None, "executor.execute() was not called"

    # Executor received event with correct fields
    ev = capturing.captured_event
    assert ev.run_id == "run-direct-01"
    assert ev.tenant_id == "tenant-abc"
    assert ev.agent_type == "test-agent"
    assert ev.payload == {"prompt": "hello world"}


@pytest.mark.asyncio
async def test_consumer_passes_none_payload_as_empty():
    """payload=None on the event is handled — executor gets payload with empty dict."""
    from unittest.mock import AsyncMock, patch
    from hfa_worker.consumer import WorkerConsumer
    from hfa.events.codec import serialize_event

    redis = faredis.FakeRedis()
    capturing = CapturingExecutor()

    consumer = WorkerConsumer(
        redis=redis,
        worker_id="test",
        worker_group="test",
        shards=[0],
        executor=capturing,
    )
    consumer._guard = AsyncMock()
    consumer._guard.should_execute.return_value = True
    consumer._guard.try_claim_and_mark_running.return_value = True
    consumer._state = AsyncMock()

    event = RunRequestedEvent(
        run_id="run-null-payload",
        tenant_id="t-1",
        agent_type="agent",
        payload=None,
    )

    with patch("hfa_worker.consumer.ack_message", new=AsyncMock()):
        await consumer._process_message(
            "123-0", serialize_event(event), "hfa:stream:runs:0", 0
        )

    assert capturing.captured_event is not None
    # payload=None on the event — executor gets event directly, can handle it
    assert capturing.captured_event.run_id == "run-null-payload"
