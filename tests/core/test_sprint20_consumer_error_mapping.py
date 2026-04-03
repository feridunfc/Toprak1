"""
tests/core/test_sprint20_consumer_error_mapping.py

Sprint 8: imports migrated from execution_types to models (canonical).
Test logic unchanged.
"""

import pytest
from unittest.mock import AsyncMock, patch

import fakeredis.aioredis as faredis

from hfa.events.codec import serialize_event
from hfa.events.schema import RunRequestedEvent
from hfa_worker.consumer import WorkerConsumer

# Sprint 8: canonical imports from hfa_worker.models
from hfa_worker.models import ExecutionPermanentError, ExecutionTransientError


class TransientFailingExecutor:
    async def execute(self, event):
        raise ExecutionTransientError("network timeout")


class PermanentFailingExecutor:
    async def execute(self, event):
        raise ExecutionPermanentError("bad request")


@pytest.mark.asyncio
async def test_transient_error_releases_claim_and_does_not_ack():
    redis = faredis.FakeRedis()
    consumer = WorkerConsumer(
        redis=redis,
        worker_id="test-worker",
        worker_group="test-group",
        shards=[0],
        executor=TransientFailingExecutor(),
    )
    consumer._guard = AsyncMock()
    consumer._guard.should_execute.return_value = True
    consumer._guard.try_claim_and_mark_running.return_value = True
    consumer._state = AsyncMock()

    event = RunRequestedEvent(
        run_id="run-transient-1", tenant_id="tenant-1", agent_type="test", payload={}
    )

    with patch("hfa_worker.consumer.ack_message", new=AsyncMock()) as mock_ack:
        await consumer._process_message(
            "123-0", serialize_event(event), "hfa:stream:runs:0", 0
        )

        mock_ack.assert_not_awaited()
        consumer._state.release_claim.assert_awaited_once_with("run-transient-1")
        consumer._state.transition_state.assert_not_called()
        consumer._state.mark_completed.assert_not_called()


@pytest.mark.asyncio
async def test_permanent_error_marks_failed_and_acks():
    redis = faredis.FakeRedis()
    consumer = WorkerConsumer(
        redis=redis,
        worker_id="test-worker",
        worker_group="test-group",
        shards=[0],
        executor=PermanentFailingExecutor(),
    )
    consumer._guard = AsyncMock()
    consumer._guard.should_execute.return_value = True
    consumer._guard.try_claim_and_mark_running.return_value = True
    consumer._state = AsyncMock()

    event = RunRequestedEvent(
        run_id="run-permanent-1", tenant_id="tenant-1", agent_type="test", payload={}
    )

    with patch("hfa_worker.consumer.ack_message", new=AsyncMock()) as mock_ack:
        await consumer._process_message(
            "123-0", serialize_event(event), "hfa:stream:runs:0", 0
        )

        mock_ack.assert_awaited_once_with(
            redis, "hfa:stream:runs:0", "worker_consumers", "123-0"
        )
        consumer._state.transition_state.assert_awaited_once_with(
            "run-permanent-1", "failed"
        )
        consumer._state.mark_completed.assert_awaited_once_with("run-permanent-1")
        consumer._state.release_claim.assert_not_called()
