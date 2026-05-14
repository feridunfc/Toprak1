
import pytest

from hfa_control.event_store import EventStore
from hfa_control.replay_engine import ReplayEngine

pytestmark = pytest.mark.asyncio


@pytest.mark.integration
async def test_replay_engine_roundtrip(redis_client):
    store = EventStore(redis_client)

    await store.append_event("run-42", EventStore.EVENT_TASK_ADMITTED, details={"tenant_id": "t1"})
    await store.append_event("run-42", EventStore.EVENT_TASK_SCHEDULED, worker_id="worker-1")
    await store.append_event("run-42", EventStore.EVENT_TASK_CLAIMED, worker_id="worker-1")
    await store.append_event("run-42", EventStore.EVENT_TASK_REQUEUED, worker_id="worker-1")
    await store.append_event("run-42", EventStore.EVENT_TASK_SCHEDULED, worker_id="worker-2")
    await store.append_event("run-42", EventStore.EVENT_TASK_CLAIMED, worker_id="worker-2")
    await store.append_event("run-42", EventStore.EVENT_TASK_COMPLETED, worker_id="worker-2")

    summary = await ReplayEngine.replay_from_store(store, "run-42")
    assert summary.final_state == "completed"
    assert summary.attempts == 2
    assert summary.requeue_count == 1
    assert summary.completed_by == "worker-2"
    assert len(summary.timeline) == 7


@pytest.mark.integration
async def test_replay_detects_state_mismatch(redis_client):
    store = EventStore(redis_client)
    await store.append_event("run-99", EventStore.EVENT_TASK_ADMITTED)
    await store.append_event("run-99", EventStore.EVENT_TASK_FAILED)

    summary = await ReplayEngine.replay_from_store(store, "run-99")
    assert ReplayEngine.detect_mismatch(summary, "failed") is True
    assert ReplayEngine.detect_mismatch(summary, "running") is False
