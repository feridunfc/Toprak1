
import pytest

from hfa_control.event_store import EventStore

pytestmark = pytest.mark.asyncio


@pytest.mark.integration
async def test_event_store_roundtrip(redis_client):
    store = EventStore(redis_client)
    await store.append_event("run-42", EventStore.EVENT_TASK_ADMITTED, details={"tenant_id": "t1"})
    await store.append_event("run-42", EventStore.EVENT_TASK_CLAIMED, worker_id="worker-1")

    history = await store.get_run_history("run-42")
    assert len(history) == 2
    assert history[0]["event_type"] == EventStore.EVENT_TASK_ADMITTED
    assert history[1]["event_type"] == EventStore.EVENT_TASK_CLAIMED
    assert history[1]["worker_id"] == "worker-1"
