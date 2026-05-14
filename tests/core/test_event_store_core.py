
import pytest
from unittest.mock import AsyncMock

from hfa_control.event_store import EventStore

pytestmark = pytest.mark.asyncio


async def test_append_and_read_history():
    redis = AsyncMock()
    storage = []

    async def rpush(key, value):
        storage.append((key, value))
        return len(storage)

    async def lrange(key, start, end):
        return [v for k, v in storage if k == key]

    redis.rpush.side_effect = rpush
    redis.lrange.side_effect = lrange

    store = EventStore(redis)
    ok = await store.append_event("run-1", EventStore.EVENT_TASK_COMPLETED, worker_id="w1", details={"x": 1})
    assert ok is True

    history = await store.get_run_history("run-1")
    assert len(history) == 1
    assert history[0]["event_type"] == EventStore.EVENT_TASK_COMPLETED
    assert history[0]["worker_id"] == "w1"
    assert history[0]["details"]["x"] == 1
