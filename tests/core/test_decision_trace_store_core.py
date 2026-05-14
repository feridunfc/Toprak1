
import pytest
from unittest.mock import AsyncMock

from hfa_control.decision_trace_store import DecisionTraceStore

pytestmark = pytest.mark.asyncio


async def test_trace_store_persists_json_to_stream():
    redis = AsyncMock()
    redis.xadd.return_value = b"1-0"

    store = DecisionTraceStore(redis, maxlen=100)
    persisted = await store.persist({"decision_id": "d-1", "selected_worker_id": "w-1"})

    assert persisted.stream_id == "1-0"
    assert persisted.decision_id == "d-1"
    redis.xadd.assert_awaited_once()
