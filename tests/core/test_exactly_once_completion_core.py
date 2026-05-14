
import asyncio
import pytest
import pytest_asyncio
from fakeredis import FakeAsyncRedis

from hfa_control.effect_ledger import EffectLedger
from hfa.runtime.state_store import StateStore


class _Lua:
    async def execute(self, *args, **kwargs):
        return 1


@pytest_asyncio.fixture
async def state_store():
    client = FakeAsyncRedis(decode_responses=True)
    await client.flushdb()
    ledger = EffectLedger(client)

    class _Store:
        def __init__(self):
            self.calls = []
        async def append_event(self, run_id, event_type, worker_id=None, details=None):
            self.calls.append((run_id, event_type, worker_id, details))
            return True

    event_store = _Store()
    store = StateStore(client, _Lua(), event_store=event_store, effect_ledger=ledger)
    try:
        yield store, client, event_store
    finally:
        await client.flushdb()
        close = getattr(client, "aclose", None) or getattr(client, "close", None)
        if close is not None:
            result = close()
            if hasattr(result, "__await__"):
                await result


@pytest.mark.asyncio
async def test_duplicate_completion_suppressed(state_store):
    store, client, event_store = state_store
    await client.set("hfa:task:task-1:owner", "worker-1")

    first = await store.complete_once(run_id="run-1", task_id="task-1", worker_id="worker-1", attempt=1, status="done")
    await asyncio.sleep(0.01)
    second = await store.complete_once(run_id="run-1", task_id="task-1", worker_id="worker-1", attempt=1, status="done")

    assert first.ok is True
    assert second.ok is False
    assert second.status == "duplicate_completion_suppressed"
    assert len(event_store.calls) == 1


@pytest.mark.asyncio
async def test_zombie_worker_completion_is_fenced(state_store):
    store, client, event_store = state_store
    await client.set("hfa:task:task-2:owner", "worker-A")

    result = await store.complete_once(run_id="run-2", task_id="task-2", worker_id="worker-B", attempt=1, status="done")

    assert result.ok is False
    assert result.status == "stale_owner_fenced"
    assert len(event_store.calls) == 0
