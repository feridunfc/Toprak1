
import asyncio
import pytest

from hfa_control.event_hooks import emit_event_background
from hfa_control.event_store import EventStore

pytestmark = pytest.mark.asyncio


class _Store(EventStore):
    def __init__(self):
        self.calls = []

    async def append_event(self, run_id, event_type, worker_id=None, details=None):
        self.calls.append((run_id, event_type, worker_id, details))
        return True


async def test_emit_event_background_schedules_without_throwing():
    store = _Store()
    emit_event_background(
        store,
        run_id="r1",
        event_type=EventStore.EVENT_TASK_SCHEDULED,
        worker_id="w1",
        details={"tenant_id": "t1"},
    )
    await asyncio.sleep(0.01)
    assert len(store.calls) == 1
    assert store.calls[0][1] == EventStore.EVENT_TASK_SCHEDULED
