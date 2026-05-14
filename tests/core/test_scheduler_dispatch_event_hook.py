
import asyncio
import pytest

from hfa_control.event_store import EventStore
from hfa_control.scheduler_reservation_dispatch import SchedulerReservationDispatcher

pytestmark = pytest.mark.asyncio


class _Reserved:
    def __init__(self, ok=True, status="reserved"):
        self.ok = ok
        self.status = status


class _ReservationManager:
    async def reserve(self, *, worker_id, run_id, reserved_at_ms=None):
        return _Reserved(True, "reserved")


class _Store:
    def __init__(self):
        self.calls = []

    async def append_event(self, run_id, event_type, worker_id=None, details=None):
        self.calls.append((run_id, event_type, worker_id, details))
        return True


async def test_dispatch_emits_scheduled_event():
    async def dispatch_fn(*, task_id, worker_id, scheduler_epoch, dispatch_payload):
        return True

    store = _Store()
    dispatcher = SchedulerReservationDispatcher(_ReservationManager(), dispatch_fn, event_store=store)
    result = await dispatcher.reserve_and_dispatch(
        task_id="run-1",
        worker_id="worker-1",
        scheduler_epoch="e1",
        dispatch_payload={"tenant_id": "tenant-a"},
        reserved_at_ms=123,
    )
    await asyncio.sleep(0.01)
    assert result.ok is True
    assert len(store.calls) == 1
    assert store.calls[0][1] == EventStore.EVENT_TASK_SCHEDULED
