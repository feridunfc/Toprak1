
import asyncio
import pytest
import pytest_asyncio
from fakeredis import FakeAsyncRedis

from hfa_control.effect_ledger import EffectLedger
from hfa_control.scheduler_reservation_dispatch import SchedulerReservationDispatcher


class _Reserved:
    def __init__(self, ok=True, status="reserved"):
        self.ok = ok
        self.status = status


class _ReservationManager:
    def __init__(self):
        self.reserve_calls = 0
        self.release_calls = 0

    async def reserve(self, *, worker_id, run_id, reserved_at_ms=None):
        self.reserve_calls += 1
        return _Reserved(True, "reserved")

    async def release(self, worker_id):
        self.release_calls += 1


class _Store:
    def __init__(self):
        self.calls = []

    async def append_event(self, run_id, event_type, worker_id=None, details=None):
        self.calls.append((run_id, event_type, worker_id, details))
        return True


@pytest_asyncio.fixture
async def ledger():
    client = FakeAsyncRedis(decode_responses=True)
    await client.flushdb()
    try:
        yield EffectLedger(client)
    finally:
        await client.flushdb()
        close = getattr(client, "aclose", None) or getattr(client, "close", None)
        if close is not None:
            result = close()
            if hasattr(result, "__await__"):
                await result


@pytest.mark.asyncio
async def test_duplicate_dispatch_suppressed_before_reservation(ledger):
    async def dispatch_fn(*, task_id, worker_id, scheduler_epoch, dispatch_payload):
        return True

    reservation = _ReservationManager()
    store = _Store()
    dispatcher = SchedulerReservationDispatcher(reservation, dispatch_fn, event_store=store, effect_ledger=ledger)

    payload = {"tenant_id": "tenant-a", "run_id": "run-1", "attempt": 1}
    first = await dispatcher.reserve_and_dispatch(
        task_id="task-1", worker_id="worker-1", scheduler_epoch="e1", dispatch_payload=payload, reserved_at_ms=1
    )
    await asyncio.sleep(0.01)
    second = await dispatcher.reserve_and_dispatch(
        task_id="task-1", worker_id="worker-1", scheduler_epoch="e1", dispatch_payload=payload, reserved_at_ms=2
    )

    assert first.ok is True
    assert second.ok is False
    assert second.status == "duplicate_dispatch_suppressed"
    assert reservation.reserve_calls == 1
    assert len(store.calls) == 1
