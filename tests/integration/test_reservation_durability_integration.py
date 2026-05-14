
import pytest

from hfa_control.reservation_reaper import ReservationReaper
from hfa_control.worker_reservation import WorkerReservationManager
from hfa.dag.schema import DagRedisKey

pytestmark = pytest.mark.asyncio


@pytest.mark.integration
async def test_reservation_records_scheduler_id(redis_client):
    mgr = WorkerReservationManager(redis_client, reservation_ttl_seconds=30, scheduler_id="sched-A")
    result = await mgr.reserve(
        worker_id="worker-1",
        task_id="task-1",
        scheduler_epoch="epoch-1",
        reserved_at_ms=123456,
    )
    assert result.ok is True
    data = await redis_client.hgetall(DagRedisKey.worker_reservation("worker-1"))
    assert data["scheduler_id"] == "sched-A"


@pytest.mark.integration
async def test_reservation_renew_extends_ttl(redis_client):
    mgr = WorkerReservationManager(redis_client, reservation_ttl_seconds=5, scheduler_id="sched-A")
    await mgr.reserve(
        worker_id="worker-2",
        task_id="task-2",
        scheduler_epoch="epoch-2",
        reserved_at_ms=123456,
    )
    ttl_before = await redis_client.ttl(DagRedisKey.worker_reservation("worker-2"))
    ok = await mgr.renew("worker-2", ttl_seconds=30)
    ttl_after = await redis_client.ttl(DagRedisKey.worker_reservation("worker-2"))
    assert ok is True
    assert ttl_after >= ttl_before


@pytest.mark.integration
async def test_reaper_lists_orphan_with_scheduler_id(redis_client):
    await redis_client.hset(
        DagRedisKey.worker_reservation("worker-3"),
        mapping={
            "task_id": "task-missing",
            "scheduler_epoch": "epoch-3",
            "reserved_at_ms": "123456",
            "scheduler_id": "sched-B",
        },
    )
    await redis_client.expire(DagRedisKey.worker_reservation("worker-3"), 30)

    reaper = ReservationReaper(redis_client)
    items = await reaper.list_orphaned_reservations()

    assert len(items) == 1
    assert items[0].task_id == "task-missing"
    assert items[0].scheduler_id == "sched-B"
    assert items[0].reason == "missing_task_state"
