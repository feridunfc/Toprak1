
import pytest
from hfa_control.worker_reservation import WorkerReservationManager
from hfa.dag.schema import DagRedisKey

pytestmark = pytest.mark.asyncio

@pytest.mark.integration
async def test_reserve_empty_worker_success(redis_client):
    mgr = WorkerReservationManager(redis_client, reservation_ttl_seconds=30)
    result = await mgr.reserve(worker_id="worker-1", task_id="task-1", scheduler_epoch="epoch-1", reserved_at_ms=123456)
    assert result.ok is True
    data = await redis_client.hgetall(DagRedisKey.worker_reservation("worker-1"))
    assert data["task_id"] == "task-1"
    assert data["scheduler_epoch"] == "epoch-1"

@pytest.mark.integration
async def test_reserve_reserved_worker_conflict(redis_client):
    mgr = WorkerReservationManager(redis_client, reservation_ttl_seconds=30)
    first = await mgr.reserve(worker_id="worker-1", task_id="task-1", scheduler_epoch="epoch-1", reserved_at_ms=123456)
    second = await mgr.reserve(worker_id="worker-1", task_id="task-2", scheduler_epoch="epoch-2", reserved_at_ms=123457)
    assert first.ok is True
    assert second.ok is False
    assert second.status == "reservation_conflict"

@pytest.mark.integration
async def test_reservation_expires_if_not_claimed(redis_client):
    mgr = WorkerReservationManager(redis_client, reservation_ttl_seconds=1)
    await mgr.reserve(worker_id="worker-1", task_id="task-1", scheduler_epoch="epoch-1", reserved_at_ms=123456)
    ttl = await redis_client.ttl(DagRedisKey.worker_reservation("worker-1"))
    assert ttl is not None
    assert ttl > 0
