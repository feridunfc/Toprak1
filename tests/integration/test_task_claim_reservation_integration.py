
import asyncio
import pytest

from hfa_control.task_claim import TaskClaimManager
from hfa_control.worker_reservation import WorkerReservationManager
from hfa_control.reservation_reaper import ReservationReaper
from hfa.dag.schema import DagRedisKey

pytestmark = pytest.mark.asyncio

@pytest.mark.integration
async def test_claim_with_matching_reservation_succeeds_and_consumes(redis_client):
    await redis_client.set(DagRedisKey.task_state("task-1"), "scheduled")
    res_mgr = WorkerReservationManager(redis_client, reservation_ttl_seconds=30)
    await res_mgr.reserve(worker_id="worker-1", task_id="task-1", scheduler_epoch="epoch-1", reserved_at_ms=123456)
    claim_mgr = TaskClaimManager(redis_client)
    result = await claim_mgr.claim_start(task_id="task-1", tenant_id="tenant-a", worker_instance_id="worker-1", claimed_at_ms=123500, scheduler_epoch="epoch-1")
    assert result.ok is True
    assert await redis_client.get(DagRedisKey.task_state("task-1")) == "running"
    assert await redis_client.exists(DagRedisKey.worker_reservation("worker-1")) == 0

@pytest.mark.integration
async def test_claim_without_reservation_rejected(redis_client):
    await redis_client.set(DagRedisKey.task_state("task-2"), "scheduled")
    claim_mgr = TaskClaimManager(redis_client)
    result = await claim_mgr.claim_start(task_id="task-2", tenant_id="tenant-a", worker_instance_id="worker-2", claimed_at_ms=123500, scheduler_epoch="epoch-1")
    assert result.ok is False
    assert result.status == "reservation_missing"
    assert await redis_client.get(DagRedisKey.task_state("task-2")) == "scheduled"

@pytest.mark.integration
async def test_claim_with_wrong_epoch_rejected(redis_client):
    await redis_client.set(DagRedisKey.task_state("task-3"), "scheduled")
    res_mgr = WorkerReservationManager(redis_client, reservation_ttl_seconds=30)
    await res_mgr.reserve(worker_id="worker-3", task_id="task-3", scheduler_epoch="epoch-real", reserved_at_ms=123456)
    claim_mgr = TaskClaimManager(redis_client)
    result = await claim_mgr.claim_start(task_id="task-3", tenant_id="tenant-a", worker_instance_id="worker-3", claimed_at_ms=123500, scheduler_epoch="epoch-wrong")
    assert result.ok is False
    assert result.status == "reservation_epoch_mismatch"
    assert await redis_client.get(DagRedisKey.task_state("task-3")) == "scheduled"

@pytest.mark.integration
async def test_claim_with_wrong_worker_rejected(redis_client):
    await redis_client.set(DagRedisKey.task_state("task-5"), "scheduled")
    res_mgr = WorkerReservationManager(redis_client, reservation_ttl_seconds=30)
    await res_mgr.reserve(worker_id="worker-real", task_id="task-5", scheduler_epoch="epoch-1", reserved_at_ms=123456)
    claim_mgr = TaskClaimManager(redis_client)
    result = await claim_mgr.claim_start(task_id="task-5", tenant_id="tenant-a", worker_instance_id="worker-fake", claimed_at_ms=123500, scheduler_epoch="epoch-1")
    assert result.ok is False
    assert result.status == "reservation_worker_mismatch"
    assert await redis_client.get(DagRedisKey.task_state("task-5")) == "scheduled"

@pytest.mark.integration
async def test_reaper_cleans_non_scheduled_orphan(redis_client):
    await redis_client.set(DagRedisKey.task_state("task-4"), "running")
    await redis_client.hset(DagRedisKey.worker_reservation("worker-4"), mapping={"task_id": "task-4", "scheduler_epoch": "epoch-1", "reserved_at_ms": "123456"})
    await redis_client.expire(DagRedisKey.worker_reservation("worker-4"), 30)
    reaper = ReservationReaper(redis_client)
    reaped = await reaper.reap_orphaned_reservations()
    assert reaped == 1
    assert await redis_client.exists(DagRedisKey.worker_reservation("worker-4")) == 0

@pytest.mark.integration
async def test_concurrent_reserve_same_worker_only_one_wins(redis_client):
    mgr = WorkerReservationManager(redis_client, reservation_ttl_seconds=30)

    async def reserve(task_id: str, epoch: str):
        return await mgr.reserve(
            worker_id="worker-race",
            task_id=task_id,
            scheduler_epoch=epoch,
            reserved_at_ms=123456,
        )

    r1, r2 = await asyncio.gather(
        reserve("task-a", "epoch-a"),
        reserve("task-b", "epoch-b"),
    )

    statuses = sorted([r1.status, r2.status])
    assert statuses == ["reservation_conflict", "reservation_created"]

    data = await redis_client.hgetall(DagRedisKey.worker_reservation("worker-race"))
    assert data["task_id"] in {"task-a", "task-b"}
