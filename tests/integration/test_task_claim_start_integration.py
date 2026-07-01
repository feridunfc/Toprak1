
import pytest

from hfa_control.task_claim import TaskClaimManager
from hfa_control.worker_reservation import WorkerReservationManager
from hfa.dag.schema import DagRedisKey

pytestmark = pytest.mark.asyncio


async def _reserve_claim_context(
    redis_client,
    *,
    worker_id: str,
    task_id: str,
    scheduler_epoch: str,
    reserved_at_ms: int,
) -> None:
    reservation_mgr = WorkerReservationManager(redis_client, reservation_ttl_seconds=30)
    result = await reservation_mgr.reserve(
        worker_id=worker_id,
        task_id=task_id,
        scheduler_epoch=scheduler_epoch,
        reserved_at_ms=reserved_at_ms,
    )
    assert result.ok is True


@pytest.mark.integration
async def test_task_claim_start_scheduled_to_running(redis_client):
    task_id = "claim-001"
    tenant_id = "tenant-a"

    await redis_client.set(DagRedisKey.task_state(task_id), "scheduled")
    await _reserve_claim_context(
        redis_client,
        worker_id="worker-1",
        task_id=task_id,
        scheduler_epoch="epoch-claim-1",
        reserved_at_ms=123450,
    )

    mgr = TaskClaimManager(redis_client)
    result = await mgr.claim_start(
        task_id=task_id,
        tenant_id=tenant_id,
        worker_instance_id="worker-1",
        claimed_at_ms=123456,
        scheduler_epoch="epoch-claim-1",
    )

    assert result.ok is True
    assert result.status == "task_claimed"
    assert await redis_client.get(DagRedisKey.task_state(task_id)) == "running"

    meta = await redis_client.hgetall(DagRedisKey.task_meta(task_id))
    assert meta["worker_instance_id"] == "worker-1"
    assert meta["claimed_at_ms"] == "123456"
    assert meta["last_heartbeat_at_ms"] == "123456"
    assert await redis_client.zscore(DagRedisKey.task_running_zset(tenant_id), task_id) is not None


@pytest.mark.integration
async def test_task_claim_start_rejects_duplicate_running(redis_client):
    task_id = "claim-002"
    tenant_id = "tenant-a"

    await redis_client.set(DagRedisKey.task_state(task_id), "running")
    await _reserve_claim_context(
        redis_client,
        worker_id="worker-1",
        task_id=task_id,
        scheduler_epoch="epoch-claim-2",
        reserved_at_ms=123450,
    )

    mgr = TaskClaimManager(redis_client)
    result = await mgr.claim_start(
        task_id=task_id,
        tenant_id=tenant_id,
        worker_instance_id="worker-1",
        claimed_at_ms=123456,
        scheduler_epoch="epoch-claim-2",
    )

    assert result.ok is False
    assert result.status == "task_already_owned"


@pytest.mark.integration
async def test_task_claim_start_rejects_terminal_state(redis_client):
    task_id = "claim-003"
    tenant_id = "tenant-a"

    await redis_client.set(DagRedisKey.task_state(task_id), "failed")
    await _reserve_claim_context(
        redis_client,
        worker_id="worker-1",
        task_id=task_id,
        scheduler_epoch="epoch-claim-3",
        reserved_at_ms=123450,
    )

    mgr = TaskClaimManager(redis_client)
    result = await mgr.claim_start(
        task_id=task_id,
        tenant_id=tenant_id,
        worker_instance_id="worker-1",
        claimed_at_ms=123456,
        scheduler_epoch="epoch-claim-3",
    )

    assert result.ok is False
    assert result.status == "task_state_conflict"
