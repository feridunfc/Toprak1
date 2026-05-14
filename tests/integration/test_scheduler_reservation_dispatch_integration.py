
import pytest

from hfa_control.scheduler_reservation_dispatch import SchedulerReservationDispatcher
from hfa_control.task_claim import TaskClaimManager
from hfa_control.worker_reservation import WorkerReservationManager
from hfa.dag.schema import DagRedisKey

pytestmark = pytest.mark.asyncio


@pytest.mark.integration
async def test_reserve_then_dispatch_then_claim_consumes_reservation(redis_client):
    async def dispatch_fn(*, task_id: str, worker_id: str, scheduler_epoch: str, dispatch_payload: dict) -> bool:
        return True

    await redis_client.set(DagRedisKey.task_state("task-1"), "scheduled")

    reservation_manager = WorkerReservationManager(redis_client, reservation_ttl_seconds=30)
    dispatcher = SchedulerReservationDispatcher(reservation_manager, dispatch_fn)

    dispatch_result = await dispatcher.reserve_and_dispatch(
        task_id="task-1",
        worker_id="worker-1",
        scheduler_epoch="epoch-1",
        dispatch_payload={"tenant_id": "tenant-a"},
        reserved_at_ms=123456,
    )

    assert dispatch_result.ok is True
    assert await redis_client.exists(DagRedisKey.worker_reservation("worker-1")) == 1

    claim_manager = TaskClaimManager(redis_client)
    claim_result = await claim_manager.claim_start(
        task_id="task-1",
        tenant_id="tenant-a",
        worker_instance_id="worker-1",
        claimed_at_ms=123500,
        scheduler_epoch="epoch-1",
    )

    assert claim_result.ok is True
    assert await redis_client.exists(DagRedisKey.worker_reservation("worker-1")) == 0
    assert await redis_client.get(DagRedisKey.task_state("task-1")) == "running"


@pytest.mark.integration
async def test_dispatch_failure_releases_reservation(redis_client):
    async def dispatch_fn(*, task_id: str, worker_id: str, scheduler_epoch: str, dispatch_payload: dict) -> bool:
        return False

    await redis_client.set(DagRedisKey.task_state("task-2"), "scheduled")

    reservation_manager = WorkerReservationManager(redis_client, reservation_ttl_seconds=30)
    dispatcher = SchedulerReservationDispatcher(reservation_manager, dispatch_fn)

    result = await dispatcher.reserve_and_dispatch(
        task_id="task-2",
        worker_id="worker-2",
        scheduler_epoch="epoch-2",
        dispatch_payload={"tenant_id": "tenant-a"},
        reserved_at_ms=123456,
    )

    assert result.ok is False
    assert result.status == "dispatch_failed"
    assert await redis_client.exists(DagRedisKey.worker_reservation("worker-2")) == 0
    assert await redis_client.get(DagRedisKey.task_state("task-2")) == "scheduled"


@pytest.mark.integration
async def test_second_scheduler_cannot_dispatch_same_reserved_worker(redis_client):
    async def dispatch_fn(*, task_id: str, worker_id: str, scheduler_epoch: str, dispatch_payload: dict) -> bool:
        return True

    await redis_client.set(DagRedisKey.task_state("task-a"), "scheduled")
    await redis_client.set(DagRedisKey.task_state("task-b"), "scheduled")

    reservation_manager = WorkerReservationManager(redis_client, reservation_ttl_seconds=30)
    dispatcher = SchedulerReservationDispatcher(reservation_manager, dispatch_fn)

    first = await dispatcher.reserve_and_dispatch(
        task_id="task-a",
        worker_id="worker-race",
        scheduler_epoch="epoch-a",
        dispatch_payload={"tenant_id": "tenant-a"},
        reserved_at_ms=111,
    )
    second = await dispatcher.reserve_and_dispatch(
        task_id="task-b",
        worker_id="worker-race",
        scheduler_epoch="epoch-b",
        dispatch_payload={"tenant_id": "tenant-b"},
        reserved_at_ms=112,
    )

    assert first.ok is True
    assert second.ok is False
    assert second.status == "reservation_conflict"

    reservation = await redis_client.hgetall(DagRedisKey.worker_reservation("worker-race"))
    assert reservation["task_id"] == "task-a"
