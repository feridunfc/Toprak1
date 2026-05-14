
import pytest

from hfa_control.scheduler_capability_fair_dispatch import (
    SchedulerCapabilityFairDispatcher,
    TaskDispatchRequest,
)
from hfa_control.scheduler_capability_selector import WorkerCandidate
from hfa_control.scheduler_reservation_dispatch import SchedulerReservationDispatcher
from hfa_control.task_claim import TaskClaimManager
from hfa_control.worker_reservation import WorkerReservationManager
from hfa.dag.schema import DagRedisKey

pytestmark = pytest.mark.asyncio


@pytest.mark.integration
async def test_incompatible_workers_never_reserved(redis_client):
    async def dispatch_fn(*, task_id: str, worker_id: str, scheduler_epoch: str, dispatch_payload: dict) -> bool:
        return True

    await redis_client.set(DagRedisKey.task_state("task-1"), "scheduled")

    reservation_manager = WorkerReservationManager(redis_client, reservation_ttl_seconds=30, scheduler_id="sched-A")
    reservation_dispatcher = SchedulerReservationDispatcher(reservation_manager, dispatch_fn)
    dispatcher = SchedulerCapabilityFairDispatcher(reservation_dispatcher)

    request = TaskDispatchRequest(
        tenant_id="tenant-a",
        task_id="task-1",
        required_capabilities=["python"],
        dispatch_payload={"tenant_id": "tenant-a"},
        vruntime=10.0,
        inflight=0,
    )
    workers = [
        WorkerCandidate(worker_id="worker-good", capabilities=["python"], current_load=2, capacity=1),
        WorkerCandidate(worker_id="worker-bad", capabilities=["frontend"], current_load=0, capacity=10),
    ]

    result = await dispatcher.dispatch_task(
        request=request,
        workers=workers,
        scheduler_epoch="epoch-1",
        reserved_at_ms=123456,
    )

    assert result.ok is True
    assert result.worker_id == "worker-good"
    assert await redis_client.exists(DagRedisKey.worker_reservation("worker-good")) == 1
    assert await redis_client.exists(DagRedisKey.worker_reservation("worker-bad")) == 0


@pytest.mark.integration
async def test_best_compatible_worker_can_be_claimed(redis_client):
    async def dispatch_fn(*, task_id: str, worker_id: str, scheduler_epoch: str, dispatch_payload: dict) -> bool:
        return True

    await redis_client.set(DagRedisKey.task_state("task-2"), "scheduled")

    reservation_manager = WorkerReservationManager(redis_client, reservation_ttl_seconds=30, scheduler_id="sched-A")
    reservation_dispatcher = SchedulerReservationDispatcher(reservation_manager, dispatch_fn)
    dispatcher = SchedulerCapabilityFairDispatcher(reservation_dispatcher)

    request = TaskDispatchRequest(
        tenant_id="tenant-b",
        task_id="task-2",
        required_capabilities=["python"],
        dispatch_payload={"tenant_id": "tenant-b"},
        vruntime=5.0,
        inflight=0,
    )
    workers = [
        WorkerCandidate(worker_id="worker-1", capabilities=["python"], current_load=5, capacity=1),
        WorkerCandidate(worker_id="worker-2", capabilities=["python"], current_load=1, capacity=5),
    ]

    result = await dispatcher.dispatch_task(
        request=request,
        workers=workers,
        scheduler_epoch="epoch-1",
        reserved_at_ms=123456,
    )

    assert result.ok is True
    assert result.worker_id == "worker-2"

    claim_manager = TaskClaimManager(redis_client)
    claim = await claim_manager.claim_start(
        task_id="task-2",
        tenant_id="tenant-b",
        worker_instance_id="worker-2",
        claimed_at_ms=123500,
        scheduler_epoch="epoch-1",
    )

    assert claim.ok is True
    assert await redis_client.get(DagRedisKey.task_state("task-2")) == "running"


@pytest.mark.integration
async def test_no_compatible_worker_is_stable(redis_client):
    async def dispatch_fn(*, task_id: str, worker_id: str, scheduler_epoch: str, dispatch_payload: dict) -> bool:
        return True

    await redis_client.set(DagRedisKey.task_state("task-3"), "scheduled")

    reservation_manager = WorkerReservationManager(redis_client, reservation_ttl_seconds=30, scheduler_id="sched-A")
    reservation_dispatcher = SchedulerReservationDispatcher(reservation_manager, dispatch_fn)
    dispatcher = SchedulerCapabilityFairDispatcher(reservation_dispatcher)

    request = TaskDispatchRequest(
        tenant_id="tenant-c",
        task_id="task-3",
        required_capabilities=["sql"],
        dispatch_payload={"tenant_id": "tenant-c"},
        vruntime=1.0,
        inflight=0,
    )
    workers = [
        WorkerCandidate(worker_id="worker-ui", capabilities=["frontend"], current_load=0, capacity=1),
        WorkerCandidate(worker_id="worker-design", capabilities=["design"], current_load=0, capacity=1),
    ]

    result = await dispatcher.dispatch_task(
        request=request,
        workers=workers,
        scheduler_epoch="epoch-1",
        reserved_at_ms=123456,
    )

    assert result.ok is False
    assert result.status == "no_compatible_workers"
    assert await redis_client.exists(DagRedisKey.worker_reservation("worker-ui")) == 0
    assert await redis_client.exists(DagRedisKey.worker_reservation("worker-design")) == 0
