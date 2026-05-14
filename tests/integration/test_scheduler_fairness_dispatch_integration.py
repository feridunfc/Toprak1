
import pytest

from hfa_control.fairness_manager import FairnessManager
from hfa_control.scheduler_fairness_dispatch import (
    FairDispatchCandidate,
    SchedulerFairnessDispatcher,
)
from hfa_control.scheduler_reservation_dispatch import SchedulerReservationDispatcher
from hfa_control.task_claim import TaskClaimManager
from hfa_control.worker_reservation import WorkerReservationManager
from hfa.dag.schema import DagRedisKey

pytestmark = pytest.mark.asyncio


@pytest.mark.integration
async def test_lower_vruntime_tenant_gets_dispatched_first(redis_client):
    async def dispatch_fn(*, task_id: str, worker_id: str, scheduler_epoch: str, dispatch_payload: dict) -> bool:
        return True

    await redis_client.set(DagRedisKey.task_state("task-a"), "scheduled")
    await redis_client.set(DagRedisKey.task_state("task-b"), "scheduled")
    await redis_client.set(DagRedisKey.tenant_vruntime("tenant-a"), "100")
    await redis_client.set(DagRedisKey.tenant_vruntime("tenant-b"), "10")
    await redis_client.set(DagRedisKey.tenant_inflight("tenant-a"), "0")
    await redis_client.set(DagRedisKey.tenant_inflight("tenant-b"), "0")

    fairness_manager = FairnessManager(redis_client)
    reservation_manager = WorkerReservationManager(redis_client, reservation_ttl_seconds=30, scheduler_id="sched-A")
    reservation_dispatcher = SchedulerReservationDispatcher(reservation_manager, dispatch_fn)
    dispatcher = SchedulerFairnessDispatcher(fairness_manager, reservation_dispatcher)

    candidates = [
        FairDispatchCandidate("tenant-a", "task-a", "worker-a", {"tenant_id": "tenant-a"}),
        FairDispatchCandidate("tenant-b", "task-b", "worker-b", {"tenant_id": "tenant-b"}),
    ]

    result = await dispatcher.choose_and_dispatch(
        candidates=candidates,
        scheduler_epoch="epoch-1",
        reserved_at_ms=123456,
    )

    assert result.ok is True
    assert result.chosen_tenant_id == "tenant-b"
    assert await redis_client.exists(DagRedisKey.worker_reservation("worker-b")) == 1
    assert await redis_client.exists(DagRedisKey.worker_reservation("worker-a")) == 0


@pytest.mark.integration
async def test_inflight_breaks_tie_when_vruntime_equal(redis_client):
    async def dispatch_fn(*, task_id: str, worker_id: str, scheduler_epoch: str, dispatch_payload: dict) -> bool:
        return True

    await redis_client.set(DagRedisKey.task_state("task-a"), "scheduled")
    await redis_client.set(DagRedisKey.task_state("task-b"), "scheduled")
    await redis_client.set(DagRedisKey.tenant_vruntime("tenant-a"), "10")
    await redis_client.set(DagRedisKey.tenant_vruntime("tenant-b"), "10")
    await redis_client.set(DagRedisKey.tenant_inflight("tenant-a"), "3")
    await redis_client.set(DagRedisKey.tenant_inflight("tenant-b"), "1")

    fairness_manager = FairnessManager(redis_client)
    reservation_manager = WorkerReservationManager(redis_client, reservation_ttl_seconds=30, scheduler_id="sched-A")
    reservation_dispatcher = SchedulerReservationDispatcher(reservation_manager, dispatch_fn)
    dispatcher = SchedulerFairnessDispatcher(fairness_manager, reservation_dispatcher)

    candidates = [
        FairDispatchCandidate("tenant-a", "task-a", "worker-a", {"tenant_id": "tenant-a"}),
        FairDispatchCandidate("tenant-b", "task-b", "worker-b", {"tenant_id": "tenant-b"}),
    ]

    result = await dispatcher.choose_and_dispatch(
        candidates=candidates,
        scheduler_epoch="epoch-1",
        reserved_at_ms=123456,
    )

    assert result.ok is True
    assert result.chosen_tenant_id == "tenant-b"
    assert result.chosen_task_id == "task-b"


@pytest.mark.integration
async def test_successful_fair_dispatch_can_be_claimed(redis_client):
    async def dispatch_fn(*, task_id: str, worker_id: str, scheduler_epoch: str, dispatch_payload: dict) -> bool:
        return True

    await redis_client.set(DagRedisKey.task_state("task-b"), "scheduled")
    await redis_client.set(DagRedisKey.tenant_vruntime("tenant-b"), "1")
    await redis_client.set(DagRedisKey.tenant_inflight("tenant-b"), "0")

    fairness_manager = FairnessManager(redis_client)
    reservation_manager = WorkerReservationManager(redis_client, reservation_ttl_seconds=30, scheduler_id="sched-A")
    reservation_dispatcher = SchedulerReservationDispatcher(reservation_manager, dispatch_fn)
    dispatcher = SchedulerFairnessDispatcher(fairness_manager, reservation_dispatcher)

    result = await dispatcher.choose_and_dispatch(
        candidates=[
            FairDispatchCandidate("tenant-b", "task-b", "worker-b", {"tenant_id": "tenant-b"}),
        ],
        scheduler_epoch="epoch-1",
        reserved_at_ms=123456,
    )

    assert result.ok is True

    claim_manager = TaskClaimManager(redis_client)
    claim = await claim_manager.claim_start(
        task_id="task-b",
        tenant_id="tenant-b",
        worker_instance_id="worker-b",
        claimed_at_ms=123500,
        scheduler_epoch="epoch-1",
    )

    assert claim.ok is True
    assert await redis_client.get(DagRedisKey.task_state("task-b")) == "running"
