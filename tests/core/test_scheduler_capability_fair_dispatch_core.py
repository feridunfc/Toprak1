
import pytest
from unittest.mock import AsyncMock

from hfa_control.scheduler_capability_fair_dispatch import (
    SchedulerCapabilityFairDispatcher,
    TaskDispatchRequest,
)
from hfa_control.scheduler_capability_selector import WorkerCandidate
from hfa_control.scheduler_reservation_dispatch import ReservationDispatchResult, SchedulerReservationDispatcher

pytestmark = pytest.mark.asyncio


async def test_no_compatible_workers_returns_reject():
    reservation_dispatcher = AsyncMock(spec=SchedulerReservationDispatcher)
    dispatcher = SchedulerCapabilityFairDispatcher(reservation_dispatcher)

    request = TaskDispatchRequest(
        tenant_id="tenant-a",
        task_id="task-1",
        required_capabilities=["python"],
        dispatch_payload={"tenant_id": "tenant-a"},
    )
    workers = [WorkerCandidate(worker_id="w1", capabilities=["frontend"])]

    result = await dispatcher.dispatch_task(
        request=request,
        workers=workers,
        scheduler_epoch="epoch-1",
    )

    assert result.ok is False
    assert result.status == "no_compatible_workers"
    reservation_dispatcher.reserve_and_dispatch.assert_not_awaited()


async def test_best_compatible_worker_is_reserved_and_dispatched():
    reservation_dispatcher = AsyncMock(spec=SchedulerReservationDispatcher)
    reservation_dispatcher.reserve_and_dispatch.return_value = ReservationDispatchResult(
        ok=True,
        status="reserved_and_dispatched",
        worker_id="w2",
        scheduler_epoch="epoch-1",
    )
    dispatcher = SchedulerCapabilityFairDispatcher(reservation_dispatcher)

    request = TaskDispatchRequest(
        tenant_id="tenant-a",
        task_id="task-1",
        required_capabilities=["python"],
        dispatch_payload={"tenant_id": "tenant-a"},
        vruntime=10.0,
        inflight=1,
    )
    workers = [
        WorkerCandidate(worker_id="w1", capabilities=["python"], current_load=5, capacity=1),
        WorkerCandidate(worker_id="w2", capabilities=["python"], current_load=1, capacity=3),
        WorkerCandidate(worker_id="w3", capabilities=["frontend"], current_load=0, capacity=10),
    ]

    result = await dispatcher.dispatch_task(
        request=request,
        workers=workers,
        scheduler_epoch="epoch-1",
        reserved_at_ms=123456,
    )

    assert result.ok is True
    assert result.worker_id == "w2"
    assert result.rejected_workers == ["w3"]
