
import pytest
from unittest.mock import AsyncMock

from hfa_control.scheduler_reservation_dispatch import SchedulerReservationDispatcher
from hfa_control.worker_reservation import WorkerReservationManager, WorkerReservationResult

pytestmark = pytest.mark.asyncio


async def test_dispatch_not_called_if_reservation_conflicts():
    reservation_manager = AsyncMock(spec=WorkerReservationManager)
    reservation_manager.reserve.return_value = WorkerReservationResult(
        ok=False,
        status="reservation_conflict",
        scheduler_epoch="",
    )
    dispatch_fn = AsyncMock(return_value=True)

    dispatcher = SchedulerReservationDispatcher(reservation_manager, dispatch_fn)
    result = await dispatcher.reserve_and_dispatch(
        task_id="task-1",
        worker_id="worker-1",
        scheduler_epoch="epoch-1",
        dispatch_payload={"x": 1},
        reserved_at_ms=123,
    )

    assert result.ok is False
    assert result.status == "reservation_conflict"
    dispatch_fn.assert_not_awaited()


async def test_release_on_dispatch_failure():
    reservation_manager = AsyncMock(spec=WorkerReservationManager)
    reservation_manager.reserve.return_value = WorkerReservationResult(
        ok=True,
        status="reservation_created",
        scheduler_epoch="epoch-1",
    )
    dispatch_fn = AsyncMock(return_value=False)

    dispatcher = SchedulerReservationDispatcher(reservation_manager, dispatch_fn)
    result = await dispatcher.reserve_and_dispatch(
        task_id="task-1",
        worker_id="worker-1",
        scheduler_epoch="epoch-1",
        dispatch_payload={"x": 1},
        reserved_at_ms=123,
    )

    assert result.ok is False
    assert result.status == "dispatch_failed"
    reservation_manager.release.assert_awaited_once_with("worker-1")


async def test_keep_reservation_on_success_for_claim_consumption():
    reservation_manager = AsyncMock(spec=WorkerReservationManager)
    reservation_manager.reserve.return_value = WorkerReservationResult(
        ok=True,
        status="reservation_created",
        scheduler_epoch="epoch-1",
    )
    dispatch_fn = AsyncMock(return_value=True)

    dispatcher = SchedulerReservationDispatcher(reservation_manager, dispatch_fn)
    result = await dispatcher.reserve_and_dispatch(
        task_id="task-1",
        worker_id="worker-1",
        scheduler_epoch="epoch-1",
        dispatch_payload={"x": 1},
        reserved_at_ms=123,
    )

    assert result.ok is True
    assert result.status == "reserved_and_dispatched"
    reservation_manager.release.assert_not_awaited()
