from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from hfa_control.dag_lua import TaskDispatchCommitResult
from hfa_control.scheduler_reservation_dispatch import (
    SchedulerReservationDispatcher,
)
from hfa_control.worker_reservation import (
    WorkerReservationManager,
    WorkerReservationResult,
)


pytestmark = pytest.mark.asyncio


def _successful_reservation_manager(
    *,
    scheduler_epoch: str = "epoch-77",
) -> AsyncMock:
    manager = AsyncMock(spec=WorkerReservationManager)
    manager.reserve.return_value = WorkerReservationResult(
        ok=True,
        status="reservation_created",
        scheduler_epoch=scheduler_epoch,
    )
    return manager


async def test_missing_explicit_task_id_is_rejected_before_reservation() -> None:
    reservation_manager = _successful_reservation_manager()
    dispatch_fn = AsyncMock(return_value=True)
    dispatcher = SchedulerReservationDispatcher(
        reservation_manager,
        dispatch_fn,
    )

    result = await dispatcher.reserve_and_dispatch(
        task_id="",
        worker_id="worker-77",
        scheduler_epoch="epoch-77",
        dispatch_payload={
            "run_id": "run-77",
            "tenant_id": "tenant-77",
        },
    )

    assert result.ok is False
    assert result.status == "dispatch_task_id_missing"
    reservation_manager.reserve.assert_not_awaited()
    dispatch_fn.assert_not_awaited()


async def test_missing_explicit_run_id_is_rejected_before_reservation() -> None:
    reservation_manager = _successful_reservation_manager()
    dispatch_fn = AsyncMock(return_value=True)
    dispatcher = SchedulerReservationDispatcher(
        reservation_manager,
        dispatch_fn,
    )

    result = await dispatcher.reserve_and_dispatch(
        task_id="task-77",
        worker_id="worker-77",
        scheduler_epoch="epoch-77",
        dispatch_payload={
            "tenant_id": "tenant-77",
        },
    )

    assert result.ok is False
    assert result.status == "dispatch_run_id_missing"
    reservation_manager.reserve.assert_not_awaited()
    dispatch_fn.assert_not_awaited()


async def test_payload_task_id_mismatch_is_rejected_before_reservation() -> None:
    reservation_manager = _successful_reservation_manager()
    dispatch_fn = AsyncMock(return_value=True)
    dispatcher = SchedulerReservationDispatcher(
        reservation_manager,
        dispatch_fn,
    )

    result = await dispatcher.reserve_and_dispatch(
        task_id="task-method-77",
        worker_id="worker-77",
        scheduler_epoch="epoch-77",
        dispatch_payload={
            "task_id": "task-payload-77",
            "run_id": "run-77",
            "tenant_id": "tenant-77",
        },
    )

    assert result.ok is False
    assert result.status == "dispatch_task_id_mismatch"
    reservation_manager.reserve.assert_not_awaited()
    dispatch_fn.assert_not_awaited()


async def test_missing_scheduler_epoch_is_rejected_before_reservation() -> None:
    reservation_manager = _successful_reservation_manager()
    dispatch_fn = AsyncMock(return_value=True)
    dispatcher = SchedulerReservationDispatcher(
        reservation_manager,
        dispatch_fn,
    )

    result = await dispatcher.reserve_and_dispatch(
        task_id="task-77",
        worker_id="worker-77",
        scheduler_epoch="",
        dispatch_payload={
            "run_id": "run-77",
            "tenant_id": "tenant-77",
        },
    )

    assert result.ok is False
    assert result.status == "dispatch_scheduler_epoch_missing"
    reservation_manager.reserve.assert_not_awaited()
    dispatch_fn.assert_not_awaited()


async def test_payload_scheduler_epoch_mismatch_is_rejected_before_reservation() -> None:
    reservation_manager = _successful_reservation_manager()
    dispatch_fn = AsyncMock(return_value=True)
    dispatcher = SchedulerReservationDispatcher(
        reservation_manager,
        dispatch_fn,
    )

    result = await dispatcher.reserve_and_dispatch(
        task_id="task-77",
        worker_id="worker-77",
        scheduler_epoch="epoch-method-77",
        dispatch_payload={
            "run_id": "run-77",
            "tenant_id": "tenant-77",
            "scheduler_epoch": "epoch-payload-77",
        },
    )

    assert result.ok is False
    assert result.status == "dispatch_scheduler_epoch_mismatch"
    reservation_manager.reserve.assert_not_awaited()
    dispatch_fn.assert_not_awaited()


async def test_equal_explicit_task_and_run_ids_are_permitted() -> None:
    reservation_manager = _successful_reservation_manager()
    dispatch_fn = AsyncMock(return_value=True)
    dispatcher = SchedulerReservationDispatcher(
        reservation_manager,
        dispatch_fn,
    )
    payload = {
        "task_id": "shared-explicit-77",
        "run_id": "shared-explicit-77",
        "tenant_id": "tenant-77",
        "scheduler_epoch": "epoch-77",
    }

    result = await dispatcher.reserve_and_dispatch(
        task_id="shared-explicit-77",
        worker_id="worker-77",
        scheduler_epoch="epoch-77",
        dispatch_payload=payload,
    )

    assert result.ok is True
    assert result.task_id == "shared-explicit-77"
    assert result.run_id == "shared-explicit-77"
    reservation_manager.reserve.assert_awaited_once()
    dispatch_fn.assert_awaited_once_with(
        task_id="shared-explicit-77",
        worker_id="worker-77",
        scheduler_epoch="epoch-77",
        dispatch_payload=payload,
    )


async def test_distinct_explicit_task_and_run_ids_are_permitted() -> None:
    reservation_manager = _successful_reservation_manager()
    dispatch_fn = AsyncMock(return_value=True)
    dispatcher = SchedulerReservationDispatcher(
        reservation_manager,
        dispatch_fn,
    )
    payload = {
        "run_id": "run-distinct-77",
        "tenant_id": "tenant-77",
    }

    result = await dispatcher.reserve_and_dispatch(
        task_id="task-distinct-77",
        worker_id="worker-77",
        scheduler_epoch="epoch-77",
        dispatch_payload=payload,
    )

    assert result.ok is True
    assert result.task_id == "task-distinct-77"
    assert result.run_id == "run-distinct-77"
    reservation_manager.reserve.assert_awaited_once()
    dispatch_fn.assert_awaited_once_with(
        task_id="task-distinct-77",
        worker_id="worker-77",
        scheduler_epoch="epoch-77",
        dispatch_payload=payload,
    )


async def test_noncommitted_structured_dispatch_result_is_failure_and_releases() -> None:
    reservation_manager = _successful_reservation_manager()
    dispatch_fn = AsyncMock(
        return_value=TaskDispatchCommitResult(
            committed=False,
            status="identity_run_id_mismatch",
            task_id="task-77",
            reason="authoritative_run_id_differs",
        )
    )
    dispatcher = SchedulerReservationDispatcher(
        reservation_manager,
        dispatch_fn,
    )

    result = await dispatcher.reserve_and_dispatch(
        task_id="task-77",
        worker_id="worker-77",
        scheduler_epoch="epoch-77",
        dispatch_payload={
            "run_id": "run-message-77",
            "tenant_id": "tenant-77",
        },
    )

    assert result.ok is False
    assert result.status == "identity_run_id_mismatch"
    assert result.reason == "authoritative_run_id_differs"
    reservation_manager.release.assert_awaited_once_with("worker-77")
