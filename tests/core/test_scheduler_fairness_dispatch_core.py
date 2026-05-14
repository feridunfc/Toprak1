
import pytest
from unittest.mock import AsyncMock

from hfa_control.scheduler_fairness_dispatch import (
    FairDispatchCandidate,
    SchedulerFairnessDecision,
    SchedulerFairnessDispatcher,
)
from hfa_control.scheduler_reservation_dispatch import ReservationDispatchResult
from hfa_control.fairness_manager import FairnessManager

pytestmark = pytest.mark.asyncio


async def test_no_candidates_returns_no_candidates():
    fairness_manager = AsyncMock(spec=FairnessManager)
    reservation_dispatcher = AsyncMock()

    dispatcher = SchedulerFairnessDispatcher(fairness_manager, reservation_dispatcher)
    result = await dispatcher.choose_and_dispatch(
        candidates=[],
        scheduler_epoch="epoch-1",
    )

    assert result.ok is False
    assert result.status == "no_candidates"


async def test_choose_fairest_tenant_then_dispatch():
    fairness_manager = AsyncMock(spec=FairnessManager)
    fairness_manager.choose_fairest_tenant.return_value = type(
        "Snap", (), {"tenant_id": "tenant-b"}
    )()
    reservation_dispatcher = AsyncMock()
    reservation_dispatcher.reserve_and_dispatch.return_value = ReservationDispatchResult(
        ok=True,
        status="reserved_and_dispatched",
        worker_id="worker-2",
        scheduler_epoch="epoch-1",
    )

    dispatcher = SchedulerFairnessDispatcher(fairness_manager, reservation_dispatcher)
    candidates = [
        FairDispatchCandidate("tenant-a", "task-a", "worker-1", {"tenant_id": "tenant-a"}),
        FairDispatchCandidate("tenant-b", "task-b", "worker-2", {"tenant_id": "tenant-b"}),
    ]

    result = await dispatcher.choose_and_dispatch(
        candidates=candidates,
        scheduler_epoch="epoch-1",
    )

    assert result.ok is True
    assert result.chosen_tenant_id == "tenant-b"
    assert result.chosen_task_id == "task-b"
    assert result.chosen_worker_id == "worker-2"


async def test_same_tenant_is_deterministic_by_task_then_worker():
    fairness_manager = AsyncMock(spec=FairnessManager)
    fairness_manager.choose_fairest_tenant.return_value = type(
        "Snap", (), {"tenant_id": "tenant-a"}
    )()
    reservation_dispatcher = AsyncMock()
    reservation_dispatcher.reserve_and_dispatch.return_value = ReservationDispatchResult(
        ok=True,
        status="reserved_and_dispatched",
        worker_id="worker-1",
        scheduler_epoch="epoch-1",
    )

    dispatcher = SchedulerFairnessDispatcher(fairness_manager, reservation_dispatcher)
    candidates = [
        FairDispatchCandidate("tenant-a", "task-z", "worker-9", {"tenant_id": "tenant-a"}),
        FairDispatchCandidate("tenant-a", "task-a", "worker-2", {"tenant_id": "tenant-a"}),
        FairDispatchCandidate("tenant-a", "task-a", "worker-1", {"tenant_id": "tenant-a"}),
    ]

    result = await dispatcher.choose_and_dispatch(
        candidates=candidates,
        scheduler_epoch="epoch-1",
    )

    assert result.ok is True
    assert result.chosen_task_id == "task-a"
    assert result.chosen_worker_id == "worker-1"
