
import pytest
from unittest.mock import AsyncMock

from hfa_control.worker_reservation import WorkerReservationManager, WorkerReservationResult

pytestmark = pytest.mark.asyncio


async def test_reservation_manager_passes_scheduler_id():
    redis = AsyncMock()
    mgr = WorkerReservationManager(redis, scheduler_id="sched-A")
    mgr._loader = AsyncMock()
    mgr._loader.run.return_value = ["reservation_created", "epoch-1"]

    result = await mgr.reserve(worker_id="worker-1", task_id="task-1", scheduler_epoch="epoch-1", reserved_at_ms=123)

    assert result.ok is True
    args = mgr._loader.run.await_args.kwargs["args"]
    assert args[-1] == "sched-A"


async def test_renew_returns_false_when_missing():
    redis = AsyncMock()
    redis.exists.return_value = 0
    mgr = WorkerReservationManager(redis)
    ok = await mgr.renew("worker-1")
    assert ok is False
