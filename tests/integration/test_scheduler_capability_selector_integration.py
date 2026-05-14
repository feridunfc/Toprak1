
import pytest

from hfa_control.scheduler_capability_selector import (
    SchedulerCapabilitySelector,
    WorkerCandidate,
)

pytestmark = pytest.mark.asyncio


@pytest.mark.integration
async def test_compatible_workers_only_enter_candidate_set():
    workers = [
        WorkerCandidate(worker_id="worker-1", capabilities=["python", "sql"], worker_group="coder"),
        WorkerCandidate(worker_id="worker-2", capabilities=["frontend"], worker_group="ui"),
        WorkerCandidate(worker_id="worker-3", capabilities=["python"], worker_group="coder"),
    ]

    result = SchedulerCapabilitySelector.filter_workers(
        required_capabilities=["python"],
        workers=workers,
    )

    assert [w.worker_id for w in result.compatible] == ["worker-1", "worker-3"]
    assert result.rejected_worker_ids == ["worker-2"]


@pytest.mark.integration
async def test_no_compatible_worker_returns_empty_candidate_set():
    workers = [
        WorkerCandidate(worker_id="worker-1", capabilities=["frontend"]),
        WorkerCandidate(worker_id="worker-2", capabilities=["design"]),
    ]

    result = SchedulerCapabilitySelector.filter_workers(
        required_capabilities=["python", "sql"],
        workers=workers,
    )

    assert result.compatible == []
    assert result.rejected_worker_ids == ["worker-1", "worker-2"]
    assert result.missing_by_worker["worker-1"] == ["python", "sql"]
    assert result.missing_by_worker["worker-2"] == ["python", "sql"]


@pytest.mark.integration
async def test_scheduler_side_filtering_reduces_worker_side_reject_surface():
    workers = [
        WorkerCandidate(worker_id="worker-good", capabilities=["python", "sql"]),
        WorkerCandidate(worker_id="worker-bad", capabilities=["frontend"]),
    ]

    result = SchedulerCapabilitySelector.filter_workers(
        required_capabilities=["python"],
        workers=workers,
    )

    compatible_ids = [w.worker_id for w in result.compatible]
    assert compatible_ids == ["worker-good"]
    assert "worker-bad" not in compatible_ids
