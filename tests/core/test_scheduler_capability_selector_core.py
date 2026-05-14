
from hfa_control.scheduler_capability_selector import (
    SchedulerCapabilitySelector,
    WorkerCandidate,
)


def test_incompatible_workers_filtered_out():
    workers = [
        WorkerCandidate(worker_id="w-python", capabilities=["python", "sql"]),
        WorkerCandidate(worker_id="w-frontend", capabilities=["frontend"]),
    ]

    result = SchedulerCapabilitySelector.filter_workers(
        required_capabilities=["python"],
        workers=workers,
    )

    assert [w.worker_id for w in result.compatible] == ["w-python"]
    assert result.rejected_worker_ids == ["w-frontend"]
    assert result.missing_by_worker["w-frontend"] == ["python"]


def test_multiple_compatible_workers_preserved_deterministically():
    workers = [
        WorkerCandidate(worker_id="w-b", capabilities=["python", "sql"]),
        WorkerCandidate(worker_id="w-a", capabilities=["python", "sql"]),
    ]

    result = SchedulerCapabilitySelector.filter_workers(
        required_capabilities=["python"],
        workers=workers,
    )

    assert [w.worker_id for w in result.compatible] == ["w-a", "w-b"]
    assert result.rejected_worker_ids == []


def test_no_required_capabilities_all_workers_survive():
    workers = [
        WorkerCandidate(worker_id="w-a", capabilities=["python"]),
        WorkerCandidate(worker_id="w-b", capabilities=["frontend"]),
    ]

    result = SchedulerCapabilitySelector.filter_workers(
        required_capabilities=[],
        workers=workers,
    )

    assert [w.worker_id for w in result.compatible] == ["w-a", "w-b"]
    assert result.rejected_worker_ids == []
