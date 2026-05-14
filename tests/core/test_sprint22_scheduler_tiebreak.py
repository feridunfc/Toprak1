from types import SimpleNamespace

from hfa_control.scheduler import Scheduler


def _worker(
    *,
    worker_id: str,
    worker_group: str,
    region: str,
    capacity: int,
    inflight: int,
    load_factor: float | None = None,
    schedulable: bool = True,
):
    if load_factor is None:
        if capacity > 0:
            load_factor = inflight / capacity
        else:
            load_factor = 1.0

    return SimpleNamespace(
        worker_id=worker_id,
        worker_group=worker_group,
        region=region,
        capacity=capacity,
        inflight=inflight,
        load_factor=load_factor,
        schedulable=schedulable,
    )


def _build_scheduler():
    scheduler = Scheduler.__new__(Scheduler)
    return scheduler


def test_pick_best_worker_prefers_lowest_primary_score():
    scheduler = _build_scheduler()

    workers = [
        _worker(
            worker_id="w-1",
            worker_group="grp-a",
            region="eu-west-1",
            capacity=10,
            inflight=2,
        ),
        _worker(
            worker_id="w-2",
            worker_group="grp-b",
            region="eu-west-1",
            capacity=10,
            inflight=1,
        ),
    ]

    selected = scheduler._pick_best_worker(
        workers,
        {
            "w-1": 200,
            "w-2": 100,
        },
    )

    assert selected.worker_id == "w-2"


def test_pick_best_worker_breaks_tie_by_load_factor():
    scheduler = _build_scheduler()

    workers = [
        _worker(
            worker_id="w-1",
            worker_group="grp-a",
            region="eu-west-1",
            capacity=10,
            inflight=4,
            load_factor=0.40,
        ),
        _worker(
            worker_id="w-2",
            worker_group="grp-b",
            region="eu-west-1",
            capacity=10,
            inflight=2,
            load_factor=0.20,
        ),
    ]

    selected = scheduler._pick_best_worker(
        workers,
        {
            "w-1": 100,
            "w-2": 100,
        },
    )

    assert selected.worker_id == "w-2"


def test_pick_best_worker_breaks_tie_by_inflight():
    scheduler = _build_scheduler()

    workers = [
        _worker(
            worker_id="w-1",
            worker_group="grp-a",
            region="eu-west-1",
            capacity=10,
            inflight=3,
            load_factor=0.30,
        ),
        _worker(
            worker_id="w-2",
            worker_group="grp-b",
            region="eu-west-1",
            capacity=20,
            inflight=2,
            load_factor=0.30,
        ),
    ]

    selected = scheduler._pick_best_worker(
        workers,
        {
            "w-1": 100,
            "w-2": 100,
        },
    )

    assert selected.worker_id == "w-2"


def test_pick_best_worker_breaks_tie_by_higher_capacity():
    scheduler = _build_scheduler()

    workers = [
        _worker(
            worker_id="w-1",
            worker_group="grp-a",
            region="eu-west-1",
            capacity=10,
            inflight=2,
            load_factor=0.20,
        ),
        _worker(
            worker_id="w-2",
            worker_group="grp-b",
            region="eu-west-1",
            capacity=20,
            inflight=2,
            load_factor=0.20,
        ),
    ]

    selected = scheduler._pick_best_worker(
        workers,
        {
            "w-1": 100,
            "w-2": 100,
        },
    )

    assert selected.worker_id == "w-2"


def test_pick_best_worker_breaks_tie_by_group_then_region_then_worker_id():
    scheduler = _build_scheduler()

    workers = [
        _worker(
            worker_id="w-2",
            worker_group="grp-b",
            region="us-east-1",
            capacity=10,
            inflight=1,
            load_factor=0.10,
        ),
        _worker(
            worker_id="w-1",
            worker_group="grp-a",
            region="us-east-1",
            capacity=10,
            inflight=1,
            load_factor=0.10,
        ),
        _worker(
            worker_id="w-0",
            worker_group="grp-a",
            region="eu-west-1",
            capacity=10,
            inflight=1,
            load_factor=0.10,
        ),
    ]

    selected = scheduler._pick_best_worker(
        workers,
        {
            "w-2": 100,
            "w-1": 100,
            "w-0": 100,
        },
    )

    assert selected.worker_id == "w-0"


def test_pick_best_worker_ignores_unschedulable_workers():
    scheduler = _build_scheduler()

    workers = [
        _worker(
            worker_id="w-1",
            worker_group="grp-a",
            region="eu-west-1",
            capacity=10,
            inflight=1,
            schedulable=False,
        ),
        _worker(
            worker_id="w-2",
            worker_group="grp-b",
            region="eu-west-1",
            capacity=10,
            inflight=2,
            schedulable=True,
        ),
    ]

    selected = scheduler._pick_best_worker(
        workers,
        {
            "w-1": 1,
            "w-2": 100,
        },
    )

    assert selected.worker_id == "w-2"


def test_pick_best_worker_returns_none_when_no_schedulable_candidates():
    scheduler = _build_scheduler()

    workers = [
        _worker(
            worker_id="w-1",
            worker_group="grp-a",
            region="eu-west-1",
            capacity=10,
            inflight=10,
            schedulable=False,
        ),
        _worker(
            worker_id="",
            worker_group="grp-b",
            region="eu-west-1",
            capacity=10,
            inflight=1,
            schedulable=True,
        ),
    ]

    selected = scheduler._pick_best_worker(
        workers,
        {
            "w-1": 100,
        },
    )

    assert selected is None