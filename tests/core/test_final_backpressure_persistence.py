
import pytest

from hfa_control.backpressure import BackpressureGuard
from hfa_control.scheduler_loop import SchedulerLoop


class _Cfg:
    backpressure_hard_inflight_cap = 0
    backpressure_soft_inflight_cap = 100
    backpressure_hysteresis_ratio = 0.8
    backpressure_max_queue_ratio = 10.0
    backpressure_worker_saturation = 0.95
    scheduler_loop_max_dispatches = 10
    scheduler_loop_max_duration_ms = 100
    dispatch_tokens_capacity = 10
    dispatch_tokens_refill_per_sec = 10.0


def test_backpressure_guard_persists_hysteresis_state():
    g = BackpressureGuard(_Cfg())
    s1 = g.evaluate(global_inflight=100, total_capacity=200, queue_depth=10, worker_load_factors=[0.4], worker_capacities=[200], max_dispatches_requested=10)
    assert s1.is_soft
    s2 = g.evaluate(global_inflight=85, total_capacity=200, queue_depth=10, worker_load_factors=[0.4], worker_capacities=[200], max_dispatches_requested=10)
    assert s2.is_soft
    s3 = g.evaluate(global_inflight=75, total_capacity=200, queue_depth=10, worker_load_factors=[0.4], worker_capacities=[200], max_dispatches_requested=10)
    assert not s3.active


@pytest.mark.asyncio
async def test_scheduler_loop_owns_single_backpressure_guard():
    loop = SchedulerLoop(None, None, None, None, type("TF", (), {"reset": lambda self: None})(), None, None, type("DC", (), {"initialise": lambda self: None})(), None, _Cfg())
    first = loop._bp_guard
    await loop.on_leadership_lost()
    await loop.on_leadership_gained()
    assert loop._bp_guard is first
