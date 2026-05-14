"""
tests/core/test_sprint21_backpressure.py
IRONCLAD Sprint 21 — Backpressure guard with hysteresis tests
"""
from unittest.mock import MagicMock
from hfa_control.backpressure import BackpressureGuard


def _cfg(**kwargs):
    cfg = MagicMock()
    cfg.backpressure_hard_inflight_cap = kwargs.get("hard_cap", 0)
    cfg.backpressure_soft_inflight_cap = kwargs.get("soft_cap", 0)
    cfg.backpressure_hysteresis_ratio  = kwargs.get("hyst", 0.8)
    cfg.backpressure_max_queue_ratio   = kwargs.get("q_ratio", 10.0)
    cfg.backpressure_worker_saturation = kwargs.get("sat", 0.95)
    return cfg


def _eval(guard, inflight=10, capacity=100, queue=10, lf=None, caps=None, budget=32):
    return guard.evaluate(
        global_inflight=inflight,
        total_capacity=capacity,
        queue_depth=queue,
        worker_load_factors=lf or [0.3],
        worker_capacities=caps,
        max_dispatches_requested=budget,
    )


def test_no_backpressure_normal():
    g = BackpressureGuard(_cfg(hard_cap=200, soft_cap=100))
    s = _eval(g, inflight=20)
    assert s.severity == "none"
    assert s.allowed_dispatches == 32

def test_hard_cap_stops_dispatch():
    g = BackpressureGuard(_cfg(hard_cap=100))
    s = _eval(g, inflight=100)
    assert s.is_hard
    assert s.allowed_dispatches == 0
    assert s.reason == "global_inflight_hard_cap"

def test_soft_cap_halves():
    g = BackpressureGuard(_cfg(soft_cap=50))
    s = _eval(g, inflight=55, budget=20)
    assert s.is_soft
    assert s.allowed_dispatches == 10

def test_hysteresis_prevents_thrashing():
    """Guard should stay in soft throttle until inflight drops below soft_low."""
    g = BackpressureGuard(_cfg(soft_cap=100, hyst=0.8))  # soft_low = 80
    # Enter throttle
    s1 = _eval(g, inflight=105, budget=20)
    assert s1.is_soft
    # Drops to 85 — still above soft_low (80), should stay throttled
    s2 = _eval(g, inflight=85, budget=20)
    assert s2.is_soft, "Should stay throttled above soft_low"
    # Drops to 75 — below soft_low, should resume
    s3 = _eval(g, inflight=75, budget=20)
    assert s3.severity == "none", "Should exit throttle below soft_low"

def test_capacity_weighted_saturation():
    """Large worker at 0.98 should trigger saturation, small at 0.98 alone should not if large is OK."""
    g = BackpressureGuard(_cfg(sat=0.95))
    # One big worker (cap=100) at 0.98, one tiny (cap=1) at 0.50
    # weighted = (0.98*100 + 0.50*1) / 101 ≈ 0.972 → above 0.95
    s = _eval(g, lf=[0.98, 0.50], caps=[100, 1], budget=16)
    assert s.is_hard
    assert s.reason == "fleet_capacity_saturated"

def test_capacity_weighted_saturation_ok_with_large_spare():
    g = BackpressureGuard(_cfg(sat=0.95))
    # Big worker (cap=100) at 0.50 → weighted avg low despite small worker high
    s = _eval(g, lf=[0.50, 0.99], caps=[100, 1], budget=16)
    assert s.severity == "none"

def test_queue_depth_pressure():
    g = BackpressureGuard(_cfg(q_ratio=5.0))
    s = _eval(g, capacity=10, queue=60, lf=[0.3], budget=16)
    assert s.is_soft
    assert s.reason == "queue_depth_pressure"

def test_hard_before_soft():
    g = BackpressureGuard(_cfg(hard_cap=100, soft_cap=50))
    s = _eval(g, inflight=105)
    assert s.is_hard

def test_empty_worker_pool_no_panic():
    g = BackpressureGuard(_cfg())
    s = _eval(g, lf=[], caps=[], budget=8)
    assert s.severity == "none"

def test_all_disabled():
    g = BackpressureGuard(_cfg(hard_cap=0, soft_cap=0))
    s = _eval(g, inflight=9999, lf=[0.3], budget=32)
    assert s.severity == "none"
