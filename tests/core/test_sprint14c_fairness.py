"""
tests/core/test_sprint14c_fairness.py
IRONCLAD Sprint 14C --- Tenant fairness tracker tests
"""

from hfa_control.tenant_fairness import TenantFairnessTracker


# ---------------------------------------------------------------------------
# Spec tests (from Sprint 14C implementation spec)
# ---------------------------------------------------------------------------


def test_new_tenant_starts_at_zero():
    tracker = TenantFairnessTracker()
    assert tracker.get("tenant-a") == 0.0


def test_update_on_dispatch_increases_vruntime():
    tracker = TenantFairnessTracker()
    tracker.update_on_dispatch("tenant-a")
    assert tracker.get("tenant-a") == 1.0


def test_update_on_dispatch_accumulates():
    tracker = TenantFairnessTracker()
    tracker.update_on_dispatch("tenant-a", cost=2.5)
    tracker.update_on_dispatch("tenant-a", cost=1.5)
    assert tracker.get("tenant-a") == 4.0


def test_tracker_is_isolated_per_tenant():
    tracker = TenantFairnessTracker()
    tracker.update_on_dispatch("tenant-a", cost=3.0)
    tracker.update_on_dispatch("tenant-b", cost=1.0)
    assert tracker.get("tenant-a") == 3.0
    assert tracker.get("tenant-b") == 1.0


def test_reset_clears_state():
    tracker = TenantFairnessTracker()
    tracker.update_on_dispatch("tenant-a", cost=2.0)
    tracker.reset()
    assert tracker.get("tenant-a") == 0.0


# ---------------------------------------------------------------------------
# Original Sprint 14C behaviour tests (preserved)
# ---------------------------------------------------------------------------


def test_fairness_balances_two_tenants():
    """Two tenants with equal load should get roughly equal picks."""
    tracker = TenantFairnessTracker()

    tenants = ["a", "b"]
    order = []

    for _ in range(10):
        t = tracker.pick_next(tenants)
        order.append(t)
        tracker.update_on_dispatch(t)

    # Distribution should be nearly equal
    assert abs(order.count("a") - order.count("b")) <= 1


def test_heavy_tenant_gets_deprioritized():
    """Tenant with higher vruntime should be deprioritized."""
    tracker = TenantFairnessTracker()

    tracker.update_on_dispatch("heavy", cost=10)

    next_tenant = tracker.pick_next(["heavy", "light"])

    assert next_tenant == "light"


def test_vruntime_accumulates_correctly():
    """Multiple dispatches should accumulate vruntime."""
    tracker = TenantFairnessTracker()

    tracker.update_on_dispatch("a")
    tracker.update_on_dispatch("a")

    assert tracker.get("a") == 2.0


def test_pick_next_with_single_tenant():
    """Single tenant should always be selected."""
    tracker = TenantFairnessTracker()

    tenant = tracker.pick_next(["only"])

    assert tenant == "only"


# ---------------------------------------------------------------------------
# New: guard and observe tests
# ---------------------------------------------------------------------------


def test_negative_cost_clamped_to_zero():
    """Negative cost must not decrease vruntime."""
    tracker = TenantFairnessTracker()
    tracker.update_on_dispatch("tenant-a", cost=5.0)
    tracker.update_on_dispatch("tenant-a", cost=-10.0)
    assert tracker.get("tenant-a") == 5.0


def test_observe_does_not_mutate():
    """observe() must be read-only."""
    tracker = TenantFairnessTracker()
    tracker.update_on_dispatch("tenant-a", cost=3.0)

    result = tracker.observe("tenant-a")

    assert result == 3.0
    assert tracker.get("tenant-a") == 3.0  # unchanged


def test_observe_unknown_tenant_returns_zero():
    """observe() on unseen tenant returns 0.0 without side effects."""
    tracker = TenantFairnessTracker()
    result = tracker.observe("unknown-tenant")
    assert result == 0.0
    assert tracker.get("unknown-tenant") == 0.0
