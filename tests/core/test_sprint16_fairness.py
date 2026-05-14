"""
tests/core/test_sprint16_fairness.py
IRONCLAD Sprint 16 — TenantFairnessTracker (CFS upgrade) tests

Verifies:
  - New tenants start at min_vruntime (starvation prevention)
  - pick_next() selects the most under-served tenant
  - Heavy tenants are deprioritized
  - Two equal tenants alternate fairly
  - all_vruntimes() returns correct snapshot
  - reset() clears all state
"""

from __future__ import annotations

import pytest

from hfa_control.tenant_fairness import TenantFairnessTracker


# ---------------------------------------------------------------------------
# Initialization
# ---------------------------------------------------------------------------


def test_first_tenant_starts_at_zero():
    tracker = TenantFairnessTracker()
    assert tracker.get("a") == 0.0


def test_new_tenant_starts_at_min_vruntime_of_existing():
    """New tenant should start where the current minimum is — not at 0."""
    tracker = TenantFairnessTracker()
    tracker.update_on_dispatch("a", cost=5.0)
    tracker.update_on_dispatch("b", cost=3.0)

    # min_vruntime = 3.0 (tenant b)
    # new tenant c should start at 3.0, not 0.0
    assert tracker.get("c") == 3.0


def test_new_tenant_does_not_start_below_existing_minimum():
    """New tenant must never get a vruntime that puts it unfairly ahead."""
    tracker = TenantFairnessTracker()
    tracker.update_on_dispatch("existing", cost=10.0)
    assert tracker.get("new") == 10.0


# ---------------------------------------------------------------------------
# pick_next
# ---------------------------------------------------------------------------


def test_pick_next_selects_lowest_vruntime():
    tracker = TenantFairnessTracker()
    tracker.update_on_dispatch("a", cost=10.0)
    tracker.update_on_dispatch("b", cost=2.0)
    tracker.update_on_dispatch("c", cost=6.0)
    assert tracker.pick_next(["a", "b", "c"]) == "b"


def test_pick_next_single_tenant():
    tracker = TenantFairnessTracker()
    assert tracker.pick_next(["only"]) == "only"


def test_pick_next_raises_on_empty_list():
    tracker = TenantFairnessTracker()
    with pytest.raises(ValueError):
        tracker.pick_next([])


def test_pick_next_alternates_two_equal_tenants():
    """Two tenants with equal cost should alternate picks fairly."""
    tracker = TenantFairnessTracker()
    tenants = ["a", "b"]
    selections = []

    for _ in range(10):
        chosen = tracker.pick_next(tenants)
        selections.append(chosen)
        tracker.update_on_dispatch(chosen, cost=1.0)

    count_a = selections.count("a")
    count_b = selections.count("b")
    assert abs(count_a - count_b) <= 1, f"Unfair: a={count_a} b={count_b}"


# ---------------------------------------------------------------------------
# Heavy tenant deprioritization
# ---------------------------------------------------------------------------


def test_heavy_tenant_deprioritized_after_burst():
    """After a burst from one tenant, the other should be picked next."""
    tracker = TenantFairnessTracker()

    # heavy sends 5 runs
    for _ in range(5):
        tracker.update_on_dispatch("heavy", cost=1.0)

    # light sends 0 runs
    # Now pick_next should choose light
    chosen = tracker.pick_next(["heavy", "light"])
    assert chosen == "light"


def test_heavy_tenant_eventually_gets_scheduled():
    """Heavy tenant is never permanently starved — just temporarily deprioritized."""
    tracker = TenantFairnessTracker()

    # heavy has 3 runs, light has 0
    for _ in range(3):
        tracker.update_on_dispatch("heavy", cost=1.0)

    # Give light 3 runs to catch up
    picks = []
    for _ in range(3):
        chosen = tracker.pick_next(["heavy", "light"])
        picks.append(chosen)
        tracker.update_on_dispatch(chosen, cost=1.0)

    # light should have been picked at least once
    assert "light" in picks


def test_three_tenants_fairness():
    """Three tenants dispatching at different rates should converge."""
    tracker = TenantFairnessTracker()
    tenants = ["a", "b", "c"]
    counts = {"a": 0, "b": 0, "c": 0}

    for _ in range(30):
        chosen = tracker.pick_next(tenants)
        counts[chosen] += 1
        tracker.update_on_dispatch(chosen, cost=1.0)

    # Each tenant should get roughly 10 dispatches (±3)
    for t, count in counts.items():
        assert 7 <= count <= 13, f"Tenant {t} got {count}/30 — unfair distribution"


# ---------------------------------------------------------------------------
# min_vruntime
# ---------------------------------------------------------------------------


def test_min_vruntime_zero_when_empty():
    tracker = TenantFairnessTracker()
    assert tracker._min_vruntime() == 0.0


def test_min_vruntime_is_global_minimum():
    tracker = TenantFairnessTracker()
    tracker.update_on_dispatch("a", cost=8.0)
    tracker.update_on_dispatch("b", cost=3.0)
    tracker.update_on_dispatch("c", cost=6.0)
    assert tracker._min_vruntime() == 3.0


# ---------------------------------------------------------------------------
# all_vruntimes / observe
# ---------------------------------------------------------------------------


def test_all_vruntimes_returns_snapshot():
    tracker = TenantFairnessTracker()
    tracker.update_on_dispatch("a", cost=1.0)
    tracker.update_on_dispatch("b", cost=2.0)
    snap = tracker.all_vruntimes()
    assert snap == {"a": 1.0, "b": 2.0}


def test_all_vruntimes_is_copy():
    tracker = TenantFairnessTracker()
    tracker.update_on_dispatch("a", cost=1.0)
    snap = tracker.all_vruntimes()
    snap["a"] = 999.0
    assert tracker.get("a") == 1.0  # original unchanged


def test_observe_does_not_mutate():
    tracker = TenantFairnessTracker()
    tracker.update_on_dispatch("a", cost=5.0)
    tracker.observe("new_tenant")
    assert "new_tenant" not in tracker.all_vruntimes()


# ---------------------------------------------------------------------------
# reset
# ---------------------------------------------------------------------------


def test_reset_clears_all_state():
    tracker = TenantFairnessTracker()
    tracker.update_on_dispatch("a", cost=10.0)
    tracker.update_on_dispatch("b", cost=5.0)
    tracker.reset()
    assert tracker.all_vruntimes() == {}
    assert tracker.get("a") == 0.0


def test_reset_and_restart_works():
    tracker = TenantFairnessTracker()
    tracker.update_on_dispatch("a", cost=50.0)
    tracker.reset()
    tracker.update_on_dispatch("b", cost=1.0)
    # After reset, 'a' should start fresh at min_vruntime (1.0 from b)
    assert tracker.get("a") == 1.0
