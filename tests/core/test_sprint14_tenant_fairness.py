from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "hfa-control" / "src"))

from hfa_control.fairness import FairnessSelector, TenantPressure


class TestSprint14TenantFairness:
    def test_lower_inflight_selected(self):
        selector = FairnessSelector()

        tenants = [
            TenantPressure("tenant-a", inflight=5, weight=1),
            TenantPressure("tenant-b", inflight=2, weight=1),
        ]

        selected = selector.select_next_tenant(tenants)
        assert selected == "tenant-b"

    def test_weight_affects_score(self):
        selector = FairnessSelector()

        tenants = [
            TenantPressure("tenant-a", inflight=10, weight=2),  # 5.0
            TenantPressure("tenant-b", inflight=6, weight=1),  # 6.0
        ]

        selected = selector.select_next_tenant(tenants)
        assert selected == "tenant-a"

    def test_deterministic_tie_break(self):
        selector = FairnessSelector()

        tenants = [
            TenantPressure("tenant-b", inflight=5, weight=1),
            TenantPressure("tenant-a", inflight=5, weight=1),
        ]

        selected = selector.select_next_tenant(tenants)
        assert selected == "tenant-a"

    def test_empty_list_returns_none(self):
        selector = FairnessSelector()
        assert selector.select_next_tenant([]) is None

    def test_single_tenant(self):
        selector = FairnessSelector()
        tenants = [TenantPressure("tenant-a", inflight=10, weight=1)]
        assert selector.select_next_tenant(tenants) == "tenant-a"

    def test_score_calculation(self):
        assert FairnessSelector.score_tenant(10, 2) == 5.0
        assert FairnessSelector.score_tenant(5, 1) == 5.0
        assert FairnessSelector.score_tenant(0, 5) == 0.0
        assert FairnessSelector.score_tenant(-5, 2) == 0.0

    def test_negative_inflight_normalized(self):
        tenant = TenantPressure("tenant-a", inflight=-5, weight=1)
        assert tenant.score == 0.0
