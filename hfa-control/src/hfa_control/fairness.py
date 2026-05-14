"""
hfa-control/src/hfa_control/fairness.py
IRONCLAD Sprint 14A --- Tenant Fairness Selection
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional


@dataclass
class TenantPressure:
    """Tenant runtime pressure for fairness scoring."""

    tenant_id: str
    inflight: int
    weight: int = 1

    @property
    def effective_weight(self) -> int:
        """Weight with safe minimum."""
        return max(1, self.weight)

    @property
    def score(self) -> float:
        """Fairness score: lower is better."""
        effective_inflight = max(0, self.inflight)
        return effective_inflight / self.effective_weight


class FairnessSelector:
    """
    Deterministic tenant selection based on weighted fairness.
    Lower inflight/weight ratio wins.

    This module only provides scoring and selection logic.
    It has no Redis/network dependencies and is fully testable in isolation.
    """

    @staticmethod
    def score_tenant(inflight: int, weight: int) -> float:
        """Calculate fairness score for a tenant."""
        effective_weight = max(1, weight)
        effective_inflight = max(0, inflight)
        return effective_inflight / effective_weight

    @staticmethod
    def select_next_tenant(tenants: List[TenantPressure]) -> Optional[str]:
        """
        Select tenant with lowest score.
        Returns None if input empty.
        Tie-break: lexicographic tenant_id.
        """
        if not tenants:
            return None

        min_score = float("inf")
        candidates: List[str] = []

        for tenant in tenants:
            score = tenant.score
            if score < min_score - 1e-9:
                min_score = score
                candidates = [tenant.tenant_id]
            elif abs(score - min_score) < 1e-9:
                candidates.append(tenant.tenant_id)

        if not candidates:
            return None

        return sorted(candidates)[0]
