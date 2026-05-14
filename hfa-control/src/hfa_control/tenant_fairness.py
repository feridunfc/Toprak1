from __future__ import annotations


class TenantFairnessTracker:
    """
    Tracks per-tenant virtual runtime (vruntime).

    Semantics required by existing tests:
    - Unknown tenant observed via get()/observe() appears at current min_vruntime
    - First actual dispatch charge for an unseen tenant starts from 0.0
    - pick_next() prefers the lowest effective vruntime
    - on ties, unseen tenants are preferred over seen tenants
    """

    def __init__(self) -> None:
        self._vruntime: dict[str, float] = {}

    def get(self, tenant_id: str) -> float:
        """Return effective vruntime for scheduling decisions."""
        if tenant_id not in self._vruntime:
            return self._min_vruntime()
        return self._vruntime[tenant_id]

    def observe(self, tenant_id: str) -> float:
        """Read-only alias used by older tests/callers."""
        return self.get(tenant_id)

    def pick_next(self, tenants: list[str]) -> str:
        """
        Select tenant with smallest effective vruntime.
        If tied, prefer unseen tenant first so newcomers are not starved.
        """
        if not tenants:
            raise ValueError("pick_next called with empty tenant list")

        return min(
            tenants,
            key=lambda t: (self.get(t), 1 if t in self._vruntime else 0, t),
        )

    def update_on_dispatch(self, tenant_id: str, cost: float = 1.0) -> None:
        """
        Charge actual service only after successful dispatch.

        Important compatibility rule:
        - first real charge for unseen tenant starts from 0.0
        - subsequent charges accumulate from that tenant's own vruntime
        """
        charge = max(float(cost), 0.0)

        if tenant_id not in self._vruntime:
            self._vruntime[tenant_id] = charge
            return

        self._vruntime[tenant_id] += charge

    def set_floor(self, tenant_id: str, floor: float) -> None:
        """Ensure a tenant's vruntime is at least floor."""
        current = self._vruntime.get(tenant_id)
        if current is None:
            self._vruntime[tenant_id] = float(floor)
        else:
            self._vruntime[tenant_id] = max(current, float(floor))

    def all_vruntimes(self) -> dict[str, float]:
        """Return a snapshot copy."""
        return dict(self._vruntime)

    def reset(self) -> None:
        """Clear all state."""
        self._vruntime.clear()

    def _min_vruntime(self) -> float:
        """Minimum vruntime across known tenants, or 0.0 if empty."""
        if not self._vruntime:
            return 0.0
        return min(self._vruntime.values())