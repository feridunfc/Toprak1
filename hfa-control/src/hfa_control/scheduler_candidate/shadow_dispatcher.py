"""Shadow scheduler dispatcher for compare-only candidate decisions.

This module deliberately does not dispatch, reserve, or mutate runtime state.
It gives Sprint 4 a concrete non-authoritative boundary for candidate
scheduler experiments.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping


@dataclass(frozen=True)
class ShadowDispatchRecord:
    """A candidate scheduler decision captured for comparison only."""

    run_id: str
    worker_id: str | None = None
    tenant_id: str | None = None
    reason: str = "shadow_only"
    authoritative: bool = False
    details: Mapping[str, Any] = field(default_factory=dict)


class ShadowDispatcher:
    """Compare-only dispatcher for candidate scheduler output.

    It never claims authority and never calls the production dispatch path.
    """

    authoritative = False

    async def record_candidate(self, *, run_id: str, worker_id: str | None = None, tenant_id: str | None = None, details: Mapping[str, Any] | None = None) -> ShadowDispatchRecord:
        return ShadowDispatchRecord(
            run_id=str(run_id),
            worker_id=str(worker_id) if worker_id else None,
            tenant_id=str(tenant_id) if tenant_id else None,
            details=dict(details or {}),
        )

    async def dispatch(self, *args: Any, **kwargs: Any) -> ShadowDispatchRecord:
        """Refuse authority even if called through a dispatch-shaped API."""
        run_id = kwargs.get("run_id") or kwargs.get("task_id") or "unknown"
        worker_id = kwargs.get("worker_id")
        tenant_id = kwargs.get("tenant_id")
        return ShadowDispatchRecord(
            run_id=str(run_id),
            worker_id=str(worker_id) if worker_id else None,
            tenant_id=str(tenant_id) if tenant_id else None,
            reason="shadow_dispatch_not_authoritative",
            authoritative=False,
            details={"blocked": "shadow_dispatcher_non_authoritative"},
        )
