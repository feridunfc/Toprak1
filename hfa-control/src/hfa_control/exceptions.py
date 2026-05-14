"""
hfa-control/src/hfa_control/exceptions.py
IRONCLAD Sprint 10 — Exception hierarchy

All Control Plane exceptions extend ControlPlaneError.
Typed exceptions prevent bare-except swallow (IRONCLAD rule).
"""


class ControlPlaneError(Exception):
    """Base for all hfa-control exceptions."""


class AdmissionError(ControlPlaneError):
    """Run rejected at admission gate."""


class QuotaExceededError(AdmissionError):
    """Tenant concurrent-run limit exceeded."""


class RateLimitedError(AdmissionError):
    """Tenant RPM limit exceeded."""


class BudgetExceededError(AdmissionError):
    """Tenant budget limit exceeded."""


class PlacementError(ControlPlaneError):
    """Scheduler could not find a suitable worker group."""


class LeadershipError(ControlPlaneError):
    """Operation requires leadership but this instance is a standby."""


class ShardOwnershipError(ControlPlaneError):
    """Shard claim failed or shard has no registered owner."""


class WorkerNotFoundError(ControlPlaneError):
    """Requested worker_id not found in registry."""


class DLQEntryNotFoundError(ControlPlaneError):
    """Requested run_id not present in DLQ."""


class TenantMismatchError(ControlPlaneError):
    """X-Tenant-ID header does not match resource tenant_id."""
