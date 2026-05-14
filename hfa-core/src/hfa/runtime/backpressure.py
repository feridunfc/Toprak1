
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class BackpressurePolicy:
    max_pending_per_tenant: int = 0
    global_pending_limit: int = 0
    max_running_per_tenant: int = 0

    def tenant_pending_allowed(self, pending: int) -> bool:
        if self.max_pending_per_tenant <= 0:
            return True
        return pending < self.max_pending_per_tenant

    def global_pending_allowed(self, pending: int) -> bool:
        if self.global_pending_limit <= 0:
            return True
        return pending < self.global_pending_limit

    def tenant_running_allowed(self, running: int) -> bool:
        if self.max_running_per_tenant <= 0:
            return True
        return running < self.max_running_per_tenant


@dataclass(frozen=True)
class LoadSheddingDecision:
    allowed: bool
    reason: str
    tenant_pending: int
    global_pending: int
    tenant_running: int
