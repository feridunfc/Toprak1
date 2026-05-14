from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class DispatchPermit:
    allowed: bool
    reason: Optional[str]
    max_dispatches: int
    tokens_remaining: int
    refill_rate_per_sec: float
    degraded: bool = False


class DispatchController:
    """Leader-local token bucket for dispatch pacing."""

    def __init__(self, redis, config) -> None:
        self._redis = redis
        self._config = config
        self._tokens = float(config.dispatch_tokens_capacity)
        self._last_refill = time.monotonic()
        self._refill_rate = float(config.dispatch_tokens_refill_per_sec)
        self._degraded = False

    async def initialise(self) -> None:
        self._tokens = float(self._config.dispatch_tokens_capacity)
        self._last_refill = time.monotonic()
        self._refill_rate = float(self._config.dispatch_tokens_refill_per_sec)
        self._degraded = False

    async def current_permit(self) -> DispatchPermit:
        self._refill()
        allowed = self._tokens >= 1.0
        return DispatchPermit(
            allowed=allowed,
            reason=None if allowed else "dispatch_budget_exhausted",
            max_dispatches=int(self._tokens),
            tokens_remaining=int(self._tokens),
            refill_rate_per_sec=self._refill_rate,
            degraded=self._degraded,
        )

    async def try_consume(self, n: int = 1) -> bool:
        self._refill()
        if self._tokens < n:
            return False
        self._tokens -= n
        return True

    async def refund(self, n: int = 1) -> None:
        self._tokens = min(
            self._tokens + n,
            float(self._config.dispatch_tokens_capacity),
        )

    async def on_dispatch_success(self) -> None:
        if getattr(self._config, "dispatch_aimd_enabled", False) and not self._degraded:
            self._refill_rate = min(
                self._refill_rate + 0.25,
                float(self._config.dispatch_tokens_refill_per_sec),
            )

    async def on_dispatch_failure(self, reason: str) -> None:
        if reason in {
            "worker_pool_empty",
            "redis_degraded",
            "dispatch_commit_rejected",
            "internal_error",
        }:
            self._refill_rate = max(1.0, self._refill_rate * 0.5)

    async def on_redis_degraded(self) -> None:
        self._degraded = True
        self._refill_rate = float(self._config.dispatch_degraded_refill_per_sec)

    def _refill(self) -> None:
        now = time.monotonic()
        elapsed = max(0.0, now - self._last_refill)
        if elapsed <= 0:
            return
        self._tokens = min(
            self._tokens + elapsed * self._refill_rate,
            float(self._config.dispatch_tokens_capacity),
        )
        self._last_refill = now
