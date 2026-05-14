"""
hfa-tools/src/hfa_tools/middleware/rate_limit.py
IRONCLAD — Per-tenant request rate limiting (fixed-window, 1-minute buckets).

Design
------
* Middleware must be added AFTER TenantMiddleware in the stack so that
  request.state.tenant is already populated when we run.
* Each tenant gets a Redis key: ``rl:<tenant_id>:<minute_bucket>``
  where minute_bucket = unix_timestamp // 60 (integer).
* TTL is set to 120 s (2 windows) so keys self-expire without a background job.
* On Redis failure: fail_open=True (default) → allow the request.
  fail_open=False → return 503 Service Unavailable.

Middleware stack order (FastAPI)
---------------------------------
    app.add_middleware(TenantRateLimitMiddleware, redis=r, rpm=120)
    app.add_middleware(TenantMiddleware)   ← must come AFTER in add_middleware calls

IRONCLAD rules applied:
  ✅ No print() — logging only
  ✅ No os.environ.get() — settings via constructor args
  ✅ No asyncio.get_event_loop()
  ✅ No ensure_future — asyncio.get_running_loop().create_task not needed here
  ✅ Fail-closed option (fail_open=False → 503 on Redis failure)
"""

from __future__ import annotations

import logging
import time
from typing import Callable, Optional

from fastapi import Request, Response
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.types import ASGIApp

logger = logging.getLogger(__name__)


class TenantRateLimitMiddleware(BaseHTTPMiddleware):
    """
    Per-tenant fixed-window rate limiter.

    Each request from an authenticated tenant increments a Redis counter
    keyed on ``rl:<tenant_id>:<minute_bucket>``.  If the counter exceeds
    ``rpm + burst`` the request is rejected with HTTP 429.

    Requires TenantMiddleware to run first (reads ``request.state.tenant``).

    Args:
        app:        ASGI application.
        redis:      aioredis.Redis (or compatible async client).
        rpm:        Requests per minute baseline (default 120).
        burst:      Additional headroom above rpm before 429 fires (default 20).
        skip_paths: Path prefixes that bypass rate limiting.
        fail_open:  When True (default), allow requests if Redis is down.
                    When False, return 503 on Redis failure.
    """

    def __init__(
        self,
        app: ASGIApp,
        redis,
        rpm: int = 120,
        burst: int = 20,
        skip_paths: Optional[set[str]] = None,
        fail_open: bool = True,
    ) -> None:
        super().__init__(app)
        self._redis = redis
        self._rpm = rpm
        self._burst = burst
        self._limit = rpm + burst
        self._skip_paths = skip_paths or {"/health", "/metrics", "/docs", "/openapi.json"}
        self._fail_open = fail_open

        logger.info(
            "TenantRateLimitMiddleware init: rpm=%d burst=%d limit=%d fail_open=%s",
            rpm,
            burst,
            self._limit,
            fail_open,
        )

    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        """
        Intercept each request, apply per-tenant rate limit, then call next.

        Returns:
            429 with Retry-After header if rate limit exceeded.
            503 if Redis is unavailable and fail_open=False.
            Original response otherwise.
        """
        # ── Skip configured paths ───────────────────────────────────────
        for skip in self._skip_paths:
            if request.url.path.startswith(skip):
                return await call_next(request)

        # ── Require TenantMiddleware to have already run ─────────────────
        ctx = getattr(request.state, "tenant", None)
        if ctx is None:
            # No tenant context — TenantMiddleware not installed or skipped
            logger.debug("No tenant context on request; skipping rate limit")
            return await call_next(request)

        tenant_id = ctx.tenant_id

        try:
            count = await self._increment(tenant_id)
        except Exception as exc:
            logger.warning("Rate limit Redis error (tenant=%s): %s", tenant_id, exc)
            if not self._fail_open:
                return Response(
                    content="Rate limit service temporarily unavailable",
                    status_code=503,
                    headers={"Retry-After": "5"},
                )
            # fail_open=True → allow through
            return await call_next(request)

        if count > self._limit:
            now = int(time.time())
            retry_after = str(60 - (now % 60))
            remaining = max(0, self._limit - count)
            logger.warning(
                "Rate limit exceeded: tenant=%s count=%d limit=%d",
                tenant_id,
                count,
                self._limit,
            )
            return Response(
                content=f"Rate limit exceeded for tenant {tenant_id!r}. Retry in {retry_after}s.",
                status_code=429,
                headers={
                    "Retry-After": retry_after,
                    "X-RateLimit-Limit": str(self._limit),
                    "X-RateLimit-Remaining": str(remaining),
                    "X-RateLimit-Reset": str((now // 60 + 1) * 60),
                },
            )

        response = await call_next(request)
        # Attach informational rate-limit headers to every passing response
        response.headers["X-RateLimit-Limit"] = str(self._limit)
        response.headers["X-RateLimit-Remaining"] = str(max(0, self._limit - count))
        return response

    # ------------------------------------------------------------------
    # Redis helpers
    # ------------------------------------------------------------------

    async def _increment(self, tenant_id: str) -> int:
        """
        Atomically increment and return the per-tenant per-minute counter.

        Uses a Redis pipeline: INCR + EXPIRE in a single round-trip.
        TTL is set to 120 s so keys vanish after at most 2 full windows.

        Args:
            tenant_id: Authenticated tenant identifier.

        Returns:
            Current request count in this 1-minute window (after increment).

        Raises:
            Any aioredis exception on connectivity failure.
        """
        now = int(time.time())
        bucket = now // 60  # integer minute bucket
        key = f"rl:{tenant_id}:{bucket}"

        pipe = self._redis.pipeline()
        pipe.incr(key, 1)  # atomic increment
        pipe.expire(key, 120)  # 2-window TTL
        results = await pipe.execute()

        count = int(results[0])
        logger.debug(
            "Rate limit counter: tenant=%s bucket=%d count=%d limit=%d",
            tenant_id,
            bucket,
            count,
            self._limit,
        )
        return count

    def _key(self, tenant_id: str) -> str:
        """Return the Redis key for the current minute bucket."""
        return f"rl:{tenant_id}:{int(time.time()) // 60}"
