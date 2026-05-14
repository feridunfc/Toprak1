"""
hfa-control/src/hfa_control/rate_limit.py
IRONCLAD Sprint 15 — Atomic tenant rate limiting via Redis EVALSHA

Architecture
------------
The Lua script lives in hfa-core/src/hfa/lua/rate_limit.lua.
LuaScriptLoader loads it once via SCRIPT LOAD (→ SHA), then calls
EVALSHA on every admission check. This is the same pattern as BudgetGuard.

Why EVALSHA over EVAL
---------------------
  EVAL    — sends the full script body on every call (network overhead)
  EVALSHA — sends only the 40-char SHA; Redis executes the cached script

On NOSCRIPT (Redis restart flushed its script cache):
  LuaScriptLoader reloads and retries automatically — no manual recovery.

Atomicity guarantee
-------------------
TRIM → COUNT → ZADD execute as one Redis transaction.
No TOCTOU window. Concurrent calls at the same millisecond cannot both
read "under limit" and both write.

Lifecycle
---------
  limiter = TenantRateLimiter(redis)
  await limiter.initialise()          # SCRIPT LOAD → SHA cached

  allowed = await limiter.check_and_consume(tenant_id, limit, now=now)

Fakeredis fallback
------------------
fakeredis does not support EVALSHA / SCRIPT LOAD.
LuaScriptLoader detects this and falls back to the non-atomic path
for unit tests. Integration tests (Testcontainers) exercise EVALSHA.

Backward compatibility
----------------------
* check_and_consume() signature unchanged
* is_allowed() preserved as read-only helper
* consume() preserved for explicit recording / testing
* Disabled behavior (None / <= 0) unchanged
"""

from __future__ import annotations

import logging
import time
import uuid
from pathlib import Path
from typing import Optional

from hfa.config.keys import RedisKey, RedisTTL
from hfa.lua.loader import LuaScriptLoader

logger = logging.getLogger(__name__)

# Absolute path to the Lua script, relative to this package's installed location
_LUA_SCRIPT_PATH = (
    Path(__file__).parent.parent.parent.parent.parent
    / "hfa-core"
    / "src"
    / "hfa"
    / "lua"
    / "rate_limit.lua"
)

# Fallback: resolve relative to hfa package inside site-packages
_LUA_SCRIPT_PATH_INSTALLED = (
    Path(__file__).parent.parent.parent.parent / "hfa" / "lua" / "rate_limit.lua"
)


def _resolve_lua_path() -> Path:
    """Find rate_limit.lua whether running from repo or installed package."""
    if _LUA_SCRIPT_PATH.exists():
        return _LUA_SCRIPT_PATH
    if _LUA_SCRIPT_PATH_INSTALLED.exists():
        return _LUA_SCRIPT_PATH_INSTALLED

    here = Path(__file__).resolve()
    for parent in here.parents:
        candidate = parent / "hfa-core" / "src" / "hfa" / "lua" / "rate_limit.lua"
        if candidate.exists():
            return candidate

        candidate2 = parent / "hfa" / "lua" / "rate_limit.lua"
        if candidate2.exists():
            return candidate2

    raise FileNotFoundError(
        "rate_limit.lua not found. Expected at hfa-core/src/hfa/lua/rate_limit.lua"
    )


class TenantRateLimiter:
    """
    Atomic sliding-window tenant rate limiter via Redis EVALSHA.

    Must call await limiter.initialise() before any check_and_consume() calls.

    Args:
        redis: aioredis.Redis client.
    """

    def __init__(self, redis) -> None:
        self._redis = redis
        self._loader: Optional[LuaScriptLoader] = None

    async def initialise(self) -> None:
        """
        Load the Lua script into Redis via SCRIPT LOAD and cache the SHA.
        Must be called once before check_and_consume().

        Safe to call multiple times (idempotent).
        On fakeredis: logs a debug message and continues without SHA.
        """
        lua_path = _resolve_lua_path()
        self._loader = LuaScriptLoader(self._redis, lua_path)
        await self._loader.load()
        logger.info(
            "TenantRateLimiter initialised: SHA=%s",
            (self._loader.sha[:8] + "…") if self._loader.sha else "fallback-mode",
        )

    def _key(self, tenant_id: str) -> str:
        return RedisKey.tenant_rate(tenant_id)

    async def check_and_consume(
        self,
        tenant_id: str,
        max_runs_per_second: Optional[float],
        *,
        now: Optional[float] = None,
    ) -> bool:
        """
        Atomically check the sliding-window rate limit and consume a slot.

        Returns True if admitted, False if the limit is exceeded.
        Disabled (always admits) when max_runs_per_second is None or <= 0.

        Uses EVALSHA on real Redis; non-atomic fallback on fakeredis.
        Auto-initialises if initialise() was not called (convenience for tests).
        """
        if max_runs_per_second is None or max_runs_per_second <= 0:
            return True

        if self._loader is None:
            await self.initialise()

        current = now if now is not None else time.time()
        key = self._key(tenant_id)
        member = f"{tenant_id}:{current:.6f}:{uuid.uuid4().hex}"

        try:
            result = await self._loader.run(
                num_keys=1,
                keys=[key],
                args=[int(max_runs_per_second), current, member, RedisTTL.TENANT_RATE],
                fallback=lambda: self._check_and_consume_non_atomic(
                    tenant_id,
                    max_runs_per_second,
                    current=current,
                ),
            )
            admitted = bool(result)
            if not admitted:
                logger.debug(
                    "RateLimit: REJECTED tenant=%s limit=%s/s",
                    tenant_id,
                    max_runs_per_second,
                )
            return admitted

        except Exception as exc:
            logger.error(
                "TenantRateLimiter.check_and_consume error: tenant=%s %s",
                tenant_id,
                exc,
                exc_info=True,
            )
            # Fail-open on unexpected Redis error — do not block admission
            return True

    async def is_allowed(
        self,
        tenant_id: str,
        max_runs_per_second: Optional[float],
        *,
        now: Optional[float] = None,
    ) -> bool:
        """
        Read-only check — does NOT consume a slot.
        Used for inspection/metrics only; admission must use check_and_consume().
        """
        if max_runs_per_second is None or max_runs_per_second <= 0:
            return True

        current = now if now is not None else time.time()
        window_start = current - 1.0
        key = self._key(tenant_id)

        await self._redis.zremrangebyscore(key, 0, window_start)
        count = await self._redis.zcard(key)
        return int(count) < int(max_runs_per_second)

    async def consume(
        self,
        tenant_id: str,
        *,
        now: Optional[float] = None,
    ) -> None:
        """
        Explicitly record one request.
        Used for testing / manual backfill — not in the normal admission path.
        """
        current = now if now is not None else time.time()
        key = self._key(tenant_id)
        member = f"{current:.6f}:{id(self)}:{tenant_id}"
        await self._redis.zadd(key, {member: current})
        await self._redis.expire(key, RedisTTL.TENANT_RATE)

    async def _check_and_consume_non_atomic(
        self,
        tenant_id: str,
        max_runs_per_second: float,
        *,
        current: float,
    ) -> bool:
        """
        Sprint 14B-equivalent check-then-consume.
        Called ONLY when Redis EVALSHA is unavailable (fakeredis in unit tests).
        Production code always takes the EVALSHA path.
        """
        window_start = current - 1.0
        key = self._key(tenant_id)

        await self._redis.zremrangebyscore(key, 0, window_start)
        count = await self._redis.zcard(key)

        if int(count) >= int(max_runs_per_second):
            return False

        member = f"{tenant_id}:{current:.6f}:{id(self)}"
        await self._redis.zadd(key, {member: current})
        await self._redis.expire(key, RedisTTL.TENANT_RATE)
        return True
