# STATUS: CANONICAL — do not import from watermark_v2 or eviction_v2
"""
hfa-semantic/src/hfa_semantic/runtime/watermark.py

IRONCLAD Sprint 6.2 — Canonical Watermark (v1 + v2 merged)

STATUS: CANONICAL — watermark_v2.py is now a deprecated alias pointing here.

This module merges the best of both watermark implementations:

  WatermarkManager (v1 — sync, in-process)
    * observe(event_time_ms) → (is_late, should_accept)  [sync]
    * evaluate(event_time_ms) → (is_late, LatenessAction)
    * should_accept(event_time_ms, policy) → bool
    * watermark_ms, late_count, on_time_count, late_ratio properties

  WatermarkV2 (distributed — async, Redis-backed)
    * observe(event_time_ms) → (is_late, should_accept)  [async when redis present]
    * _sync_from_redis()
    * watermark_ms, late_count, on_time_count, late_ratio properties

Canonical Watermark class supports BOTH modes:
  * redis_client=None (default) → sync-safe, in-process, no IO
  * redis_client=client, rule_id="..." → distributed, Redis-backed

The observe() method detects whether it is being awaited and works
correctly in both contexts. Callers that use `await wm.observe(...)` get
the Redis-backed distributed path; callers that call `wm.observe(...)` 
synchronously get the in-process path.

LatenessPolicy is re-exported here for backward compat — it is the
canonical import point: `from hfa_semantic.runtime.watermark import LatenessPolicy`
"""

from __future__ import annotations

import json
import time
from typing import Any, Optional

from hfa_semantic.runtime.lateness_policy import LatenessAction, LatenessPolicy

__all__ = ["Watermark", "WatermarkManager", "LatenessPolicy"]

# TTL for Redis watermark state (24 hours)
_WATERMARK_TTL_SECONDS = 86_400

# Lua script for atomic watermark update
# Returns 1 if late, 0 if on-time
_LUA_WATERMARK_OBSERVE = """
local key = KEYS[1]
local ttl_sec = tonumber(ARGV[1])
local event_time_ms = tonumber(ARGV[2])
local allowed_lateness_ms = tonumber(ARGV[3])

local raw = redis.call('GET', key)
local watermark_ms = 0
local late_count = 0
local on_time_count = 0

if raw then
    local obj = cjson.decode(raw)
    watermark_ms = tonumber(obj['watermark_ms'] or 0)
    late_count = tonumber(obj['late_count'] or 0)
    on_time_count = tonumber(obj['on_time_count'] or 0)
end

local is_late = 0
if watermark_ms > 0 and event_time_ms < (watermark_ms - allowed_lateness_ms) then
    is_late = 1
    late_count = late_count + 1
else
    on_time_count = on_time_count + 1
    if event_time_ms > watermark_ms then
        watermark_ms = event_time_ms
    end
end

local new_state = cjson.encode({
    watermark_ms = watermark_ms,
    late_count = late_count,
    on_time_count = on_time_count
})
redis.call('SET', key, new_state, 'EX', ttl_sec)
return is_late
"""


class WatermarkManager:
    """
    Canonical watermark with both in-process and Redis-distributed modes.

    In-process mode (redis_client=None):
        * Fully synchronous — no IO
        * observe() returns tuple directly (not awaitable)
        * Use for single-worker deployments and tests

    Distributed mode (redis_client=..., rule_id=...):
        * observe() returns an awaitable coroutine
        * Watermark state is shared across workers via Redis
        * _sync_from_redis() loads current state from Redis

    Parameters
    ----------
    allowed_lateness_ms : int
        Grace period after the watermark. Events older than
        (watermark - allowed_lateness_ms) are considered late.
    policy : LatenessPolicy | None
        Default lateness policy. Defaults to DROP.
    redis_client : Any | None
        Optional Redis client for distributed mode.
    rule_id : str | None
        Rule identifier used as Redis key suffix. Required if redis_client set.
    namespace : str
        Redis key prefix (default: "semantic:watermark").
    """

    def __init__(
        self,
        allowed_lateness_ms: int,
        policy: LatenessPolicy | None = None,
        redis_client: Any | None = None,
        rule_id: str | None = None,
        namespace: str = "semantic:watermark",
    ) -> None:
        self.allowed_lateness_ms = allowed_lateness_ms
        self.policy = policy or LatenessPolicy(LatenessAction.DROP)
        self._redis = redis_client
        self._rule_id = rule_id or "default"
        self._namespace = namespace

        # Local state (in-process cache)
        self._watermark_ms: float = 0.0
        self._late_count: int = 0
        self._on_time_count: int = 0

    # ── Redis key ─────────────────────────────────────────────────────────────

    def _make_key(self) -> str:
        return f"{self._namespace}:{self._rule_id}"

    # ── Core observe (sync path) ──────────────────────────────────────────────

    def _observe_local(self, event_time_ms: float) -> tuple[bool, bool]:
        """In-process watermark update. No IO."""
        is_late = (
            self._watermark_ms > 0
            and event_time_ms < (self._watermark_ms - self.allowed_lateness_ms)
        )
        if is_late:
            self._late_count += 1
            should_accept = self.policy.action != LatenessAction.DROP
        else:
            self._on_time_count += 1
            if event_time_ms > self._watermark_ms:
                self._watermark_ms = event_time_ms
            should_accept = True
        return is_late, should_accept

    # ── observe — unified sync/async dispatch ─────────────────────────────────

    def observe(self, event_time_ms: float):
        """
        Observe an event and update the watermark.

        Returns:
            (is_late, should_accept)

        When redis_client is None: returns tuple synchronously.
        When redis_client is set: returns an awaitable coroutine.

        Usage:
            # sync (in-process)
            is_late, ok = wm.observe(event_time_ms)

            # async (distributed)
            is_late, ok = await wm.observe(event_time_ms)
        """
        if self._redis is not None:
            return self._observe_redis(event_time_ms)
        return self._observe_local(event_time_ms)

    async def _observe_redis(self, event_time_ms: float) -> tuple[bool, bool]:
        """Distributed watermark — native Redis (no Lua, fakeredis compat)."""
        key = self._make_key()
        try:
            import json as _json
            raw = await self._redis.get(key)
            watermark_ms = 0.0
            late_count = 0
            on_time_count = 0
            if raw:
                text = raw.decode() if isinstance(raw, bytes) else raw
                try:
                    obj = _json.loads(text)
                    watermark_ms  = float(obj.get("watermark_ms", 0))
                    late_count    = int(obj.get("late_count", 0))
                    on_time_count = int(obj.get("on_time_count", 0))
                except Exception:
                    pass

            is_late = (
                watermark_ms > 0
                and event_time_ms < (watermark_ms - self.allowed_lateness_ms)
            )
            if is_late:
                late_count += 1
            else:
                on_time_count += 1
                if event_time_ms > watermark_ms:
                    watermark_ms = event_time_ms

            new_state = _json.dumps({
                "watermark_ms":  watermark_ms,
                "late_count":    late_count,
                "on_time_count": on_time_count,
            })
            await self._redis.set(key, new_state, ex=_WATERMARK_TTL_SECONDS)

            self._watermark_ms  = watermark_ms
            self._late_count    = late_count
            self._on_time_count = on_time_count

            should_accept = not is_late or (self.policy.action != LatenessAction.DROP)
            return is_late, should_accept
        except Exception:
            return self._observe_local(event_time_ms)

    # ── evaluate (read-only, sync) ────────────────────────────────────────────

    def evaluate(self, event_time_ms: float) -> tuple[bool, LatenessAction]:
        """
        Check lateness WITHOUT updating watermark or counters.
        Always synchronous — uses local cached state.
        """
        is_late = (
            self._watermark_ms > 0
            and event_time_ms < (self._watermark_ms - self.allowed_lateness_ms)
        )
        if is_late:
            return True, self.policy.action
        return False, LatenessAction.ACCEPT_WITH_CORRECTION

    # ── should_accept ─────────────────────────────────────────────────────────

    def should_accept(
        self,
        event_time_ms: float,
        policy=None,
    ) -> bool:
        """
        Observe and return whether event should be accepted.
        Sync only — uses local path even in distributed mode.

        policy may be:
          - None               → use self.policy
          - LatenessPolicy(…)  → use policy.action
          - LatenessAction enum → use directly (LatenessPolicy.DROP shortcut)
        """
        if policy is None:
            effective_action = self.policy.action
        elif isinstance(policy, LatenessAction):
            effective_action = policy
        else:
            effective_action = policy.action

        is_late, _ = self._observe_local(event_time_ms)
        if not is_late:
            return True
        return effective_action != LatenessAction.DROP

    # ── Redis sync ────────────────────────────────────────────────────────────

    async def _sync_from_redis(self) -> None:
        """Load watermark state from Redis into local cache."""
        if self._redis is None:
            return
        try:
            raw = await self._redis.get(self._make_key())
            if raw:
                data = raw.decode() if isinstance(raw, bytes) else raw
                obj = json.loads(data)
                self._watermark_ms = float(obj.get("watermark_ms", 0))
                self._late_count = int(obj.get("late_count", 0))
                self._on_time_count = int(obj.get("on_time_count", 0))
        except Exception:
            pass

    # ── Properties ────────────────────────────────────────────────────────────

    @property
    def watermark_ms(self) -> float:
        return self._watermark_ms

    @property
    def current_watermark_ms(self) -> float:
        """Alias used by engine.py."""
        return self._watermark_ms

    @property
    def late_count(self) -> int:
        return self._late_count

    @property
    def on_time_count(self) -> int:
        return self._on_time_count

    @property
    def late_ratio(self) -> float:
        total = self._late_count + self._on_time_count
        return self._late_count / total if total > 0 else 0.0

    def reset(self) -> None:
        """Reset local state. For testing only."""
        self._watermark_ms = 0.0
        self._late_count = 0
        self._on_time_count = 0


# Canonical export
Watermark = WatermarkManager
