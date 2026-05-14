"""
hfa-control/src/hfa_control/tenant_queue.py
IRONCLAD Sprint 16 — Per-tenant run queue

Design
------
Each tenant has a Redis ZSET acting as a priority queue.

  hfa:tenant:{tenant_id}:queue   ZSET
    member = run_id
    score  = priority_score  (lower = higher priority)

Priority score formula:
  score = priority * 1e12 + admitted_at_micros

This ensures:
  1. Lower priority number = lower score = higher urgency. priority=1 is dispatched first.
  2. Within the same priority band, FIFO ordering by admission time.

Active tenant tracking:
  hfa:cp:tenant:active   SET  — tenant_ids with non-empty queues
  Maintained atomically alongside queue operations.

TTL policy:
  Queue entries are self-cleaning — dequeued on scheduling.
  The ZSET key expires after QUEUE_IDLE_TTL seconds of inactivity
  (refreshed on every enqueue).

IRONCLAD rules
--------------
* No print() — logging only.
* All key strings from RedisKey.
* Enqueue is idempotent — ZADD NX prevents double-enqueue.
* Dequeue returns None when queue is empty (never raises).
"""

from __future__ import annotations

import logging
import time
from typing import Optional

from hfa.config.keys import RedisKey

logger = logging.getLogger(__name__)

MAX_PRIORITY = 100  # priorities are 1–100; priority=1 → lowest score → ZPOPMIN returns it first (highest urgency)
QUEUE_IDLE_TTL = 86_400  # 24h TTL on idle queues


class TenantQueue:
    """
    Per-tenant run queue backed by Redis ZSET.

    Score = priority * 1e12 + admitted_at_micros
    Lower score = higher urgency.

    Args:
        redis: aioredis.Redis client.
    """

    def __init__(self, redis) -> None:
        self._redis = redis

    # ------------------------------------------------------------------
    # Enqueue
    # ------------------------------------------------------------------

    async def enqueue(
        self,
        tenant_id: str,
        run_id: str,
        priority: int = 5,
        *,
        now: Optional[float] = None,
    ) -> None:
        """
        Add run_id to the tenant's queue.

        Idempotent — NX flag means re-enqueueing the same run_id is a no-op.
        Also adds tenant_id to the active-tenant SET.

        Args:
            tenant_id: Tenant owning this run.
            run_id:    Run identifier (unique).
            priority:  1 (highest) … 100 (lowest). Default 5.
            now:       Override admission timestamp (for testing).
        """
        priority = max(1, min(int(priority), MAX_PRIORITY))
        ts = now if now is not None else time.time()
        ts_micros = int(ts * 1_000_000) % int(1e12)  # micros, bounded
        score = float(priority * int(1e12) + ts_micros)

        key = RedisKey.tenant_queue(tenant_id)
        active_key = RedisKey.tenant_active_set()

        pipe = self._redis.pipeline()
        pipe.zadd(key, {run_id: score}, nx=True)
        pipe.sadd(active_key, tenant_id)
        pipe.expire(key, QUEUE_IDLE_TTL)
        await pipe.execute()

        logger.debug(
            "TenantQueue.enqueue: tenant=%s run=%s priority=%d score=%.0f",
            tenant_id,
            run_id,
            priority,
            score,
        )

    # ------------------------------------------------------------------
    # Dequeue
    # ------------------------------------------------------------------

    async def dequeue(self, tenant_id: str) -> Optional[str]:
        """
        Pop the highest-priority run from the tenant's queue.

        Returns run_id or None if the queue is empty.
        Removes tenant from active-tenant SET when queue becomes empty.
        """
        key = RedisKey.tenant_queue(tenant_id)
        active_key = RedisKey.tenant_active_set()

        # ZPOPMIN atomically removes and returns the lowest-score (highest-priority) entry
        result = await self._redis.zpopmin(key, count=1)
        if not result:
            # Queue is empty — remove from active set
            await self._redis.srem(active_key, tenant_id)
            return None

        run_id_raw, score = result[0]
        run_id = run_id_raw.decode() if isinstance(run_id_raw, bytes) else run_id_raw

        # If queue is now empty, remove from active set
        remaining = await self._redis.zcard(key)
        if remaining == 0:
            await self._redis.srem(active_key, tenant_id)

        logger.debug(
            "TenantQueue.dequeue: tenant=%s run=%s score=%.0f remaining=%d",
            tenant_id,
            run_id,
            score,
            remaining,
        )
        return run_id

    async def remove(self, tenant_id: str, run_id: str) -> bool:
        """
        Remove a specific run_id from the tenant's queue (e.g. on cancel).
        Returns True if removed, False if not found.
        """
        key = RedisKey.tenant_queue(tenant_id)
        removed = await self._redis.zrem(key, run_id)
        if removed:
            remaining = await self._redis.zcard(key)
            if remaining == 0:
                await self._redis.srem(RedisKey.tenant_active_set(), tenant_id)
        return bool(removed)

    # ------------------------------------------------------------------
    # Inspection
    # ------------------------------------------------------------------

    async def depth(self, tenant_id: str) -> int:
        """Return number of queued runs for tenant."""
        return await self._redis.zcard(RedisKey.tenant_queue(tenant_id))

    async def peek(self, tenant_id: str) -> Optional[str]:
        """
        Return the next run_id without removing it.
        Returns None if queue is empty.
        """
        key = RedisKey.tenant_queue(tenant_id)
        result = await self._redis.zrange(key, 0, 0)
        if not result:
            return None
        raw = result[0]
        return raw.decode() if isinstance(raw, bytes) else raw

    async def active_tenants(self) -> list[str]:
        """
        Return list of tenant_ids with non-empty queues.
        Sourced from the active-tenant SET (eventually consistent).
        """
        raw = await self._redis.smembers(RedisKey.tenant_active_set())
        return [r.decode() if isinstance(r, bytes) else r for r in raw]

    async def all_depths(self) -> dict[str, int]:
        """Return {tenant_id: depth} for all active tenants."""
        tenants = await self.active_tenants()
        result: dict[str, int] = {}
        for tid in tenants:
            d = await self.depth(tid)
            if d > 0:
                result[tid] = d
            else:
                # Stale entry — clean up
                await self._redis.srem(RedisKey.tenant_active_set(), tid)
        return result
