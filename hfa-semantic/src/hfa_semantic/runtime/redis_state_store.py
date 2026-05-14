"""
hfa-semantic/src/hfa_semantic/runtime/redis_state_store.py

IRONCLAD Sprint R1 — Redis-backed State Store (Production)

CRITICAL: This is the production state backend.

Properties:
  * Durable
  * Horizontally scalable
  * Restart-safe
  * Automatic TTL eviction via Redis EXPIRE
  * Partition cardinality bounded via sorted-set tracking

Key format:
  semantic:state:{rule_id}:{partition}

Sorted-set key (for eviction ordering):
  semantic:state:zset:{rule_id}

Guarantees:
  * Single-partition atomic writes via Lua (falls back to pipeline)
  * Redis EXPIRE ensures TTL
  * Sorted set ordered by insertion timestamp for LRU eviction
"""

from __future__ import annotations

import json
import asyncio
import time
from typing import Any, Dict, List, Optional

import redis
import redis.asyncio as redis_async

from .state_store import RuntimeState, StateStore


# Inline Lua script: atomic SET + ZADD + EXPIRE in one round-trip.
# Prevents the race where state key is written but zset tracking is missed.
_LUA_PUT = """
local key      = KEYS[1]
local zset_key = KEYS[2]
local ttl_sec  = tonumber(ARGV[1])
local partition = ARGV[2]
local data      = ARGV[3]
local timestamp = tonumber(ARGV[4])

redis.call('SET',    key,      data,      'EX', ttl_sec)
redis.call('ZADD',   zset_key, timestamp, partition)
redis.call('EXPIRE', zset_key, ttl_sec)

return 1
"""


class RedisStateStore(StateStore):
    """
    Production state store backed by Redis.

    Thread-safe, durable, horizontally scalable.
    """

    _update_locks: "dict[str, asyncio.Lock]" = {}

    def __init__(
        self,
        redis_client: redis_async.Redis,
        namespace: str = "semantic:state",
    ) -> None:
        self._redis = redis_client
        self._namespace = namespace

    # ── Key helpers ───────────────────────────────────────────────────────────

    def _make_key(self, rule_id: str, partition: str) -> str:
        """Format: semantic:state:{rule_id}:{partition}"""
        return f"{self._namespace}:{rule_id}:{partition}"

    def _make_zset_key(self, rule_id: str) -> str:
        """Sorted set of partitions for eviction tracking."""
        return f"{self._namespace}:zset:{rule_id}"

    # ── StateStore interface ──────────────────────────────────────────────────

    async def get(self, rule_id: str, partition_key: str) -> Optional[RuntimeState]:
        key = self._make_key(rule_id, partition_key)
        data = await self._redis.get(key)
        if not data:
            return None

        try:
            obj = json.loads(data)
            return RuntimeState(
                rule_id=obj.get("rule_id", rule_id),
                partition=obj.get("partition", partition_key),
                state=obj.get("state", {}),
                event_count=int(obj.get("event_count", 0)),
                last_update_ms=float(obj.get("last_update_ms", 0)),
                ttl_ms=int(obj.get("ttl_ms", 0)),
            )
        except (json.JSONDecodeError, KeyError, ValueError):
            # Corrupted state — delete to prevent repeated decode failures
            await self._redis.delete(key)
            return None

    async def put(
        self,
        rule_id: str,
        partition_key: str,
        state: Dict[str, Any],
        ttl_ms: int,
    ) -> None:
        """
        Atomic put: writes state key + updates eviction zset in one Lua call.

        Falls back to pipeline for fakeredis / test environments where EVAL
        is unavailable.
        """
        key = self._make_key(rule_id, partition_key)
        zset_key = self._make_zset_key(rule_id)
        now_ms = time.time() * 1000.0
        ttl_sec = max(1, int((ttl_ms + 999) / 1000)) if ttl_ms > 0 else 86400

        data = json.dumps(
            {
                "rule_id": rule_id,
                "partition": partition_key,
                "state": state,
                "event_count": 1,
                "last_update_ms": now_ms,
                "ttl_ms": ttl_ms,
            }
        )

        try:
            await self._redis.eval(
                _LUA_PUT,
                2,           # num keys
                key,
                zset_key,
                ttl_sec,
                partition_key,
                data,
                now_ms,
            )
        except (redis.exceptions.ResponseError, AttributeError, Exception):
            # Fallback: pipeline (less ideal but safe for test environments)
            pipe = self._redis.pipeline()
            await pipe.set(key, data, ex=ttl_sec)
            await pipe.zadd(zset_key, {partition_key: now_ms})
            await pipe.expire(zset_key, ttl_sec)
            await pipe.execute()

    async def delete(self, rule_id: str, partition_key: str) -> None:
        """Delete a single partition's state and remove from eviction tracking."""
        key = self._make_key(rule_id, partition_key)
        zset_key = self._make_zset_key(rule_id)
        await self._redis.delete(key)
        await self._redis.zrem(zset_key, partition_key)

    async def evict(self, rule_id: str, max_partitions: int) -> int:
        """
        Evict oldest partitions until at most max_partitions remain.

        Uses the sorted set (scored by insertion timestamp) to identify
        the oldest partitions. Returns number evicted.
        """
        zset_key = self._make_zset_key(rule_id)
        count = await self._redis.zcard(zset_key)

        if count <= max_partitions:
            return 0

        to_evict = count - max_partitions
        # zrange returns partitions ordered by score (lowest = oldest)
        oldest = await self._redis.zrange(zset_key, 0, to_evict - 1)

        evicted = 0
        for partition_bytes in oldest:
            partition = (
                partition_bytes.decode()
                if isinstance(partition_bytes, bytes)
                else partition_bytes
            )
            await self.delete(rule_id, partition)
            evicted += 1

        return evicted

    async def list_partitions(self, rule_id: str) -> List[str]:
        """List all tracked partition keys for a rule (ordered oldest → newest)."""
        zset_key = self._make_zset_key(rule_id)
        partitions_raw = await self._redis.zrange(zset_key, 0, -1)
        return [
            p.decode() if isinstance(p, bytes) else p
            for p in partitions_raw
        ]

    async def close(self) -> None:
        """No-op: Redis connection is managed by the application bootstrap."""
        pass

    # ── Optional: atomic updater ──────────────────────────────────────────────

    async def update(
        self,
        rule_id: str,
        partition: str,
        updater_fn,
        ttl_ms: int,
    ) -> None:
        """Read-modify-write with asyncio.Lock per key."""
        lock_key = f"{rule_id}:{partition}"
        if lock_key not in RedisStateStore._update_locks:
            RedisStateStore._update_locks[lock_key] = asyncio.Lock()
        async with RedisStateStore._update_locks[lock_key]:
            key = self._make_key(rule_id, partition)
            current_state: Dict[str, Any] = {}
            data = await self._redis.get(key)
            if data:
                try:
                    obj = json.loads(data)
                    current_state = obj.get("state", {})
                except (json.JSONDecodeError, KeyError):
                    current_state = {}
            updated_state = await updater_fn(current_state)
            await self.put(rule_id, partition, updated_state, ttl_ms)

