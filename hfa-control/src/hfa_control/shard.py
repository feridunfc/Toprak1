"""
hfa-control/src/hfa_control/shard.py
IRONCLAD Sprint 10 — Shard Ownership Manager

Workers claim shard groups via Redis SET NX.
Control Plane reads the map when making placement decisions.
A background monitor detects orphaned shards (owner TTL expired).

Redis key schema
----------------
  hfa:cp:shard:owner:{shard}   STRING  worker_group  TTL=OWNER_TTL
  hfa:cp:shard:owners          HASH    shard -> worker_group  (no TTL)

IRONCLAD rules
--------------
* No print() — logging only.
* No asyncio.get_event_loop() — get_running_loop().
* close() always safe.
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
from typing import Dict, List, Optional

from hfa.config.keys import RedisKey
from hfa_control.exceptions import ShardOwnershipError

logger = logging.getLogger(__name__)

OWNER_TTL = 60  # seconds; worker must publish heartbeat to renew
MONITOR_INTERVAL = 15  # seconds


class ShardOwnershipManager:
    def __init__(self, redis, config) -> None:
        self._redis = redis
        self._config = config
        self._task: Optional[asyncio.Task] = None

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> None:
        loop = asyncio.get_running_loop()
        self._task = loop.create_task(self._ownership_monitor(), name="shard.monitor")
        logger.info("ShardOwnershipManager started")

    async def close(self) -> None:
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info("ShardOwnershipManager closed")

    # ------------------------------------------------------------------
    # Placement interface
    # ------------------------------------------------------------------

    async def shard_for_group(self, worker_group: str, run_id: str) -> int:
        """
        Deterministically select a shard owned by worker_group for run_id.
        Same run_id always maps to the same shard within a worker_group
        (hash-consistent assignment).
        """
        owned = await self.shards_for_group(worker_group)
        if not owned:
            raise ShardOwnershipError(
                f"No shards owned by worker_group={worker_group!r}"
            )
        idx = int(hashlib.sha1(run_id.encode()).hexdigest(), 16) % len(owned)
        return owned[idx]

    async def shards_for_group(self, worker_group: str) -> List[int]:
        """Return sorted list of shard IDs owned by worker_group."""
        try:
            owners = await self._redis.hgetall(RedisKey.cp_shard_owners())
            return sorted(
                int(shard.decode() if isinstance(shard, bytes) else shard)
                for shard, group in owners.items()
                if (group.decode() if isinstance(group, bytes) else group)
                == worker_group
            )
        except Exception as exc:
            logger.error("ShardOwnershipManager.shards_for_group error: %s", exc)
            return []

    async def all_owners(self) -> Dict[int, str]:
        """Return {shard: worker_group} map."""
        try:
            raw = await self._redis.hgetall(RedisKey.cp_shard_owners())
            return {
                int(k.decode() if isinstance(k, bytes) else k): (
                    v.decode() if isinstance(v, bytes) else v
                )
                for k, v in raw.items()
            }
        except Exception as exc:
            logger.error("ShardOwnershipManager.all_owners error: %s", exc)
            return {}

    # ------------------------------------------------------------------
    # Claim / renew (called by worker at startup and on heartbeat)
    # ------------------------------------------------------------------

    async def claim_shard(self, shard: int, worker_group: str) -> bool:
        """
        Atomically claim shard for worker_group (SET NX EX).
        Returns True if claim succeeded, False if already owned by another group.
        """
        key = RedisKey.cp_shard_owner(shard)
        ok = await self._redis.set(key, worker_group, nx=True, ex=OWNER_TTL)
        if ok:
            await self._redis.hset(RedisKey.cp_shard_owners(), shard, worker_group)
            logger.info("Shard claimed: shard=%d group=%s", shard, worker_group)
        return bool(ok)

    async def renew_shard(self, shard: int, worker_group: str) -> bool:
        """
        Extend TTL for an owned shard.
        Returns False if the shard is no longer owned by this group.
        """
        key = RedisKey.cp_shard_owner(shard)
        current = await self._redis.get(key)
        if (
            current
            and (current.decode() if isinstance(current, bytes) else current)
            == worker_group
        ):
            await self._redis.expire(key, OWNER_TTL)
            return True
        logger.warning(
            "Shard renew failed: shard=%d requested_group=%s current_owner=%s",
            shard,
            worker_group,
            current.decode() if current else "none",
        )
        return False

    # ------------------------------------------------------------------
    # Background monitor — detects orphaned shards
    # ------------------------------------------------------------------

    async def _ownership_monitor(self) -> None:
        while True:
            try:
                await asyncio.sleep(MONITOR_INTERVAL)
                await self._check_orphans()
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.error("ShardOwnershipManager._ownership_monitor error: %s", exc)

    async def _check_orphans(self) -> None:
        """
        Scan shard owners map. For each shard, verify the
        owner key still exists (TTL alive).
        If not → orphaned shard: remove from owners map and log.
        """
        try:
            owners = await self._redis.hgetall(RedisKey.cp_shard_owners())
            for shard_b, group_b in owners.items():
                shard = int(shard_b.decode() if isinstance(shard_b, bytes) else shard_b)
                group = group_b.decode() if isinstance(group_b, bytes) else group_b
                alive = await self._redis.exists(RedisKey.cp_shard_owner(shard))
                if not alive:
                    await self._redis.hdel(RedisKey.cp_shard_owners(), shard)
                    logger.warning(
                        "Shard orphaned: shard=%d (was %s) — removed from map",
                        shard,
                        group,
                    )
        except Exception as exc:
            logger.error("ShardOwnershipManager._check_orphans error: %s", exc)
