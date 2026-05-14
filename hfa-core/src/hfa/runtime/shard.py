"""IRONCLAD Sprint 9 — shard ownership helpers (worker instance level)."""

from __future__ import annotations

import logging
from typing import Iterable, List

from hfa.config.keys import RedisKey

logger = logging.getLogger("hfa.runtime.shard")


class ShardClaimer:
    """Owns shard leases via Redis keys keyed by `worker_id`.

    Sprint 9 contract:
    - ownership is per worker instance, not per group
    - key: `hfa:cp:shard:owner:{shard}`
    - value: canonical `worker_id`
    """

    OWNER_TTL_SECONDS = 60

    def __init__(self, redis: object, worker_id: str) -> None:
        self._redis = redis
        self._worker_id = worker_id

    async def claim(self, shard: int) -> bool:
        key = RedisKey.cp_shard_owner(shard)
        claimed = await self._redis.set(
            key,
            self._worker_id,
            nx=True,
            ex=self.OWNER_TTL_SECONDS,
        )
        if claimed:
            logger.info("Claimed shard=%s worker_id=%s", shard, self._worker_id)
        return bool(claimed)

    async def renew(self, shards: Iterable[int]) -> None:
        for shard in shards:
            key = RedisKey.cp_shard_owner(shard)
            current = await self._redis.get(key)
            current_text = current.decode() if isinstance(current, bytes) else current
            if current_text == self._worker_id:
                await self._redis.expire(key, self.OWNER_TTL_SECONDS)

    async def release(self, shards: Iterable[int]) -> None:
        for shard in shards:
            key = RedisKey.cp_shard_owner(shard)
            current = await self._redis.get(key)
            current_text = current.decode() if isinstance(current, bytes) else current
            if current_text == self._worker_id:
                await self._redis.delete(key)
                logger.info("Released shard=%s worker_id=%s", shard, self._worker_id)

    async def currently_owned(self, shards: Iterable[int]) -> List[int]:
        owned: List[int] = []
        for shard in shards:
            key = RedisKey.cp_shard_owner(shard)
            current = await self._redis.get(key)
            current_text = current.decode() if isinstance(current, bytes) else current
            if current_text == self._worker_id:
                owned.append(shard)
        return owned
