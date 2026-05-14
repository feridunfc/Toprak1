"""
hfa-semantic/src/hfa_semantic/memory/semantic_memory_v2.py

Sprint 10.1 + 10.5 — Idempotent Memory + Read Consistency

Sprint 10.1: idempotent writes
  * append() returns True for new write, False for duplicate skip
  * Redis path uses EXISTS check before write (atomic via Lua)
  * Local path deduplicates by event_id (same behavior)
  * Log: memory_write_skipped_duplicate / memory_write_new

Sprint 10.5: read consistency
  * _redis_weighted() removes stale index entries (key missing from Redis)
  * Log: memory_stale_cleanup

Sprint 9.x: Redis-backed persistent memory, exponential decay weighting.

Backends:
  * redis_client=None  → in-memory dict (DEV/TEST ONLY — not persistent)
  * redis_client=...   → Redis-backed (PRODUCTION — survives pod restarts)

Public API:
  await append(outcome) -> bool   (True=written, False=duplicate skipped)
  await weighted(now_ms) -> list[WeightedOutcome]
  await size() -> int
  all() -> list[ValidatedOutcome]   (local fallback only)
  .size property (sync, local fallback — test backward compat)
  .utilization property (sync, local fallback — test backward compat)
"""

from __future__ import annotations

import logging
import math
from dataclasses import dataclass
from typing import Optional

from hfa_semantic.api.models import ValidatedOutcome

logger = logging.getLogger("hfa.semantic_memory")

_MEMORY_KEY_PREFIX = "semantic:memory:outcome:"
_MEMORY_INDEX_KEY  = "semantic:memory:index"   # sorted set: member=event_id, score=ts_ms
_DEDUP_KEY_PREFIX  = "semantic:memory:seen:"   # dedup guard (short TTL)
_DEFAULT_TTL_SECONDS = 86400 * 30              # 30 days
_DEDUP_TTL_SECONDS   = 3600                    # 1 hour dedup window

# ── Atomic idempotent write + trim (Sprint 10.1) ─────────────────────────────
# Returns 0 if duplicate (exists already), 1 if written new.
_IDEMPOTENT_APPEND_LUA = """
-- KEYS[1] = index key
-- KEYS[2] = dedup key
-- ARGV[1] = item key       (semantic:memory:outcome:{event_id})
-- ARGV[2] = event_id
-- ARGV[3] = payload json
-- ARGV[4] = ttl_seconds    (item TTL)
-- ARGV[5] = timestamp_ms
-- ARGV[6] = max_items
-- ARGV[7] = key_prefix
-- ARGV[8] = dedup_ttl      (dedup guard TTL)

-- Idempotency check: if item key already exists, skip
if redis.call('EXISTS', ARGV[1]) == 1 then
  return 0
end

redis.call('SET',  ARGV[1], ARGV[3], 'EX', ARGV[4])
redis.call('ZADD', KEYS[1], ARGV[5], ARGV[2])
redis.call('SET',  KEYS[2], '1', 'EX', ARGV[8])

local card = redis.call('ZCARD', KEYS[1])
local max_items = tonumber(ARGV[6])

if card > max_items then
  local overflow = card - max_items
  local victims = redis.call('ZRANGE', KEYS[1], 0, overflow - 1)
  if #victims > 0 then
    for _, victim in ipairs(victims) do
      redis.call('DEL', ARGV[7] .. victim)
    end
    redis.call('ZREM', KEYS[1], unpack(victims))
  end
end

return 1
"""


@dataclass(slots=True, frozen=True)
class WeightedOutcome:
    outcome:             ValidatedOutcome
    age_ms:              int
    decay_weight:        float
    weighted_confidence: float


class SemanticMemoryV2:
    """
    Bounded semantic memory with exponential time-decay weighting.

    append() is idempotent: same event_id written twice → second write is a no-op.
    Returns True for new write, False for duplicate skip.
    """

    def __init__(
        self,
        max_items:     int = 10_000,
        half_life_ms:  int = 86_400_000,
        redis_client          = None,
        ttl_seconds:   int = _DEFAULT_TTL_SECONDS,
        dedup_ttl_seconds: int = _DEDUP_TTL_SECONDS,
    ) -> None:
        self._max_items  = max_items
        self._half_life  = half_life_ms
        self._redis      = redis_client
        self._ttl        = ttl_seconds
        self._dedup_ttl  = dedup_ttl_seconds

        # in-memory fallback (dev/test only)
        self._items: dict[str, ValidatedOutcome] = {}

    # ── Public API ────────────────────────────────────────────────────────────

    async def append(self, outcome: ValidatedOutcome) -> bool:
        """
        Persist outcome. Idempotent: duplicate event_id → skip, return False.

        Returns:
            True  — new outcome written
            False — duplicate, skipped (memory_write_skipped_duplicate)
        """
        if self._redis is not None:
            written = await self._redis_append(outcome)
        else:
            written = self._local_append(outcome)

        if written:
            logger.debug(
                "memory_write_new event_id=%s", outcome.event_id
            )
        else:
            logger.debug(
                "memory_write_skipped_duplicate event_id=%s", outcome.event_id
            )
        return written

    async def weighted(self, now_ms: int) -> list[WeightedOutcome]:
        """Return all outcomes with exponential decay weights applied."""
        if self._redis is not None:
            return await self._redis_weighted(now_ms)
        return self._local_weighted(now_ms)

    async def size(self) -> int:
        if self._redis is not None:
            return int(await self._redis.zcard(_MEMORY_INDEX_KEY))
        return len(self._items)

    def all(self) -> list[ValidatedOutcome]:
        """Return all outcomes (local fallback only — test helper)."""
        return sorted(self._items.values(), key=lambda x: x.timestamp_ms, reverse=True)

    # ── Sync properties (backward compat for tests + OutcomeWriter) ───────────

    @property
    def size(self) -> int:  # type: ignore[override]
        """Sync size (local path only). Use await size() in async contexts."""
        return len(self._items)

    @property
    def utilization(self) -> float:
        return len(self._items) / self._max_items if self._max_items > 0 else 0.0

    # ── Redis path ────────────────────────────────────────────────────────────

    async def _redis_append(self, outcome: ValidatedOutcome) -> bool:
        event_id = outcome.event_id
        item_key  = f"{_MEMORY_KEY_PREFIX}{event_id}"
        payload   = outcome.model_dump_json()

        # SET NX — atomic first-write-wins (fakeredis compatible, no eval)
        written = await self._redis.set(item_key, payload, nx=True, ex=self._ttl)
        if not written:
            return False  # duplicate

        # Update sorted-set index
        pipe = self._redis.pipeline()
        pipe.zadd(_MEMORY_INDEX_KEY, {event_id: outcome.timestamp_ms})
        pipe.expire(_MEMORY_INDEX_KEY, self._ttl)
        await pipe.execute()

        # Trim overflow (best-effort, non-atomic — acceptable for bounded memory)
        card = await self._redis.zcard(_MEMORY_INDEX_KEY)
        if card > self._max_items:
            overflow = card - self._max_items
            victims_raw = await self._redis.zrange(_MEMORY_INDEX_KEY, 0, overflow - 1)
            if victims_raw:
                victims = [v.decode() if isinstance(v, bytes) else v for v in victims_raw]
                pipe2 = self._redis.pipeline()
                for v in victims:
                    pipe2.delete(f"{_MEMORY_KEY_PREFIX}{v}")
                pipe2.zrem(_MEMORY_INDEX_KEY, *victims)
                await pipe2.execute()

        return True

    async def _redis_weighted(self, now_ms: int) -> list[WeightedOutcome]:
        event_ids_raw = await self._redis.zrevrange(
            _MEMORY_INDEX_KEY, 0, self._max_items - 1
        )
        if not event_ids_raw:
            return []

        event_ids = [
            eid.decode("utf-8") if isinstance(eid, bytes) else str(eid)
            for eid in event_ids_raw
        ]
        keys = [f"{_MEMORY_KEY_PREFIX}{eid}" for eid in event_ids]
        raw_items = await self._redis.mget(keys)

        results: list[WeightedOutcome] = []
        stale_ids: list[str] = []

        for event_id, raw in zip(event_ids, raw_items):
            if raw is None:
                stale_ids.append(event_id)
                continue
            text = raw.decode("utf-8") if isinstance(raw, bytes) else raw
            try:
                outcome = ValidatedOutcome.model_validate_json(text)
            except Exception:
                stale_ids.append(event_id)
                continue
            results.append(self._to_weighted(outcome, now_ms))

        # Sprint 10.5: clean up stale index entries
        if stale_ids:
            logger.warning(
                "memory_stale_cleanup count=%d ids=%s",
                len(stale_ids), stale_ids[:5],
            )
            pipe = self._redis.pipeline()
            for eid in stale_ids:
                pipe.zrem(_MEMORY_INDEX_KEY, eid)
                pipe.delete(f"{_MEMORY_KEY_PREFIX}{eid}")
            await pipe.execute()

        return results

    # ── Local path (in-memory, dev/test) ─────────────────────────────────────

    def _local_append(self, outcome: ValidatedOutcome) -> bool:
        """Idempotent local write: skip if event_id already present."""
        if outcome.event_id in self._items:
            return False   # duplicate
        self._items[outcome.event_id] = outcome
        if len(self._items) > self._max_items:
            oldest_key = min(self._items, key=lambda k: self._items[k].timestamp_ms)
            self._items.pop(oldest_key, None)
        return True

    def _local_weighted(self, now_ms: int) -> list[WeightedOutcome]:
        items = sorted(self._items.values(), key=lambda x: x.timestamp_ms, reverse=True)
        return [self._to_weighted(item, now_ms) for item in items[: self._max_items]]

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _to_weighted(self, outcome: ValidatedOutcome, now_ms: int) -> WeightedOutcome:
        age_ms = max(0, now_ms - outcome.timestamp_ms)
        decay  = self._decay(age_ms)
        return WeightedOutcome(
            outcome=outcome,
            age_ms=age_ms,
            decay_weight=decay,
            weighted_confidence=outcome.confidence * decay,
        )

    def _decay(self, age_ms: int) -> float:
        if age_ms <= 0:
            return 1.0
        return math.exp(-math.log(2) * age_ms / self._half_life)
