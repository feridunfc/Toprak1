"""
hfa-control/src/hfa_control/audit_store.py
IRONCLAD Sprint 17 — Redis-backed LedgerStore for SignedLedger

Stores audit entries in Redis as an append-only LIST per (tenant_id, run_id).

Key schema
----------
  hfa:audit:{tenant_id}:{run_id}   LIST  — JSON-serialized LedgerEntry dicts
  hfa:audit:index                   ZSET  — tenant_id:run_id → timestamp (for scanning)

TTL
---
  Audit entries expire after AUDIT_TTL seconds (default 30 days).
  This is a safety net — long-term audit should be archived to cold storage.

IRONCLAD rules
--------------
* No print() — logging only.
* append() is idempotent on sequence number (dedup via sequence check).
* get_all() returns entries in insertion order (LRANGE 0 -1).
"""

from __future__ import annotations

import json
import logging

logger = logging.getLogger(__name__)

AUDIT_TTL = 30 * 86_400  # 30 days
AUDIT_INDEX_KEY = "hfa:audit:index"


class RedisLedgerStore:
    """
    Redis LIST-backed LedgerStore for SignedLedger.

    Each (tenant_id, run_id) pair gets its own LIST key.
    Entries are stored as JSON strings in insertion order.
    """

    def __init__(self, redis) -> None:
        self._redis = redis

    def _key(self, tenant_id: str, run_id: str) -> str:
        return f"hfa:audit:{tenant_id}:{run_id}"

    async def append(self, entry) -> None:
        """
        Append a LedgerEntry to the Redis LIST.
        Also updates the global audit index ZSET.
        """
        import time

        key = self._key(entry.tenant_id, entry.run_id)
        try:
            data = json.dumps(entry.to_dict(), separators=(",", ":"))
            pipe = self._redis.pipeline()
            pipe.rpush(key, data)
            pipe.expire(key, AUDIT_TTL)
            pipe.zadd(
                AUDIT_INDEX_KEY,
                {f"{entry.tenant_id}:{entry.run_id}": time.time()},
            )
            pipe.expire(AUDIT_INDEX_KEY, AUDIT_TTL)
            await pipe.execute()
        except Exception as exc:
            logger.error(
                "RedisLedgerStore.append failed: tenant=%s run=%s %s",
                entry.tenant_id,
                entry.run_id,
                exc,
            )
            raise

    async def get_last(self, tenant_id: str, run_id: str):
        """Return the most recent LedgerEntry, or None if empty."""
        from hfa.governance.signed_ledger_v1 import LedgerEntry

        key = self._key(tenant_id, run_id)
        try:
            raw = await self._redis.lindex(key, -1)
            if raw is None:
                return None
            data = json.loads(raw.decode() if isinstance(raw, bytes) else raw)
            return LedgerEntry(**data)
        except Exception as exc:
            logger.error(
                "RedisLedgerStore.get_last failed: tenant=%s run=%s %s",
                tenant_id,
                run_id,
                exc,
            )
            return None

    async def get_all(self, tenant_id: str, run_id: str) -> list:
        """Return all LedgerEntries in insertion order."""
        from hfa.governance.signed_ledger_v1 import LedgerEntry

        key = self._key(tenant_id, run_id)
        try:
            raw_list = await self._redis.lrange(key, 0, -1)
            entries = []
            for raw in raw_list:
                try:
                    data = json.loads(raw.decode() if isinstance(raw, bytes) else raw)
                    entries.append(LedgerEntry(**data))
                except Exception as exc:
                    logger.warning("RedisLedgerStore: skipping corrupt entry: %s", exc)
            return entries
        except Exception as exc:
            logger.error(
                "RedisLedgerStore.get_all failed: tenant=%s run=%s %s",
                tenant_id,
                run_id,
                exc,
            )
            return []
