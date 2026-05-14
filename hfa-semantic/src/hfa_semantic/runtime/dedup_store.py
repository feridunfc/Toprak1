"""
hfa-semantic/src/hfa_semantic/runtime/dedup_store.py

Fix: replaced Lua eval with native Redis SET NX (fakeredis compatible).
try_accept() and mark_processed() both use SET key value NX EX ttl.
"""
from __future__ import annotations

import json
import time
from typing import Any, Dict, Optional

_DEFAULT_TTL_MS  = 3_600_000
_DEFAULT_WINDOW_MS = 86_400_000


class DedupStore:
    def __init__(self, redis_client, prefix: str = "hfa:semantic:dedup") -> None:
        self._redis = redis_client
        self._prefix = prefix

    def _key(self, event_id: str) -> str:
        return f"{self._prefix}:{event_id}"

    # ── Primary: atomic try_accept (SET NX) ──────────────────────────────────

    async def try_accept(
        self,
        event_id: str,
        event_time_ms: float,
        ttl_ms: int = _DEFAULT_TTL_MS,
    ) -> bool:
        """
        Atomically claim event_id.  Returns True on first claim, False if duplicate.
        Uses SET NX — safe on both real Redis and fakeredis.
        """
        key     = self._key(event_id)
        ttl_sec = max(1, int((ttl_ms + 999) / 1000))
        payload = json.dumps({
            "event_id":        event_id,
            "event_time_ms":   event_time_ms,
            "processed_at_ms": time.time() * 1000.0,
        })
        # SET key value NX EX ttl  →  "OK" on first write, None on duplicate
        result = await self._redis.set(key, payload, nx=True, ex=ttl_sec)
        return result is not None  # "OK" → True, None → False

    # ── Legacy API ────────────────────────────────────────────────────────────

    async def is_duplicate(self, event_id: str, window_ms: int = _DEFAULT_WINDOW_MS) -> bool:
        """Read-only check.  Does NOT mark as processed."""
        exists = await self._redis.exists(self._key(event_id))
        return bool(exists)

    async def mark_processed(
        self,
        event_id: str,
        event_time_ms: float | None = None,
        ttl_ms: int = _DEFAULT_TTL_MS,
    ) -> bool:
        """
        Mark event as processed (SET NX).
        Returns True on first write, False if already marked.
        """
        key     = self._key(event_id)
        ttl_sec = max(1, int((ttl_ms + 999) / 1000))
        payload = json.dumps({
            "event_id":        event_id,
            "event_time_ms":   event_time_ms if event_time_ms is not None else time.time() * 1000.0,
            "processed_at_ms": time.time() * 1000.0,
        })
        result = await self._redis.set(key, payload, nx=True, ex=ttl_sec)
        return result is not None

    async def get_entry(self, event_id: str) -> Optional[Dict[str, Any]]:
        raw = await self._redis.get(self._key(event_id))
        if raw is None:
            return None
        try:
            data = raw.decode() if isinstance(raw, bytes) else raw
            return json.loads(data)
        except (json.JSONDecodeError, ValueError):
            return None

    async def close(self) -> None:
        pass
