"""
hfa-core/src/hfa/obs/graph_store.py
IRONCLAD Sprint 10 — GraphStore abstraction + RedisGraphStore

Key schema
----------
  hfa:graph:snap:{run_id}    STRING   full JSON snapshot        TTL=24h
  hfa:graph:patch:{run_id}   LIST     append-only patch log     TTL=24h

Each patch LIST element is a JSON string:
  { "seq": <int>, "op": <str>, "node_id": <str>, "ts": <float>, "data": <dict> }
  data["cost_cents"] is always int — no float USD.

IRONCLAD rules
--------------
* No print() — logging only.
* close() not needed (no background tasks).
* All load_* methods return None/[] and log on error — never raise.
"""

from __future__ import annotations

import abc
import json
import logging
from typing import Any

from hfa.config.keys import RedisKey, TTL

logger = logging.getLogger(__name__)


class GraphStore(abc.ABC):
    @abc.abstractmethod
    async def save_snapshot(self, run_id: str, graph_json: str) -> None:
        """Persist full JSON snapshot (overwrite-safe, idempotent)."""

    @abc.abstractmethod
    async def load_snapshot(self, run_id: str) -> str | None:
        """Return raw JSON string, or None if not found."""

    @abc.abstractmethod
    async def append_patch(self, run_id: str, patch: dict[str, Any]) -> None:
        """
        Append a graph mutation event (append-only).
        patch must include: seq (int), op (str), node_id (str), ts (float), data (dict).
        data["cost_cents"] must be int — no float USD.
        """

    @abc.abstractmethod
    async def load_patches(
        self, run_id: str, after_seq: int = 0
    ) -> list[dict[str, Any]]:
        """Return patches with seq >= after_seq.  Returns [] on error."""

    @abc.abstractmethod
    async def delete(self, run_id: str) -> None:
        """Remove all data for run_id (GDPR / manual TTL eviction)."""

    async def next_seq(self, run_id: str) -> int:
        """Return the next monotonic patch sequence number (LLEN-based)."""
        raise NotImplementedError


class RedisGraphStore(GraphStore):
    # Class-level aliases kept for any subclasses; use TTL constants directly.
    SNAP_TTL = TTL.RUN_META
    PATCH_TTL = TTL.RUN_META

    def __init__(self, redis) -> None:
        self._redis = redis

    # ------------------------------------------------------------------

    async def save_snapshot(self, run_id: str, graph_json: str) -> None:
        try:
            await self._redis.set(
                RedisKey.graph_snapshot(run_id), graph_json, ex=TTL.RUN_META
            )
        except Exception as exc:
            logger.error("GraphStore.save_snapshot error run=%s: %s", run_id, exc)

    async def load_snapshot(self, run_id: str) -> str | None:
        try:
            raw = await self._redis.get(RedisKey.graph_snapshot(run_id))
            if raw is None:
                return None
            return raw.decode() if isinstance(raw, bytes) else raw
        except Exception as exc:
            logger.error("GraphStore.load_snapshot error run=%s: %s", run_id, exc)
            return None

    async def append_patch(self, run_id: str, patch: dict[str, Any]) -> None:
        try:
            key = RedisKey.graph_patch(run_id)
            await self._redis.rpush(key, json.dumps(patch, default=str))
            await self._redis.expire(key, TTL.RUN_META)
        except Exception as exc:
            logger.error("GraphStore.append_patch error run=%s: %s", run_id, exc)

    async def load_patches(
        self, run_id: str, after_seq: int = 0
    ) -> list[dict[str, Any]]:
        try:
            raw_list = await self._redis.lrange(RedisKey.graph_patch(run_id), 0, -1)
            out: list[dict[str, Any]] = []
            for i, raw in enumerate(raw_list):
                try:
                    patch = json.loads(raw.decode() if isinstance(raw, bytes) else raw)
                    if patch.get("seq", 0) >= after_seq:
                        out.append(patch)
                except json.JSONDecodeError:
                    logger.warning(
                        "GraphStore.load_patches JSON error run=%s idx=%d", run_id, i
                    )
            return out
        except Exception as exc:
            logger.error("GraphStore.load_patches error run=%s: %s", run_id, exc)
            return []

    async def delete(self, run_id: str) -> None:
        try:
            await self._redis.delete(
                RedisKey.graph_snapshot(run_id),
                RedisKey.graph_patch(run_id),
            )
        except Exception as exc:
            logger.error("GraphStore.delete error run=%s: %s", run_id, exc)

    async def next_seq(self, run_id: str) -> int:
        try:
            return await self._redis.llen(RedisKey.graph_patch(run_id))
        except Exception:
            return 0
