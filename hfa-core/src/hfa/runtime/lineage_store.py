"""
hfa-core/src/hfa/runtime/lineage_store.py

IRONCLAD Sprint 1 — Lineage Store (External Storage Abstraction)

TIERED STORAGE LAYER: Lineage → Pluggable external backend

Provenance / lineage data (produced-output records, consumer edges,
DAG edge graph) must eventually leave Redis because:
  * It is write-once, append-only — ideal for cold storage
  * TTL-based eviction silently destroys audit trails
  * Redis memory is too valuable for historical lineage

Sprint 1 target: introduce a backend abstraction WITHOUT changing the
public LineageStore API or breaking any existing caller.

LineageBackend (new ABC)
    Storage interface for lineage records.
    Implementations: RedisLineageBackend (default), InMemoryLineageBackend (tests).
    Future: S3LineageBackend, PostgresLineageBackend, etc.

LineageStore (unchanged public API)
    Constructor gains an optional `backend` parameter.
    If not provided, builds a RedisLineageBackend from redis_client
    — identical to current behavior.
    All existing method signatures and return types are unchanged.
"""

from __future__ import annotations

import inspect
import json
import time
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional

from hfa.runtime.lineage_config import get_lineage_ttl_seconds, is_lineage_enabled


async def _maybe_await(result):
    if inspect.isawaitable(result):
        return await result
    return result


# ── Redis key helpers (unchanged) ─────────────────────────────────────────────

def _produced_key(run_id: str, task_id: str) -> str:
    return f"hfa:lineage:run:{run_id}:task:{task_id}:produced"


def _consumers_key(run_id: str, task_id: str) -> str:
    return f"hfa:lineage:run:{run_id}:task:{task_id}:consumers"


def _edges_key(run_id: str) -> str:
    return f"hfa:lineage:run:{run_id}:edges"


# ── LineageBackend — Abstract Interface ───────────────────────────────────────

class LineageBackend(ABC):
    """
    Storage backend interface for lineage records.

    Semantics:
      write_produced(key, value, ttl_seconds) → bool (True = newly written)
      write_consumer(key, field, value, ttl_seconds) → bool (True = newly written)
      write_edge(key, field, value, ttl_seconds) → bool (True = newly written)
      read_produced(key) → JSON string | None
      read_consumers(key) → {field: JSON string}
      read_edges(key) → {field: JSON string}

    The key/field/value convention mirrors the existing Redis HSET/SET
    pattern so that RedisLineageBackend is a thin wrapper with no logic
    duplication.

    Contract:
      * write_produced() must be idempotent (NX semantics — first write wins).
      * write_consumer() and write_edge() must be idempotent per field.
      * TTL is best-effort — backends that do not support TTL may ignore it.
      * All methods are async.
    """

    @abstractmethod
    async def write_produced(
        self, key: str, value: str, ttl_seconds: int
    ) -> bool:
        """Write a produced-output record (NX). Returns True if newly created."""
        raise NotImplementedError

    @abstractmethod
    async def write_consumer(
        self, key: str, field: str, value: str, ttl_seconds: int
    ) -> bool:
        """Write a consumer record (HSETNX). Returns True if newly created."""
        raise NotImplementedError

    @abstractmethod
    async def write_edge(
        self, key: str, field: str, value: str, ttl_seconds: int
    ) -> bool:
        """Write a DAG edge record (HSETNX). Returns True if newly created."""
        raise NotImplementedError

    @abstractmethod
    async def read_produced(self, key: str) -> Optional[str]:
        """Read a produced-output record. Returns JSON string or None."""
        raise NotImplementedError

    @abstractmethod
    async def read_consumers(self, key: str) -> Dict[str, str]:
        """Read all consumer records for a key. Returns {field: JSON string}."""
        raise NotImplementedError

    @abstractmethod
    async def read_edges(self, key: str) -> Dict[str, str]:
        """Read all edge records for a key. Returns {field: JSON string}."""
        raise NotImplementedError


# ── RedisLineageBackend — Default Implementation ──────────────────────────────

class RedisLineageBackend(LineageBackend):
    """
    Redis-backed lineage backend. Default production implementation.

    Mirrors the key/TTL patterns used in the original LineageStore
    exactly — no behavioral change.
    """

    def __init__(self, redis_client: Any) -> None:
        self._redis = redis_client

    async def write_produced(self, key: str, value: str, ttl_seconds: int) -> bool:
        ok = await _maybe_await(
            self._redis.set(key, value, nx=True, ex=ttl_seconds)
        )
        return bool(ok)

    async def write_consumer(
        self, key: str, field: str, value: str, ttl_seconds: int
    ) -> bool:
        created = await _maybe_await(self._redis.hsetnx(key, field, value))
        await _maybe_await(self._redis.expire(key, ttl_seconds))
        return bool(created)

    async def write_edge(
        self, key: str, field: str, value: str, ttl_seconds: int
    ) -> bool:
        created = await _maybe_await(self._redis.hsetnx(key, field, value))
        await _maybe_await(self._redis.expire(key, ttl_seconds))
        return bool(created)

    async def read_produced(self, key: str) -> Optional[str]:
        raw = await _maybe_await(self._redis.get(key))
        if raw is None:
            return None
        return raw.decode("utf-8") if isinstance(raw, bytes) else raw

    async def read_consumers(self, key: str) -> Dict[str, str]:
        raw = await _maybe_await(self._redis.hgetall(key))
        return {
            (k.decode("utf-8") if isinstance(k, bytes) else k): (
                v.decode("utf-8") if isinstance(v, bytes) else v
            )
            for k, v in raw.items()
        }

    async def read_edges(self, key: str) -> Dict[str, str]:
        raw = await _maybe_await(self._redis.hgetall(key))
        return {
            (k.decode("utf-8") if isinstance(k, bytes) else k): (
                v.decode("utf-8") if isinstance(v, bytes) else v
            )
            for k, v in raw.items()
        }


# ── InMemoryLineageBackend — Tests / Unit Isolation (new, Sprint 1) ───────────

class InMemoryLineageBackend(LineageBackend):
    """
    In-memory lineage backend. For tests and unit isolation.

    No external dependencies. NX / HSETNX semantics are honoured.
    TTL is ignored (data persists until the instance is GC'd or clear() called).
    """

    def __init__(self) -> None:
        # str keys → str values (for produced records)
        self._strings: Dict[str, str] = {}
        # str keys → {field: str} (for consumer + edge hashes)
        self._hashes: Dict[str, Dict[str, str]] = {}

    async def write_produced(self, key: str, value: str, ttl_seconds: int) -> bool:
        if key in self._strings:
            return False  # NX: first write wins
        self._strings[key] = value
        return True

    async def write_consumer(
        self, key: str, field: str, value: str, ttl_seconds: int
    ) -> bool:
        if key not in self._hashes:
            self._hashes[key] = {}
        if field in self._hashes[key]:
            return False  # HSETNX: first write wins
        self._hashes[key][field] = value
        return True

    async def write_edge(
        self, key: str, field: str, value: str, ttl_seconds: int
    ) -> bool:
        return await self.write_consumer(key, field, value, ttl_seconds)

    async def read_produced(self, key: str) -> Optional[str]:
        return self._strings.get(key)

    async def read_consumers(self, key: str) -> Dict[str, str]:
        return dict(self._hashes.get(key, {}))

    async def read_edges(self, key: str) -> Dict[str, str]:
        return dict(self._hashes.get(key, {}))

    def clear(self) -> None:
        """Reset all state. Useful in test teardown."""
        self._strings.clear()
        self._hashes.clear()


# ── LineageStore — Public API (unchanged) ─────────────────────────────────────

class LineageStore:
    """
    Provenance / lineage tracker for run executions.

    All public method signatures are 100% backward-compatible.

    New (Sprint 1): optional `backend` constructor parameter.
    If not provided, a RedisLineageBackend is built from redis_client —
    identical to current behavior. Existing callers that pass only
    redis_client are unaffected.

    To use a custom backend:
        store = LineageStore(redis_client=None, backend=InMemoryLineageBackend())
    """

    def __init__(
        self,
        redis_client: Any,
        # New (Sprint 1): inject a pre-built backend.
        # If None, a RedisLineageBackend is built from redis_client.
        backend: LineageBackend | None = None,
    ) -> None:
        self._redis = redis_client  # kept for any direct redis access (none currently)

        if backend is not None:
            self._backend: LineageBackend = backend
        else:
            self._backend = RedisLineageBackend(redis_client)

    # ── Public methods (unchanged signatures) ─────────────────────────────────

    async def record_produced_output(
        self,
        *,
        run_id: str,
        task_id: str,
        output_ref: str | None,
        payload_mode: str,
        payload_size: int,
        checksum: str,
        payload_type: str,
        producer_worker_id: str,
        producer_attempt: int,
    ) -> bool:
        if not is_lineage_enabled():
            return False
        key = _produced_key(run_id, task_id)
        value = json.dumps({
            "run_id": run_id,
            "task_id": task_id,
            "output_ref": output_ref,
            "payload_mode": payload_mode,
            "payload_size": payload_size,
            "checksum": checksum,
            "payload_type": payload_type,
            "producer_worker_id": producer_worker_id,
            "producer_attempt": producer_attempt,
            "produced_at_ms": int(time.time() * 1000),
        })
        return await self._backend.write_produced(key, value, get_lineage_ttl_seconds())

    async def record_consumed_input(
        self,
        *,
        run_id: str,
        parent_task_id: str,
        consumer_task_id: str,
        consumed_output_ref: str | None,
        checksum: str,
        consumer_worker_id: str,
        consumer_attempt: int,
    ) -> bool:
        if not is_lineage_enabled():
            return False
        key = _consumers_key(run_id, parent_task_id)
        field = f"{consumer_task_id}:{consumer_attempt}"
        value = json.dumps({
            "run_id": run_id,
            "parent_task_id": parent_task_id,
            "consumer_task_id": consumer_task_id,
            "consumed_output_ref": consumed_output_ref,
            "checksum": checksum,
            "consumer_worker_id": consumer_worker_id,
            "consumer_attempt": consumer_attempt,
            "consumed_at_ms": int(time.time() * 1000),
        })
        return await self._backend.write_consumer(key, field, value, get_lineage_ttl_seconds())

    async def record_lineage_edge(
        self,
        *,
        run_id: str,
        parent_task_id: str,
        child_task_id: str,
    ) -> bool:
        if not is_lineage_enabled():
            return False
        key = _edges_key(run_id)
        field = f"{parent_task_id}->{child_task_id}"
        value = json.dumps({
            "run_id": run_id,
            "parent_task_id": parent_task_id,
            "child_task_id": child_task_id,
            "recorded_at_ms": int(time.time() * 1000),
        })
        return await self._backend.write_edge(key, field, value, get_lineage_ttl_seconds())

    async def get_produced_output(
        self, *, run_id: str, task_id: str
    ) -> Optional[Dict[str, Any]]:
        raw = await self._backend.read_produced(_produced_key(run_id, task_id))
        if raw is None:
            return None
        return json.loads(raw)

    async def get_consumers(
        self, *, run_id: str, task_id: str
    ) -> List[Dict[str, Any]]:
        raw = await self._backend.read_consumers(_consumers_key(run_id, task_id))
        items = [json.loads(v) for v in raw.values()]
        items.sort(key=lambda x: (x["consumer_task_id"], x["consumer_attempt"]))
        return items

    async def get_run_edges(self, *, run_id: str) -> List[Dict[str, Any]]:
        raw = await self._backend.read_edges(_edges_key(run_id))
        items = [json.loads(v) for v in raw.values()]
        items.sort(key=lambda x: (x["parent_task_id"], x["child_task_id"]))
        return items

    async def get_lineage_summary(
        self, *, run_id: str, task_id: str
    ) -> Dict[str, Any]:
        return {
            "produced": await self.get_produced_output(run_id=run_id, task_id=task_id),
            "consumers": await self.get_consumers(run_id=run_id, task_id=task_id),
            "edges": await self.get_run_edges(run_id=run_id),
        }
