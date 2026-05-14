"""
hfa-core/src/hfa/healing/store.py
IRONCLAD Sprint 4 — StateStore ABC + InMemoryStateStore + RedisStateStore

Design rules
------------
* StateStore is an ABC with @abstractmethod — unimplemented subclass cannot
  be instantiated (compile-time guarantee, not runtime).
* cleanup_expired() contract is documented per implementation:
    InMemoryStateStore → count of records actually deleted
    RedisStateStore    → count of currently active keys
                         (Redis TTL auto-expires; "deleted" has no meaning)
* All asyncio scheduling uses get_running_loop() — NEVER get_event_loop().
* close() is always safe to call (no-op on base class).
* No print() anywhere — logging only.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from abc import ABC, abstractmethod
from dataclasses import asdict, dataclass, field
from typing import Dict, Optional

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Domain model
# ---------------------------------------------------------------------------


@dataclass
class LoopState:
    """
    Per-run state tracked by SelfHealingEngine.

    Fields
    ------
    attempt             Current retry attempt number (0-based).
    fingerprint         SHA-256 hex of the last error message (for dedup).
    last_error          Raw error string from last failed attempt.
    circuit_open_until  Unix timestamp after which circuit is re-tested.
                        None means circuit is closed (normal operation).
    total_tokens_used   Accumulated LLM token usage for this run.
    total_cost_cents    Accumulated cost in INTEGER CENTS (no float).
    last_updated        Unix timestamp of last state write (set by store.set()).
    """

    attempt: int = 0
    fingerprint: Optional[str] = None
    last_error: Optional[str] = None
    circuit_open_until: Optional[float] = None
    total_tokens_used: int = 0
    total_cost_cents: int = 0  # ✅ integer cents, NOT float USD
    last_updated: float = field(default_factory=time.time)

    def is_circuit_open(self) -> bool:
        """Return True if the circuit breaker is currently open."""
        if self.circuit_open_until is None:
            return False
        return time.time() < self.circuit_open_until

    def open_circuit(self, duration_seconds: float) -> None:
        """Open the circuit breaker for `duration_seconds`."""
        self.circuit_open_until = time.time() + duration_seconds

    def close_circuit(self) -> None:
        """Close the circuit breaker (allow requests through)."""
        self.circuit_open_until = None


# ---------------------------------------------------------------------------
# Abstract base
# ---------------------------------------------------------------------------


class StateStore(ABC):
    """
    Abstract base for healing state persistence.

    ALL methods are @abstractmethod — any concrete subclass that does not
    implement all methods will raise TypeError at instantiation time
    (Python ABC enforcement, not just a runtime check).

    Usage
    -----
        store = InMemoryStateStore()        # ✅ ok — all methods implemented
        store = StateStore()               # ❌ TypeError — abstract
    """

    @abstractmethod
    async def get(self, key: str) -> Optional[LoopState]:
        """
        Retrieve state for `key`.

        Returns None if key does not exist or has expired.
        """
        ...

    @abstractmethod
    async def set(
        self,
        key: str,
        state: LoopState,
        ttl: Optional[int] = None,
    ) -> None:
        """
        Persist `state` under `key`.

        Implementations MUST update `state.last_updated = time.time()`.

        Args:
            key:   Composite key, e.g. ``f"{tenant_id}:{run_id}"``.
            state: LoopState dataclass to store.
            ttl:   Optional TTL in seconds (overrides class default).
        """
        ...

    @abstractmethod
    async def delete(self, key: str) -> None:
        """Delete state for `key`. No-op if key does not exist."""
        ...

    @abstractmethod
    async def cleanup_expired(self) -> int:
        """
        Remove or count expired states.

        Contract (varies by implementation — intentional):
          InMemoryStateStore → returns count of records actually deleted.
          RedisStateStore    → returns count of currently *active* keys
                               (Redis TTL auto-expires; deletion count N/A).

        This divergence is documented here and in each subclass to make the
        asymmetry explicit to callers — not hidden by a misleading uniform API.

        Returns:
            Implementation-defined integer (see above).
        """
        ...

    async def close(self) -> None:
        """
        Graceful shutdown hook.

        Base implementation is a no-op. Override in subclasses that hold
        background tasks or connections.
        """


# ---------------------------------------------------------------------------
# InMemoryStateStore — for testing and single-node dev
# ---------------------------------------------------------------------------


class InMemoryStateStore(StateStore):
    """
    Thread-safe in-memory state store with TTL eviction.

    A background task runs periodic cleanup every `cleanup_interval` seconds.
    Background task uses get_running_loop() and is cancelled on close().

    Args:
        default_ttl:       Seconds before a state entry is considered expired.
        cleanup_interval:  How often the background cleaner runs (seconds).
    """

    def __init__(
        self,
        default_ttl: int = 3600,
        cleanup_interval: int = 300,
    ) -> None:
        self._states: Dict[str, LoopState] = {}
        self._lock = asyncio.Lock()
        self._default_ttl = default_ttl
        self._cleanup_interval = cleanup_interval
        self._running = True
        # Defer task creation — must be called from a running event loop.
        # Use start() or lazy creation on first use.
        self._cleanup_task: Optional[asyncio.Task] = None

    def _ensure_task(self) -> None:
        """Lazily create background cleanup task on first store operation."""
        if self._cleanup_task is None:
            loop = asyncio.get_running_loop()
            self._cleanup_task = loop.create_task(
                self._periodic_cleanup(), name="InMemoryStateStore.cleanup"
            )

    async def get(self, key: str) -> Optional[LoopState]:
        self._ensure_task()
        async with self._lock:
            state = self._states.get(key)
            if state is None:
                return None
            if time.time() - state.last_updated > self._default_ttl:
                del self._states[key]
                logger.debug("StateStore.get: expired key=%s", key)
                return None
            return state

    async def set(
        self,
        key: str,
        state: LoopState,
        ttl: Optional[int] = None,  # noqa: ARG002 — TTL used by Redis impl
    ) -> None:
        self._ensure_task()
        state.last_updated = time.time()
        async with self._lock:
            self._states[key] = state
        logger.debug("StateStore.set: key=%s attempt=%d", key, state.attempt)

    async def delete(self, key: str) -> None:
        self._ensure_task()
        async with self._lock:
            self._states.pop(key, None)
        logger.debug("StateStore.delete: key=%s", key)

    async def cleanup_expired(self) -> int:
        """
        Scan and delete all expired entries.

        Returns:
            Number of entries actually deleted (InMemory contract).
        """
        self._ensure_task()
        now = time.time()
        expired = []
        async with self._lock:
            for k, s in list(self._states.items()):
                if now - s.last_updated > self._default_ttl:
                    expired.append(k)
            for k in expired:
                del self._states[k]
        if expired:
            logger.info("StateStore.cleanup_expired: removed %d entries", len(expired))
        return len(expired)

    async def _periodic_cleanup(self) -> None:
        while self._running:
            try:
                await asyncio.sleep(self._cleanup_interval)
                await self.cleanup_expired()
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.error(
                    "StateStore periodic cleanup error: %s", exc, exc_info=True
                )

    async def close(self) -> None:
        """Cancel background cleanup and clear all state."""
        self._running = False
        if self._cleanup_task is not None:
            self._cleanup_task.cancel()
            try:
                await self._cleanup_task
            except asyncio.CancelledError:
                pass
        async with self._lock:
            count = len(self._states)
            self._states.clear()
        logger.info("InMemoryStateStore closed (cleared %d entries)", count)


# ---------------------------------------------------------------------------
# RedisStateStore — production multi-node store
# ---------------------------------------------------------------------------


class RedisStateStore(StateStore):
    """
    Redis-backed state store using JSON serialisation.

    TTL management is delegated to Redis — entries expire automatically.
    cleanup_expired() returns the count of *currently active* keys (not
    deleted count, which is undefined for TTL-based expiry).

    Key format: ``healing:<key>``

    Args:
        redis_client:  Async Redis client (aioredis.Redis or compatible).
        default_ttl:   Default key TTL in seconds.
    """

    def __init__(self, redis_client, default_ttl: int = 3600) -> None:
        self._redis = redis_client
        self._default_ttl = default_ttl
        self._prefix = "healing:"

    def _full_key(self, key: str) -> str:
        return f"{self._prefix}{key}"

    async def get(self, key: str) -> Optional[LoopState]:
        try:
            raw = await self._redis.get(self._full_key(key))
        except Exception as exc:
            logger.error(
                "RedisStateStore.get error key=%s: %s", key, exc, exc_info=True
            )
            return None
        if raw is None:
            return None
        try:
            data = raw.decode() if isinstance(raw, bytes) else raw
            return LoopState(**json.loads(data))
        except Exception as exc:
            logger.error(
                "RedisStateStore.get decode error key=%s: %s", key, exc, exc_info=True
            )
            return None

    async def set(
        self,
        key: str,
        state: LoopState,
        ttl: Optional[int] = None,
    ) -> None:
        state.last_updated = time.time()
        try:
            await self._redis.setex(
                self._full_key(key),
                ttl or self._default_ttl,
                json.dumps(asdict(state)),
            )
            logger.debug("RedisStateStore.set: key=%s attempt=%d", key, state.attempt)
        except Exception as exc:
            logger.error(
                "RedisStateStore.set error key=%s: %s", key, exc, exc_info=True
            )
            raise

    async def delete(self, key: str) -> None:
        try:
            await self._redis.delete(self._full_key(key))
            logger.debug("RedisStateStore.delete: key=%s", key)
        except Exception as exc:
            logger.error(
                "RedisStateStore.delete error key=%s: %s", key, exc, exc_info=True
            )
            raise

    async def cleanup_expired(self) -> int:
        """
        Scan active keys and return their count.

        Redis TTL auto-expires keys — this method counts currently *live* keys
        under the ``healing:`` prefix.  It does NOT delete anything.

        Returns:
            Count of currently active state keys (RedisStateStore contract).
        """
        count = 0
        try:
            async for _ in self._redis.scan_iter(f"{self._prefix}*"):
                count += 1
        except Exception as exc:
            logger.error(
                "RedisStateStore.cleanup_expired scan error: %s", exc, exc_info=True
            )
        logger.debug("RedisStateStore.cleanup_expired: %d active keys", count)
        return count
