"""
hfa-semantic/src/hfa_semantic/runtime/factory.py

IRONCLAD Sprint 9.1 — Canonical Runtime Factory (Final Lock)

SEALED CONTRACT:
  build_pipeline_sync() ALWAYS returns a SemanticPipeline instance.
  NEVER returns None. NEVER returns a tuple.

  Two concrete types:
    SemanticPipeline   — fully operational, Redis-backed components
    DegradedPipeline   — no-op components, used when Redis unavailable

  Both types expose the same attribute interface:
    .state_store   — StateStore (read/write)
    .dedup_store   — DedupStore (dedup check + mark)
    .watermark     — WatermarkManager (lateness detection)
    .metrics       — SemanticMetrics (counters)

  SemanticBridge and CognitiveExecutor use attribute access only —
  no tuple unpacking in any runtime path.
"""

from __future__ import annotations

import logging

import redis.asyncio as redis

from hfa_semantic.config import settings

from .dedup_store import DedupStore
from .eviction import EvictionPolicy, EvictionStrategy
from .inmemory_state_store import InMemoryStateStore
from .redis_state_store import RedisStateStore
from .runtime_metrics import RuntimeMetrics
from .state_store import StateStore
from .watermark import LatenessPolicy, Watermark

logger = logging.getLogger(__name__)


# ── SemanticPipeline — canonical pipeline type ────────────────────────────────

class SemanticPipeline:
    """
    Canonical semantic runtime pipeline.

    All components are fully operational. Requires a Redis client.

    Attributes
    ----------
    state_store   : StateStore backed by Redis
    dedup_store   : DedupStore backed by Redis
    watermark     : WatermarkManager (in-process, no Redis dependency)
    metrics       : SemanticMetrics (in-process counters)
    """

    def __init__(
        self,
        state_store: StateStore,
        dedup_store: DedupStore,
        watermark: Watermark,
        metrics: RuntimeMetrics,
    ) -> None:
        self.state_store = state_store
        self.dedup_store = dedup_store
        self.watermark   = watermark
        self.metrics     = metrics

        # Validate contract — fail fast at construction time
        assert hasattr(self.dedup_store, "is_duplicate"), \
            "dedup_store must implement is_duplicate()"
        assert hasattr(self.watermark, "observe"), \
            "watermark must implement observe()"
        assert hasattr(self.metrics, "inc_processed"), \
            "metrics must implement inc_processed()"

        logger.info("semantic_pipeline_built: type=SemanticPipeline")

    @property
    def is_degraded(self) -> bool:
        return False


# ── No-op components for DegradedPipeline ────────────────────────────────────

class _DegradedDedupStore:
    """No-op dedup — never reports duplicates, never writes."""

    async def is_duplicate(self, event_id: str, **_) -> bool:
        return False

    async def mark_processed(self, event_id: str, *args, **kwargs) -> None:
        pass

    async def get_entry(self, event_id: str):
        return None

    async def close(self) -> None:
        pass


class _DegradedWatermark:
    """No-op watermark — nothing is ever late."""

    allowed_lateness_ms: int = 0
    _watermark_ms: float = 0.0
    _late_count: int = 0
    _on_time_count: int = 0

    def observe(self, event_time_ms: float):
        return (False, True)

    def evaluate(self, event_time_ms: float):
        from hfa_semantic.runtime.lateness_policy import LatenessAction
        return (False, LatenessAction.ACCEPT_WITH_CORRECTION)

    def should_accept(self, event_time_ms: float, policy=None) -> bool:
        return True

    @property
    def watermark_ms(self) -> float:
        return self._watermark_ms

    @property
    def current_watermark_ms(self) -> float:
        return self._watermark_ms

    @property
    def late_count(self) -> int:
        return self._late_count

    @property
    def on_time_count(self) -> int:
        return self._on_time_count


class _DegradedStateStore:
    """No-op state store."""

    async def get(self, *args, **kwargs):
        return None

    async def put(self, *args, **kwargs) -> None:
        pass

    async def delete(self, *args, **kwargs) -> None:
        pass

    async def evict(self, *args, **kwargs) -> int:
        return 0

    async def list_partitions(self, *args, **kwargs):
        return []

    async def close(self) -> None:
        pass


# ── DegradedPipeline — no-op subclass of SemanticPipeline ────────────────────

class DegradedPipeline(SemanticPipeline):
    """
    Degraded-mode pipeline. Returned when Redis is unavailable.

    Subclasses SemanticPipeline so isinstance checks pass.
    All components are no-ops — events pass through without dedup,
    watermark checks, or state persistence.

    Logged once on construction to make degraded mode visible in logs.
    """

    def __init__(self) -> None:
        # Bypass parent __init__ validation — build no-op components directly
        self.state_store = _DegradedStateStore()
        self.dedup_store = _DegradedDedupStore()
        self.watermark   = _DegradedWatermark()
        self.metrics     = RuntimeMetrics()
        self.metrics.inc_degraded()
        logger.warning(
            "semantic_degraded_mode: DegradedPipeline active — "
            "no Redis, no dedup, no watermark, no state persistence"
        )

    @property
    def is_degraded(self) -> bool:
        return True


# ── RuntimeFactory (async factory methods — unchanged) ───────────────────────

class RuntimeFactory:
    """Factory for creating runtime components."""

    @staticmethod
    async def create_state_store() -> StateStore:
        backend = settings.SEMANTIC_STATE_BACKEND.lower()
        if backend == "memory":
            return InMemoryStateStore()
        elif backend == "redis":
            redis_url = settings.SEMANTIC_REDIS_URL
            if not redis_url:
                raise ValueError("SEMANTIC_REDIS_URL required for Redis backend")
            client = redis.from_url(redis_url, decode_responses=True)
            await client.ping()
            return RedisStateStore(client)
        else:
            raise ValueError(f"Unknown state backend: {backend}")

    @staticmethod
    async def create_dedup_store() -> DedupStore:
        redis_url = settings.SEMANTIC_REDIS_URL
        if not redis_url:
            raise ValueError("SEMANTIC_REDIS_URL required for DedupStore")
        client = redis.from_url(redis_url, decode_responses=True)
        await client.ping()
        return DedupStore(client)

    @staticmethod
    def create_watermark() -> Watermark:
        return Watermark(allowed_lateness_ms=settings.SEMANTIC_ALLOWED_LATENESS_MS)

    @staticmethod
    def create_eviction_policy() -> EvictionPolicy:
        return EvictionPolicy(
            strategy=EvictionStrategy.HYBRID,
            max_partitions_per_rule=settings.SEMANTIC_MAX_PARTITIONS_PER_RULE,
            ttl_ms=settings.SEMANTIC_STATE_TTL_MS,
            watermark_window_ms=settings.SEMANTIC_STATE_TTL_MS,
        )

    @staticmethod
    def create_metrics() -> RuntimeMetrics:
        return RuntimeMetrics()

    @staticmethod
    async def create_all() -> dict:
        return {
            "state_store":     await RuntimeFactory.create_state_store(),
            "dedup_store":     await RuntimeFactory.create_dedup_store(),
            "watermark":       RuntimeFactory.create_watermark(),
            "eviction_policy": RuntimeFactory.create_eviction_policy(),
            "metrics":         RuntimeFactory.create_metrics(),
        }


# ── build_pipeline (async) ────────────────────────────────────────────────────

async def build_pipeline(redis_client=None) -> SemanticPipeline:
    """
    Build a SemanticPipeline (async version).

    Returns SemanticPipeline if redis_client provided, else DegradedPipeline.
    NEVER returns None or a tuple.
    """
    if redis_client:
        state_store = RedisStateStore(redis_client)
        dedup_store = DedupStore(redis_client)
        watermark   = RuntimeFactory.create_watermark()
        metrics     = RuntimeFactory.create_metrics()
        return SemanticPipeline(state_store, dedup_store, watermark, metrics)

    logger.warning("build_pipeline: no redis_client — returning DegradedPipeline")
    return DegradedPipeline()


# ── build_pipeline_sync ───────────────────────────────────────────────────────

def build_pipeline_sync(redis_client=None) -> SemanticPipeline:
    """
    Build a SemanticPipeline (synchronous version).

    SEALED CONTRACT:
      * ALWAYS returns SemanticPipeline or DegradedPipeline (subclass)
      * NEVER returns None
      * NEVER returns a tuple

    Args:
        redis_client: Pre-built Redis client. If None, returns DegradedPipeline.

    Returns:
        SemanticPipeline with operational components, or
        DegradedPipeline with no-op components.
    """
    if not redis_client:
        logger.warning(
            "build_pipeline_sync: no redis_client — returning DegradedPipeline"
        )
        return DegradedPipeline()

    state_store = RedisStateStore(redis_client)
    dedup_store = DedupStore(redis_client)
    watermark   = RuntimeFactory.create_watermark()
    metrics     = RuntimeFactory.create_metrics()

    pipeline = SemanticPipeline(state_store, dedup_store, watermark, metrics)
    return pipeline
