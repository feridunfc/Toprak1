# STATUS: CANONICAL — do not import from eviction_v2
"""
hfa-semantic/src/hfa_semantic/runtime/eviction.py

IRONCLAD Sprint 1 — Eviction Policy & Cardinality Guard

Fix: EvictionPolicy now accepts partition_pressure_threshold and exposes
     pressure monitoring methods used by production hardening tests.
"""

from __future__ import annotations

import time
from enum import Enum
from typing import Dict, List, Optional


class EvictionStrategy(Enum):
    LRU    = "lru"
    FIFO   = "fifo"
    TTL    = "ttl"
    HYBRID = "hybrid"


class EvictionPolicy:
    """Policy for deciding when and how to evict state."""

    def __init__(
        self,
        strategy: EvictionStrategy = EvictionStrategy.TTL,
        max_partitions: int = 10_000,
        max_partitions_per_rule: int = 10_000,
        ttl_ms: int = 3_600_000,
        watermark_window_ms: int = 3_600_000,
        partition_pressure_threshold: float = 0.80,  # Sprint fix
    ) -> None:
        self.strategy = strategy
        self.max_partitions = max_partitions
        self.max_partitions_per_rule = max_partitions_per_rule
        self.ttl_ms = ttl_ms
        self.watermark_window_ms = watermark_window_ms
        self.partition_pressure_threshold = partition_pressure_threshold

        self._current_partitions: int = 0

    # ── Pressure monitoring (Sprint fix) ─────────────────────────────────────

    def update_partition_count(self, count: int) -> None:
        """Update tracked partition count (call after each state mutation)."""
        self._current_partitions = max(0, count)

    @property
    def partition_pressure(self) -> float:
        """Current fill ratio: 0.0–1.0."""
        cap = self.max_partitions_per_rule
        if cap == 0:
            return 0.0
        return min(1.0, self._current_partitions / cap)

    @property
    def under_pressure(self) -> bool:
        """True when fill ratio exceeds threshold."""
        return self.partition_pressure >= self.partition_pressure_threshold

    def aggressive_eviction_needed(self) -> bool:
        """True when fill ratio >= 95% — emergency eviction."""
        return self.partition_pressure >= 0.95

    def get_eviction_batch_size(self) -> int:
        """
        Adaptive batch size based on current pressure.

        Normal  (<threshold): 1% of max
        Pressure (>=threshold): 10% of max
        Emergency (>=95%): 20% of max
        """
        p = self.partition_pressure
        cap = self.max_partitions_per_rule
        if p >= 0.95:
            return max(1, int(cap * 0.20))
        if p >= self.partition_pressure_threshold:
            return max(1, int(cap * 0.10))
        return max(1, int(cap * 0.01))


class EvictionMetrics:
    def __init__(self) -> None:
        self.total_evictions: int = 0
        self.total_allocations: int = 0
        self._window_start_ms: float = time.time() * 1000.0
        self._window_evictions: int = 0

    def record_eviction(self) -> None:
        self.total_evictions += 1
        self._window_evictions += 1

    def record_allocation(self) -> None:
        self.total_allocations += 1

    @property
    def eviction_rate(self) -> float:
        elapsed_sec = (time.time() * 1000.0 - self._window_start_ms) / 1000.0
        if elapsed_sec < 1.0:
            return 0.0
        return self.total_evictions / elapsed_sec

    def reset_window(self) -> None:
        self._window_start_ms = time.time() * 1000.0
        self._window_evictions = 0


class CardinalityGuard:
    """In-memory cardinality guard with TTL enforcement."""

    def __init__(
        self,
        max_partitions_per_rule: int = 10_000,
        default_ttl_ms: int = 3_600_000,
    ) -> None:
        self.max_partitions = max_partitions_per_rule
        self.default_ttl_ms = default_ttl_ms
        self._partitions: Dict[str, Dict[str, float]] = {}
        self._expiry: Dict[str, Dict[str, float]] = {}

    def is_safe_to_allocate(self, rule_id: str, ttl_ms: int | None = None) -> bool:
        self._evict_expired(rule_id)
        return len(self._partitions.get(rule_id, {})) < self.max_partitions

    def mark_allocation(
        self,
        rule_id: str,
        partition_key: str = "__default__",
        ttl_ms: int | None = None,
    ) -> None:
        now_ms = time.time() * 1000.0
        effective_ttl = ttl_ms if ttl_ms is not None else self.default_ttl_ms
        if rule_id not in self._partitions:
            self._partitions[rule_id] = {}
            self._expiry[rule_id] = {}
        self._partitions[rule_id][partition_key] = now_ms
        self._expiry[rule_id][partition_key] = now_ms + effective_ttl

    def mark_eviction(self, rule_id: str, partition_key: str = "__default__") -> None:
        self._partitions.get(rule_id, {}).pop(partition_key, None)
        self._expiry.get(rule_id, {}).pop(partition_key, None)

    def partition_count(self, rule_id: str) -> int:
        self._evict_expired(rule_id)
        return len(self._partitions.get(rule_id, {}))

    def oldest_partition(self, rule_id: str) -> Optional[str]:
        parts = self._partitions.get(rule_id)
        if not parts:
            return None
        return min(parts, key=lambda k: parts[k])

    def all_rule_ids(self) -> List[str]:
        return list(self._partitions.keys())

    def _evict_expired(self, rule_id: str) -> int:
        expiry_map = self._expiry.get(rule_id)
        if not expiry_map:
            return 0
        now_ms = time.time() * 1000.0
        expired = [k for k, exp in expiry_map.items() if exp <= now_ms]
        for key in expired:
            self._partitions.get(rule_id, {}).pop(key, None)
            expiry_map.pop(key, None)
        return len(expired)
