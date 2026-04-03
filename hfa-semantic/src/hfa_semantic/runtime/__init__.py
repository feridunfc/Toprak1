"""
hfa-semantic/src/hfa_semantic/runtime/__init__.py

IRONCLAD Sprint 6.2 — Runtime Foundation — Canonical Exports

This is the single authoritative import surface for the semantic runtime.
All internal code should import from here, not from submodules directly.

Canonical classes:
  StateStore, RuntimeState        — abstract store + model
  InMemoryStateStore              — dev / test store
  RedisStateStore                 — production store
  DedupStore                      — event deduplication
  Watermark, WatermarkManager     — watermark (sync + async)
  LatenessPolicy                  — lateness handling enum
  RuntimeMetrics, SemanticMetrics — lightweight in-process counters
  Partitioner, PartitionStrategy  — partition key computation
  EvictionPolicy, EvictionStrategy, EvictionMetrics, CardinalityGuard
"""

from .dedup_store import DedupStore
from .eviction import (
    CardinalityGuard,
    EvictionMetrics,
    EvictionPolicy,
    EvictionStrategy,
)
from .inmemory_state_store import InMemoryStateStore
from .partitioning import PartitionStrategy, Partitioner
from .redis_state_store import RedisStateStore
from .runtime_metrics import RuntimeMetrics, SemanticMetrics
from .state_store import RuntimeState, StateStore
from .watermark import LatenessPolicy, Watermark, WatermarkManager

__all__ = [
    # State
    "StateStore",
    "RuntimeState",
    "InMemoryStateStore",
    "RedisStateStore",
    # Dedup
    "DedupStore",
    # Watermark
    "Watermark",
    "WatermarkManager",
    "LatenessPolicy",
    # Metrics
    "RuntimeMetrics",
    "SemanticMetrics",
    # Partitioning
    "Partitioner",
    "PartitionStrategy",
    # Eviction
    "EvictionPolicy",
    "EvictionStrategy",
    "EvictionMetrics",
    "CardinalityGuard",
]
