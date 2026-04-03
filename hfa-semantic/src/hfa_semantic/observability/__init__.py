"""
hfa-semantic/src/hfa_semantic/observability/__init__.py

IRONCLAD Sprint 6.1 — Observability Layer

Exports the unified metrics surface for the semantic runtime.

Usage:
    from hfa_semantic.observability import aggregate_snapshot, RuntimeMetrics

    # Get a full cross-package metrics snapshot (semantic + cognitive + feedback)
    snapshot = aggregate_snapshot()
    logger.info("metrics: %s", snapshot)

    # Get the semantic-only singleton
    metrics = get_global_metrics()
    metrics.inc_processed()
"""

from ..runtime.runtime_metrics import (
    RuntimeMetrics,
    SemanticMetrics,
    aggregate_snapshot,
    get_global_metrics,
)

__all__ = [
    "RuntimeMetrics",
    "SemanticMetrics",
    "aggregate_snapshot",
    "get_global_metrics",
]
