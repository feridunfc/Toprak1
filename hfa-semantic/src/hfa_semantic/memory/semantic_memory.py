"""
hfa-semantic/src/hfa_semantic/memory/semantic_memory.py

STATUS: DEPRECATED — use semantic_memory_v2.SemanticMemoryV2 instead.

SemanticMemory (simple bounded deque) is superseded by SemanticMemoryV2
(exponential decay weighting). This module keeps the old class name as
an alias for backward compat.

MIGRATION:
    # Old
    from hfa_semantic.memory.semantic_memory import SemanticMemory

    # New
    from hfa_semantic.memory.semantic_memory_v2 import SemanticMemoryV2
    # SemanticMemoryV2 has the same append() interface, plus weighted()
"""

from __future__ import annotations

from hfa_semantic.memory.semantic_memory_v2 import SemanticMemoryV2, WeightedOutcome

# Backward-compat alias
SemanticMemory = SemanticMemoryV2

__all__ = ["SemanticMemory", "SemanticMemoryV2", "WeightedOutcome"]
