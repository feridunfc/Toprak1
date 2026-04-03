"""
hfa-semantic/src/hfa_semantic/memory/relevance_policy.py

Sprint 15 — Memory Retention Policy

What gets stored in semantic memory?
Only validated outcomes with high confidence.
"""

from __future__ import annotations

from hfa_semantic.api.models import ValidatedOutcome


def retain(outcome: ValidatedOutcome, min_confidence: float = 0.70) -> bool:
    """
    Decide if outcome should be retained in memory.
    
    CRITICAL: Only validated, high-confidence outcomes are kept.
    False negatives from validators are discarded.
    """
    return outcome.validated and outcome.confidence >= min_confidence

