"""
hfa-semantic/src/hfa_semantic/policy/rules.py

Sprint 16 — Policy Rules

Domain-specific logic for when to adapt policy.
"""

from __future__ import annotations


def policy_reason(false_positive_rate: float, false_negative_rate: float) -> str:
    """
    Generate human-readable reason for policy signal.
    """
    if false_positive_rate > 0.30:
        return "false_positive_rate_high"
    if false_negative_rate > 0.20:
        return "false_negative_rate_high"
    return "no_policy_signal"

