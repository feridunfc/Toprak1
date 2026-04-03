"""
hfa-semantic/src/hfa_semantic/policy/optimizer.py

Sprint 16 — Policy Optimizer

Proposes policy changes based on outcome feedback.
"""

from __future__ import annotations


def propose_threshold(
    current_value: float,
    false_positive_rate: float,
    false_negative_rate: float,
    raise_step_abs: float = 2.0,
    lower_step_abs: float = 2.0,
) -> float:
    """
    Propose new threshold based on false positive / negative rates.
    
    Logic:
      * High FP rate → raise threshold (be more strict)
      * High FN rate → lower threshold (be more lenient)
    """
    if false_positive_rate > 0.30:
        return current_value + raise_step_abs
    if false_negative_rate > 0.20:
        return current_value - lower_step_abs
    return current_value

