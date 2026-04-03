"""
hfa-semantic/src/hfa_semantic/policy/safety_guard.py

Sprint 16 — Safety Guard

Final veto on policy changes. Combines bounds + cooldown + step limits.
"""

from __future__ import annotations

from hfa_semantic.policy.bounds import clamp, within_bounds
from hfa_semantic.policy.cooldown import cooldown_elapsed


def guard_policy_change(
    proposed_value: float,
    current_value: float,
    minimum: float,
    maximum: float,
    now_ms: int,
    last_change_ms: int | None,
    cooldown_ms: int,
    max_step_abs: float,
) -> tuple[float, bool, list[str]]:
    """
    Apply all safety guards to proposed policy change.
    
    Returns:
        (guarded_value, allowed, guard_tags)
    """
    tags: list[str] = []

    # GUARD 1: Cooldown
    if not cooldown_elapsed(now_ms, last_change_ms, cooldown_ms):
        return current_value, False, ["guard:cooldown_active"]

    # GUARD 2: Step limiter (prevent large jumps)
    raw_delta = proposed_value - current_value
    if raw_delta > max_step_abs:
        proposed_value = current_value + max_step_abs
        tags.append("guard:step_capped_up")
    elif raw_delta < -max_step_abs:
        proposed_value = current_value - max_step_abs
        tags.append("guard:step_capped_down")

    # GUARD 3: Bounds clamping
    bounded = clamp(proposed_value, minimum, maximum)
    if bounded != proposed_value:
        tags.append("guard:bounds_clamped")

    # GUARD 4: Final bounds check (veto if impossible)
    if not within_bounds(bounded, minimum, maximum):
        return current_value, False, tags + ["guard:bounds_violation"]

    return bounded, True, tags

