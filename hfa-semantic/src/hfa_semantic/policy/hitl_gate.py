from __future__ import annotations
def requires_hitl(delta_abs: float, hitl_step_abs: float) -> bool:
    return abs(delta_abs) >= hitl_step_abs
