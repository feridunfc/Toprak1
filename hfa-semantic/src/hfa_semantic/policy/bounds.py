from __future__ import annotations


def clamp(value: float, minimum: float, maximum: float) -> float:
    return max(minimum, min(maximum, value))


def within_bounds(value: float, min_val: float, max_val: float) -> bool:
    return min_val <= value <= max_val
