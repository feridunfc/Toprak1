from __future__ import annotations
def delayed_truth_available(outcome_age_ms: int, min_delay_ms: int = 300_000) -> bool:
    return outcome_age_ms >= min_delay_ms
