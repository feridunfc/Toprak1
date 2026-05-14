from __future__ import annotations

from dataclasses import dataclass
from hfa_semantic.api.models import OutcomeType
from hfa_semantic.memory.semantic_memory_v2 import WeightedOutcome


@dataclass(slots=True, frozen=True)
class CostModel:
    false_positive_cost: float = 1.0
    false_negative_cost: float = 5.0

    def score(self, outcomes: list[WeightedOutcome]) -> float:
        total = 0.0
        for item in outcomes:
            if item.outcome.outcome_type == OutcomeType.FALSE_NEGATIVE:
                total += self.false_negative_cost * item.weighted_confidence
            elif item.outcome.outcome_type == OutcomeType.FALSE_POSITIVE:
                total += self.false_positive_cost * item.weighted_confidence
        return total
