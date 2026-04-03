from __future__ import annotations

from dataclasses import dataclass
from hfa_semantic.api.models import ValidatorType


@dataclass(slots=True, frozen=True)
class ValidationDecision:
    validated: bool
    validator: ValidatorType
    confidence: float
    notes: str = ""
