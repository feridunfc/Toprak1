from __future__ import annotations

from typing import Any
from pydantic import BaseModel, Field


class RuleCondition(BaseModel):
    field: str
    operator: str
    value: Any


class SemanticRule(BaseModel):
    rule_id: str
    rule_type: str = Field(..., description="repeated_event or threshold_breach")
    window_ms: int = 0
    trigger_count: int = 1
    conditions: list[RuleCondition]
    action_severity: str
    action_confidence: float
