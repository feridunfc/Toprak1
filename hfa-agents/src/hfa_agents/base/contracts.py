from __future__ import annotations

from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field

try:
    from hfa_semantic.api.models import ValidatedOutcome, QueryResult
except Exception:  # pragma: no cover - bootstrap friendliness
    ValidatedOutcome = Any
    QueryResult = Any


class EnrichedEvent(BaseModel):
    event_id: str
    lineage_run_id: Optional[str] = None
    goal: str
    context: Dict[str, Any] = Field(default_factory=dict)
    semantic_matches: List[Dict[str, Any]] = Field(default_factory=list)
    validated_outcomes: List[Any] = Field(default_factory=list)
    artifacts: Dict[str, Any] = Field(default_factory=dict)


class ExecutionResult(BaseModel):
    status: str = Field(..., pattern="^(success|failed|escalated|delegated)$")
    output_data: Dict[str, Any] = Field(default_factory=dict)
    reasoning_trace: List[str] = Field(default_factory=list)
    suggested_feedback: Optional[str] = None
    artifacts: Dict[str, Any] = Field(default_factory=dict)
    requires_hitl: bool = False
    confidence: float = Field(default=0.0, ge=0.0, le=1.0)


class DelegationMessage(BaseModel):
    from_agent: str
    to_agent: str
    payload: Dict[str, Any] = Field(default_factory=dict)
    reason: str = ""
