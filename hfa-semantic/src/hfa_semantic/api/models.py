from __future__ import annotations

from enum import Enum
from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, Field


class OutcomeType(str, Enum):
    SUCCESS = "success"
    FAILURE = "failure"
    FALSE_POSITIVE = "false_positive"
    FALSE_NEGATIVE = "false_negative"
    RESOLVED = "resolved"


class ValidatorType(str, Enum):
    RULE = "rule"
    HUMAN = "human"
    DELAYED_SIGNAL = "delayed_signal"


class RawEvent(BaseModel):
    event_id: str
    entity_id: str
    event_type: str
    timestamp_ms: int
    metric_name: Optional[str] = None
    metric_value: Optional[float] = None
    threshold: Optional[float] = None
    lineage_run_id: Optional[str] = None
    payload: Dict[str, Any] = Field(default_factory=dict)


class RawOutcome(BaseModel):
    event_id: str
    lineage_run_id: Optional[str] = None
    decision_id: Optional[str] = None
    outcome_type: OutcomeType
    confidence: float = Field(ge=0.0, le=1.0)
    timestamp_ms: int
    notes: str = ""


class ValidatedOutcome(BaseModel):
    event_id: str
    lineage_run_id: Optional[str] = None
    decision_id: Optional[str] = None
    outcome_type: OutcomeType
    validated: bool
    validator: ValidatorType
    confidence: float = Field(ge=0.0, le=1.0)
    timestamp_ms: int
    notes: str = ""


class RuleMatch(BaseModel):
    rule_id: str
    partition_key: str
    matched_at_ms: int
    severity: Literal["info", "warning", "critical"]
    confidence: float
    reason: str


class SemanticQueryRequest(BaseModel):
    intent: str
    context_event_id: str
    vector_query: str
    graph_depth: int = 2
    min_confidence: float = 0.85
    lineage_run_id: Optional[str] = None


class GraphInsight(BaseModel):
    relation_type: str
    target_node_id: str
    target_label: str


class QueryResult(BaseModel):
    historical_event_id: str
    similarity_score: float
    graph_insights: List[GraphInsight]
    validated_truth: bool = True


class PolicyDecision(BaseModel):
    decision_id: str
    policy_key: str
    previous_value: float
    proposed_value: float
    approved: bool = False
    reason: str = ""
    requires_hitl: bool = False
    cooldown_applied: bool = False
