"""
hfa-semantic/src/hfa_semantic/memory/feedback_loop.py

Sprint 18 — Closed-Loop Feedback

CRITICAL:
  * Policy decision -> outcome -> evaluation
  * decision_id links policy changes to their results
  * Enables system self-monitoring
  * Feedback is actionable (not fire-and-forget)
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from hfa_semantic.api.models import PolicyDecision, ValidatedOutcome


@dataclass(slots=True, frozen=True)
class DecisionFeedbackLink:
    """Link between a policy decision and its outcome."""
    decision_id: str
    event_id: str
    outcome_type: str
    confidence: float
    timestamp_ms: int


class FeedbackLoop:
    """
    Minimal closed-loop implementation.
    
    Tracks which policy decisions led to which outcomes.
    Enables second-order learning: "did this policy change help?"
    """

    def __init__(self) -> None:
        # decision_id -> PolicyDecision
        self._decision_index: dict[str, PolicyDecision] = {}
        
        # List of all decision -> outcome links
        self._links: list[DecisionFeedbackLink] = []

    def register_decision(self, decision: PolicyDecision) -> None:
        """Record a policy decision (before execution)."""
        self._decision_index[decision.decision_id] = decision

    def attach_outcome(self, outcome: ValidatedOutcome) -> bool:
        """
        Link a validated outcome to a prior decision.
        
        Returns:
            True if link created, False if decision not found
        """
        if not outcome.decision_id:
            return False
        
        if outcome.decision_id not in self._decision_index:
            return False

        self._links.append(
            DecisionFeedbackLink(
                decision_id=outcome.decision_id,
                event_id=outcome.event_id,
                outcome_type=outcome.outcome_type.value,
                confidence=outcome.confidence,
                timestamp_ms=outcome.timestamp_ms,
            )
        )
        
        return True

    def links_for_decision(self, decision_id: str) -> list[DecisionFeedbackLink]:
        """Get all feedback links for a policy decision."""
        return [x for x in self._links if x.decision_id == decision_id]

    def get_decision(self, decision_id: str) -> Optional[PolicyDecision]:
        """Retrieve original decision by ID."""
        return self._decision_index.get(decision_id)

    @property
    def total_links(self) -> int:
        """Total feedback links recorded."""
        return len(self._links)

    @property
    def total_decisions(self) -> int:
        """Total decisions registered."""
        return len(self._decision_index)

