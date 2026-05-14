"""
hfa-semantic/src/hfa_semantic/validation/human_review.py

Sprint 15 — Human Review Gate (HITL)

Interface for escalating validation decisions to humans.
Replace with Slack, Jira, or internal UI.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(slots=True, frozen=True)
class HumanReviewRequest:
    """Request for human validation."""
    subject: str
    details: str
    severity: str = "normal"


class HumanReviewAdapter:
    """
    Skeleton adapter for human review system.
    
    In production, replace with real Slack bot, Jira API, or internal UI integration.
    """

    def request_review(self, request: HumanReviewRequest) -> dict:
        """
        Submit review request.
        
        Returns:
            {
                'status': 'pending' | 'approved' | 'rejected',
                'subject': str,
                'ticket_id': str (if applicable),
            }
        """
        # TODO: Replace with real integration
        return {
            "status": "pending",
            "subject": request.subject,
            "details": request.details,
            "severity": request.severity,
            "ticket_id": None,
        }

