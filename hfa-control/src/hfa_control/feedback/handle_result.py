"""Single correction-loop result normalization path.

The control plane may receive agent, semantic, or validation outcomes from
multiple components.  Sprint 1 does not migrate every producer; it provides one
small helper that downstream callers can use to avoid double-emitting feedback
or silently treating advisory feedback as authoritative truth.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping


@dataclass(frozen=True)
class CorrectionFeedbackDecision:
    status: str
    should_record_feedback: bool
    suggested_feedback: str | None = None
    requires_hitl: bool = False
    reason: str = ""


def handle_result(result: Any, *, strict_mode: bool = False) -> CorrectionFeedbackDecision:
    """
    Normalize one execution result into one correction-loop decision.

    strict_mode fails closed for ambiguous or missing statuses.  Advisory
    feedback remains visible, but this helper emits a single decision object so
    callers do not independently create competing correction-loop records.
    """
    if isinstance(result, Mapping):
        status = result.get("status")
        suggested_feedback = result.get("suggested_feedback")
        requires_hitl = bool(result.get("requires_hitl", False))
    else:
        status = getattr(result, "status", None)
        suggested_feedback = getattr(result, "suggested_feedback", None)
        requires_hitl = bool(getattr(result, "requires_hitl", False))

    if not status:
        if strict_mode:
            return CorrectionFeedbackDecision(
                status="failed",
                should_record_feedback=True,
                suggested_feedback=suggested_feedback or "Missing execution status.",
                requires_hitl=True,
                reason="missing_status_fail_closed",
            )
        return CorrectionFeedbackDecision(
            status="unknown",
            should_record_feedback=False,
            suggested_feedback=suggested_feedback,
            requires_hitl=requires_hitl,
            reason="missing_status_advisory",
        )

    normalized = str(status)
    return CorrectionFeedbackDecision(
        status=normalized,
        should_record_feedback=bool(suggested_feedback) or normalized != "success" or requires_hitl,
        suggested_feedback=suggested_feedback,
        requires_hitl=requires_hitl,
        reason="single_feedback_path",
    )
