"""
hfa-semantic/src/hfa_semantic/validation/feedback_validator.py

IRONCLAD Sprint 9.4 — Feedback Validation Layer

Validates execution outcomes before writing to semantic memory.
Prevents the system from learning from invalid, incomplete, or
poisoned feedback signals.

Rejection criteria:
  1. confidence < min_confidence (default 0.60)
  2. requires_hitl == True (human review pending)
  3. status not in allowed set ("success" only)
  4. missing critical fields (output_data, reasoning_trace)

Usage:
    validator = FeedbackValidator()
    decision = validator.validate(result)
    if decision.accepted:
        await memory.record_outcome(feedback)
    else:
        logger.debug("feedback rejected: reason=%s", decision.reason)
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


# Minimum confidence required for feedback to be accepted
_DEFAULT_MIN_CONFIDENCE: float = 0.60

# Only these statuses produce feedback worth learning from
_ACCEPTED_STATUSES: frozenset[str] = frozenset({"success"})


@dataclass(frozen=True)
class FeedbackDecision:
    """Result of a feedback validation check."""
    accepted:   bool
    reason:     str
    confidence: float = 0.0

    @property
    def rejected(self) -> bool:
        return not self.accepted


class FeedbackValidator:
    """
    Validates execution outcomes before writing to semantic memory.

    Stateless. Thread-safe. Zero dependencies.

    Parameters
    ----------
    min_confidence : float
        Minimum confidence score required to accept feedback.
        Default: 0.60. Set higher (e.g. 0.70) for stricter filtering.
    """

    def __init__(self, min_confidence: float = _DEFAULT_MIN_CONFIDENCE) -> None:
        self._min_confidence = min_confidence

    def validate(self, result: Any) -> FeedbackDecision:
        """
        Validate an execution result for feedback acceptance.

        Args:
            result: Any object with .status, .confidence, .requires_hitl,
                    .output_data, .reasoning_trace attributes.

        Returns:
            FeedbackDecision(accepted=True)  — feedback is safe to write
            FeedbackDecision(accepted=False) — feedback must be rejected
        """
        confidence = float(getattr(result, "confidence", 0.0))

        # Check 1: status must be in accepted set
        status = str(getattr(result, "status", ""))
        if status not in _ACCEPTED_STATUSES:
            return FeedbackDecision(
                accepted=False,
                reason=f"non_accepted_status:{status}",
                confidence=confidence,
            )

        # Check 2: confidence threshold
        if confidence < self._min_confidence:
            return FeedbackDecision(
                accepted=False,
                reason=f"confidence_below_threshold:{confidence:.3f}<{self._min_confidence}",
                confidence=confidence,
            )

        # Check 3: HITL gate — human review not yet completed
        if getattr(result, "requires_hitl", False):
            return FeedbackDecision(
                accepted=False,
                reason="requires_hitl_pending",
                confidence=confidence,
            )

        # Check 4: required fields must be present and non-empty
        output_data = getattr(result, "output_data", None)
        if output_data is None:
            return FeedbackDecision(
                accepted=False,
                reason="missing_output_data",
                confidence=confidence,
            )

        reasoning_trace = getattr(result, "reasoning_trace", None)
        if reasoning_trace is None:
            return FeedbackDecision(
                accepted=False,
                reason="missing_reasoning_trace",
                confidence=confidence,
            )

        return FeedbackDecision(
            accepted=True,
            reason="all_checks_passed",
            confidence=confidence,
        )
