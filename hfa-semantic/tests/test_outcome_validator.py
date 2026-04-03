"""
hfa-semantic/tests/test_outcome_validator.py

Sprint 15 — Outcome Validator Tests
"""

import pytest
from hfa_semantic.api.models import OutcomeType, RawOutcome, ValidatorType
from hfa_semantic.validation.outcome_validator import OutcomeValidator


def test_validator_rule_accepts_high_confidence_success() -> None:
    """High-confidence success is immediately validated by rule."""
    validator = OutcomeValidator(min_confidence=0.80)
    outcome = RawOutcome(
        event_id="e1",
        outcome_type=OutcomeType.SUCCESS,
        confidence=0.95,
        timestamp_ms=1_000,
    )
    decision = validator.validate(outcome, now_ms=2_000)
    assert decision.validated is True
    assert decision.validator == ValidatorType.RULE


def test_validator_rejects_low_confidence() -> None:
    """Low-confidence outcome escalates to human."""
    validator = OutcomeValidator(min_confidence=0.80)
    outcome = RawOutcome(
        event_id="e2",
        outcome_type=OutcomeType.SUCCESS,
        confidence=0.50,
        timestamp_ms=1_000,
    )
    decision = validator.validate(outcome, now_ms=2_000)
    assert decision.validated is False
    assert decision.validator == ValidatorType.HUMAN


def test_validator_delayed_signal_false_positive() -> None:
    """False positive validated after delay."""
    validator = OutcomeValidator(
        min_confidence=0.60,
        delayed_signal_ms=100_000,
    )
    outcome = RawOutcome(
        event_id="e3",
        outcome_type=OutcomeType.FALSE_POSITIVE,
        confidence=0.75,
        timestamp_ms=1_000,
    )
    # Too recent
    decision_early = validator.validate(outcome, now_ms=50_000)
    assert decision_early.validated is False

    # After delay
    decision_late = validator.validate(outcome, now_ms=200_000)
    assert decision_late.validated is True
    assert decision_late.validator == ValidatorType.DELAYED_SIGNAL


def test_validator_stats() -> None:
    """Validator tracks metrics."""
    validator = OutcomeValidator()
    
    # Process several outcomes
    for i in range(5):
        outcome = RawOutcome(
            event_id=f"e{i}",
            outcome_type=OutcomeType.SUCCESS,
            confidence=0.90,
            timestamp_ms=1_000 + i,
        )
        validator.validate(outcome, now_ms=2_000)
    
    stats = validator.stats
    assert stats["validated_total"] == 5
    assert stats["human_review_escalated"] >= 0

