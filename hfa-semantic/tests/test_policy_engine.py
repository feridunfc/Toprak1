"""
hfa-semantic/tests/test_policy_engine.py

Sprint 16 — Policy Engine Tests
"""

import pytest
from hfa_semantic.api.models import OutcomeType, ValidatedOutcome, ValidatorType
from hfa_semantic.policy.engine import PolicyEngine, PolicyEngineConfig


def test_policy_engine_rejects_insufficient_samples() -> None:
    """Engine requires minimum sample size."""
    engine = PolicyEngine()
    config = PolicyEngineConfig(
        policy_key="cpu_threshold",
        current_value=85.0,
        minimum=60.0,
        maximum=95.0,
        cooldown_ms=0,
        min_sample_size=20,
    )
    outcomes = []  # Too few
    decision = engine.evaluate(config, outcomes, now_ms=1_000)
    assert decision.approved is False
    assert "insufficient" in decision.reason


def test_policy_engine_raises_on_high_false_positive() -> None:
    """High FP rate triggers threshold increase."""
    engine = PolicyEngine()
    config = PolicyEngineConfig(
        policy_key="cpu_threshold",
        current_value=85.0,
        minimum=60.0,
        maximum=95.0,
        cooldown_ms=0,
        min_sample_size=5,
        min_avg_confidence=0.70,
        max_step_abs=5.0,
    )
    
    # 3 FP, 2 success = 60% FP rate
    outcomes = [
        ValidatedOutcome(
            event_id="1",
            outcome_type=OutcomeType.FALSE_POSITIVE,
            validated=True,
            validator=ValidatorType.RULE,
            confidence=0.9,
            timestamp_ms=1,
        ),
        ValidatedOutcome(
            event_id="2",
            outcome_type=OutcomeType.FALSE_POSITIVE,
            validated=True,
            validator=ValidatorType.RULE,
            confidence=0.9,
            timestamp_ms=2,
        ),
        ValidatedOutcome(
            event_id="3",
            outcome_type=OutcomeType.FALSE_POSITIVE,
            validated=True,
            validator=ValidatorType.RULE,
            confidence=0.9,
            timestamp_ms=3,
        ),
        ValidatedOutcome(
            event_id="4",
            outcome_type=OutcomeType.SUCCESS,
            validated=True,
            validator=ValidatorType.RULE,
            confidence=0.9,
            timestamp_ms=4,
        ),
        ValidatedOutcome(
            event_id="5",
            outcome_type=OutcomeType.SUCCESS,
            validated=True,
            validator=ValidatorType.RULE,
            confidence=0.9,
            timestamp_ms=5,
        ),
    ]
    
    decision = engine.evaluate(config, outcomes, now_ms=10_000)
    assert decision.proposed_value > config.current_value


def test_policy_engine_respects_bounds() -> None:
    """Proposed change is clamped to bounds."""
    engine = PolicyEngine()
    config = PolicyEngineConfig(
        policy_key="threshold",
        current_value=90.0,
        minimum=60.0,
        maximum=95.0,
        cooldown_ms=0,
        min_sample_size=5,
        raise_step_abs=20.0,  # Will try to go to 110, but clamped to 95
    )
    
    # All false positives
    outcomes = [
        ValidatedOutcome(
            event_id=f"{i}",
            outcome_type=OutcomeType.FALSE_POSITIVE,
            validated=True,
            validator=ValidatorType.RULE,
            confidence=0.9,
            timestamp_ms=i,
        )
        for i in range(5)
    ]
    
    decision = engine.evaluate(config, outcomes, now_ms=1_000)
    # Should be clamped to maximum
    assert decision.proposed_value <= config.maximum


def test_policy_engine_requires_hitl_on_large_change() -> None:
    """Large changes require human approval."""
    engine = PolicyEngine()
    config = PolicyEngineConfig(
        policy_key="threshold",
        current_value=85.0,
        minimum=60.0,
        maximum=95.0,
        cooldown_ms=0,
        min_sample_size=5,
        min_avg_confidence=0.70,
        max_step_abs=5.0,
        hitl_step_abs=3.0,  # Require HITL for changes >= 3.0
        raise_step_abs=5.0,  # Will propose +5.0
    )
    
    outcomes = [
        ValidatedOutcome(
            event_id="1",
            outcome_type=OutcomeType.FALSE_POSITIVE,
            validated=True,
            validator=ValidatorType.RULE,
            confidence=0.9,
            timestamp_ms=i,
        )
        for i in range(5)
    ]
    
    decision = engine.evaluate(config, outcomes, now_ms=1_000)
    # Change is 5.0, which >= 3.0 threshold
    assert decision.requires_hitl is True
    assert decision.approved is False


def test_policy_engine_cooldown() -> None:
    """Cooldown prevents rapid consecutive changes."""
    engine = PolicyEngine()
    last_change = 1_000
    now = 2_000
    cooldown_ms = 86_400_000  # 24 hours
    
    config = PolicyEngineConfig(
        policy_key="threshold",
        current_value=85.0,
        minimum=60.0,
        maximum=95.0,
        cooldown_ms=cooldown_ms,
        last_change_ms=last_change,  # Just changed
        min_sample_size=5,
    )
    
    outcomes = [
        ValidatedOutcome(
            event_id=f"{i}",
            outcome_type=OutcomeType.FALSE_POSITIVE,
            validated=True,
            validator=ValidatorType.RULE,
            confidence=0.9,
            timestamp_ms=i,
        )
        for i in range(5)
    ]
    
    decision = engine.evaluate(config, outcomes, now_ms=now)
    # Too soon after last change
    assert decision.approved is False
    assert decision.cooldown_applied is True

