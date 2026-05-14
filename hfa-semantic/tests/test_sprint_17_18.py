"""
hfa-semantic/tests/test_sprint_17_18.py

Sprint 17-18 Tests: Memory Decay + Cost-Aware Policy + Feedback Loop
Sprint 10 fix: converted to async (append/weighted/size are now async).

Changes from original:
  * All memory.append() calls → await memory.append()
  * memory.weighted() → await memory.weighted()
  * memory.size (property) kept as-is (local path exposes sync .size)
  * memory.all() used for direct content inspection
  * assert memory.append(...) is True → kept (append now returns bool)
  * assert memory.append(dup) is False → new dedup semantics
  * Tests marked @pytest.mark.asyncio where async calls are made

Logic, assertions, and intent are UNCHANGED.
"""

import pytest
from hfa_semantic.api.models import (
    OutcomeType,
    ValidatedOutcome,
    ValidatorType,
    PolicyDecision,
)
from hfa_semantic.memory.semantic_memory_v2 import SemanticMemoryV2
from hfa_semantic.memory.feedback_loop import FeedbackLoop
from hfa_semantic.policy.cost_model import CostModel
from hfa_semantic.policy.engine_v2 import PolicyEngineV2, PolicyEngineV2Config


# ═══════════════════════════════════════════════════════════════════════════
# SPRINT 17 TESTS: Memory Decay
# ═══════════════════════════════════════════════════════════════════════════

@pytest.mark.asyncio
async def test_memory_decay_reduces_old_weight() -> None:
    """Old outcomes have less weight than new ones (decay)."""
    memory = SemanticMemoryV2(half_life_ms=1000)

    await memory.append(
        ValidatedOutcome(
            event_id="old_event",
            outcome_type=OutcomeType.FALSE_POSITIVE,
            validated=True,
            validator=ValidatorType.RULE,
            confidence=1.0,
            timestamp_ms=0,
        )
    )

    await memory.append(
        ValidatedOutcome(
            event_id="new_event",
            outcome_type=OutcomeType.FALSE_POSITIVE,
            validated=True,
            validator=ValidatorType.RULE,
            confidence=1.0,
            timestamp_ms=900,
        )
    )

    weighted_list = await memory.weighted(now_ms=1000)
    weighted_dict = {x.outcome.event_id: x for x in weighted_list}

    assert len(weighted_dict) == 2
    assert "old_event" in weighted_dict
    assert "new_event" in weighted_dict

    # Old outcome (age=1000ms) ~ 0.5 decay; new (age=100ms) ~ 0.93
    assert weighted_dict["old_event"].decay_weight < weighted_dict["new_event"].decay_weight
    assert weighted_dict["old_event"].decay_weight < 0.6


@pytest.mark.asyncio
async def test_memory_dedup_by_event_id() -> None:
    """Memory dedups by event_id (same event appended twice)."""
    memory = SemanticMemoryV2()

    outcome1 = ValidatedOutcome(
        event_id="e1",
        outcome_type=OutcomeType.SUCCESS,
        validated=True,
        validator=ValidatorType.RULE,
        confidence=0.8,
        timestamp_ms=1000,
    )
    outcome2 = ValidatedOutcome(
        event_id="e1",       # same event_id
        outcome_type=OutcomeType.FAILURE,
        validated=True,
        validator=ValidatorType.RULE,
        confidence=0.9,
        timestamp_ms=2000,
    )

    # First append: new write → True
    assert await memory.append(outcome1) is True
    # Second append: duplicate → False
    assert await memory.append(outcome2) is False

    # Size must still be 1 (dedup enforced)
    assert memory.size == 1

    # Content: the first write is kept (idempotent — second is skipped)
    items = memory.all()
    assert items[0].outcome_type == OutcomeType.SUCCESS


@pytest.mark.asyncio
async def test_memory_bounded_size() -> None:
    """Memory enforces max_items limit."""
    memory = SemanticMemoryV2(max_items=5)

    for i in range(10):
        await memory.append(
            ValidatedOutcome(
                event_id=f"e{i}",
                outcome_type=OutcomeType.SUCCESS,
                validated=True,
                validator=ValidatorType.RULE,
                confidence=0.9,
                timestamp_ms=1000 + i,
            )
        )

    assert memory.size == 5
    assert memory.utilization == 1.0


# ═══════════════════════════════════════════════════════════════════════════
# SPRINT 18 TESTS: Cost-Aware Policy + Feedback Loop
# ═══════════════════════════════════════════════════════════════════════════

def test_cost_model_false_negative_expensive() -> None:
    """False negatives weighted more than false positives in default model."""
    cost = CostModel()
    assert cost.false_negative_cost > cost.false_positive_cost


@pytest.mark.asyncio
async def test_policy_engine_v2_raises_on_false_negative_pressure() -> None:
    """High false negative cost triggers threshold lowering (more sensitive)."""
    memory = SemanticMemoryV2()

    for i in range(20):
        await memory.append(
            ValidatedOutcome(
                event_id=f"fn-{i}",
                outcome_type=OutcomeType.FALSE_NEGATIVE,
                validated=True,
                validator=ValidatorType.RULE,
                confidence=0.95,
                timestamp_ms=1000 + i,
            )
        )

    engine = PolicyEngineV2()
    config = PolicyEngineV2Config(
        policy_key="detection_threshold",
        current_value=85.0,
        minimum=60.0,
        maximum=95.0,
        cooldown_ms=0,
        min_sample_size=10,
        min_avg_weighted_confidence=0.10,
        lower_step_abs=2.0,
    )

    decision = engine.evaluate(config, memory, now_ms=10_000)

    assert decision.proposed_value < config.current_value
    assert "false_negative_pressure" in decision.reason


@pytest.mark.asyncio
async def test_policy_engine_v2_lowers_on_false_positive_pressure() -> None:
    """High false positive cost triggers threshold raising (more strict)."""
    memory = SemanticMemoryV2()

    for i in range(20):
        await memory.append(
            ValidatedOutcome(
                event_id=f"fp-{i}",
                outcome_type=OutcomeType.FALSE_POSITIVE,
                validated=True,
                validator=ValidatorType.RULE,
                confidence=0.95,
                timestamp_ms=1000 + i,
            )
        )

    engine = PolicyEngineV2()
    config = PolicyEngineV2Config(
        policy_key="detection_threshold",
        current_value=50.0,
        minimum=40.0,
        maximum=95.0,
        cooldown_ms=0,
        min_sample_size=10,
        min_avg_weighted_confidence=0.10,
        raise_step_abs=2.0,
    )

    decision = engine.evaluate(config, memory, now_ms=10_000)

    assert decision.proposed_value > config.current_value
    assert "false_positive_pressure" in decision.reason


def test_feedback_loop_tracks_decision() -> None:
    """Feedback loop links outcomes back to decisions."""
    loop = FeedbackLoop()

    decision = PolicyDecision(
        decision_id="dec-001",
        policy_key="cpu_threshold",
        previous_value=80.0,
        proposed_value=82.0,
        approved=True,
        reason="cost_model:false_negative_pressure",
    )
    loop.register_decision(decision)

    outcome = ValidatedOutcome(
        event_id="e1",
        decision_id="dec-001",
        outcome_type=OutcomeType.SUCCESS,
        validated=True,
        validator=ValidatorType.RULE,
        confidence=0.9,
        timestamp_ms=2000,
    )

    assert loop.attach_outcome(outcome) is True

    links = loop.links_for_decision("dec-001")
    assert len(links) == 1
    assert links[0].event_id == "e1"


def test_feedback_loop_rejects_orphan_outcome() -> None:
    """Outcome with unknown decision_id is rejected."""
    loop = FeedbackLoop()

    outcome = ValidatedOutcome(
        event_id="e1",
        decision_id="unknown-dec",
        outcome_type=OutcomeType.SUCCESS,
        validated=True,
        validator=ValidatorType.RULE,
        confidence=0.9,
        timestamp_ms=1000,
    )

    assert loop.attach_outcome(outcome) is False


@pytest.mark.asyncio
async def test_policy_decision_has_unique_id() -> None:
    """Each policy decision gets unique decision_id."""
    memory = SemanticMemoryV2()

    for i in range(5):
        await memory.append(
            ValidatedOutcome(
                event_id=f"e{i}",
                outcome_type=OutcomeType.FALSE_POSITIVE,
                validated=True,
                validator=ValidatorType.RULE,
                confidence=0.9,
                timestamp_ms=1000 + i,
            )
        )

    engine = PolicyEngineV2()
    config = PolicyEngineV2Config(
        policy_key="threshold",
        current_value=80.0,
        minimum=60.0,
        maximum=95.0,
        cooldown_ms=0,
        min_sample_size=3,
        min_avg_weighted_confidence=0.05,
    )

    decision1 = engine.evaluate(config, memory, now_ms=2000)
    decision2 = engine.evaluate(config, memory, now_ms=3000)

    assert decision1.decision_id != decision2.decision_id
    assert len(decision1.decision_id) > 0
    assert len(decision2.decision_id) > 0
