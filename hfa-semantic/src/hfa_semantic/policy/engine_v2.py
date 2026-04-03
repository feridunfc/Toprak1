"""
hfa-semantic/src/hfa_semantic/policy/engine_v2.py

Sprint 10 — PolicyEngineV2 (Cost-Aware, Decay-Weighted)

Canonical policy engine. Uses exponential time-decay + cost model to
decide when to raise or lower a policy threshold.

engine.py re-exports from here for backward compat.
Tests import directly from this module.

Public API:
    engine = PolicyEngineV2()
    decision = engine.evaluate(config, memory, now_ms=...)
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from typing import Optional

from hfa_semantic.api.models import OutcomeType, PolicyDecision
from hfa_semantic.memory.semantic_memory_v2 import SemanticMemoryV2
from hfa_semantic.policy.cost_model import CostModel


@dataclass(slots=True, frozen=True)
class PolicyEngineV2Config:
    """Configuration for PolicyEngineV2."""
    policy_key:     str
    current_value:  float
    minimum:        float
    maximum:        float
    cooldown_ms:    int

    last_change_ms:              Optional[int]  = None
    min_sample_size:             int   = 10
    min_avg_weighted_confidence: float = 0.50
    max_step_abs:                float = 5.0
    raise_step_abs:              float = 2.0
    lower_step_abs:              float = 2.0
    hitl_step_abs:               float = 3.0
    fn_pressure_threshold:       float = 0.20  # FN ratio above this triggers lower
    fp_pressure_threshold:       float = 0.30  # FP ratio above this triggers raise


class PolicyEngineV2:
    """
    Cost-aware, decay-weighted policy engine.

    Evaluates semantic memory outcomes and proposes threshold adjustments
    based on false positive / false negative pressure.

    Sprint 10: evaluate() accepts both sync SemanticMemoryV2 (local fallback)
    and async Redis-backed instances. For sync (in-memory) instances, call
    evaluate() directly. For async (Redis-backed), call await evaluate_async().
    """

    def __init__(self, cost_model: CostModel | None = None) -> None:
        self._cost = cost_model or CostModel()

    # ── Sync path (in-memory SemanticMemoryV2, dev/test) ─────────────────────

    def evaluate(
        self,
        config: PolicyEngineV2Config,
        memory: SemanticMemoryV2,
        now_ms: int,
    ) -> PolicyDecision:
        """
        Evaluate policy against in-memory outcomes.

        Sprint 10 note: This sync version works with memory._local_weighted()
        directly for test compatibility. For Redis-backed memory, use
        evaluate_async().
        """
        # Use local weighted outcomes (sync path — dev/test only)
        outcomes = memory._local_weighted(now_ms)
        return self._compute_decision(config, outcomes, now_ms)

    # ── Async path (Redis-backed SemanticMemoryV2, production) ───────────────

    async def evaluate_async(
        self,
        config: PolicyEngineV2Config,
        memory: SemanticMemoryV2,
        now_ms: int,
    ) -> PolicyDecision:
        """
        Evaluate policy against Redis-backed outcomes.
        """
        outcomes = await memory.weighted(now_ms)
        return self._compute_decision(config, outcomes, now_ms)

    # ── Core logic ────────────────────────────────────────────────────────────

    def _compute_decision(self, config: PolicyEngineV2Config, outcomes, now_ms: int) -> PolicyDecision:
        decision_id = f"dec-{uuid.uuid4().hex[:12]}"

        if len(outcomes) < config.min_sample_size:
            return PolicyDecision(
                decision_id=decision_id,
                policy_key=config.policy_key,
                previous_value=config.current_value,
                proposed_value=config.current_value,
                approved=False,
                reason=f"insufficient_samples:{len(outcomes)}<{config.min_sample_size}",
            )

        # Cooldown check
        if (
            config.last_change_ms is not None
            and config.cooldown_ms > 0
            and (now_ms - config.last_change_ms) < config.cooldown_ms
        ):
            return PolicyDecision(
                decision_id=decision_id,
                policy_key=config.policy_key,
                previous_value=config.current_value,
                proposed_value=config.current_value,
                approved=False,
                reason="cooldown_active",
            )

        # Confidence gate
        if outcomes:
            avg_conf = sum(o.weighted_confidence for o in outcomes) / len(outcomes)
            if avg_conf < config.min_avg_weighted_confidence:
                return PolicyDecision(
                    decision_id=decision_id,
                    policy_key=config.policy_key,
                    previous_value=config.current_value,
                    proposed_value=config.current_value,
                    approved=False,
                    reason=f"low_avg_confidence:{avg_conf:.3f}",
                )

        # Count pressures
        fn_count = sum(
            1 for o in outcomes
            if o.outcome.outcome_type == OutcomeType.FALSE_NEGATIVE
        )
        fp_count = sum(
            1 for o in outcomes
            if o.outcome.outcome_type == OutcomeType.FALSE_POSITIVE
        )
        total = len(outcomes)

        fn_ratio = fn_count / total if total else 0.0
        fp_ratio = fp_count / total if total else 0.0

        proposed = config.current_value
        reason = "no_policy_signal"

        if fn_ratio > config.fn_pressure_threshold:
            # Many false negatives → lower threshold (more sensitive)
            proposed = config.current_value - config.lower_step_abs
            reason = f"false_negative_pressure:fn_ratio={fn_ratio:.3f}"
        elif fp_ratio > config.fp_pressure_threshold:
            # Many false positives → raise threshold (more strict)
            proposed = config.current_value + config.raise_step_abs
            reason = f"false_positive_pressure:fp_ratio={fp_ratio:.3f}"

        # Clamp to bounds
        proposed = max(config.minimum, min(config.maximum, proposed))

        approved = proposed != config.current_value
        return PolicyDecision(
            decision_id=decision_id,
            policy_key=config.policy_key,
            previous_value=config.current_value,
            proposed_value=proposed,
            approved=approved,
            reason=reason,
        )
