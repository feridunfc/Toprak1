"""
hfa-semantic/src/hfa_semantic/policy/engine.py

STATUS: PolicyEngine (v1) is deprecated for new code.
        PolicyEngineV2 is the canonical implementation.

Sprint 6.2:
  * PolicyEngine (v1) is preserved — test_policy_engine.py uses it directly
    and it embodies a different evaluation strategy (raw confidence gates vs
    cost-model decay weighting). Both are valid; v2 is preferred for new code.
  * This module now also re-exports PolicyEngineV2 so callers can import
    either engine from one place.

MIGRATION (for new code):
    # Old
    from hfa_semantic.policy.engine import PolicyEngine, PolicyEngineConfig

    # New (cost-aware, decay-weighted)
    from hfa_semantic.policy.engine_v2 import PolicyEngineV2, PolicyEngineV2Config
    # or equivalently:
    from hfa_semantic.policy.engine import PolicyEngineV2, PolicyEngineV2Config
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass

from hfa_semantic.api.models import OutcomeType, PolicyDecision, ValidatedOutcome
from hfa_semantic.policy.hitl_gate import requires_hitl
from hfa_semantic.policy.optimizer import propose_threshold
from hfa_semantic.policy.rules import policy_reason
from hfa_semantic.policy.safety_guard import guard_policy_change

# Re-export canonical v2 from this module for convenience
from hfa_semantic.policy.engine_v2 import (  # noqa: F401
    PolicyEngineV2,
    PolicyEngineV2Config,
)


@dataclass(slots=True, frozen=True)
class PolicyEngineConfig:
    """Configuration for PolicyEngine (v1). Use PolicyEngineV2Config for new code."""

    policy_key: str
    current_value: float
    minimum: float
    maximum: float
    cooldown_ms: int

    last_change_ms: int | None = None
    min_sample_size: int = 20
    min_avg_confidence: float = 0.80
    max_step_abs: float = 5.0
    hitl_step_abs: float = 3.0
    raise_step_abs: float = 2.0
    lower_step_abs: float = 2.0


class PolicyEngine:
    """
    Bounded adaptive policy engine (v1).

    Deprecated for new code — prefer PolicyEngineV2 which uses
    cost-model weighting and exponential decay via SemanticMemoryV2.

    Preserved because test_policy_engine.py tests this implementation
    and the raw-confidence evaluation strategy remains valid.
    """

    def evaluate(
        self,
        config: PolicyEngineConfig,
        outcomes: list[ValidatedOutcome],
        now_ms: int,
    ) -> PolicyDecision:
        validated = [o for o in outcomes if o.validated]

        if len(validated) < config.min_sample_size:
            return PolicyDecision(
                decision_id=f"dec-{uuid.uuid4().hex[:12]}",
                policy_key=config.policy_key,
                previous_value=config.current_value,
                proposed_value=config.current_value,
                approved=False,
                reason="insufficient_validated_outcomes",
                requires_hitl=False,
                cooldown_applied=False,
            )

        avg_conf = sum(o.confidence for o in validated) / len(validated)
        if avg_conf < config.min_avg_confidence:
            return PolicyDecision(
                decision_id=f"dec-{uuid.uuid4().hex[:12]}",
                policy_key=config.policy_key,
                previous_value=config.current_value,
                proposed_value=config.current_value,
                approved=False,
                reason="validated_outcomes_confidence_too_low",
                requires_hitl=False,
                cooldown_applied=False,
            )

        fp_rate = sum(
            1 for o in validated if o.outcome_type == OutcomeType.FALSE_POSITIVE
        ) / len(validated)
        fn_rate = sum(
            1 for o in validated if o.outcome_type == OutcomeType.FALSE_NEGATIVE
        ) / len(validated)

        proposed = propose_threshold(
            current_value=config.current_value,
            false_positive_rate=fp_rate,
            false_negative_rate=fn_rate,
            raise_step_abs=config.raise_step_abs,
            lower_step_abs=config.lower_step_abs,
        )

        reason = policy_reason(fp_rate, fn_rate)
        if proposed == config.current_value:
            return PolicyDecision(
                decision_id=f"dec-{uuid.uuid4().hex[:12]}",
                policy_key=config.policy_key,
                previous_value=config.current_value,
                proposed_value=proposed,
                approved=False,
                reason=reason,
                requires_hitl=False,
                cooldown_applied=False,
            )

        guarded_value, allowed, guard_tags = guard_policy_change(
            proposed_value=proposed,
            current_value=config.current_value,
            minimum=config.minimum,
            maximum=config.maximum,
            now_ms=now_ms,
            last_change_ms=config.last_change_ms,
            cooldown_ms=config.cooldown_ms,
            max_step_abs=config.max_step_abs,
        )

        if not allowed:
            return PolicyDecision(
                decision_id=f"dec-{uuid.uuid4().hex[:12]}",
                policy_key=config.policy_key,
                previous_value=config.current_value,
                proposed_value=config.current_value,
                approved=False,
                reason="guard_blocked",
                requires_hitl=False,
                cooldown_applied="guard:cooldown_active" in guard_tags,
                tags=guard_tags,
            )

        delta_abs = abs(guarded_value - config.current_value)
        hitl = requires_hitl(delta_abs, config.hitl_step_abs)

        if hitl:
            return PolicyDecision(
                decision_id=f"dec-{uuid.uuid4().hex[:12]}",
                policy_key=config.policy_key,
                previous_value=config.current_value,
                proposed_value=guarded_value,
                approved=False,
                reason=reason,
                requires_hitl=True,
                cooldown_applied=False,
                tags=guard_tags + ["requires_human_approval"],
            )

        return PolicyDecision(
                decision_id=f"dec-{uuid.uuid4().hex[:12]}",
            policy_key=config.policy_key,
            previous_value=config.current_value,
            proposed_value=guarded_value,
            approved=True,
            reason=reason,
            requires_hitl=False,
            cooldown_applied=False,
            tags=guard_tags,
        )
