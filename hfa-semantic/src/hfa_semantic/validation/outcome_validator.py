from __future__ import annotations

from hfa_semantic.api.models import OutcomeType, RawOutcome, ValidatorType, ValidatedOutcome
from hfa_semantic.validation.delayed_signal import delayed_truth_available
from hfa_semantic.validation.validation_contract import ValidationDecision


class OutcomeValidator:
    def __init__(self, min_confidence: float = 0.80, delayed_signal_ms: int = 300_000) -> None:
        self._min_confidence = min_confidence
        self._delayed_signal_ms = delayed_signal_ms
        self._validated_total: int = 0
        self._human_review_escalated: int = 0

    def validate(self, outcome: RawOutcome, now_ms: int) -> ValidationDecision:
        self._validated_total += 1
        age_ms = max(0, now_ms - outcome.timestamp_ms)

        if outcome.outcome_type in {OutcomeType.SUCCESS, OutcomeType.FAILURE} \
                and outcome.confidence >= self._min_confidence:
            return ValidationDecision(True, ValidatorType.RULE, outcome.confidence, "rule_validated_immediately")

        if outcome.outcome_type in {OutcomeType.FALSE_POSITIVE, OutcomeType.FALSE_NEGATIVE} \
                and delayed_truth_available(age_ms, self._delayed_signal_ms):
            return ValidationDecision(True, ValidatorType.DELAYED_SIGNAL, outcome.confidence, "validated_by_delayed_signal_window")

        self._human_review_escalated += 1
        return ValidationDecision(False, ValidatorType.HUMAN, outcome.confidence, "human_review_requested")

    def to_validated(self, raw: RawOutcome, decision: ValidationDecision) -> ValidatedOutcome:
        return ValidatedOutcome(
            event_id=raw.event_id,
            lineage_run_id=raw.lineage_run_id,
            decision_id=raw.decision_id,
            outcome_type=raw.outcome_type,
            validated=decision.validated,
            validator=decision.validator,
            confidence=decision.confidence,
            timestamp_ms=raw.timestamp_ms,
            notes=decision.notes,
        )

    @property
    def stats(self) -> dict:
        return {
            "validated_total": self._validated_total,
            "human_review_escalated": self._human_review_escalated,
        }
