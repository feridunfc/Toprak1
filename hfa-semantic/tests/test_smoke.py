from hfa_semantic.api.models import OutcomeType, RawOutcome
from hfa_semantic.validation.outcome_validator import OutcomeValidator


def test_validation_smoke():
    raw = RawOutcome(event_id="e1", outcome_type=OutcomeType.SUCCESS, confidence=0.9, timestamp_ms=1)
    decision = OutcomeValidator().validate(raw, now_ms=1000)
    assert decision.validated is True
