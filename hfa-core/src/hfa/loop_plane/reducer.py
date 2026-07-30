from dataclasses import replace
from .errors import DomainTransitionError, InvalidEventStream
from .model import LoopEvent, Recommendation
from .state import LoopState

ALLOWED_EVENT_TYPES = {
    "LoopStarted",
    "RuntimeObservationRecorded",
    "AttemptObserved",
    "CriteriaEvaluated",
    "ShadowDecisionRecorded",
    "ProposalRecorded",
    "TimeoutRecorded",
}


def apply(state: LoopState, event: LoopEvent) -> LoopState:
    if event.event_type not in ALLOWED_EVENT_TYPES:
        raise InvalidEventStream("unknown event type")
    if event.loop_id != state.loop_id:
        raise InvalidEventStream("wrong loop")
    if event.revision != state.revision + 1:
        raise InvalidEventStream("noncontiguous revision")
    p = event.payload

    if event.event_type == "LoopStarted":
        if state.started:
            raise DomainTransitionError("second LoopStarted")
        contract = p.get("contract")
        if contract is None:
            raise DomainTransitionError("contract required")
        return replace(state, revision=event.revision, started=True, contract=contract, run_id=p["run_id"], task_id=p["task_id"])

    if not state.started:
        raise DomainTransitionError("loop not started")
    if state.closed:
        raise DomainTransitionError("closed loop")

    if event.event_type == "RuntimeObservationRecorded":
        return replace(state, revision=event.revision)

    if event.event_type == "AttemptObserved":
        if state.contract is None:
            raise DomainTransitionError("contract unavailable")
        if state.attempt_count >= state.contract.max_attempts:
            raise DomainTransitionError("max attempts")
        return replace(state, revision=event.revision, current_attempt_id=p["attempt_id"], current_input_hash=p["input_hash"], attempt_count=state.attempt_count + 1, decision_event_id=None, latest_recommendation=None)

    if event.event_type in {"CriteriaEvaluated", "ShadowDecisionRecorded", "ProposalRecorded"}:
        if not state.current_attempt_id:
            raise DomainTransitionError("attempt required")
        if p.get("attempt_id") != state.current_attempt_id:
            raise DomainTransitionError("wrong attempt")

    if event.event_type == "CriteriaEvaluated":
        return replace(state, revision=event.revision)

    if event.event_type == "ShadowDecisionRecorded":
        recommendation = Recommendation(p["recommendation"])
        if state.decision_event_id is not None:
            raise DomainTransitionError("decision already recorded for current attempt")
        closed = recommendation in {Recommendation.ACCEPT, Recommendation.ESCALATE}
        rework_depth = state.rework_depth + (1 if recommendation is Recommendation.REWORK_RECOMMENDED else 0)
        if state.contract and rework_depth > state.contract.max_rework_depth:
            raise DomainTransitionError("max rework depth")
        return replace(state, revision=event.revision, latest_recommendation=recommendation, decision_event_id=event.event_id, decision_count=state.decision_count + 1, closed=closed, closure_reason=recommendation.value if closed else None, rework_depth=rework_depth)

    if event.event_type == "ProposalRecorded":
        if state.decision_event_id is None:
            raise DomainTransitionError("committed decision required")
        if p.get("decision_event_id") != state.decision_event_id:
            raise DomainTransitionError("proposal decision mismatch")
        if p.get("recommendation") != state.latest_recommendation.value:
            raise DomainTransitionError("proposal recommendation mismatch")
        return replace(state, revision=event.revision)

    if event.event_type == "TimeoutRecorded":
        return replace(state, revision=event.revision)

    raise InvalidEventStream("unhandled event type")


def rebuild(loop_id: str, events: list[LoopEvent]) -> LoopState:
    state = LoopState(loop_id)
    for event in events:
        state = apply(state, event)
    return state
