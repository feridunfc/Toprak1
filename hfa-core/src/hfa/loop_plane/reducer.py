from dataclasses import replace
from .model import LoopEvent, Recommendation
from .state import LoopState
from .errors import InvalidEventStream, DomainTransitionError

ALLOWED = {'LoopStarted','RuntimeObservationRecorded','AttemptObserved','CriteriaEvaluated','ShadowDecisionRecorded','ProposalRecorded','TimeoutRecorded'}
TERMINAL = {Recommendation.ACCEPT, Recommendation.ESCALATE}

def apply(state: LoopState, event: LoopEvent) -> LoopState:
    if event.event_type not in ALLOWED: raise InvalidEventStream('unknown event type')
    if event.revision != state.revision + 1: raise InvalidEventStream('noncontiguous revision')
    if event.loop_id != state.loop_id: raise InvalidEventStream('wrong loop')
    p = event.payload
    if event.event_type == 'LoopStarted':
        if state.started: raise DomainTransitionError('second start')
        return replace(state, revision=event.revision, started=True, contract=p['contract'], run_id=p['run_id'], task_id=p['task_id'])
    if event.event_type == 'RuntimeObservationRecorded':
        return replace(state, revision=event.revision)
    if not state.started: raise DomainTransitionError('loop not started')
    if state.closed: raise DomainTransitionError('closed loop')
    if event.event_type == 'AttemptObserved':
        if state.contract and state.attempt_count >= state.contract.max_attempts: raise DomainTransitionError('max attempts')
        return replace(state, revision=event.revision, current_attempt_id=p['attempt_id'], attempt_count=state.attempt_count + 1)
    if event.event_type in {'CriteriaEvaluated','ShadowDecisionRecorded','ProposalRecorded'} and not state.current_attempt_id: raise DomainTransitionError('attempt required')
    if event.event_type in {'CriteriaEvaluated','ShadowDecisionRecorded','ProposalRecorded'} and p.get('attempt_id') != state.current_attempt_id: raise DomainTransitionError('wrong attempt')
    if event.event_type == 'ShadowDecisionRecorded':
        recommendation = Recommendation(p['recommendation'])
        return replace(state, revision=event.revision, terminal_recommendation=recommendation, closed=recommendation in TERMINAL)
    return replace(state, revision=event.revision)

def rebuild(loop_id: str, events: list[LoopEvent]) -> LoopState:
    state = LoopState(loop_id)
    for event in events: state = apply(state, event)
    return state
