from .model import TranslationResult, TranslationStatus, LoopEvent, canonical_hash

SUPPORTED = {
    'TaskAdmitted': 'RuntimeObservationRecorded',
    'TaskClaimed': 'AttemptObserved',
    'TaskCompleted': 'CriteriaEvaluated',
    'TaskFailed': 'CriteriaEvaluated',
    'RunFinalized': 'CriteriaEvaluated',
}
REQUIRED = ('event_id','event_type','run_id','task_id','canonical_operation_id','source_transition_id','source_task_revision','occurred_at_ms','correlation_id','causation_id')

class RuntimeEventTranslator:
    def translate(self, data: dict, revision: int) -> TranslationResult:
        event_type = data.get('event_type')
        if event_type not in SUPPORTED: return TranslationResult(TranslationStatus.UNSUPPORTED_EVENT_TYPE, errors=('event_type',))
        missing = tuple(key for key in REQUIRED if data.get(key) in (None, ''))
        if missing: return TranslationResult(TranslationStatus.MISSING_REQUIRED_FIELD, errors=missing)
        if not isinstance(data['source_task_revision'], int): return TranslationResult(TranslationStatus.INVALID_REVISION, errors=('source_task_revision',))
        if not isinstance(data['occurred_at_ms'], int) or data['occurred_at_ms'] < 0: return TranslationResult(TranslationStatus.INVALID_TIMESTAMP, errors=('occurred_at_ms',))
        loop_id = data.get('loop_id') or f"loop:{data['run_id']}"
        attempt_id = data.get('attempt_id') or (f"attempt:{data['task_id']}:{data.get('execution_generation', 0)}" if event_type != 'TaskAdmitted' else None)
        payload = {key: value for key, value in data.items() if key not in {'event_id','event_type','occurred_at_ms','correlation_id','causation_id','loop_id'}}
        if attempt_id: payload['attempt_id'] = attempt_id
        payload['source_event_hash'] = canonical_hash(data)
        event = LoopEvent(
            event_id=f"loop-event:{data['event_id']}", loop_id=loop_id, revision=revision,
            event_type=SUPPORTED[event_type], occurred_at_ms=data['occurred_at_ms'],
            causation_id=data['causation_id'], correlation_id=data['correlation_id'], payload=payload,
        )
        return TranslationResult(TranslationStatus.TRANSLATED, event)
