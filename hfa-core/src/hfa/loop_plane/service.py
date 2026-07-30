from .model import LoopMode, CommandReceipt, canonical_hash, CanonicalCommandProposal
from .errors import ModeViolation
from .translation import RuntimeEventTranslator

class LoopPlaneService:
    def __init__(self, store, mode: LoopMode = LoopMode.OFF):
        self.store = store
        self.mode = mode
        self.translator = RuntimeEventTranslator()

    def observe(self, data: dict, expected_revision: int):
        if self.mode is LoopMode.OFF: raise ModeViolation('OFF')
        translated = self.translator.translate(data, expected_revision + 1)
        if not translated.event: return translated
        event = translated.event
        receipt = CommandReceipt(
            operation='RUNTIME_EVENT_OBSERVE', loop_id=event.loop_id,
            idempotency_key=data['event_id'], command_hash=canonical_hash(data),
            result_event_ids=(event.event_id,), resulting_revision=event.revision,
            causation_id=event.causation_id, correlation_id=event.correlation_id,
        )
        self.store.commit(event.loop_id, [event], receipt, expected_revision)
        return translated

    def evaluate(self):
        if self.mode not in {LoopMode.SHADOW_DECIDE, LoopMode.PROPOSE}: raise ModeViolation('evaluation forbidden')

    def proposal(self, **kwargs):
        if self.mode is not LoopMode.PROPOSE: raise ModeViolation('proposal forbidden')
        return CanonicalCommandProposal(**kwargs)
