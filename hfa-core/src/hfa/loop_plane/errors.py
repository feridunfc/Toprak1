class LoopPlaneError(Exception): pass
class ModeViolation(LoopPlaneError): pass
class InvalidEventStream(LoopPlaneError): pass
class ReceiptConflict(LoopPlaneError): pass
class IdempotencyConflict(LoopPlaneError): pass
class DomainTransitionError(LoopPlaneError): pass
