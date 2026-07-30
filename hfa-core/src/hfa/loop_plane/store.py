from __future__ import annotations
from dataclasses import dataclass
from typing import Protocol
from .model import LoopEvent, CommandReceipt
from .reducer import rebuild, apply
from .errors import InvalidEventStream, ReceiptConflict, IdempotencyConflict

class DurableLoopStore(Protocol):
    def commit(self, loop_id: str, events: list[LoopEvent], receipt: CommandReceipt, expected_revision: int) -> CommandReceipt: ...
    def load_events(self, loop_id: str) -> list[LoopEvent]: ...

@dataclass
class InMemoryPrototypeLoopStore:
    def __post_init__(self):
        self._events = {}
        self._event_ids = set()
        self._receipts = {}

    def load_events(self, loop_id: str) -> list[LoopEvent]:
        return list(self._events.get(loop_id, ()))

    def load_state(self, loop_id: str):
        return rebuild(loop_id, self.load_events(loop_id))

    def commit(self, loop_id: str, events: list[LoopEvent], receipt: CommandReceipt, expected_revision: int) -> CommandReceipt:
        key = (receipt.operation, loop_id, receipt.idempotency_key)
        existing = self._receipts.get(key)
        if existing:
            if existing.command_hash == receipt.command_hash: return existing
            raise IdempotencyConflict('same idempotency key with different command')
        state = rebuild(loop_id, self.load_events(loop_id))
        if state.revision != expected_revision: raise InvalidEventStream('stale revision')
        ids = [event.event_id for event in events]
        if len(ids) != len(set(ids)) or any(event_id in self._event_ids for event_id in ids): raise InvalidEventStream('duplicate event id')
        if any(event.loop_id != loop_id for event in events): raise InvalidEventStream('wrong loop id')
        if ids != list(receipt.result_event_ids): raise ReceiptConflict('event id mismatch')
        final_revision = events[-1].revision if events else expected_revision
        if receipt.loop_id != loop_id or receipt.resulting_revision != final_revision: raise ReceiptConflict('loop/revision mismatch')
        if not receipt.operation or not receipt.idempotency_key or len(receipt.command_hash) != 64: raise ReceiptConflict('invalid receipt identity')
        if any(event.causation_id != receipt.causation_id or event.correlation_id != receipt.correlation_id for event in events): raise ReceiptConflict('causation/correlation mismatch')
        staged = state
        for event in events: staged = apply(staged, event)
        self._events.setdefault(loop_id, []).extend(events)
        self._event_ids.update(ids)
        self._receipts[key] = receipt
        return receipt
