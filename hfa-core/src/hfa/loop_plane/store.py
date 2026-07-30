from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from .errors import IdempotencyConflict, InvalidEventStream, ReceiptConflict
from .model import CommandReceipt, LoopContract, LoopEvent
from .reducer import apply, rebuild

SUPPORTED_OPERATIONS = {"RUNTIME_EVENT_OBSERVE", "SHADOW_EVALUATE", "PASSIVE_PROPOSAL"}


class DurableLoopStore(Protocol):
    def commit(self, loop_id: str, events: list[LoopEvent], receipt: CommandReceipt, expected_revision: int) -> CommandReceipt: ...
    def load_events(self, loop_id: str) -> list[LoopEvent]: ...
    def load_contract(self, loop_id: str) -> LoopContract | None: ...


@dataclass
class InMemoryPrototypeLoopStore:
    def __post_init__(self) -> None:
        self._events: dict[str, list[LoopEvent]] = {}
        self._event_ids: set[str] = set()
        self._receipts: dict[tuple[str, str, str], CommandReceipt] = {}
        self._contracts: dict[str, LoopContract] = {}

    def load_events(self, loop_id: str) -> list[LoopEvent]:
        return list(self._events.get(loop_id, ()))

    def load_state(self, loop_id: str):
        return rebuild(loop_id, self.load_events(loop_id))

    def load_contract(self, loop_id: str) -> LoopContract | None:
        return self._contracts.get(loop_id)

    def load_receipt(self, operation: str, loop_id: str, idempotency_key: str) -> CommandReceipt | None:
        return self._receipts.get((operation, loop_id, idempotency_key))

    def find_event(self, loop_id: str, event_id: str) -> LoopEvent | None:
        return next((event for event in self._events.get(loop_id, ()) if event.event_id == event_id), None)

    def snapshot_counts(self) -> tuple[int, int, int]:
        return (sum(len(value) for value in self._events.values()), len(self._receipts), len(self._contracts))

    def commit(self, loop_id: str, events: list[LoopEvent], receipt: CommandReceipt, expected_revision: int) -> CommandReceipt:
        if not events:
            raise InvalidEventStream("empty event batch forbidden")
        key = (receipt.operation, loop_id, receipt.idempotency_key)
        existing = self._receipts.get(key)
        if existing is not None:
            if existing == receipt:
                return existing
            if existing.command_hash == receipt.command_hash:
                raise ReceiptConflict("stored receipt result contract differs")
            raise IdempotencyConflict("same idempotency key with different command")

        if receipt.operation not in SUPPORTED_OPERATIONS:
            raise ReceiptConflict("unsupported operation")
        if receipt.loop_id != loop_id:
            raise ReceiptConflict("receipt loop mismatch")

        state = rebuild(loop_id, self.load_events(loop_id))
        if state.revision != expected_revision:
            raise InvalidEventStream("stale revision")

        ids = [event.event_id for event in events]
        if ids != list(receipt.result_event_ids):
            raise ReceiptConflict("event id mismatch")
        if len(ids) != len(set(ids)) or any(event_id in self._event_ids for event_id in ids):
            raise InvalidEventStream("duplicate event id")
        if any(event.loop_id != loop_id for event in events):
            raise InvalidEventStream("wrong loop id")
        expected_revisions = list(range(expected_revision + 1, expected_revision + len(events) + 1))
        if [event.revision for event in events] != expected_revisions:
            raise InvalidEventStream("noncontiguous batch revisions")
        if receipt.resulting_revision != expected_revisions[-1]:
            raise ReceiptConflict("final revision mismatch")
        if any(event.causation_id != receipt.causation_id for event in events):
            raise ReceiptConflict("causation mismatch")
        if any(event.correlation_id != receipt.correlation_id for event in events):
            raise ReceiptConflict("correlation mismatch")

        staged = state
        for event in events:
            staged = apply(staged, event)

        contract_to_store = None
        for event in events:
            if event.event_type == "LoopStarted":
                contract_to_store = event.payload["contract"]
                existing_contract = self._contracts.get(loop_id)
                if existing_contract is not None and existing_contract != contract_to_store:
                    raise ReceiptConflict("contract identity conflict")

        self._events.setdefault(loop_id, []).extend(events)
        self._event_ids.update(ids)
        self._receipts[key] = receipt
        if contract_to_store is not None:
            self._contracts[loop_id] = contract_to_store
        return receipt
