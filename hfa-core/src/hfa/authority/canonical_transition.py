"""Canonical transition and monotonic aggregate-revision authority primitives.

Sprint 81.1 implements the pure, persistence-independent core accepted by
ADR-080C.3. Redis/Lua storage and runtime cutover are intentionally outside
this module.
"""

from __future__ import annotations

import base64
import hashlib
import math
import unicodedata
from dataclasses import dataclass
from enum import Enum
from types import MappingProxyType
from typing import Any, Mapping, Sequence

import rfc8785

JsonValue = None | bool | int | float | str | list["JsonValue"] | dict[str, "JsonValue"]


class AuthorityContractError(ValueError):
    """Raised when an input cannot satisfy the canonical authority contract."""


class AggregateType(str, Enum):
    TASK = "task"
    RUN = "run"


class OperationType(str, Enum):
    TASK_ADMIT = "TASK_ADMIT"
    TASK_DISPATCH = "TASK_DISPATCH"
    TASK_CLAIM = "TASK_CLAIM"
    TASK_HEARTBEAT = "TASK_HEARTBEAT"
    TASK_COMPLETE = "TASK_COMPLETE"
    TASK_FAIL = "TASK_FAIL"
    TASK_REQUEUE = "TASK_REQUEUE"
    TASK_CANCEL = "TASK_CANCEL"
    TASK_DEPENDENCY_APPLY = "TASK_DEPENDENCY_APPLY"
    RUN_CREATE = "RUN_CREATE"
    RUN_TERMINATE = "RUN_TERMINATE"
    LEGACY_RUN_COMPLETE = "LEGACY_RUN_COMPLETE"
    TERMINAL_DUPLICATE_CLEANUP = "TERMINAL_DUPLICATE_CLEANUP"
    MESSAGE_APPEND = "MESSAGE_APPEND"
    MESSAGE_ACK = "MESSAGE_ACK"


class MutationClass(str, Enum):
    ACCEPTED_AUTHORITY_MUTATION = "ACCEPTED_AUTHORITY_MUTATION"
    COORDINATION_ONLY_MUTATION = "COORDINATION_ONLY_MUTATION"
    TRANSPORT_ONLY_MUTATION = "TRANSPORT_ONLY_MUTATION"
    UNSUPPORTED_LEGACY_OPERATION = "UNSUPPORTED_LEGACY_OPERATION"


@dataclass(frozen=True)
class OperationContract:
    operation_type: OperationType
    aggregate_type: AggregateType | None
    mutation_class: MutationClass
    allowed_previous_states: frozenset[str | None]
    allowed_next_states: frozenset[str | None]
    consumes_revision: bool
    operation_receipt_required: bool
    required_projection_intents: frozenset[str] = frozenset()
    conditional_projection_intents: frozenset[str] = frozenset()


def _contract(
    operation_type: OperationType,
    aggregate_type: AggregateType | None,
    mutation_class: MutationClass,
    before: set[str | None],
    after: set[str | None],
    *,
    consumes_revision: bool,
    receipt: bool,
    required_intents: set[str] | None = None,
    conditional_intents: set[str] | None = None,
) -> OperationContract:
    return OperationContract(
        operation_type=operation_type,
        aggregate_type=aggregate_type,
        mutation_class=mutation_class,
        allowed_previous_states=frozenset(before),
        allowed_next_states=frozenset(after),
        consumes_revision=consumes_revision,
        operation_receipt_required=receipt,
        required_projection_intents=frozenset(required_intents or set()),
        conditional_projection_intents=frozenset(conditional_intents or set()),
    )


OPERATION_CONTRACTS: Mapping[OperationType, OperationContract] = MappingProxyType(
    {
        OperationType.TASK_ADMIT: _contract(
            OperationType.TASK_ADMIT,
            AggregateType.TASK,
            MutationClass.ACCEPTED_AUTHORITY_MUTATION,
            {None},
            {"pending", "ready"},
            consumes_revision=True,
            receipt=True,
            conditional_intents={"READY_QUEUE_IF_READY"},
        ),
        OperationType.TASK_DISPATCH: _contract(
            OperationType.TASK_DISPATCH,
            AggregateType.TASK,
            MutationClass.ACCEPTED_AUTHORITY_MUTATION,
            {"ready"},
            {"scheduled"},
            consumes_revision=True,
            receipt=True,
            required_intents={"CONTROL_NOTIFICATION", "TASK_REQUEST_MESSAGE"},
        ),
        OperationType.TASK_CLAIM: _contract(
            OperationType.TASK_CLAIM,
            AggregateType.TASK,
            MutationClass.ACCEPTED_AUTHORITY_MUTATION,
            {"scheduled"},
            {"running"},
            consumes_revision=True,
            receipt=True,
            required_intents={"RUNNING_SET"},
        ),
        OperationType.TASK_HEARTBEAT: _contract(
            OperationType.TASK_HEARTBEAT,
            AggregateType.TASK,
            MutationClass.COORDINATION_ONLY_MUTATION,
            {"running"},
            {"running"},
            consumes_revision=False,
            receipt=False,
            required_intents={"LIVENESS_TTL"},
        ),
        OperationType.TASK_COMPLETE: _contract(
            OperationType.TASK_COMPLETE,
            AggregateType.TASK,
            MutationClass.ACCEPTED_AUTHORITY_MUTATION,
            {"running"},
            {"done"},
            consumes_revision=True,
            receipt=True,
            required_intents={"OUTPUT_PROJECTION", "DEPENDENCY_FANOUT_INTENT"},
        ),
        OperationType.TASK_FAIL: _contract(
            OperationType.TASK_FAIL,
            AggregateType.TASK,
            MutationClass.ACCEPTED_AUTHORITY_MUTATION,
            {"running"},
            {"failed"},
            consumes_revision=True,
            receipt=True,
            required_intents={"DEPENDENCY_FAILURE_FANOUT_INTENT"},
        ),
        OperationType.TASK_REQUEUE: _contract(
            OperationType.TASK_REQUEUE,
            AggregateType.TASK,
            MutationClass.ACCEPTED_AUTHORITY_MUTATION,
            {"running"},
            {"ready"},
            consumes_revision=True,
            receipt=True,
            required_intents={"READY_QUEUE", "REQUEUE_NOTIFICATION"},
        ),
        OperationType.TASK_CANCEL: _contract(
            OperationType.TASK_CANCEL,
            AggregateType.TASK,
            MutationClass.ACCEPTED_AUTHORITY_MUTATION,
            {"pending", "ready", "scheduled", "running"},
            {"skipped"},
            consumes_revision=True,
            receipt=True,
            required_intents={"TERMINAL_PROJECTION"},
        ),
        OperationType.TASK_DEPENDENCY_APPLY: _contract(
            OperationType.TASK_DEPENDENCY_APPLY,
            AggregateType.TASK,
            MutationClass.ACCEPTED_AUTHORITY_MUTATION,
            {"pending"},
            {"pending", "ready", "blocked_by_failure"},
            consumes_revision=True,
            receipt=True,
            conditional_intents={"READY_QUEUE_IF_READY"},
        ),
        OperationType.RUN_CREATE: _contract(
            OperationType.RUN_CREATE,
            AggregateType.RUN,
            MutationClass.ACCEPTED_AUTHORITY_MUTATION,
            {None},
            {"pending"},
            consumes_revision=True,
            receipt=True,
            required_intents={"RUN_STATUS_PROJECTION"},
        ),
        OperationType.RUN_TERMINATE: _contract(
            OperationType.RUN_TERMINATE,
            AggregateType.RUN,
            MutationClass.ACCEPTED_AUTHORITY_MUTATION,
            {"pending", "running"},
            {"done", "failed"},
            consumes_revision=True,
            receipt=True,
            required_intents={"RUN_RESULT_PROJECTION"},
        ),
        OperationType.LEGACY_RUN_COMPLETE: _contract(
            OperationType.LEGACY_RUN_COMPLETE,
            AggregateType.RUN,
            MutationClass.UNSUPPORTED_LEGACY_OPERATION,
            {"running"},
            {"done"},
            consumes_revision=False,
            receipt=False,
        ),
        OperationType.TERMINAL_DUPLICATE_CLEANUP: _contract(
            OperationType.TERMINAL_DUPLICATE_CLEANUP,
            AggregateType.TASK,
            MutationClass.TRANSPORT_ONLY_MUTATION,
            {"terminal"},
            {"terminal"},
            consumes_revision=False,
            receipt=False,
            required_intents={"AUDIT_INTENT", "AUDIT_OUTCOME"},
        ),
        OperationType.MESSAGE_APPEND: _contract(
            OperationType.MESSAGE_APPEND,
            None,
            MutationClass.TRANSPORT_ONLY_MUTATION,
            {"NOT_APPLICABLE"},
            {"NOT_APPLICABLE"},
            consumes_revision=False,
            receipt=False,
            required_intents={"STREAM_APPEND"},
        ),
        OperationType.MESSAGE_ACK: _contract(
            OperationType.MESSAGE_ACK,
            None,
            MutationClass.TRANSPORT_ONLY_MUTATION,
            {"NOT_APPLICABLE"},
            {"NOT_APPLICABLE"},
            consumes_revision=False,
            receipt=False,
            required_intents={"STREAM_ACK"},
        ),
    }
)


class AuthorityDecisionCode(str, Enum):
    ACCEPTED = "ACCEPTED"
    AUTHORITY_ENTRY_REJECTED = "AUTHORITY_ENTRY_REJECTED"
    ALREADY_APPLIED = "ALREADY_APPLIED"
    IDEMPOTENCY_CONFLICT = "IDEMPOTENCY_CONFLICT"
    AGGREGATE_ALREADY_EXISTS_CONFLICT = "AGGREGATE_ALREADY_EXISTS_CONFLICT"
    STALE_REVISION_CONFLICT = "STALE_REVISION_CONFLICT"
    FUTURE_REVISION_CONFLICT = "FUTURE_REVISION_CONFLICT"
    ILLEGAL_STATE_TRANSITION = "ILLEGAL_STATE_TRANSITION"
    CANONICAL_RECORD_CORRUPTION_CONFLICT = "CANONICAL_RECORD_CORRUPTION_CONFLICT"
    OPERATION_NOT_AUTHORITY_MUTATION = "OPERATION_NOT_AUTHORITY_MUTATION"
    UNSUPPORTED_LEGACY_OPERATION = "UNSUPPORTED_LEGACY_OPERATION"


class CanonicalStoreDecision(str, Enum):
    INSERT = "INSERT"
    ALREADY_PRESENT = "ALREADY_PRESENT"
    CANONICAL_RECORD_CORRUPTION_CONFLICT = "CANONICAL_RECORD_CORRUPTION_CONFLICT"


class ProjectionDecision(str, Enum):
    APPLY = "APPLY"
    DUPLICATE_NOOP = "DUPLICATE_NOOP"
    OLDER_REVISION_NOOP = "OLDER_REVISION_NOOP"
    GAP_FAIL_CLOSED_AND_REPLAY_REQUIRED = "GAP_FAIL_CLOSED_AND_REPLAY_REQUIRED"
    PROJECTION_CORRUPTION_CONFLICT = "PROJECTION_CORRUPTION_CONFLICT"


def _nfc(value: str, *, field_name: str) -> str:
    if not isinstance(value, str):
        raise AuthorityContractError(f"{field_name} must be a string")
    normalized = unicodedata.normalize("NFC", value)
    if not normalized:
        raise AuthorityContractError(f"{field_name} must not be empty")
    return normalized


def _length_prefixed_utf8(value: str) -> bytes:
    encoded = value.encode("utf-8")
    return len(encoded).to_bytes(8, "big", signed=False) + encoded


def _component_digest(*components: str) -> str:
    digest = hashlib.sha256()
    for component in components:
        digest.update(_length_prefixed_utf8(component))
    return digest.hexdigest()


def _base64url(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).rstrip(b"=").decode("ascii")


def _normalize_json(value: Any, *, path: str = "$") -> JsonValue:
    if value is None or isinstance(value, bool):
        return value
    if isinstance(value, str):
        return unicodedata.normalize("NFC", value)
    if isinstance(value, bytes):
        return {"$type": "bytes", "$base64url": _base64url(value)}
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise AuthorityContractError(f"{path} contains a non-finite number")
        return value
    if isinstance(value, Mapping):
        normalized: dict[str, JsonValue] = {}
        for raw_key, raw_value in value.items():
            if not isinstance(raw_key, str):
                raise AuthorityContractError(f"{path} object keys must be strings")
            key = unicodedata.normalize("NFC", raw_key)
            if key in normalized:
                raise AuthorityContractError(
                    f"{path} contains duplicate keys after NFC normalization: {key!r}"
                )
            normalized[key] = _normalize_json(raw_value, path=f"{path}.{key}")
        return normalized
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return [
            _normalize_json(item, path=f"{path}[{index}]")
            for index, item in enumerate(value)
        ]
    raise AuthorityContractError(f"{path} contains unsupported type {type(value).__name__}")


def _freeze_normalized(value: JsonValue) -> Any:
    if isinstance(value, dict):
        return MappingProxyType(
            {key: _freeze_normalized(item) for key, item in value.items()}
        )
    if isinstance(value, list):
        return tuple(_freeze_normalized(item) for item in value)
    return value


def _freeze_json(value: Any) -> Any:
    return _freeze_normalized(_normalize_json(value))


def canonical_json_bytes(value: Any) -> bytes:
    """Return RFC 8785 JCS bytes after accepted NFC/type normalization."""

    try:
        return rfc8785.dumps(_normalize_json(value))
    except rfc8785.CanonicalizationError as exc:
        raise AuthorityContractError(str(exc)) from exc


def canonical_json_sha256(value: Any) -> str:
    return hashlib.sha256(canonical_json_bytes(value)).hexdigest()


def _coerce_operation_type(value: OperationType | str) -> OperationType:
    try:
        return value if isinstance(value, OperationType) else OperationType(value)
    except ValueError as exc:
        raise AuthorityContractError(f"unsupported operation_type: {value!r}") from exc


def _projection_kinds(value: Sequence[Mapping[str, Any]] | None) -> frozenset[str]:
    if value is None:
        return frozenset()
    kinds: set[str] = set()
    for index, item in enumerate(value):
        if not isinstance(item, Mapping):
            raise AuthorityContractError(
                f"requested_projection_intents[{index}] must be an object"
            )
        kind = _nfc(
            item.get("kind"),
            field_name=f"requested_projection_intents[{index}].kind",
        )
        if kind in kinds:
            raise AuthorityContractError(f"duplicate projection intent kind: {kind}")
        kinds.add(kind)
    return frozenset(kinds)


@dataclass(frozen=True)
class CanonicalAggregateIdentity:
    aggregate_type: AggregateType
    run_id: str
    task_id: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "run_id", _nfc(self.run_id, field_name="run_id"))
        if self.aggregate_type is AggregateType.TASK:
            if self.task_id is None:
                raise AuthorityContractError("task aggregate identity requires task_id")
            object.__setattr__(
                self,
                "task_id",
                _nfc(self.task_id, field_name="task_id"),
            )
        elif self.task_id is not None:
            raise AuthorityContractError("run aggregate identity must not include task_id")

    @property
    def value(self) -> str:
        if self.aggregate_type is AggregateType.TASK:
            return f"task:{self.run_id}:{self.task_id}"
        return f"run:{self.run_id}"

    @property
    def sha256(self) -> str:
        components = [self.aggregate_type.value, self.run_id]
        if self.task_id is not None:
            components.append(self.task_id)
        return _component_digest(*components)


@dataclass(frozen=True)
class AuthorityCommand:
    aggregate_identity: CanonicalAggregateIdentity
    operation_type: OperationType | str
    operation_id: str
    expected_revision: int
    intended_previous_state: str | None
    intended_next_state: str | None
    authoritative_payload: Mapping[str, Any] | None = None
    authoritative_metadata_changes: Mapping[str, Any] | None = None
    requested_child_effects: Sequence[Mapping[str, Any]] | None = None
    requested_projection_intents: Sequence[Mapping[str, Any]] | None = None
    causation_id: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "operation_type",
            _coerce_operation_type(self.operation_type),
        )
        object.__setattr__(
            self,
            "operation_id",
            _nfc(self.operation_id, field_name="operation_id"),
        )
        if not isinstance(self.expected_revision, int) or isinstance(
            self.expected_revision, bool
        ):
            raise AuthorityContractError("expected_revision must be an integer")
        if self.expected_revision < 0:
            raise AuthorityContractError("expected_revision must be >= 0")
        if self.intended_previous_state is not None:
            object.__setattr__(
                self,
                "intended_previous_state",
                _nfc(
                    self.intended_previous_state,
                    field_name="intended_previous_state",
                ),
            )
        if self.intended_next_state is not None:
            object.__setattr__(
                self,
                "intended_next_state",
                _nfc(self.intended_next_state, field_name="intended_next_state"),
            )
        if self.causation_id is not None:
            object.__setattr__(
                self,
                "causation_id",
                _nfc(self.causation_id, field_name="causation_id"),
            )
        object.__setattr__(
            self,
            "authoritative_payload",
            _freeze_json(self.authoritative_payload),
        )
        object.__setattr__(
            self,
            "authoritative_metadata_changes",
            _freeze_json(self.authoritative_metadata_changes),
        )
        object.__setattr__(
            self,
            "requested_child_effects",
            _freeze_json(self.requested_child_effects),
        )
        object.__setattr__(
            self,
            "requested_projection_intents",
            _freeze_json(self.requested_projection_intents),
        )
        validate_operation_contract(self)
        canonical_json_bytes(self.hash_payload())

    def hash_payload(self) -> dict[str, Any]:
        return {
            "aggregate_type": self.aggregate_identity.aggregate_type.value,
            "canonical_aggregate_identity": self.aggregate_identity.value,
            "operation_type": self.operation_type.value,
            "operation_id": self.operation_id,
            "expected_revision": self.expected_revision,
            "intended_previous_state": self.intended_previous_state,
            "intended_next_state": self.intended_next_state,
            "authoritative_payload": self.authoritative_payload,
            "authoritative_metadata_changes": self.authoritative_metadata_changes,
            "requested_child_effects": self.requested_child_effects,
            "requested_projection_intents": self.requested_projection_intents,
            "causation_id": self.causation_id,
        }

    @property
    def canonical_command_hash(self) -> str:
        return canonical_json_sha256(self.hash_payload())

    @property
    def operation_id_sha256(self) -> str:
        return _component_digest(self.operation_id)

    @property
    def operation_receipt_key(self) -> str:
        return (
            f"oprcpt:v1:{self.aggregate_identity.sha256}:"
            f"{self.operation_id_sha256}"
        )


def validate_operation_contract(command: AuthorityCommand) -> OperationContract:
    contract = OPERATION_CONTRACTS[command.operation_type]
    identity_type = command.aggregate_identity.aggregate_type
    if contract.aggregate_type is not None and identity_type is not contract.aggregate_type:
        raise AuthorityContractError(
            f"{command.operation_type.value} requires "
            f"{contract.aggregate_type.value} aggregate"
        )
    if command.intended_previous_state not in contract.allowed_previous_states:
        raise AuthorityContractError(
            f"illegal previous state for {command.operation_type.value}: "
            f"{command.intended_previous_state!r}"
        )
    if command.intended_next_state not in contract.allowed_next_states:
        raise AuthorityContractError(
            f"illegal next state for {command.operation_type.value}: "
            f"{command.intended_next_state!r}"
        )
    observed_intents = _projection_kinds(command.requested_projection_intents)
    active_conditional_intents = frozenset(
        intent
        for intent in contract.conditional_projection_intents
        if intent == "READY_QUEUE_IF_READY"
        and command.intended_next_state == "ready"
    )
    allowed_intents = contract.required_projection_intents | active_conditional_intents
    unexpected = observed_intents - allowed_intents
    if unexpected:
        raise AuthorityContractError(
            f"undeclared projection intent for {command.operation_type.value}: "
            f"{sorted(unexpected)}"
        )
    missing = (
        contract.required_projection_intents | active_conditional_intents
    ) - observed_intents
    if missing:
        raise AuthorityContractError(
            f"missing required projection intents for {command.operation_type.value}: "
            f"{sorted(missing)}"
        )
    return contract


@dataclass(frozen=True)
class AuthorityEntryContext:
    authenticated_writer_id: str | None
    authorized_operations: frozenset[OperationType | str]
    target_aggregate_identity: CanonicalAggregateIdentity
    fence_required: bool = False
    fence_valid: bool = True

    def __post_init__(self) -> None:
        writer_id = self.authenticated_writer_id
        if writer_id is not None:
            writer_id = _nfc(writer_id, field_name="authenticated_writer_id")
        object.__setattr__(self, "authenticated_writer_id", writer_id)
        object.__setattr__(
            self,
            "authorized_operations",
            frozenset(
                _coerce_operation_type(value)
                for value in self.authorized_operations
            ),
        )

    def validate(self, command: AuthorityCommand) -> bool:
        if self.authenticated_writer_id is None:
            return False
        if command.operation_type not in self.authorized_operations:
            return False
        if self.target_aggregate_identity != command.aggregate_identity:
            return False
        if self.fence_required and not self.fence_valid:
            return False
        return True


@dataclass(frozen=True)
class OperationReceipt:
    operation_id: str
    canonical_command_hash: str
    canonical_record_hash: str
    transition_id: str
    aggregate_revision: int
    operation_type: str
    committed_at_ms: int


@dataclass(frozen=True)
class ReceiptProbe:
    receipt: OperationReceipt
    canonical_store_record_hash: str | None


@dataclass(frozen=True)
class AuthorityDecision:
    code: AuthorityDecisionCode
    receipt_disclosed: bool = False
    aggregate_mutation: int = 0
    revision_increment: int = 0
    canonical_record_count: int = 0
    durable_conflict_record_count: int = 0
    reconciliation_candidate: bool = False
    existing_transition_id: str | None = None
    accepted_command_hash: str | None = None
    accepted_aggregate_identity_sha256: str | None = None
    accepted_operation_id: str | None = None
    authorized_writer_id: str | None = None


@dataclass(frozen=True)
class CanonicalTransitionRecord:
    schema_version: str
    transition_id: str
    aggregate_type: str
    canonical_aggregate_identity: str
    canonical_aggregate_identity_sha256: str
    from_revision: int
    to_revision: int
    operation_type: str
    operation_id: str
    canonical_command_hash: str
    canonical_record_hash: str
    previous_state: str | None
    next_state: str | None
    authoritative_metadata_changes: Any
    child_effects: Any
    causation_id: str | None
    correlation_id: str | None
    writer_id: str
    committed_at_ms: int
    durable_projection_intents: Any

    @classmethod
    def create(
        cls,
        command: AuthorityCommand,
        *,
        writer_id: str,
        committed_at_ms: int,
        correlation_id: str | None = None,
    ) -> "CanonicalTransitionRecord":
        contract = validate_operation_contract(command)
        if not contract.consumes_revision:
            raise AuthorityContractError(
                f"{command.operation_type.value} does not create "
                "a canonical transition record"
            )
        writer_id = _nfc(writer_id, field_name="writer_id")
        if not isinstance(committed_at_ms, int) or isinstance(committed_at_ms, bool):
            raise AuthorityContractError("committed_at_ms must be an integer")
        if committed_at_ms < 0:
            raise AuthorityContractError("committed_at_ms must be >= 0")
        if correlation_id is not None:
            correlation_id = _nfc(correlation_id, field_name="correlation_id")
        to_revision = command.expected_revision + 1
        transition_id = (
            f"ctr:v1:{command.aggregate_identity.sha256}:"
            f"{to_revision}:{command.operation_id_sha256}"
        )
        base: dict[str, Any] = {
            "schema_version": "ctr.v1",
            "transition_id": transition_id,
            "aggregate_type": command.aggregate_identity.aggregate_type.value,
            "canonical_aggregate_identity": command.aggregate_identity.value,
            "canonical_aggregate_identity_sha256": command.aggregate_identity.sha256,
            "from_revision": command.expected_revision,
            "to_revision": to_revision,
            "operation_type": command.operation_type.value,
            "operation_id": command.operation_id,
            "canonical_command_hash": command.canonical_command_hash,
            "previous_state": command.intended_previous_state,
            "next_state": command.intended_next_state,
            "authoritative_metadata_changes": command.authoritative_metadata_changes,
            "child_effects": command.requested_child_effects,
            "causation_id": command.causation_id,
            "correlation_id": correlation_id,
            "writer_id": writer_id,
            "committed_at_ms": committed_at_ms,
            "durable_projection_intents": command.requested_projection_intents,
        }
        return cls(
            **base,
            canonical_record_hash=canonical_json_sha256(base),
        )

    def immutable_payload(self) -> dict[str, Any]:
        return {
            field_name: value
            for field_name, value in self.__dict__.items()
            if field_name != "canonical_record_hash"
        }

    def verify_hash(self) -> bool:
        return canonical_json_sha256(self.immutable_payload()) == self.canonical_record_hash

    def to_receipt(self) -> OperationReceipt:
        return OperationReceipt(
            operation_id=self.operation_id,
            canonical_command_hash=self.canonical_command_hash,
            canonical_record_hash=self.canonical_record_hash,
            transition_id=self.transition_id,
            aggregate_revision=self.to_revision,
            operation_type=self.operation_type,
            committed_at_ms=self.committed_at_ms,
        )


@dataclass(frozen=True)
class AuthorityCommitPlan:
    canonical_aggregate_identity: str
    from_revision: int
    to_revision: int
    next_state: str | None
    canonical_record: CanonicalTransitionRecord
    operation_receipt: OperationReceipt
    authoritative_metadata_changes: Any
    child_effects: Any
    durable_projection_intents: Any
    revision_increment: int = 1
    canonical_record_count: int = 1
    operation_receipt_count: int = 1

    @classmethod
    def create(
        cls,
        decision: AuthorityDecision,
        command: AuthorityCommand,
        *,
        committed_at_ms: int,
        correlation_id: str | None = None,
    ) -> "AuthorityCommitPlan":
        if decision.code is not AuthorityDecisionCode.ACCEPTED:
            raise AuthorityContractError("commit plan requires an ACCEPTED decision")
        if (
            decision.revision_increment != 1
            or decision.canonical_record_count != 1
            or decision.aggregate_mutation != 1
        ):
            raise AuthorityContractError("accepted decision has invalid mutation cardinality")
        if (
            decision.accepted_command_hash != command.canonical_command_hash
            or decision.accepted_aggregate_identity_sha256
            != command.aggregate_identity.sha256
            or decision.accepted_operation_id != command.operation_id
        ):
            raise AuthorityContractError("accepted decision is not bound to command")
        if decision.authorized_writer_id is None:
            raise AuthorityContractError("accepted decision has no authorized writer")
        record = CanonicalTransitionRecord.create(
            command,
            writer_id=decision.authorized_writer_id,
            committed_at_ms=committed_at_ms,
            correlation_id=correlation_id,
        )
        return cls(
            canonical_aggregate_identity=command.aggregate_identity.value,
            from_revision=command.expected_revision,
            to_revision=record.to_revision,
            next_state=command.intended_next_state,
            canonical_record=record,
            operation_receipt=record.to_receipt(),
            authoritative_metadata_changes=command.authoritative_metadata_changes,
            child_effects=command.requested_child_effects,
            durable_projection_intents=command.requested_projection_intents,
        )


@dataclass(frozen=True)
class ProjectionApplicationReceipt:
    applied_revision: int
    applied_transition_id: str
    applied_record_hash: str


def evaluate_authority_command(
    *,
    context: AuthorityEntryContext,
    command: AuthorityCommand,
    current_revision: int,
    current_state: str | None,
    receipt_probe: ReceiptProbe | None = None,
) -> AuthorityDecision:
    """Evaluate outer-gate, idempotency, CAS and state rules without mutation."""

    if not context.validate(command):
        return AuthorityDecision(code=AuthorityDecisionCode.AUTHORITY_ENTRY_REJECTED)

    contract = validate_operation_contract(command)
    if contract.mutation_class is MutationClass.UNSUPPORTED_LEGACY_OPERATION:
        return AuthorityDecision(code=AuthorityDecisionCode.UNSUPPORTED_LEGACY_OPERATION)
    if not contract.consumes_revision:
        return AuthorityDecision(
            code=AuthorityDecisionCode.OPERATION_NOT_AUTHORITY_MUTATION
        )

    if receipt_probe is not None:
        receipt = receipt_probe.receipt
        expected_revision = command.expected_revision + 1
        expected_transition_id = (
            f"ctr:v1:{command.aggregate_identity.sha256}:"
            f"{expected_revision}:{command.operation_id_sha256}"
        )
        receipt_identity_valid = (
            receipt.operation_id == command.operation_id
            and receipt.operation_type == command.operation_type.value
            and receipt.aggregate_revision == expected_revision
            and receipt.transition_id == expected_transition_id
            and isinstance(receipt.committed_at_ms, int)
            and not isinstance(receipt.committed_at_ms, bool)
            and receipt.committed_at_ms >= 0
        )
        if not receipt_identity_valid:
            return AuthorityDecision(
                code=AuthorityDecisionCode.CANONICAL_RECORD_CORRUPTION_CONFLICT,
                receipt_disclosed=True,
                durable_conflict_record_count=1,
                reconciliation_candidate=True,
            )
        if receipt.canonical_command_hash != command.canonical_command_hash:
            return AuthorityDecision(
                code=AuthorityDecisionCode.IDEMPOTENCY_CONFLICT,
                receipt_disclosed=True,
                durable_conflict_record_count=1,
                reconciliation_candidate=True,
            )
        if (
            receipt_probe.canonical_store_record_hash is None
            or receipt_probe.canonical_store_record_hash
            != receipt.canonical_record_hash
        ):
            return AuthorityDecision(
                code=AuthorityDecisionCode.CANONICAL_RECORD_CORRUPTION_CONFLICT,
                receipt_disclosed=True,
                durable_conflict_record_count=1,
                reconciliation_candidate=True,
            )
        return AuthorityDecision(
            code=AuthorityDecisionCode.ALREADY_APPLIED,
            receipt_disclosed=True,
            existing_transition_id=receipt.transition_id,
        )

    if command.expected_revision < current_revision:
        if command.operation_type in {
            OperationType.TASK_ADMIT,
            OperationType.RUN_CREATE,
        }:
            return AuthorityDecision(
                code=AuthorityDecisionCode.AGGREGATE_ALREADY_EXISTS_CONFLICT,
                durable_conflict_record_count=1,
            )
        return AuthorityDecision(code=AuthorityDecisionCode.STALE_REVISION_CONFLICT)
    if command.expected_revision > current_revision:
        return AuthorityDecision(
            code=AuthorityDecisionCode.FUTURE_REVISION_CONFLICT,
            reconciliation_candidate=True,
        )
    if command.intended_previous_state != current_state:
        return AuthorityDecision(code=AuthorityDecisionCode.ILLEGAL_STATE_TRANSITION)
    return AuthorityDecision(
        code=AuthorityDecisionCode.ACCEPTED,
        aggregate_mutation=1,
        revision_increment=1,
        canonical_record_count=1,
        accepted_command_hash=command.canonical_command_hash,
        accepted_aggregate_identity_sha256=command.aggregate_identity.sha256,
        accepted_operation_id=command.operation_id,
        authorized_writer_id=context.authenticated_writer_id,
    )


def classify_canonical_store_write(
    existing: CanonicalTransitionRecord | None,
    candidate: CanonicalTransitionRecord,
) -> CanonicalStoreDecision:
    if existing is None:
        return CanonicalStoreDecision.INSERT
    if existing.transition_id != candidate.transition_id:
        raise AuthorityContractError(
            "store collision classifier requires the same transition_id"
        )
    if not existing.verify_hash() or not candidate.verify_hash():
        return CanonicalStoreDecision.CANONICAL_RECORD_CORRUPTION_CONFLICT
    if existing.canonical_record_hash == candidate.canonical_record_hash:
        return CanonicalStoreDecision.ALREADY_PRESENT
    return CanonicalStoreDecision.CANONICAL_RECORD_CORRUPTION_CONFLICT


def evaluate_projection_application(
    *,
    current: ProjectionApplicationReceipt | None,
    incoming: CanonicalTransitionRecord,
) -> ProjectionDecision:
    if not incoming.verify_hash():
        return ProjectionDecision.PROJECTION_CORRUPTION_CONFLICT
    if current is None:
        if incoming.to_revision != 1:
            return ProjectionDecision.GAP_FAIL_CLOSED_AND_REPLAY_REQUIRED
        return ProjectionDecision.APPLY
    if incoming.to_revision == current.applied_revision:
        if (
            incoming.transition_id == current.applied_transition_id
            and incoming.canonical_record_hash == current.applied_record_hash
        ):
            return ProjectionDecision.DUPLICATE_NOOP
        return ProjectionDecision.PROJECTION_CORRUPTION_CONFLICT
    if incoming.to_revision < current.applied_revision:
        return ProjectionDecision.OLDER_REVISION_NOOP
    if incoming.to_revision == current.applied_revision + 1:
        return ProjectionDecision.APPLY
    return ProjectionDecision.GAP_FAIL_CLOSED_AND_REPLAY_REQUIRED
