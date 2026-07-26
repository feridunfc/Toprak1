"""Persistence-independent canonical transition policy-evaluation core.

Sprint 81.1 deliberately uses a *trusted-process boundary* threat model.
``AuthorityEntryContext`` is trusted adapter input; this module validates the
claims carried by that context against a command but does not authenticate the
issuer, mint capabilities, or verify a lease against an external store.  Those
security boundaries belong to a later trusted authority adapter.

The module-level construction token is only an accidental-misuse guard.  It is
not presented as protection from an arbitrary in-process Python caller.
"""
from __future__ import annotations

import base64
import hashlib
import math
import re
import unicodedata
from dataclasses import dataclass
from enum import Enum
from types import MappingProxyType
from typing import Any, Mapping, Sequence

import rfc8785

JsonValue = None | bool | int | float | str | list["JsonValue"] | dict[str, "JsonValue"]
_MAX_SAFE_INTEGER = 2**53 - 1
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_INTERNAL_TOKEN = object()  # accidental-misuse guard only; not a security boundary

AUTHORITY_CONTEXT_TRUST_MODEL: Mapping[str, str] = MappingProxyType(
    {
        "model": "TRUSTED_PROCESS_BOUNDARY",
        "arbitrary_in_process_python_caller": "TRUSTED",
        "AuthorityEntryContext": "TRUSTED_ADAPTER_INPUT",
        "internal_token": "ACCIDENTAL_MISUSE_GUARD_ONLY",
        "security_boundary": "OUTSIDE_THIS_MODULE",
        "later_adapter_responsibilities": (
            "AUTHENTICATE_PRINCIPAL;ISSUE_OPERATION_CAPABILITY;"
            "VERIFY_TARGET_IDENTITY;VERIFY_LEASE_OR_FENCE"
        ),
    }
)


class AuthorityContractError(ValueError):
    """Raised when an authority primitive violates a normative invariant."""


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
        operation_type,
        aggregate_type,
        mutation_class,
        frozenset(before),
        frozenset(after),
        consumes_revision,
        receipt,
        frozenset(required_intents or ()),
        frozenset(conditional_intents or ()),
    )


OPERATION_CONTRACTS: Mapping[OperationType, OperationContract] = MappingProxyType(
    {
        OperationType.TASK_ADMIT: _contract(OperationType.TASK_ADMIT, AggregateType.TASK, MutationClass.ACCEPTED_AUTHORITY_MUTATION, {None}, {"pending", "ready"}, consumes_revision=True, receipt=True, conditional_intents={"READY_QUEUE_IF_READY"}),
        OperationType.TASK_DISPATCH: _contract(OperationType.TASK_DISPATCH, AggregateType.TASK, MutationClass.ACCEPTED_AUTHORITY_MUTATION, {"ready"}, {"scheduled"}, consumes_revision=True, receipt=True, required_intents={"CONTROL_NOTIFICATION", "TASK_REQUEST_MESSAGE"}),
        OperationType.TASK_CLAIM: _contract(OperationType.TASK_CLAIM, AggregateType.TASK, MutationClass.ACCEPTED_AUTHORITY_MUTATION, {"scheduled"}, {"running"}, consumes_revision=True, receipt=True, required_intents={"RUNNING_SET"}),
        OperationType.TASK_HEARTBEAT: _contract(OperationType.TASK_HEARTBEAT, AggregateType.TASK, MutationClass.COORDINATION_ONLY_MUTATION, {"running"}, {"running"}, consumes_revision=False, receipt=False, required_intents={"LIVENESS_TTL"}),
        OperationType.TASK_COMPLETE: _contract(OperationType.TASK_COMPLETE, AggregateType.TASK, MutationClass.ACCEPTED_AUTHORITY_MUTATION, {"running"}, {"done"}, consumes_revision=True, receipt=True, required_intents={"OUTPUT_PROJECTION", "DEPENDENCY_FANOUT_INTENT"}),
        OperationType.TASK_FAIL: _contract(OperationType.TASK_FAIL, AggregateType.TASK, MutationClass.ACCEPTED_AUTHORITY_MUTATION, {"running"}, {"failed"}, consumes_revision=True, receipt=True, required_intents={"DEPENDENCY_FAILURE_FANOUT_INTENT"}),
        OperationType.TASK_REQUEUE: _contract(OperationType.TASK_REQUEUE, AggregateType.TASK, MutationClass.ACCEPTED_AUTHORITY_MUTATION, {"running"}, {"ready"}, consumes_revision=True, receipt=True, required_intents={"READY_QUEUE", "REQUEUE_NOTIFICATION"}),
        OperationType.TASK_CANCEL: _contract(OperationType.TASK_CANCEL, AggregateType.TASK, MutationClass.ACCEPTED_AUTHORITY_MUTATION, {"pending", "ready", "scheduled", "running"}, {"skipped"}, consumes_revision=True, receipt=True, required_intents={"TERMINAL_PROJECTION"}),
        OperationType.TASK_DEPENDENCY_APPLY: _contract(OperationType.TASK_DEPENDENCY_APPLY, AggregateType.TASK, MutationClass.ACCEPTED_AUTHORITY_MUTATION, {"pending"}, {"pending", "ready", "blocked_by_failure"}, consumes_revision=True, receipt=True, conditional_intents={"READY_QUEUE_IF_READY"}),
        OperationType.RUN_CREATE: _contract(OperationType.RUN_CREATE, AggregateType.RUN, MutationClass.ACCEPTED_AUTHORITY_MUTATION, {None}, {"pending"}, consumes_revision=True, receipt=True, required_intents={"RUN_STATUS_PROJECTION"}),
        OperationType.RUN_TERMINATE: _contract(OperationType.RUN_TERMINATE, AggregateType.RUN, MutationClass.ACCEPTED_AUTHORITY_MUTATION, {"pending", "running"}, {"done", "failed"}, consumes_revision=True, receipt=True, required_intents={"RUN_RESULT_PROJECTION"}),
        OperationType.LEGACY_RUN_COMPLETE: _contract(OperationType.LEGACY_RUN_COMPLETE, AggregateType.RUN, MutationClass.UNSUPPORTED_LEGACY_OPERATION, {"running"}, {"done"}, consumes_revision=False, receipt=False),
        OperationType.TERMINAL_DUPLICATE_CLEANUP: _contract(OperationType.TERMINAL_DUPLICATE_CLEANUP, AggregateType.TASK, MutationClass.TRANSPORT_ONLY_MUTATION, {"terminal"}, {"terminal"}, consumes_revision=False, receipt=False, required_intents={"AUDIT_INTENT", "AUDIT_OUTCOME"}),
        OperationType.MESSAGE_APPEND: _contract(OperationType.MESSAGE_APPEND, None, MutationClass.TRANSPORT_ONLY_MUTATION, {"NOT_APPLICABLE"}, {"NOT_APPLICABLE"}, consumes_revision=False, receipt=False, required_intents={"STREAM_APPEND"}),
        OperationType.MESSAGE_ACK: _contract(OperationType.MESSAGE_ACK, None, MutationClass.TRANSPORT_ONLY_MUTATION, {"NOT_APPLICABLE"}, {"NOT_APPLICABLE"}, consumes_revision=False, receipt=False, required_intents={"STREAM_ACK"}),
    }
)

_CREATE_OPERATIONS = frozenset({OperationType.TASK_ADMIT, OperationType.RUN_CREATE})


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


def _nfc(value: Any, *, field_name: str, allow_empty: bool = False) -> str:
    if type(value) is not str:
        raise AuthorityContractError(f"{field_name} must be an exact string")
    normalized = unicodedata.normalize("NFC", value)
    if not allow_empty and not normalized:
        raise AuthorityContractError(f"{field_name} must not be empty")
    return normalized


def _safe_int(value: Any, *, field_name: str, minimum: int = 0) -> int:
    if type(value) is not int:
        raise AuthorityContractError(f"{field_name} must be an exact int, not bool/coercible")
    if value < minimum or value > _MAX_SAFE_INTEGER:
        raise AuthorityContractError(f"{field_name} is outside the JCS safe integer domain")
    return value


def _exact_bool(value: Any, *, field_name: str) -> bool:
    if type(value) is not bool:
        raise AuthorityContractError(f"{field_name} must be an exact bool")
    return value


def _sha256_hex(value: Any, *, field_name: str) -> str:
    if type(value) is not str or _SHA256_RE.fullmatch(value) is None:
        raise AuthorityContractError(f"{field_name} must be lowercase SHA-256 hex")
    return value


def _length_prefixed_utf8(value: str) -> bytes:
    encoded = value.encode("utf-8")
    return len(encoded).to_bytes(8, "big") + encoded


def _component_digest(*components: str) -> str:
    digest = hashlib.sha256()
    for component in components:
        digest.update(_length_prefixed_utf8(component))
    return digest.hexdigest()


def _base64url(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).rstrip(b"=").decode("ascii")


def _normalize_json(value: Any, *, path: str = "$") -> JsonValue:
    if value is None or type(value) is bool:
        return value
    if type(value) is str:
        return unicodedata.normalize("NFC", value)
    if type(value) is bytes:
        return {"$type": "bytes", "$base64url": _base64url(value)}
    if type(value) is int:
        return _safe_int(value, field_name=path, minimum=-_MAX_SAFE_INTEGER)
    if type(value) is float:
        if not math.isfinite(value):
            raise AuthorityContractError(f"{path} contains a non-finite number")
        return value
    if isinstance(value, Mapping):
        result: dict[str, JsonValue] = {}
        for raw_key, raw_value in value.items():
            key = _nfc(raw_key, field_name=f"{path} key")
            if key in result:
                raise AuthorityContractError(f"{path} contains duplicate NFC-normalized key {key!r}")
            result[key] = _normalize_json(raw_value, path=f"{path}.{key}")
        return result
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return [_normalize_json(item, path=f"{path}[{index}]") for index, item in enumerate(value)]
    raise AuthorityContractError(f"{path} contains unsupported type {type(value).__name__}")


def _freeze(value: Any) -> Any:
    normalized = _normalize_json(value)
    if isinstance(normalized, dict):
        return MappingProxyType({key: _freeze(item) for key, item in normalized.items()})
    if isinstance(normalized, list):
        return tuple(_freeze(item) for item in normalized)
    return normalized


def _thaw(value: Any) -> JsonValue:
    if isinstance(value, Mapping):
        return {str(key): _thaw(item) for key, item in value.items()}
    if isinstance(value, tuple):
        return [_thaw(item) for item in value]
    return value


def canonical_json_bytes(value: Any) -> bytes:
    try:
        return rfc8785.dumps(_normalize_json(value))
    except rfc8785.CanonicalizationError as exc:
        raise AuthorityContractError(str(exc)) from exc


def canonical_json_sha256(value: Any) -> str:
    return hashlib.sha256(canonical_json_bytes(value)).hexdigest()


def _coerce_operation(value: OperationType | str) -> OperationType:
    if isinstance(value, OperationType):
        return value
    if type(value) is not str:
        raise AuthorityContractError("operation_type must be OperationType or exact string")
    try:
        return OperationType(value)
    except ValueError as exc:
        raise AuthorityContractError(f"unsupported operation_type: {value!r}") from exc


def _projection_kinds(value: Sequence[Mapping[str, Any]] | None) -> frozenset[str]:
    if value is None:
        return frozenset()
    kinds: set[str] = set()
    for index, item in enumerate(value):
        if not isinstance(item, Mapping):
            raise AuthorityContractError(f"projection intent {index} must be an object")
        kind = _nfc(item.get("kind"), field_name=f"projection intent {index}.kind")
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
        if not isinstance(self.aggregate_type, AggregateType):
            raise AuthorityContractError("aggregate_type must be AggregateType")
        object.__setattr__(self, "run_id", _nfc(self.run_id, field_name="run_id"))
        if self.aggregate_type is AggregateType.TASK:
            if self.task_id is None:
                raise AuthorityContractError("task identity requires task_id")
            object.__setattr__(self, "task_id", _nfc(self.task_id, field_name="task_id"))
        elif self.task_id is not None:
            raise AuthorityContractError("run identity must not include task_id")

    @property
    def value(self) -> str:
        return f"task:{self.run_id}:{self.task_id}" if self.aggregate_type is AggregateType.TASK else f"run:{self.run_id}"

    @property
    def structured(self) -> dict[str, str | None]:
        return {"aggregate_type": self.aggregate_type.value, "run_id": self.run_id, "task_id": self.task_id}

    @property
    def sha256(self) -> str:
        return _component_digest(self.aggregate_type.value, self.run_id, self.task_id or "")


@dataclass(frozen=True)
class AuthorityCommand:
    aggregate_identity: CanonicalAggregateIdentity
    operation_type: OperationType
    operation_id: str
    expected_revision: int
    intended_previous_state: str | None
    intended_next_state: str | None
    authoritative_payload: Any = None
    authoritative_metadata_changes: Any = None
    requested_child_effects: Any = None
    requested_projection_intents: tuple[Mapping[str, Any], ...] = ()
    causation_id: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.aggregate_identity, CanonicalAggregateIdentity):
            raise AuthorityContractError("aggregate_identity must be CanonicalAggregateIdentity")
        object.__setattr__(self, "operation_type", _coerce_operation(self.operation_type))
        object.__setattr__(self, "operation_id", _nfc(self.operation_id, field_name="operation_id"))
        object.__setattr__(self, "expected_revision", _safe_int(self.expected_revision, field_name="expected_revision"))
        for name in ("intended_previous_state", "intended_next_state", "causation_id"):
            value = getattr(self, name)
            if value is not None:
                object.__setattr__(self, name, _nfc(value, field_name=name))
        object.__setattr__(self, "authoritative_payload", _freeze(self.authoritative_payload))
        object.__setattr__(self, "authoritative_metadata_changes", _freeze(self.authoritative_metadata_changes))
        object.__setattr__(self, "requested_child_effects", _freeze(self.requested_child_effects))
        intents = tuple(_freeze(item) for item in self.requested_projection_intents)
        _projection_kinds(intents)
        object.__setattr__(self, "requested_projection_intents", intents)

    @property
    def canonical_hash_payload(self) -> dict[str, Any]:
        return {
            "aggregate_type": self.aggregate_identity.aggregate_type.value,
            "canonical_aggregate_identity": self.aggregate_identity.structured,
            "canonical_aggregate_identity_sha256": self.aggregate_identity.sha256,
            "operation_type": self.operation_type.value,
            "operation_id": self.operation_id,
            "expected_revision": self.expected_revision,
            "intended_previous_state": self.intended_previous_state,
            "intended_next_state": self.intended_next_state,
            "authoritative_payload": _thaw(self.authoritative_payload),
            "authoritative_metadata_changes": _thaw(self.authoritative_metadata_changes),
            "requested_child_effects": _thaw(self.requested_child_effects),
            "requested_projection_intents": _thaw(self.requested_projection_intents),
            "causation_id": self.causation_id,
        }

    @property
    def canonical_command_hash(self) -> str:
        return canonical_json_sha256(self.canonical_hash_payload)


@dataclass(frozen=True)
class AuthorityEntryContext:
    """Claims issued and verified by a trusted adapter outside this module."""

    authenticated_writer_id: str
    allowed_operations: frozenset[OperationType]
    target_aggregate_identity_sha256: str
    fence_required: bool = False
    fence_valid: bool = True

    def __post_init__(self) -> None:
        object.__setattr__(self, "authenticated_writer_id", _nfc(self.authenticated_writer_id, field_name="authenticated_writer_id"))
        if not isinstance(self.allowed_operations, frozenset) or any(not isinstance(value, OperationType) for value in self.allowed_operations):
            raise AuthorityContractError("allowed_operations must be frozenset[OperationType]")
        object.__setattr__(self, "target_aggregate_identity_sha256", _sha256_hex(self.target_aggregate_identity_sha256, field_name="target_aggregate_identity_sha256"))
        _exact_bool(self.fence_required, field_name="fence_required")
        _exact_bool(self.fence_valid, field_name="fence_valid")

    def permits(self, command: AuthorityCommand) -> bool:
        """Evaluate trusted adapter claims; this does not prove their issuer."""
        return (
            command.operation_type in self.allowed_operations
            and self.target_aggregate_identity_sha256 == command.aggregate_identity.sha256
            and (not self.fence_required or self.fence_valid)
        )


def _transition_id(identity_sha256: str, revision: int, operation_id: str) -> str:
    _sha256_hex(identity_sha256, field_name="identity_sha256")
    _safe_int(revision, field_name="revision", minimum=1)
    return f"ctr:v1:{identity_sha256}:{revision}:{_component_digest(operation_id)}"


def validate_operation_contract(
    operation_type: OperationType,
    aggregate_type: AggregateType,
    previous_state: str | None,
    next_state: str | None,
    projection_intents: Sequence[Mapping[str, Any]],
) -> OperationContract:
    operation_type = _coerce_operation(operation_type)
    if not isinstance(aggregate_type, AggregateType):
        raise AuthorityContractError("aggregate_type must be AggregateType")
    contract = OPERATION_CONTRACTS[operation_type]
    if contract.aggregate_type is not aggregate_type:
        raise AuthorityContractError("operation aggregate type mismatch")
    if previous_state not in contract.allowed_previous_states or next_state not in contract.allowed_next_states:
        raise AuthorityContractError("illegal operation state transition")
    actual = _projection_kinds(projection_intents)
    expected = set(contract.required_projection_intents)
    if next_state == "ready":
        expected.update(contract.conditional_projection_intents)
    if actual != frozenset(expected):
        raise AuthorityContractError(f"projection intent set mismatch: expected {sorted(expected)}, got {sorted(actual)}")
    return contract


@dataclass(frozen=True, init=False)
class CanonicalTransitionRecord:
    schema_version: str
    transition_id: str
    aggregate_identity: CanonicalAggregateIdentity
    aggregate_identity_sha256: str
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
    durable_projection_intents: tuple[Mapping[str, Any], ...]

    def __init__(self, *, _token: object, **values: Any) -> None:
        if _token is not _INTERNAL_TOKEN:
            raise AuthorityContractError("CanonicalTransitionRecord is produced only by policy evaluation")
        for field_name in self.__dataclass_fields__:  # type: ignore[attr-defined]
            object.__setattr__(self, field_name, values[field_name])

    @property
    def canonical_aggregate_identity(self) -> str:
        return self.aggregate_identity.value

    def immutable_payload(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "transition_id": self.transition_id,
            "aggregate_type": self.aggregate_identity.aggregate_type.value,
            "canonical_aggregate_identity": self.aggregate_identity.structured,
            "canonical_aggregate_identity_sha256": self.aggregate_identity_sha256,
            "from_revision": self.from_revision,
            "to_revision": self.to_revision,
            "operation_type": self.operation_type,
            "operation_id": self.operation_id,
            "canonical_command_hash": self.canonical_command_hash,
            "previous_state": self.previous_state,
            "next_state": self.next_state,
            "authoritative_metadata_changes": _thaw(self.authoritative_metadata_changes),
            "child_effects": _thaw(self.child_effects),
            "causation_id": self.causation_id,
            "correlation_id": self.correlation_id,
            "writer_id": self.writer_id,
            "committed_at_ms": self.committed_at_ms,
            "durable_projection_intents": _thaw(self.durable_projection_intents),
        }

    def verify_hash(self) -> bool:
        try:
            return canonical_json_sha256(self.immutable_payload()) == self.canonical_record_hash
        except AuthorityContractError:
            return False


@dataclass(frozen=True, init=False)
class OperationReceipt:
    operation_id: str
    canonical_command_hash: str
    canonical_record_hash: str
    transition_id: str
    aggregate_revision: int
    operation_type: str
    committed_at_ms: int

    def __init__(self, *, _token: object, **values: Any) -> None:
        if _token is not _INTERNAL_TOKEN:
            raise AuthorityContractError("OperationReceipt is produced only by policy evaluation")
        for field_name in self.__dataclass_fields__:  # type: ignore[attr-defined]
            object.__setattr__(self, field_name, values[field_name])


@dataclass(frozen=True)
class ReceiptProbe:
    receipt: OperationReceipt
    canonical_store_record: CanonicalTransitionRecord | None

    def __post_init__(self) -> None:
        if not isinstance(self.receipt, OperationReceipt):
            raise AuthorityContractError("receipt must be OperationReceipt")
        if self.canonical_store_record is not None and not isinstance(self.canonical_store_record, CanonicalTransitionRecord):
            raise AuthorityContractError("canonical_store_record must be CanonicalTransitionRecord or None")


@dataclass(frozen=True, init=False)
class ProjectionApplicationReceipt:
    canonical_aggregate_identity_sha256: str
    applied_revision: int
    applied_transition_id: str
    applied_record_hash: str

    def __init__(self, *, _token: object, canonical_aggregate_identity_sha256: str, applied_revision: int, applied_transition_id: str, applied_record_hash: str) -> None:
        if _token is not _INTERNAL_TOKEN:
            raise AuthorityContractError("use ProjectionApplicationReceipt.create")
        object.__setattr__(self, "canonical_aggregate_identity_sha256", canonical_aggregate_identity_sha256)
        object.__setattr__(self, "applied_revision", applied_revision)
        object.__setattr__(self, "applied_transition_id", applied_transition_id)
        object.__setattr__(self, "applied_record_hash", applied_record_hash)

    @classmethod
    def create(
        cls,
        *,
        canonical_aggregate_identity_sha256: str,
        applied_revision: int,
        applied_transition_id: str,
        applied_record_hash: str,
    ) -> "ProjectionApplicationReceipt":
        return cls(
            _token=_INTERNAL_TOKEN,
            canonical_aggregate_identity_sha256=_sha256_hex(canonical_aggregate_identity_sha256, field_name="canonical_aggregate_identity_sha256"),
            applied_revision=_safe_int(applied_revision, field_name="applied_revision", minimum=1),
            applied_transition_id=_nfc(applied_transition_id, field_name="applied_transition_id"),
            applied_record_hash=_sha256_hex(applied_record_hash, field_name="applied_record_hash"),
        )


@dataclass(frozen=True, init=False)
class AuthorityDecision:
    code: AuthorityDecisionCode
    aggregate_mutation: int
    revision_increment: int
    canonical_record_count: int
    return_existing_transition_id: str | None
    durable_conflict_record: bool
    reconciliation_candidate: bool

    def __init__(self, *, _token: object, **values: Any) -> None:
        if _token is not _INTERNAL_TOKEN:
            raise AuthorityContractError("AuthorityDecision is evaluator-produced")
        for field_name in self.__dataclass_fields__:  # type: ignore[attr-defined]
            object.__setattr__(self, field_name, values[field_name])


def _decision(
    code: AuthorityDecisionCode,
    *,
    aggregate_mutation: int = 0,
    revision_increment: int = 0,
    canonical_record_count: int = 0,
    return_existing_transition_id: str | None = None,
    durable_conflict_record: bool = False,
    reconciliation_candidate: bool = False,
) -> AuthorityDecision:
    return AuthorityDecision(
        _token=_INTERNAL_TOKEN,
        code=code,
        aggregate_mutation=aggregate_mutation,
        revision_increment=revision_increment,
        canonical_record_count=canonical_record_count,
        return_existing_transition_id=return_existing_transition_id,
        durable_conflict_record=durable_conflict_record,
        reconciliation_candidate=reconciliation_candidate,
    )


@dataclass(frozen=True, init=False)
class AuthorityCommitPlan:
    aggregate_identity: CanonicalAggregateIdentity
    aggregate_revision: int
    record: CanonicalTransitionRecord
    receipt: OperationReceipt

    def __init__(self, *, _token: object, aggregate_identity: CanonicalAggregateIdentity, aggregate_revision: int, record: CanonicalTransitionRecord, receipt: OperationReceipt) -> None:
        if _token is not _INTERNAL_TOKEN:
            raise AuthorityContractError("AuthorityCommitPlan is created only by evaluate_authority_commit")
        object.__setattr__(self, "aggregate_identity", aggregate_identity)
        object.__setattr__(self, "aggregate_revision", aggregate_revision)
        object.__setattr__(self, "record", record)
        object.__setattr__(self, "receipt", receipt)

    @property
    def canonical_aggregate_identity_sha256(self) -> str:
        return self.aggregate_identity.sha256


@dataclass(frozen=True)
class AuthorityEvaluation:
    decision: AuthorityDecision
    commit_plan: AuthorityCommitPlan | None


def _new_record(command: AuthorityCommand, *, writer_id: str, committed_at_ms: int, correlation_id: str | None) -> CanonicalTransitionRecord:
    to_revision = command.expected_revision + 1
    values: dict[str, Any] = {
        "schema_version": "ctr.v1",
        "transition_id": _transition_id(command.aggregate_identity.sha256, to_revision, command.operation_id),
        "aggregate_identity": command.aggregate_identity,
        "aggregate_identity_sha256": command.aggregate_identity.sha256,
        "from_revision": command.expected_revision,
        "to_revision": to_revision,
        "operation_type": command.operation_type.value,
        "operation_id": command.operation_id,
        "canonical_command_hash": command.canonical_command_hash,
        "canonical_record_hash": "0" * 64,
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
    provisional = CanonicalTransitionRecord(_token=_INTERNAL_TOKEN, **values)
    values["canonical_record_hash"] = canonical_json_sha256(provisional.immutable_payload())
    record = CanonicalTransitionRecord(_token=_INTERNAL_TOKEN, **values)
    validate_canonical_transition_record(record)
    return record


def _new_receipt(record: CanonicalTransitionRecord) -> OperationReceipt:
    return OperationReceipt(
        _token=_INTERNAL_TOKEN,
        operation_id=record.operation_id,
        canonical_command_hash=record.canonical_command_hash,
        canonical_record_hash=record.canonical_record_hash,
        transition_id=record.transition_id,
        aggregate_revision=record.to_revision,
        operation_type=record.operation_type,
        committed_at_ms=record.committed_at_ms,
    )


def validate_canonical_transition_record(record: CanonicalTransitionRecord) -> None:
    if not isinstance(record, CanonicalTransitionRecord):
        raise AuthorityContractError("record must be CanonicalTransitionRecord")
    if record.schema_version != "ctr.v1":
        raise AuthorityContractError("record schema_version must be ctr.v1")
    if not isinstance(record.aggregate_identity, CanonicalAggregateIdentity):
        raise AuthorityContractError("record aggregate identity is invalid")
    if record.aggregate_identity_sha256 != record.aggregate_identity.sha256:
        raise AuthorityContractError("record aggregate identity hash mismatch")
    from_revision = _safe_int(record.from_revision, field_name="from_revision")
    to_revision = _safe_int(record.to_revision, field_name="to_revision", minimum=1)
    if to_revision != from_revision + 1:
        raise AuthorityContractError("record revision must be strictly contiguous")
    operation = _coerce_operation(record.operation_type)
    _nfc(record.operation_id, field_name="record.operation_id")
    _sha256_hex(record.canonical_command_hash, field_name="canonical_command_hash")
    _sha256_hex(record.canonical_record_hash, field_name="canonical_record_hash")
    if record.transition_id != _transition_id(record.aggregate_identity_sha256, to_revision, record.operation_id):
        raise AuthorityContractError("record transition_id is not deterministic")
    if record.previous_state is not None:
        _nfc(record.previous_state, field_name="previous_state")
    if record.next_state is not None:
        _nfc(record.next_state, field_name="next_state")
    validate_operation_contract(operation, record.aggregate_identity.aggregate_type, record.previous_state, record.next_state, record.durable_projection_intents)
    _nfc(record.writer_id, field_name="writer_id")
    _safe_int(record.committed_at_ms, field_name="committed_at_ms")
    if record.causation_id is not None:
        _nfc(record.causation_id, field_name="causation_id")
    if record.correlation_id is not None:
        _nfc(record.correlation_id, field_name="correlation_id")
    if not record.verify_hash():
        raise AuthorityContractError("canonical record hash mismatch")


def _validate_receipt_internal(receipt: OperationReceipt) -> OperationType:
    if not isinstance(receipt, OperationReceipt):
        raise AuthorityContractError("receipt must be OperationReceipt")
    _nfc(receipt.operation_id, field_name="receipt.operation_id")
    _sha256_hex(receipt.canonical_command_hash, field_name="receipt.canonical_command_hash")
    _sha256_hex(receipt.canonical_record_hash, field_name="receipt.canonical_record_hash")
    _nfc(receipt.transition_id, field_name="receipt.transition_id")
    _safe_int(receipt.aggregate_revision, field_name="receipt.aggregate_revision", minimum=1)
    _safe_int(receipt.committed_at_ms, field_name="receipt.committed_at_ms")
    return _coerce_operation(receipt.operation_type)


def _validate_stored_duplicate_proof(
    probe: ReceiptProbe,
    *,
    lookup_aggregate_identity_sha256: str,
    lookup_operation_id: str,
) -> tuple[OperationReceipt, CanonicalTransitionRecord]:
    """Validate stored proof independently from the incoming command payload."""
    receipt = probe.receipt
    _validate_receipt_internal(receipt)
    record = probe.canonical_store_record
    if record is None:
        raise AuthorityContractError("actual canonical store record is required")
    validate_canonical_transition_record(record)
    if record.aggregate_identity_sha256 != lookup_aggregate_identity_sha256:
        raise AuthorityContractError("stored proof aggregate lookup mismatch")
    if receipt.operation_id != lookup_operation_id or record.operation_id != lookup_operation_id:
        raise AuthorityContractError("stored proof operation lookup mismatch")
    if (
        receipt.operation_id != record.operation_id
        or receipt.operation_type != record.operation_type
        or receipt.aggregate_revision != record.to_revision
        or receipt.transition_id != record.transition_id
        or receipt.canonical_command_hash != record.canonical_command_hash
        or receipt.canonical_record_hash != record.canonical_record_hash
        or receipt.committed_at_ms != record.committed_at_ms
    ):
        raise AuthorityContractError("receipt and canonical record proof mismatch")
    return receipt, record


def evaluate_authority_commit(
    *,
    context: AuthorityEntryContext,
    command: AuthorityCommand,
    current_revision: int,
    current_state: str | None,
    receipt_probe: ReceiptProbe | None,
    committed_at_ms: int,
    correlation_id: str | None = None,
) -> AuthorityEvaluation:
    """Evaluate trusted adapter claims and return a deterministic commit plan.

    ``context`` provenance is an external precondition under the Sprint 81.1
    trusted-process threat model.  This function evaluates those claims; it does
    not authenticate or mint them.
    """
    if not isinstance(context, AuthorityEntryContext) or not isinstance(command, AuthorityCommand):
        raise AuthorityContractError("context and command must be authority primitives")
    current_revision = _safe_int(current_revision, field_name="current_revision")
    committed_at_ms = _safe_int(committed_at_ms, field_name="committed_at_ms")
    if current_state is not None:
        current_state = _nfc(current_state, field_name="current_state")
    if correlation_id is not None:
        correlation_id = _nfc(correlation_id, field_name="correlation_id")
    if receipt_probe is not None and not isinstance(receipt_probe, ReceiptProbe):
        raise AuthorityContractError("receipt_probe must be ReceiptProbe or None")

    if not context.permits(command):
        return AuthorityEvaluation(_decision(AuthorityDecisionCode.AUTHORITY_ENTRY_REJECTED), None)

    # Receipt resolution precedes every operation-class, revision and state
    # classification. Operation receipt keys are aggregate identity + operation
    # ID; operation type and mutation class are command payload, not lookup-key
    # material. A durable receipt therefore always gets first right of refusal
    # after the outer authority-entry gate.

    # Receipt resolution precedes incoming revision/state comparison.  Stored
    # proof is validated against itself and lookup keys, never against mutable
    # fields of the incoming command.  The command hash comparison is the sole
    # same-operation/same-command discriminator.
    if receipt_probe is not None:
        try:
            receipt, record = _validate_stored_duplicate_proof(
                receipt_probe,
                lookup_aggregate_identity_sha256=command.aggregate_identity.sha256,
                lookup_operation_id=command.operation_id,
            )
        except AuthorityContractError:
            return AuthorityEvaluation(
                _decision(
                    AuthorityDecisionCode.CANONICAL_RECORD_CORRUPTION_CONFLICT,
                    durable_conflict_record=True,
                    reconciliation_candidate=True,
                ),
                None,
            )
        if receipt.canonical_command_hash != command.canonical_command_hash:
            return AuthorityEvaluation(
                _decision(
                    AuthorityDecisionCode.IDEMPOTENCY_CONFLICT,
                    durable_conflict_record=True,
                    reconciliation_candidate=True,
                ),
                None,
            )
        return AuthorityEvaluation(
            _decision(AuthorityDecisionCode.ALREADY_APPLIED, return_existing_transition_id=record.transition_id),
            None,
        )

    contract = OPERATION_CONTRACTS[command.operation_type]
    if contract.mutation_class is MutationClass.UNSUPPORTED_LEGACY_OPERATION:
        return AuthorityEvaluation(_decision(AuthorityDecisionCode.UNSUPPORTED_LEGACY_OPERATION), None)
    if contract.mutation_class is not MutationClass.ACCEPTED_AUTHORITY_MUTATION:
        return AuthorityEvaluation(_decision(AuthorityDecisionCode.OPERATION_NOT_AUTHORITY_MUTATION), None)

    if current_revision < command.expected_revision:
        return AuthorityEvaluation(_decision(AuthorityDecisionCode.FUTURE_REVISION_CONFLICT, reconciliation_candidate=True), None)
    if current_revision > command.expected_revision:
        is_create_conflict = command.operation_type in _CREATE_OPERATIONS and command.expected_revision == 0
        code = AuthorityDecisionCode.AGGREGATE_ALREADY_EXISTS_CONFLICT if is_create_conflict else AuthorityDecisionCode.STALE_REVISION_CONFLICT
        return AuthorityEvaluation(_decision(code, durable_conflict_record=is_create_conflict), None)
    if current_state != command.intended_previous_state:
        return AuthorityEvaluation(_decision(AuthorityDecisionCode.ILLEGAL_STATE_TRANSITION), None)
    try:
        validate_operation_contract(command.operation_type, command.aggregate_identity.aggregate_type, current_state, command.intended_next_state, command.requested_projection_intents)
    except AuthorityContractError:
        return AuthorityEvaluation(_decision(AuthorityDecisionCode.ILLEGAL_STATE_TRANSITION), None)

    record = _new_record(command, writer_id=context.authenticated_writer_id, committed_at_ms=committed_at_ms, correlation_id=correlation_id)
    receipt = _new_receipt(record)
    plan = AuthorityCommitPlan(_token=_INTERNAL_TOKEN, aggregate_identity=command.aggregate_identity, aggregate_revision=record.to_revision, record=record, receipt=receipt)
    decision = _decision(AuthorityDecisionCode.ACCEPTED, aggregate_mutation=1, revision_increment=1, canonical_record_count=1)
    return AuthorityEvaluation(decision, plan)


def evaluate_authority_command(**kwargs: Any) -> AuthorityDecision:
    """Compatibility read-only evaluator; commit plans are never accepted as input."""
    return evaluate_authority_commit(**kwargs).decision


def classify_canonical_store_write(existing: CanonicalTransitionRecord | None, candidate: CanonicalTransitionRecord) -> CanonicalStoreDecision:
    try:
        validate_canonical_transition_record(candidate)
    except AuthorityContractError:
        return CanonicalStoreDecision.CANONICAL_RECORD_CORRUPTION_CONFLICT
    if existing is None:
        return CanonicalStoreDecision.INSERT
    try:
        validate_canonical_transition_record(existing)
    except AuthorityContractError:
        return CanonicalStoreDecision.CANONICAL_RECORD_CORRUPTION_CONFLICT
    if existing.transition_id != candidate.transition_id:
        return CanonicalStoreDecision.CANONICAL_RECORD_CORRUPTION_CONFLICT
    if existing.canonical_record_hash == candidate.canonical_record_hash and existing.immutable_payload() == candidate.immutable_payload():
        return CanonicalStoreDecision.ALREADY_PRESENT
    return CanonicalStoreDecision.CANONICAL_RECORD_CORRUPTION_CONFLICT


def evaluate_projection_application(current: ProjectionApplicationReceipt | None, incoming: CanonicalTransitionRecord) -> ProjectionDecision:
    try:
        validate_canonical_transition_record(incoming)
    except AuthorityContractError:
        return ProjectionDecision.PROJECTION_CORRUPTION_CONFLICT
    if current is None:
        return ProjectionDecision.APPLY if incoming.to_revision == 1 else ProjectionDecision.GAP_FAIL_CLOSED_AND_REPLAY_REQUIRED
    if not isinstance(current, ProjectionApplicationReceipt):
        return ProjectionDecision.PROJECTION_CORRUPTION_CONFLICT
    try:
        current_identity = _sha256_hex(current.canonical_aggregate_identity_sha256, field_name="canonical_aggregate_identity_sha256")
        _safe_int(current.applied_revision, field_name="applied_revision", minimum=1)
        _nfc(current.applied_transition_id, field_name="applied_transition_id")
        _sha256_hex(current.applied_record_hash, field_name="applied_record_hash")
    except AuthorityContractError:
        return ProjectionDecision.PROJECTION_CORRUPTION_CONFLICT
    if current_identity != incoming.aggregate_identity_sha256:
        return ProjectionDecision.PROJECTION_CORRUPTION_CONFLICT
    if incoming.to_revision == current.applied_revision:
        if incoming.transition_id == current.applied_transition_id and incoming.canonical_record_hash == current.applied_record_hash:
            return ProjectionDecision.DUPLICATE_NOOP
        return ProjectionDecision.PROJECTION_CORRUPTION_CONFLICT
    if incoming.to_revision < current.applied_revision:
        return ProjectionDecision.OLDER_REVISION_NOOP
    if incoming.to_revision == current.applied_revision + 1:
        return ProjectionDecision.APPLY
    return ProjectionDecision.GAP_FAIL_CLOSED_AND_REPLAY_REQUIRED
