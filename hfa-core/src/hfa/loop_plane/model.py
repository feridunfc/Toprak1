from __future__ import annotations

from dataclasses import dataclass, field, fields, is_dataclass
from enum import Enum
from hashlib import sha256
from types import MappingProxyType
from typing import Any, Mapping
import json
import re

_HASH_RE = re.compile(r"^[0-9a-f]{64}$")


def _identity(value: Any, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} must be a non-empty string")
    return value


def _integer(value: Any, name: str, minimum: int = 0) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise ValueError(f"{name} must be an integer >= {minimum}")
    return value


def _hash(value: Any, name: str, *, optional: bool = False) -> str | None:
    if optional and value is None:
        return None
    _identity(value, name)
    if not _HASH_RE.fullmatch(value):
        raise ValueError(f"{name} must be a lowercase sha256 hex digest")
    return value


def freeze(value: Any) -> Any:
    if isinstance(value, Mapping):
        return MappingProxyType({str(k): freeze(v) for k, v in value.items()})
    if isinstance(value, list):
        return tuple(freeze(v) for v in value)
    if isinstance(value, tuple):
        return tuple(freeze(v) for v in value)
    if isinstance(value, set):
        return frozenset(freeze(v) for v in value)
    return value


def thaw(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(k): thaw(v) for k, v in value.items()}
    if isinstance(value, (tuple, list)):
        return [thaw(v) for v in value]
    if isinstance(value, (set, frozenset)):
        return sorted(thaw(v) for v in value)
    if isinstance(value, Enum):
        return value.value
    if is_dataclass(value):
        return {f.name: thaw(getattr(value, f.name)) for f in fields(value)}
    return value


def canonical_hash(value: Any) -> str:
    return sha256(
        json.dumps(thaw(value), sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    ).hexdigest()


class LoopMode(str, Enum):
    OFF = "OFF"
    OBSERVE = "OBSERVE"
    SHADOW_DECIDE = "SHADOW_DECIDE"
    PROPOSE = "PROPOSE"


class Recommendation(str, Enum):
    ACCEPT = "ACCEPT"
    RETRY_RECOMMENDED = "RETRY_RECOMMENDED"
    REWORK_RECOMMENDED = "REWORK_RECOMMENDED"
    REPLAN_RECOMMENDED = "REPLAN_RECOMMENDED"
    BLOCK = "BLOCK"
    ESCALATE = "ESCALATE"
    UNKNOWN = "UNKNOWN"


class Outcome(str, Enum):
    PASS = "PASS"
    FAIL = "FAIL"
    UNKNOWN = "UNKNOWN"


class TranslationStatus(str, Enum):
    TRANSLATED = "TRANSLATED"
    UNSUPPORTED_EVENT_TYPE = "UNSUPPORTED_EVENT_TYPE"
    MISSING_REQUIRED_FIELD = "MISSING_REQUIRED_FIELD"
    PROVENANCE_INSUFFICIENT = "PROVENANCE_INSUFFICIENT"
    INVALID_FIELD_TYPE = "INVALID_FIELD_TYPE"
    INVALID_IDENTITY = "INVALID_IDENTITY"
    INVALID_REVISION = "INVALID_REVISION"
    INVALID_TIMESTAMP = "INVALID_TIMESTAMP"
    IDENTITY_CONFLICT = "IDENTITY_CONFLICT"


@dataclass(frozen=True)
class LoopContract:
    contract_id: str
    version: str
    policy_version: str
    required_criteria: tuple[str, ...]
    max_attempts: int
    max_rework_depth: int

    def __post_init__(self) -> None:
        _identity(self.contract_id, "contract_id")
        _identity(self.version, "version")
        _identity(self.policy_version, "policy_version")
        object.__setattr__(self, "required_criteria", tuple(self.required_criteria))
        if not self.required_criteria or any(not isinstance(x, str) or not x.strip() for x in self.required_criteria):
            raise ValueError("required_criteria must contain non-empty strings")
        if len(set(self.required_criteria)) != len(self.required_criteria):
            raise ValueError("duplicate required criteria")
        _integer(self.max_attempts, "max_attempts", 1)
        _integer(self.max_rework_depth, "max_rework_depth", 0)

    @property
    def contract_hash(self) -> str:
        return canonical_hash(self)


@dataclass(frozen=True)
class LoopEvent:
    event_id: str
    loop_id: str
    revision: int
    event_type: str
    occurred_at_ms: int
    causation_id: str
    correlation_id: str
    payload: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        for name in ("event_id", "loop_id", "event_type", "causation_id", "correlation_id"):
            _identity(getattr(self, name), name)
        _integer(self.revision, "revision", 1)
        _integer(self.occurred_at_ms, "occurred_at_ms", 0)
        object.__setattr__(self, "payload", freeze(dict(self.payload)))


@dataclass(frozen=True)
class CriterionResult:
    criterion_id: str
    outcome: Outcome
    loop_id: str
    attempt_id: str
    task_id: str
    run_id: str
    input_hash: str
    artifact_hash: str | None
    evaluator_principal: str
    evaluator_version: str
    independence_scope: str
    evidence_refs: tuple[str, ...]
    evidence_hash: str
    produced_at_ms: int | None
    valid_until_ms: int | None
    source_transition_id: str
    source_task_revision: int
    canonical_claim_operation_id: str
    claim_epoch_or_fence: str
    execution_generation: int

    def __post_init__(self) -> None:
        for name in (
            "criterion_id", "loop_id", "attempt_id", "task_id", "run_id", "evaluator_principal",
            "evaluator_version", "independence_scope", "source_transition_id",
            "canonical_claim_operation_id", "claim_epoch_or_fence",
        ):
            _identity(getattr(self, name), name)
        _hash(self.input_hash, "input_hash")
        _hash(self.artifact_hash, "artifact_hash", optional=True)
        _hash(self.evidence_hash, "evidence_hash")
        object.__setattr__(self, "evidence_refs", tuple(self.evidence_refs))
        _integer(self.source_task_revision, "source_task_revision", 0)
        _integer(self.execution_generation, "execution_generation", 0)
        if self.produced_at_ms is not None:
            _integer(self.produced_at_ms, "produced_at_ms", 0)
        if self.valid_until_ms is not None:
            _integer(self.valid_until_ms, "valid_until_ms", 0)


@dataclass(frozen=True)
class CanonicalCommandProposal:
    proposal_id: str
    loop_id: str
    attempt_id: str
    decision_event_id: str
    recommendation: Recommendation
    requested_canonical_operation: str
    expected_runtime_revision: int
    reason_code: str
    evidence_set_hash: str
    source_transition_ids: tuple[str, ...]
    policy_version: str
    created_at_ms: int
    requires_human_approval: bool = True
    executable: bool = False
    auto_submit: bool = False
    approval_status: str = "REQUIRED"

    def __post_init__(self) -> None:
        for name in (
            "proposal_id", "loop_id", "attempt_id", "decision_event_id", "requested_canonical_operation",
            "reason_code", "policy_version",
        ):
            _identity(getattr(self, name), name)
        _integer(self.expected_runtime_revision, "expected_runtime_revision", 0)
        _integer(self.created_at_ms, "created_at_ms", 0)
        _hash(self.evidence_set_hash, "evidence_set_hash")
        object.__setattr__(self, "source_transition_ids", tuple(self.source_transition_ids))
        if not self.source_transition_ids or any(not isinstance(x, str) or not x.strip() for x in self.source_transition_ids):
            raise ValueError("source_transition_ids required")
        if not self.requires_human_approval or self.executable or self.auto_submit or self.approval_status != "REQUIRED":
            raise ValueError("proposal safety invariant")


@dataclass(frozen=True)
class CommandReceipt:
    operation: str
    loop_id: str
    idempotency_key: str
    command_hash: str
    result_event_ids: tuple[str, ...]
    resulting_revision: int
    causation_id: str
    correlation_id: str

    def __post_init__(self) -> None:
        for name in ("operation", "loop_id", "idempotency_key", "causation_id", "correlation_id"):
            _identity(getattr(self, name), name)
        _hash(self.command_hash, "command_hash")
        object.__setattr__(self, "result_event_ids", tuple(self.result_event_ids))
        if not self.result_event_ids or any(not isinstance(x, str) or not x.strip() for x in self.result_event_ids):
            raise ValueError("result_event_ids required")
        _integer(self.resulting_revision, "resulting_revision", 1)


@dataclass(frozen=True)
class TranslationResult:
    status: TranslationStatus
    event: LoopEvent | None = None
    errors: tuple[str, ...] = ()


@dataclass(frozen=True)
class DecisionResult:
    recommendation: Recommendation
    reason_code: str
    evidence_set_hash: str
    decision_event_id: str
    resulting_revision: int
    receipt: CommandReceipt


@dataclass(frozen=True)
class ProposalResult:
    proposal: CanonicalCommandProposal
    proposal_event_id: str
    resulting_revision: int
    receipt: CommandReceipt
