"""Feature-flagged canonical RUN_CREATE admission binding.

Sprint 84.4 composes operation-scoped resource reservation, canonical RUN_CREATE
persistence and one replay-safe legacy admitted projection.  The canonical
record is authority; the legacy RUN state and RunAdmitted stream message are
projections.
"""
from __future__ import annotations

import hashlib
import math
import re
import time
import unicodedata
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any, Mapping

from hfa.authority import (
    AggregateType,
    AuthorityCommand,
    AuthorityDecisionCode,
    AuthorityEntryContext,
    CanonicalAggregateIdentity,
    OperationType,
    RedisAuthorityCommitStatus,
    RedisAuthorityCorruptionError,
    RedisAuthorityPersistenceError,
    RedisCanonicalAuthorityStore,
    canonical_json_bytes,
    evaluate_authority_commit,
)
from hfa.config.keys import RedisKey, RedisTTL
from hfa.events.codec import serialize_event
from hfa.events.schema import RunAdmittedEvent
from hfa.governance.admission_resource_reservation import (
    AdmissionResourceReservationInput,
    AdmissionResourceReservationManager,
    RESERVATION_STATE_FINALIZED,
    RESERVATION_STATE_RELEASED,
    RESERVATION_STATE_RESERVED,
    RESERVATION_STATUS_ALREADY_FINALIZED,
    RESERVATION_STATUS_ALREADY_RESERVED,
    RESERVATION_STATUS_BUDGET_EXCEEDED,
    RESERVATION_STATUS_CONFLICT,
    RESERVATION_STATUS_FINALIZED,
    RESERVATION_STATUS_INFLIGHT_EXCEEDED,
    RESERVATION_STATUS_QUOTA_EXCEEDED,
    RESERVATION_STATUS_RELEASED,
    RESERVATION_STATUS_ALREADY_RELEASED,
    RESERVATION_STATUS_RESERVED,
    RESERVATION_STATUS_RESOURCE_STATE_CONFLICT,
    RESERVATION_STATUS_STATE_CONFLICT,
)
from hfa.lua.loader import LuaScriptLoader

FEATURE_FLAG = "HFA_CANONICAL_RUN_CREATE_BINDING"
WRITER_ID = "hfa-control/run-create-writer:v1"

RUN_CREATE_PROJECTED_STATUS = "run_admitted"
RUN_CREATE_DUPLICATE_STATUS = "canonical_run_create_already_projected"
RUN_CREATE_PROJECTION_PENDING_STATUS = "canonical_projection_pending"
RUN_CREATE_EVIDENCE_CONFLICT_STATUS = "canonical_run_create_evidence_conflict"
RUN_CREATE_AUTHORITY_CONFLICT_STATUS = "canonical_run_create_authority_conflict"
RUN_CREATE_CONFIGURATION_CONFLICT_STATUS = "canonical_run_create_configuration_conflict"
RUN_CREATE_RESOURCE_RESERVATION_CONFLICT_STATUS = "resource_reservation_conflict"
RUN_CREATE_RESOURCE_STATE_CONFLICT_STATUS = "resource_state_conflict"
RUN_CREATE_LEGACY_FOOTPRINT_CONFLICT_STATUS = "legacy_RUN_CREATE_footprint_conflict"

_MAX_SAFE_INTEGER = 2**53 - 1
_SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")
_TRUE_VALUES = frozenset({"1", "true", "yes", "on"})
_FALSE_VALUES = frozenset({"", "0", "false", "no", "off"})
_DEFINITIVE_NON_DURABLE_COMMIT_STATUSES = frozenset(
    {
        RedisAuthorityCommitStatus.IDEMPOTENCY_CONFLICT,
        RedisAuthorityCommitStatus.AGGREGATE_ALREADY_EXISTS_CONFLICT,
        RedisAuthorityCommitStatus.STALE_REVISION_CONFLICT,
        RedisAuthorityCommitStatus.FUTURE_REVISION_CONFLICT,
        RedisAuthorityCommitStatus.ILLEGAL_STATE_TRANSITION,
        RedisAuthorityCommitStatus.INVALID_COMMIT_PLAN,
    }
)


def parse_run_create_binding_flag(value: str | None) -> bool:
    normalized = "" if value is None else value.strip().lower()
    if normalized in _TRUE_VALUES:
        return True
    if normalized in _FALSE_VALUES:
        return False
    raise ValueError(f"invalid {FEATURE_FLAG} value: {value!r}")


def _required_text(value: Any, field_name: str, *, allow_empty: bool = False) -> str:
    if type(value) is not str:
        raise ValueError(f"{field_name} must be an exact string")
    if value != value.strip():
        raise ValueError(f"{field_name} must not contain surrounding whitespace")
    normalized = unicodedata.normalize("NFC", value)
    if not allow_empty and not normalized:
        raise ValueError(f"{field_name} must not be empty")
    return normalized


def _required_sha256(value: Any, field_name: str) -> str:
    normalized = _required_text(value, field_name)
    if _SHA256_PATTERN.fullmatch(normalized) is None:
        raise ValueError(f"{field_name} must be lowercase SHA-256 hex")
    return normalized


def _exact_safe_integer(value: Any, field_name: str, *, minimum: int = 0) -> int:
    if type(value) is bool:
        raise ValueError(f"{field_name} must not be bool")
    if type(value) is int:
        result = value
    elif type(value) is float and math.isfinite(value) and value.is_integer():
        result = int(value)
    else:
        raise ValueError(f"{field_name} must be an exact integer")
    if result < minimum or result > _MAX_SAFE_INTEGER:
        raise ValueError(f"{field_name} is outside the safe integer domain")
    return result


def _optional_safe_limit(value: Any, field_name: str) -> int | None:
    if value is None:
        return None
    return _exact_safe_integer(value, field_name)


def _normalize_json(value: Any, *, path: str = "payload") -> Any:
    if value is None or type(value) is bool:
        return value
    if type(value) is str:
        return unicodedata.normalize("NFC", value)
    if type(value) is int:
        return _exact_safe_integer(value, path, minimum=-_MAX_SAFE_INTEGER)
    if type(value) is float:
        if not math.isfinite(value):
            raise ValueError(f"{path} contains a non-finite number")
        return value
    if isinstance(value, Mapping):
        result: dict[str, Any] = {}
        for raw_key in sorted(value, key=lambda item: unicodedata.normalize("NFC", str(item))):
            key = _required_text(raw_key, f"{path} key")
            if key in result:
                raise ValueError(f"{path} contains duplicate normalized key {key!r}")
            result[key] = _normalize_json(value[raw_key], path=f"{path}.{key}")
        return result
    if isinstance(value, (list, tuple)):
        return [
            _normalize_json(item, path=f"{path}[{index}]")
            for index, item in enumerate(value)
        ]
    raise ValueError(f"{path} contains unsupported type {type(value).__name__}")


def _decode(value: Any) -> str:
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="strict")
    return "" if value is None else str(value)


def run_create_identity(run_id: str) -> CanonicalAggregateIdentity:
    return CanonicalAggregateIdentity(
        aggregate_type=AggregateType.RUN,
        run_id=_required_text(run_id, "run_id"),
        task_id=None,
    )


def run_create_operation_id(run_id: str) -> str:
    identity = run_create_identity(run_id)
    return f"run-create:v1:{identity.sha256}"


@dataclass(frozen=True)
class RunCreateAuthorityInput:
    run_id: str
    tenant_id: str
    agent_type: str
    priority: int
    payload: Mapping[str, Any]
    estimated_cost_cents: int
    preferred_region: str
    preferred_placement: str
    created_at_ms: int
    control_stream: str
    run_state_ttl_seconds: int = RedisTTL.RUN_STATE


def normalize_run_create_input(value: RunCreateAuthorityInput) -> RunCreateAuthorityInput:
    if not isinstance(value, RunCreateAuthorityInput):
        raise ValueError("RUN_CREATE input must be RunCreateAuthorityInput")
    payload = _normalize_json(value.payload)
    if not isinstance(payload, dict):
        raise ValueError("payload must normalize to an object")
    return replace(
        value,
        run_id=_required_text(value.run_id, "run_id"),
        tenant_id=_required_text(value.tenant_id, "tenant_id"),
        agent_type=_required_text(value.agent_type, "agent_type"),
        priority=_exact_safe_integer(value.priority, "priority"),
        payload=payload,
        estimated_cost_cents=_exact_safe_integer(
            value.estimated_cost_cents,
            "estimated_cost_cents",
        ),
        preferred_region=_required_text(
            value.preferred_region,
            "preferred_region",
            allow_empty=True,
        ),
        preferred_placement=_required_text(
            value.preferred_placement,
            "preferred_placement",
        ),
        created_at_ms=_exact_safe_integer(value.created_at_ms, "created_at_ms"),
        control_stream=_required_text(value.control_stream, "control_stream"),
        run_state_ttl_seconds=_exact_safe_integer(
            value.run_state_ttl_seconds,
            "run_state_ttl_seconds",
            minimum=1,
        ),
    )


def build_run_create_command(value: RunCreateAuthorityInput) -> AuthorityCommand:
    normalized = normalize_run_create_input(value)
    identity = run_create_identity(normalized.run_id)
    operation_id = f"run-create:v1:{identity.sha256}"
    metadata = {
        "run_id": normalized.run_id,
        "tenant_id": normalized.tenant_id,
        "agent_type": normalized.agent_type,
        "priority": normalized.priority,
        "payload": dict(normalized.payload),
        "estimated_cost_cents": normalized.estimated_cost_cents,
        "preferred_region": normalized.preferred_region,
        "preferred_placement": normalized.preferred_placement,
        "created_at_ms": normalized.created_at_ms,
        "control_stream": normalized.control_stream,
        "run_state_ttl_seconds": normalized.run_state_ttl_seconds,
        "legacy_projection_state": "admitted",
    }
    return AuthorityCommand(
        aggregate_identity=identity,
        operation_type=OperationType.RUN_CREATE,
        operation_id=operation_id,
        expected_revision=0,
        intended_previous_state=None,
        intended_next_state="pending",
        authoritative_payload=metadata,
        authoritative_metadata_changes=metadata,
        requested_child_effects={},
        requested_projection_intents=(
            {
                "kind": "RUN_STATUS_PROJECTION",
                "legacy_state": "admitted",
                "control_stream": normalized.control_stream,
            },
        ),
        causation_id=None,
    )


def build_run_create_context(command: AuthorityCommand) -> AuthorityEntryContext:
    if command.operation_type is not OperationType.RUN_CREATE:
        raise ValueError("RUN_CREATE binding rejects every other operation")
    return AuthorityEntryContext(
        authenticated_writer_id=WRITER_ID,
        allowed_operations=frozenset({OperationType.RUN_CREATE}),
        target_aggregate_identity_sha256=command.aggregate_identity.sha256,
        fence_required=False,
        fence_valid=True,
    )


def _input_from_record(record: Any) -> RunCreateAuthorityInput:
    if record.operation_type != OperationType.RUN_CREATE.value:
        raise ValueError("canonical record operation is not RUN_CREATE")
    if record.aggregate_identity.aggregate_type is not AggregateType.RUN:
        raise ValueError("canonical RUN_CREATE record has wrong aggregate type")
    if record.from_revision != 0 or record.to_revision != 1:
        raise ValueError("canonical RUN_CREATE revision is invalid")
    if record.previous_state is not None or record.next_state != "pending":
        raise ValueError("canonical RUN_CREATE state transition is invalid")
    if record.writer_id != WRITER_ID:
        raise ValueError("canonical RUN_CREATE writer is invalid")
    raw = record.authoritative_metadata_changes
    if not isinstance(raw, Mapping):
        raise ValueError("canonical RUN_CREATE metadata is not an object")
    value = RunCreateAuthorityInput(
        run_id=raw.get("run_id"),
        tenant_id=raw.get("tenant_id"),
        agent_type=raw.get("agent_type"),
        priority=raw.get("priority"),
        payload=raw.get("payload"),
        estimated_cost_cents=raw.get("estimated_cost_cents"),
        preferred_region=raw.get("preferred_region"),
        preferred_placement=raw.get("preferred_placement"),
        created_at_ms=raw.get("created_at_ms"),
        control_stream=raw.get("control_stream"),
        run_state_ttl_seconds=raw.get("run_state_ttl_seconds"),
    )
    normalized = normalize_run_create_input(value)
    if raw.get("legacy_projection_state") != "admitted":
        raise ValueError("canonical RUN_CREATE projection state is invalid")
    rebuilt = build_run_create_command(normalized)
    if rebuilt.operation_id != record.operation_id:
        raise ValueError("canonical RUN_CREATE operation identity mismatch")
    if rebuilt.canonical_command_hash != record.canonical_command_hash:
        raise ValueError("canonical RUN_CREATE command proof mismatch")
    return normalized


def _stable_event_fields(value: RunCreateAuthorityInput, operation_id: str) -> dict[str, str]:
    normalized = normalize_run_create_input(value)
    event_digest = hashlib.sha256(
        f"run-admitted-event:v1:{operation_id}".encode("utf-8")
    ).hexdigest()
    seconds = normalized.created_at_ms / 1000.0
    event = RunAdmittedEvent(
        event_id=event_digest,
        timestamp=seconds,
        trace_parent=None,
        trace_state=None,
        run_id=normalized.run_id,
        tenant_id=normalized.tenant_id,
        agent_type=normalized.agent_type,
        priority=normalized.priority,
        preferred_region=normalized.preferred_region,
        preferred_placement=normalized.preferred_placement,
        payload=dict(normalized.payload),
        estimated_cost_cents=normalized.estimated_cost_cents,
        admitted_at=seconds,
    )
    return serialize_event(event)


def _projection_lua_path() -> Path:
    filename = "run_create_projection.lua"
    here = Path(__file__).resolve()
    candidates = (
        here.parent.parent.parent.parent.parent
        / "hfa-core"
        / "src"
        / "hfa"
        / "lua"
        / filename,
        here.parent.parent.parent.parent / "hfa" / "lua" / filename,
    )
    for candidate in candidates:
        if candidate.exists():
            return candidate
    for parent in here.parents:
        for candidate in (
            parent / "hfa-core" / "src" / "hfa" / "lua" / filename,
            parent / "hfa" / "lua" / filename,
        ):
            if candidate.exists():
                return candidate
    raise FileNotFoundError(f"{filename} not found")


@dataclass(frozen=True)
class RunCreateProjectionInput:
    operation_id: str
    reservation_proof_sha256: str
    canonical_transition_id: str
    canonical_record_hash: str
    canonical_command_hash: str
    canonical_revision: int
    run_id: str
    tenant_id: str
    legacy_state: str
    state_ttl_seconds: int
    control_stream: str
    event_fields: Mapping[str, str]


@dataclass(frozen=True)
class RunCreateProjectionResult:
    status: str
    stream_entry_id: str
    projected: bool


class RunCreateProjectionManager:
    def __init__(self, redis: Any, *, loader: Any | None = None) -> None:
        self._redis = redis
        self._loader = loader
        self._initialised = False

    @staticmethod
    def receipt_key(operation_id: str) -> str:
        operation = _required_text(operation_id, "operation_id")
        digest = hashlib.sha256(operation.encode("utf-8")).hexdigest()
        return f"{RedisKey.PREFIX}:run-create:projection:v1:{digest}"

    async def initialise(self) -> None:
        if self._initialised:
            return
        if self._loader is None:
            self._loader = LuaScriptLoader(self._redis, _projection_lua_path())
        await self._loader.load()
        self._initialised = True

    async def project(
        self,
        value: RunCreateProjectionInput,
    ) -> RunCreateProjectionResult:
        if not isinstance(value, RunCreateProjectionInput):
            raise TypeError("projection must be RunCreateProjectionInput")
        operation_id = _required_text(value.operation_id, "operation_id")
        reservation_proof = _required_sha256(
            value.reservation_proof_sha256,
            "reservation_proof_sha256",
        )
        transition_id = _required_text(
            value.canonical_transition_id,
            "canonical_transition_id",
        )
        record_hash = _required_sha256(
            value.canonical_record_hash,
            "canonical_record_hash",
        )
        command_hash = _required_sha256(
            value.canonical_command_hash,
            "canonical_command_hash",
        )
        revision = _exact_safe_integer(
            value.canonical_revision,
            "canonical_revision",
            minimum=1,
        )
        run_id = _required_text(value.run_id, "run_id")
        tenant_id = _required_text(value.tenant_id, "tenant_id")
        legacy_state = _required_text(value.legacy_state, "legacy_state")
        state_ttl = _exact_safe_integer(
            value.state_ttl_seconds,
            "state_ttl_seconds",
            minimum=1,
        )
        control_stream = _required_text(value.control_stream, "control_stream")
        if operation_id != run_create_operation_id(run_id):
            raise ValueError("projection operation_id does not match RUN identity")
        if revision != 1:
            raise ValueError("RUN_CREATE projection revision must be 1")
        if legacy_state != "admitted":
            raise ValueError("RUN_CREATE legacy projection state must be admitted")
        if not isinstance(value.event_fields, Mapping):
            raise ValueError("event_fields must be a mapping")
        fields = {
            _required_text(key, "event field name"): _required_text(
                field_value,
                f"event_fields.{key}",
                allow_empty=True,
            )
            for key, field_value in value.event_fields.items()
        }
        if fields.get("event_type") != "RunAdmitted":
            raise ValueError("projection event_type must be RunAdmitted")
        if fields.get("run_id") != run_id or fields.get("tenant_id") != tenant_id:
            raise ValueError("projection event identity mismatch")
        event_json = canonical_json_bytes(fields).decode("utf-8")
        event_hash = hashlib.sha256(event_json.encode("utf-8")).hexdigest()
        flattened: list[str] = []
        for key, field_value in fields.items():
            flattened.extend((key, field_value))
        await self.initialise()
        assert self._loader is not None
        raw = await self._loader.run(
            num_keys=3,
            keys=[
                self.receipt_key(operation_id),
                RedisKey.run_state(run_id),
                control_stream,
            ],
            args=[
                operation_id,
                reservation_proof,
                transition_id,
                record_hash,
                command_hash,
                str(revision),
                run_id,
                tenant_id,
                legacy_state,
                str(state_ttl),
                event_hash,
                str(RedisTTL.STREAM_MAXLEN),
                str(len(fields)),
                *flattened,
            ],
        )
        if not isinstance(raw, (list, tuple)) or len(raw) < 2:
            raise RunCreateEvidenceConflictError(
                f"invalid RUN_CREATE projection result: {raw!r}"
            )
        status = _decode(raw[0])
        stream_entry_id = _decode(raw[1])
        if status == RUN_CREATE_PROJECTED_STATUS:
            return RunCreateProjectionResult(status, stream_entry_id, True)
        if status == RUN_CREATE_DUPLICATE_STATUS:
            return RunCreateProjectionResult(status, stream_entry_id, False)
        if status == RUN_CREATE_EVIDENCE_CONFLICT_STATUS:
            detail = _decode(raw[2]) if len(raw) > 2 else "projection conflict"
            raise RunCreateEvidenceConflictError(detail, canonical_commit_durable=True)
        raise RunCreateEvidenceConflictError(
            f"unknown RUN_CREATE projection status: {status!r}",
            canonical_commit_durable=True,
        )


class RunCreateAuthorityError(RuntimeError):
    def __init__(
        self,
        *,
        status: str,
        detail: str = "",
        canonical_commit_durable: bool = False,
        resource_status: str | None = None,
    ) -> None:
        super().__init__(f"{status}: {detail}".rstrip(": "))
        self.status = status
        self.detail = detail
        self.canonical_commit_durable = canonical_commit_durable
        self.resource_status = resource_status
        self.execution_allowed = False
        self.automatic_release = False
        self.automatic_repair = False


class RunCreateEvidenceConflictError(RunCreateAuthorityError):
    def __init__(self, detail: str, *, canonical_commit_durable: bool = False) -> None:
        super().__init__(
            status=RUN_CREATE_EVIDENCE_CONFLICT_STATUS,
            detail=detail,
            canonical_commit_durable=canonical_commit_durable,
        )


class RunCreateProjectionPendingError(RunCreateAuthorityError):
    def __init__(self, detail: str) -> None:
        super().__init__(
            status=RUN_CREATE_PROJECTION_PENDING_STATUS,
            detail=detail,
            canonical_commit_durable=True,
        )
        self.retry_safe = True


class RunCreateAuthorityConflictError(RunCreateAuthorityError):
    def __init__(
        self,
        *,
        status: str = RUN_CREATE_AUTHORITY_CONFLICT_STATUS,
        detail: str = "",
        canonical_commit_durable: bool = False,
    ) -> None:
        super().__init__(
            status=status,
            detail=detail,
            canonical_commit_durable=canonical_commit_durable,
        )


class RunCreateResourceError(RunCreateAuthorityError):
    def __init__(self, *, resource_status: str, detail: str = "") -> None:
        status = (
            RUN_CREATE_RESOURCE_STATE_CONFLICT_STATUS
            if resource_status == RESERVATION_STATUS_RESOURCE_STATE_CONFLICT
            else RUN_CREATE_RESOURCE_RESERVATION_CONFLICT_STATUS
        )
        super().__init__(
            status=status,
            detail=detail or resource_status,
            resource_status=resource_status,
        )


@dataclass(frozen=True)
class RunCreateBindingResult:
    status: str
    run_id: str
    stream_entry_id: str
    first_projection: bool
    canonical_transition_id: str
    canonical_record_hash: str
    canonical_command_hash: str
    canonical_revision: int


@dataclass
class RunCreateAuthorityBinding:
    redis: Any
    resource_manager: AdmissionResourceReservationManager
    control_stream: str
    store: RedisCanonicalAuthorityStore | None = None
    projection_manager: RunCreateProjectionManager | None = None
    run_state_ttl_seconds: int = RedisTTL.RUN_STATE

    def __post_init__(self) -> None:
        self.control_stream = _required_text(self.control_stream, "control_stream")
        self.run_state_ttl_seconds = _exact_safe_integer(
            self.run_state_ttl_seconds,
            "run_state_ttl_seconds",
            minimum=1,
        )
        if self.store is None:
            self.store = RedisCanonicalAuthorityStore(self.redis)
        if self.projection_manager is None:
            self.projection_manager = RunCreateProjectionManager(self.redis)
        self._initialised = False

    async def initialise(self) -> None:
        if self._initialised:
            return
        assert self.store is not None
        assert self.projection_manager is not None
        try:
            await self.resource_manager.initialise()
            await self.store.initialise()
            await self.projection_manager.initialise()
        except Exception as exc:
            raise RunCreateAuthorityConflictError(
                status=RUN_CREATE_CONFIGURATION_CONFLICT_STATUS,
                detail=str(exc),
            ) from exc
        self._initialised = True

    @staticmethod
    def _continuity_error(snapshot: Any, probe: Any) -> str | None:
        if (snapshot is None) != (probe is None):
            return "snapshot_receipt_presence_mismatch"
        if snapshot is None:
            return None
        record = probe.canonical_store_record
        receipt = probe.receipt
        if record is None:
            return "receipt_record_missing"
        checks = (
            snapshot.operation_id == receipt.operation_id == record.operation_id,
            snapshot.transition_id == receipt.transition_id == record.transition_id,
            snapshot.canonical_command_hash
            == receipt.canonical_command_hash
            == record.canonical_command_hash,
            snapshot.canonical_record_hash
            == receipt.canonical_record_hash
            == record.canonical_record_hash,
            snapshot.revision == receipt.aggregate_revision == record.to_revision,
            snapshot.state == record.next_state,
        )
        return None if all(checks) else "snapshot_receipt_continuity_mismatch"

    @staticmethod
    def _persistence_status(exc: Exception) -> str:
        if isinstance(exc, RedisAuthorityCorruptionError):
            return "CANONICAL_RECORD_CORRUPTION_CONFLICT"
        return "CONFLICT_EVIDENCE_STORE_UNAVAILABLE"

    async def _legacy_footprint_exists(self, run_id: str) -> bool:
        try:
            if await self.redis.exists(RedisKey.run_state(run_id)):
                return True
            if await self.redis.exists(RedisKey.run_meta(run_id)):
                return True
            kind = _decode(await self.redis.type(self.control_stream))
            if kind not in {"none", "stream"}:
                raise RunCreateEvidenceConflictError(
                    f"control stream has wrong Redis type: {kind}"
                )
            if kind == "stream":
                entries = await self.redis.xrange(
                    self.control_stream,
                    min="-",
                    max="+",
                )
                for _entry_id, raw_fields in entries:
                    fields = {
                        _decode(key): _decode(field_value)
                        for key, field_value in raw_fields.items()
                    }
                    if (
                        fields.get("event_type") == "RunAdmitted"
                        and fields.get("run_id") == run_id
                    ):
                        return True
            return False
        except RunCreateAuthorityError:
            raise
        except Exception as exc:
            raise RunCreateEvidenceConflictError(
                f"legacy RUN footprint inspection failed: {exc}"
            ) from exc

    async def _validate_exact_head(self, record: Any, receipt: Any) -> None:
        assert self.store is not None
        try:
            await self.store.validate_authority_head(
                record.aggregate_identity,
                expected_operation_id=record.operation_id,
                expected_operation_digest=self.store.keyspace(
                    record.aggregate_identity_sha256
                ).operation_field(record.operation_id),
                expected_transition_id=record.transition_id,
                expected_revision=record.to_revision,
                expected_canonical_command_hash=record.canonical_command_hash,
                expected_canonical_record_hash=record.canonical_record_hash,
                expected_record=record,
                expected_receipt=receipt,
                expected_state=record.next_state,
                expected_projection_intents_json=canonical_json_bytes(
                    record.durable_projection_intents
                ).decode("utf-8"),
                expected_updated_at_ms=record.committed_at_ms,
            )
        except Exception as exc:
            raise RunCreateAuthorityConflictError(
                status=self._persistence_status(exc),
                detail=str(exc),
                canonical_commit_durable=True,
            ) from exc

    async def _release_if_owned(
        self,
        reservation: AdmissionResourceReservationInput,
        *,
        created_here: bool,
    ) -> None:
        if not created_here:
            return
        try:
            result = await self.resource_manager.release_once(reservation)
        except Exception as exc:
            raise RunCreateResourceError(
                resource_status=RESERVATION_STATUS_RESOURCE_STATE_CONFLICT,
                detail=f"precommit release failed: {exc}",
            ) from exc
        if (
            result.status
            not in {
                RESERVATION_STATUS_RELEASED,
                RESERVATION_STATUS_ALREADY_RELEASED,
            }
            or result.state != RESERVATION_STATE_RELEASED
        ):
            raise RunCreateResourceError(
                resource_status=result.status,
                detail="precommit release did not reach RELEASED",
            )

    @staticmethod
    def _validate_reservation_result(result: Any) -> bool:
        expected = {
            RESERVATION_STATUS_RESERVED: (RESERVATION_STATE_RESERVED, True),
            RESERVATION_STATUS_ALREADY_RESERVED: (
                RESERVATION_STATE_RESERVED,
                False,
            ),
            RESERVATION_STATUS_ALREADY_FINALIZED: (
                RESERVATION_STATE_FINALIZED,
                False,
            ),
        }
        contract = expected.get(result.status)
        if contract is None:
            raise RunCreateResourceError(
                resource_status=result.status,
                detail="resource reservation rejected RUN_CREATE",
            )
        expected_state, expected_mutation = contract
        if (
            result.state != expected_state
            or result.resource_mutated is not expected_mutation
        ):
            raise RunCreateResourceError(
                resource_status=RESERVATION_STATUS_STATE_CONFLICT,
                detail=(
                    "resource reservation status/state/mutation contract "
                    "is inconsistent"
                ),
            )
        return result.status == RESERVATION_STATUS_RESERVED

    async def _record_policy_conflict(
        self,
        command: AuthorityCommand,
        *,
        decision_code: AuthorityDecisionCode,
        snapshot: Any,
        probe: Any,
    ) -> None:
        assert self.store is not None
        try:
            status = RedisAuthorityCommitStatus(decision_code.value)
        except ValueError as exc:
            raise RunCreateAuthorityConflictError(
                status=decision_code.value,
                detail="canonical policy blocked RUN_CREATE",
            ) from exc
        record = None if probe is None else probe.canonical_store_record
        try:
            await self.store.record_authority_conflict(
                command.aggregate_identity,
                status=status,
                operation_id=command.operation_id,
                incoming_command_hash=command.canonical_command_hash,
                stored_command_hash=(
                    None if record is None else record.canonical_command_hash
                ),
                existing_transition_id=(
                    None if record is None else record.transition_id
                ),
                aggregate_revision=(
                    None if snapshot is None else snapshot.revision
                ),
                observed_at_ms=command.authoritative_payload["created_at_ms"],
                detail_code=f"policy_{decision_code.value.lower()}",
                detail="canonical policy blocked RUN_CREATE projection",
            )
        except Exception as exc:
            raise RunCreateAuthorityConflictError(
                status="CONFLICT_EVIDENCE_STORE_UNAVAILABLE",
                detail=str(exc),
            ) from exc

    async def _resolve_ambiguous_commit(
        self,
        command: AuthorityCommand,
    ) -> tuple[Any | None, Any | None]:
        assert self.store is not None
        try:
            snapshot = await self.store.get_aggregate_snapshot(
                command.aggregate_identity
            )
            probe = await self.store.load_receipt_probe(
                command.aggregate_identity,
                command.operation_id,
            )
        except Exception as exc:
            raise RunCreateAuthorityConflictError(
                status="CONFLICT_EVIDENCE_STORE_UNAVAILABLE",
                detail=f"ambiguous commit proof unavailable: {exc}",
            ) from exc
        continuity = self._continuity_error(snapshot, probe)
        if continuity:
            raise RunCreateAuthorityConflictError(
                status="CANONICAL_RECORD_CORRUPTION_CONFLICT",
                detail=continuity,
                canonical_commit_durable=probe is not None,
            )
        if snapshot is None and probe is None:
            return None, None
        assert probe is not None
        record = probe.canonical_store_record
        if (
            record is None
            or record.canonical_command_hash != command.canonical_command_hash
        ):
            raise RunCreateAuthorityConflictError(
                status="IDEMPOTENCY_CONFLICT",
                detail="ambiguous commit resolved to different canonical proof",
                canonical_commit_durable=True,
            )
        await self._validate_exact_head(record, probe.receipt)
        return record, probe.receipt

    async def admit(
        self,
        request: Any,
        *,
        tenant_inflight_limit: int | None,
        concurrent_run_limit: int | None = None,
        budget_limit_cents: int | None = None,
    ) -> RunCreateBindingResult:
        # Validate every request-controlled field before touching Redis, loading
        # Lua, or initializing canonical persistence.
        run_id = _required_text(getattr(request, "run_id", None), "run_id")
        tenant_id = _required_text(
            getattr(request, "tenant_id", None),
            "tenant_id",
        )
        agent_type = _required_text(
            getattr(request, "agent_type", None),
            "agent_type",
        )
        priority = _exact_safe_integer(
            getattr(request, "priority", None),
            "priority",
        )
        payload = _normalize_json(getattr(request, "payload", None))
        if not isinstance(payload, dict):
            raise ValueError("payload must normalize to an object")
        estimated = _exact_safe_integer(
            getattr(request, "estimated_cost_cents", 0),
            "estimated_cost_cents",
        )
        preferred_region = _required_text(
            getattr(request, "preferred_region", ""),
            "preferred_region",
            allow_empty=True,
        )
        preferred_placement = _required_text(
            getattr(request, "preferred_placement", "LEAST_LOADED"),
            "preferred_placement",
        )
        tenant_inflight_limit = _optional_safe_limit(
            tenant_inflight_limit,
            "tenant_inflight_limit",
        )
        concurrent_run_limit = _optional_safe_limit(
            concurrent_run_limit,
            "concurrent_run_limit",
        )
        budget_limit_cents = _optional_safe_limit(
            budget_limit_cents,
            "budget_limit_cents",
        )
        await self.initialise()
        operation_id = run_create_operation_id(run_id)
        reservation = AdmissionResourceReservationInput(
            operation_id=operation_id,
            run_id=run_id,
            tenant_id=tenant_id,
            estimated_cost_cents=estimated,
        )
        try:
            reserved = await self.resource_manager.reserve_once(
                reservation,
                concurrent_run_limit=concurrent_run_limit,
                budget_limit_cents=budget_limit_cents,
                tenant_inflight_limit=tenant_inflight_limit,
            )
        except Exception as exc:
            raise RunCreateResourceError(
                resource_status=RESERVATION_STATUS_RESOURCE_STATE_CONFLICT,
                detail=str(exc),
            ) from exc

        created_here = self._validate_reservation_result(reserved)

        try:
            receipt = await self.resource_manager.get_receipt(reservation)
        except Exception as exc:
            raise RunCreateResourceError(
                resource_status=RESERVATION_STATUS_CONFLICT,
                detail=str(exc),
            ) from exc
        if receipt is None:
            raise RunCreateResourceError(
                resource_status=RESERVATION_STATUS_STATE_CONFLICT,
                detail="resource receipt is missing after reservation",
            )

        identity = run_create_identity(run_id)
        assert self.store is not None
        try:
            snapshot = await self.store.get_aggregate_snapshot(identity)
            probe = await self.store.load_receipt_probe(
                identity,
                operation_id,
            )
        except Exception as exc:
            raise RunCreateAuthorityConflictError(
                status=self._persistence_status(exc),
                detail=str(exc),
            ) from exc

        committed_template = None
        if probe is not None and probe.canonical_store_record is not None:
            try:
                committed_template = _input_from_record(
                    probe.canonical_store_record
                )
            except Exception as exc:
                raise RunCreateAuthorityConflictError(
                    status="CANONICAL_RECORD_CORRUPTION_CONFLICT",
                    detail=str(exc),
                    canonical_commit_durable=True,
                ) from exc

        value = normalize_run_create_input(
            RunCreateAuthorityInput(
                run_id=run_id,
                tenant_id=tenant_id,
                agent_type=agent_type,
                priority=priority,
                payload=payload,
                estimated_cost_cents=estimated,
                preferred_region=preferred_region,
                preferred_placement=preferred_placement,
                created_at_ms=receipt.created_at_ms,
                control_stream=(
                    self.control_stream
                    if committed_template is None
                    else committed_template.control_stream
                ),
                run_state_ttl_seconds=(
                    self.run_state_ttl_seconds
                    if committed_template is None
                    else committed_template.run_state_ttl_seconds
                ),
            )
        )
        command = build_run_create_command(value)
        context = build_run_create_context(command)

        continuity = self._continuity_error(snapshot, probe)
        if continuity:
            raise RunCreateAuthorityConflictError(
                status="CANONICAL_RECORD_CORRUPTION_CONFLICT",
                detail=continuity,
                canonical_commit_durable=probe is not None,
            )

        if snapshot is None and probe is None:
            try:
                legacy_footprint = await self._legacy_footprint_exists(run_id)
            except Exception:
                await self._release_if_owned(
                    reservation,
                    created_here=created_here,
                )
                raise
            if legacy_footprint:
                await self._release_if_owned(
                    reservation,
                    created_here=created_here,
                )
                raise RunCreateEvidenceConflictError(
                    RUN_CREATE_LEGACY_FOOTPRINT_CONFLICT_STATUS
                )

        evaluation = evaluate_authority_commit(
            context=context,
            command=command,
            current_revision=0 if snapshot is None else snapshot.revision,
            current_state=None if snapshot is None else snapshot.state,
            receipt_probe=probe,
            committed_at_ms=receipt.created_at_ms,
            correlation_id=None,
        )

        record = None
        canonical_receipt = None
        if evaluation.decision.code is AuthorityDecisionCode.ALREADY_APPLIED:
            if probe is None or probe.canonical_store_record is None:
                raise RunCreateAuthorityConflictError(
                    status="CANONICAL_RECORD_CORRUPTION_CONFLICT",
                    detail="duplicate RUN_CREATE proof is incomplete",
                    canonical_commit_durable=True,
                )
            record = probe.canonical_store_record
            canonical_receipt = probe.receipt
            await self._validate_exact_head(record, canonical_receipt)
        elif evaluation.decision.code is AuthorityDecisionCode.ACCEPTED:
            if evaluation.commit_plan is None:
                await self._release_if_owned(
                    reservation,
                    created_here=created_here,
                )
                raise RunCreateAuthorityConflictError(
                    status="AUTHORITY_ENTRY_REJECTED",
                    detail="accepted RUN_CREATE has no commit plan",
                )
            try:
                persisted = await self.store.commit(evaluation.commit_plan)
            except Exception as commit_exc:
                record, canonical_receipt = await self._resolve_ambiguous_commit(
                    command
                )
                if record is None:
                    await self._release_if_owned(
                        reservation,
                        created_here=created_here,
                    )
                    raise RunCreateAuthorityConflictError(
                        status="CONFLICT_EVIDENCE_STORE_UNAVAILABLE",
                        detail=f"canonical commit failed and absence was proven: {commit_exc}",
                    ) from commit_exc
            else:
                if persisted.status is RedisAuthorityCommitStatus.COMMITTED:
                    record = evaluation.commit_plan.record
                    canonical_receipt = evaluation.commit_plan.receipt
                elif persisted.status is RedisAuthorityCommitStatus.ALREADY_APPLIED:
                    record, canonical_receipt = await self._resolve_ambiguous_commit(
                        command
                    )
                    if record is None or canonical_receipt is None:
                        raise RunCreateAuthorityConflictError(
                            status="CANONICAL_RECORD_CORRUPTION_CONFLICT",
                            detail=(
                                "ALREADY_APPLIED did not resolve to exact "
                                "canonical proof"
                            ),
                        )
                else:
                    if (
                        persisted.status
                        in _DEFINITIVE_NON_DURABLE_COMMIT_STATUSES
                    ):
                        await self._release_if_owned(
                            reservation,
                            created_here=created_here,
                        )
                    raise RunCreateAuthorityConflictError(
                        status=persisted.status.value,
                        detail=persisted.detail,
                    )
        else:
            # Policy evaluation occurred before any commit attempt, and the
            # exact snapshot/receipt read above proves the incoming command was
            # not durably committed by this invocation.
            await self._release_if_owned(
                reservation,
                created_here=created_here,
            )
            await self._record_policy_conflict(
                command,
                decision_code=evaluation.decision.code,
                snapshot=snapshot,
                probe=probe,
            )
            raise RunCreateAuthorityConflictError(
                status=evaluation.decision.code.value,
                detail="canonical policy blocked RUN_CREATE",
                canonical_commit_durable=probe is not None,
            )

        assert record is not None
        assert canonical_receipt is not None
        await self._validate_exact_head(record, canonical_receipt)
        try:
            committed_value = _input_from_record(record)
        except Exception as exc:
            raise RunCreateProjectionPendingError(
                f"canonical RUN_CREATE record cannot drive projection: {exc}"
            ) from exc

        try:
            finalized = await self.resource_manager.finalize_once(reservation)
        except Exception as exc:
            raise RunCreateProjectionPendingError(
                f"resource finalization failed after canonical commit: {exc}"
            ) from exc
        if (
            finalized.status
            not in {RESERVATION_STATUS_FINALIZED, RESERVATION_STATUS_ALREADY_FINALIZED}
            or finalized.state != RESERVATION_STATE_FINALIZED
        ):
            raise RunCreateProjectionPendingError(
                "resource receipt did not reach FINALIZED after canonical commit"
            )

        event_fields = _stable_event_fields(committed_value, record.operation_id)
        projection = RunCreateProjectionInput(
            operation_id=record.operation_id,
            reservation_proof_sha256=reservation.proof_sha256,
            canonical_transition_id=record.transition_id,
            canonical_record_hash=record.canonical_record_hash,
            canonical_command_hash=record.canonical_command_hash,
            canonical_revision=record.to_revision,
            run_id=committed_value.run_id,
            tenant_id=committed_value.tenant_id,
            legacy_state="admitted",
            state_ttl_seconds=committed_value.run_state_ttl_seconds,
            control_stream=committed_value.control_stream,
            event_fields=event_fields,
        )
        assert self.projection_manager is not None
        try:
            projected = await self.projection_manager.project(projection)
        except RunCreateAuthorityError as exc:
            raise RunCreateProjectionPendingError(str(exc)) from exc
        except Exception as exc:
            raise RunCreateProjectionPendingError(
                f"legacy admitted projection failed: {exc}"
            ) from exc

        return RunCreateBindingResult(
            status=projected.status,
            run_id=committed_value.run_id,
            stream_entry_id=projected.stream_entry_id,
            first_projection=projected.projected,
            canonical_transition_id=record.transition_id,
            canonical_record_hash=record.canonical_record_hash,
            canonical_command_hash=record.canonical_command_hash,
            canonical_revision=record.to_revision,
        )


__all__ = [
    "FEATURE_FLAG",
    "RUN_CREATE_AUTHORITY_CONFLICT_STATUS",
    "RUN_CREATE_CONFIGURATION_CONFLICT_STATUS",
    "RUN_CREATE_DUPLICATE_STATUS",
    "RUN_CREATE_EVIDENCE_CONFLICT_STATUS",
    "RUN_CREATE_PROJECTED_STATUS",
    "RUN_CREATE_PROJECTION_PENDING_STATUS",
    "RUN_CREATE_RESOURCE_RESERVATION_CONFLICT_STATUS",
    "RUN_CREATE_RESOURCE_STATE_CONFLICT_STATUS",
    "RunCreateAuthorityBinding",
    "RunCreateAuthorityConflictError",
    "RunCreateAuthorityError",
    "RunCreateAuthorityInput",
    "RunCreateBindingResult",
    "RunCreateEvidenceConflictError",
    "RunCreateProjectionInput",
    "RunCreateProjectionManager",
    "RunCreateProjectionPendingError",
    "RunCreateProjectionResult",
    "RunCreateResourceError",
    "build_run_create_command",
    "build_run_create_context",
    "normalize_run_create_input",
    "parse_run_create_binding_flag",
    "run_create_identity",
    "run_create_operation_id",
]
