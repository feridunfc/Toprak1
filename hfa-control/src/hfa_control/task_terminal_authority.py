"""Canonical TASK_COMPLETE / TASK_FAIL authority binding.

Sprint 84.7C1 establishes the missing operation-specific TASK_COMPLETE and
TASK_FAIL authority foundation on the existing canonical TASK aggregate.
Canonical authority is committed before the existing Redis/Lua terminal path
is allowed to act as a proof-bound projection. Production worker composition
remains a later sprint.
"""
from __future__ import annotations

import hashlib
import json
import math
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
    RedisCanonicalAuthorityStore,
    canonical_json_bytes,
    evaluate_authority_commit,
)
from hfa.config.keys import RedisKey, RedisTTL
from hfa.dag.schema import DagRedisKey
from hfa.lua.loader import LuaScriptLoader

WRITER_ID = "hfa-worker/task-terminal-writer:v1"

TASK_TERMINAL_PROJECTED_STATUS = "task_terminal_projected"
TASK_TERMINAL_DUPLICATE_STATUS = "canonical_terminal_already_projected"
TASK_TERMINAL_PROJECTION_PENDING_STATUS = "canonical_terminal_projection_pending"
TASK_TERMINAL_NOT_COMMITTED_STATUS = "canonical_terminal_not_committed"
TASK_TERMINAL_EVIDENCE_CONFLICT_STATUS = "canonical_terminal_evidence_conflict"
TASK_TERMINAL_AUTHORITY_CONFLICT_STATUS = "canonical_terminal_authority_conflict"
TASK_TERMINAL_CONFIGURATION_CONFLICT_STATUS = "canonical_terminal_configuration_conflict"

_MAX_SAFE_INTEGER = 2**53 - 1
_SHA256_HEX = frozenset("0123456789abcdef")


def _required_text(
    value: Any,
    field_name: str,
    *,
    allow_empty: bool = False,
) -> str:
    if type(value) is not str:
        raise ValueError(f"{field_name} must be a string")
    normalized = value.strip()
    if not allow_empty and not normalized:
        raise ValueError(f"{field_name} must not be empty")
    return normalized


def _safe_integer(value: Any, field_name: str, *, minimum: int = 0) -> int:
    if type(value) is bool:
        raise ValueError(f"{field_name} must not be bool")
    if type(value) is int:
        result = value
    elif type(value) is float and math.isfinite(value) and value.is_integer():
        result = int(value)
    elif type(value) is str and value.isdecimal():
        result = int(value)
    else:
        raise ValueError(f"{field_name} must be an exact integer")
    if result < minimum or result > _MAX_SAFE_INTEGER:
        raise ValueError(f"{field_name} is outside the safe integer domain")
    return result


def _required_sha256(value: Any, field_name: str) -> str:
    digest = _required_text(value, field_name)
    if len(digest) != 64 or any(char not in _SHA256_HEX for char in digest):
        raise ValueError(f"{field_name} must be lowercase SHA-256")
    return digest


def _decode(value: Any) -> str:
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return "" if value is None else str(value)


async def _maybe_await(value: Any) -> Any:
    if hasattr(value, "__await__"):
        return await value
    return value


def _canonical_output_data(value: Any, *, terminal_state: str) -> str:
    raw = "" if value is None else str(value)
    if terminal_state == "failed":
        # Existing TASK_FAIL projection does not persist executor output. Keep
        # the canonical terminal command aligned with that locked behavior.
        return ""
    try:
        decoded = json.loads(raw)
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise ValueError("output_data must be valid JSON for TASK_COMPLETE") from exc
    if not isinstance(decoded, dict):
        raise ValueError("output_data must encode a JSON object for TASK_COMPLETE")
    return canonical_json_bytes(decoded).decode("utf-8")


def _output_sha256(output_data: str) -> str:
    return hashlib.sha256(output_data.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class TaskTerminalAuthorityInput:
    task_id: str
    run_id: str
    tenant_id: str
    terminal_state: str
    finished_at_ms: int
    reason_code: str
    worker_instance_id: str
    scheduler_epoch: str
    claim_epoch: int
    output_data: str
    output_sha256: str
    claim_transition_id: str
    claim_record_hash: str
    claim_command_hash: str
    claim_revision: int
    claim_operation_id: str

    @property
    def operation_type(self) -> OperationType:
        return (
            OperationType.TASK_COMPLETE
            if self.terminal_state == "done"
            else OperationType.TASK_FAIL
        )


def normalize_task_terminal_input(
    value: TaskTerminalAuthorityInput,
) -> TaskTerminalAuthorityInput:
    if not isinstance(value, TaskTerminalAuthorityInput):
        raise ValueError("terminal input must be TaskTerminalAuthorityInput")

    terminal_state = _required_text(value.terminal_state, "terminal_state")
    if terminal_state not in {"done", "failed"}:
        raise ValueError("terminal_state must be done or failed")

    output_data = _canonical_output_data(
        value.output_data,
        terminal_state=terminal_state,
    )
    expected_output_sha256 = (
        _output_sha256(output_data) if terminal_state == "done" else ""
    )
    supplied_output_sha256 = _required_text(
        value.output_sha256,
        "output_sha256",
        allow_empty=True,
    )
    if supplied_output_sha256 != expected_output_sha256:
        raise ValueError("output_sha256 does not match canonical output_data")

    normalized = replace(
        value,
        task_id=_required_text(value.task_id, "task_id"),
        run_id=_required_text(value.run_id, "run_id"),
        tenant_id=_required_text(value.tenant_id, "tenant_id"),
        terminal_state=terminal_state,
        finished_at_ms=_safe_integer(value.finished_at_ms, "finished_at_ms"),
        reason_code=_required_text(value.reason_code, "reason_code"),
        worker_instance_id=_required_text(
            value.worker_instance_id,
            "worker_instance_id",
        ),
        scheduler_epoch=_required_text(value.scheduler_epoch, "scheduler_epoch"),
        claim_epoch=_safe_integer(value.claim_epoch, "claim_epoch", minimum=1),
        output_data=output_data,
        output_sha256=expected_output_sha256,
        claim_transition_id=_required_text(
            value.claim_transition_id,
            "claim_transition_id",
        ),
        claim_record_hash=_required_sha256(
            value.claim_record_hash,
            "claim_record_hash",
        ),
        claim_command_hash=_required_sha256(
            value.claim_command_hash,
            "claim_command_hash",
        ),
        claim_revision=_safe_integer(
            value.claim_revision,
            "claim_revision",
            minimum=1,
        ),
        claim_operation_id=_required_text(
            value.claim_operation_id,
            "claim_operation_id",
        ),
    )
    if normalized.scheduler_epoch == "0":
        raise ValueError("scheduler_epoch must represent acquired authority")
    return normalized


def task_terminal_identity(
    value: TaskTerminalAuthorityInput,
) -> CanonicalAggregateIdentity:
    terminal = normalize_task_terminal_input(value)
    return CanonicalAggregateIdentity(
        aggregate_type=AggregateType.TASK,
        run_id=terminal.run_id,
        task_id=terminal.task_id,
    )


def task_terminal_operation_id(value: TaskTerminalAuthorityInput) -> str:
    terminal = normalize_task_terminal_input(value)
    identity = CanonicalAggregateIdentity(
        aggregate_type=AggregateType.TASK,
        run_id=terminal.run_id,
        task_id=terminal.task_id,
    )
    # Exactly one terminal decision is allowed per claim generation.  Changing
    # done<->failed, output, owner, or reason under the same claim therefore
    # collides with the same operation identity instead of creating a second
    # terminal authority transition.
    return (
        f"task-terminal:v1:{identity.sha256}:"
        f"claim:{terminal.claim_epoch}"
    )


def build_task_terminal_command(
    value: TaskTerminalAuthorityInput,
) -> AuthorityCommand:
    terminal = normalize_task_terminal_input(value)
    identity = CanonicalAggregateIdentity(
        aggregate_type=AggregateType.TASK,
        run_id=terminal.run_id,
        task_id=terminal.task_id,
    )
    metadata = {
        "task_id": terminal.task_id,
        "run_id": terminal.run_id,
        "tenant_id": terminal.tenant_id,
        "terminal_state": terminal.terminal_state,
        "finished_at_ms": terminal.finished_at_ms,
        "reason_code": terminal.reason_code,
        "worker_instance_id": terminal.worker_instance_id,
        "scheduler_epoch": terminal.scheduler_epoch,
        "claim_epoch": terminal.claim_epoch,
        # Durable payload is required so OUTPUT_PROJECTION can be replayed
        # after a process/network crash without re-executing the task.
        "output_data": terminal.output_data,
        "output_sha256": terminal.output_sha256,
        "claim_transition_id": terminal.claim_transition_id,
        "claim_record_hash": terminal.claim_record_hash,
        "claim_command_hash": terminal.claim_command_hash,
        "claim_revision": terminal.claim_revision,
        "claim_operation_id": terminal.claim_operation_id,
    }
    projection_intents = (
        (
            {"kind": "OUTPUT_PROJECTION"},
            {"kind": "DEPENDENCY_FANOUT_INTENT"},
        )
        if terminal.terminal_state == "done"
        else ({"kind": "DEPENDENCY_FAILURE_FANOUT_INTENT"},)
    )
    return AuthorityCommand(
        aggregate_identity=identity,
        operation_type=terminal.operation_type,
        operation_id=task_terminal_operation_id(terminal),
        expected_revision=terminal.claim_revision,
        intended_previous_state="running",
        intended_next_state=terminal.terminal_state,
        authoritative_payload=metadata,
        authoritative_metadata_changes=metadata,
        requested_child_effects={},
        requested_projection_intents=projection_intents,
        causation_id=terminal.claim_transition_id,
    )


def build_task_terminal_context(
    command: AuthorityCommand,
    *,
    scheduler_epoch: str,
) -> AuthorityEntryContext:
    if command.operation_type not in {
        OperationType.TASK_COMPLETE,
        OperationType.TASK_FAIL,
    }:
        raise ValueError("terminal binding rejects non-terminal TASK operation")
    epoch = _required_text(scheduler_epoch, "scheduler_epoch")
    return AuthorityEntryContext(
        authenticated_writer_id=WRITER_ID,
        allowed_operations=frozenset({command.operation_type}),
        target_aggregate_identity_sha256=command.aggregate_identity.sha256,
        fence_required=True,
        fence_valid=epoch != "0",
    )


@dataclass(frozen=True)
class TaskTerminalCanonicalProjectionInput:
    task_id: str
    run_id: str
    tenant_id: str
    terminal_state: str
    finished_at_ms: int
    reason_code: str
    worker_instance_id: str
    scheduler_epoch: str
    claim_epoch: int
    output_data: str
    output_sha256: str
    claim_transition_id: str
    claim_record_hash: str
    claim_command_hash: str
    claim_revision: int
    claim_operation_id: str
    canonical_transition_id: str
    canonical_record_hash: str
    canonical_command_hash: str
    canonical_revision: int
    canonical_operation_id: str
    canonical_operation_type: str


def normalize_task_terminal_projection(
    value: TaskTerminalCanonicalProjectionInput,
) -> TaskTerminalCanonicalProjectionInput:
    if not isinstance(value, TaskTerminalCanonicalProjectionInput):
        raise ValueError(
            "canonical terminal projection must be "
            "TaskTerminalCanonicalProjectionInput"
        )
    terminal = normalize_task_terminal_input(
        TaskTerminalAuthorityInput(
            task_id=value.task_id,
            run_id=value.run_id,
            tenant_id=value.tenant_id,
            terminal_state=value.terminal_state,
            finished_at_ms=value.finished_at_ms,
            reason_code=value.reason_code,
            worker_instance_id=value.worker_instance_id,
            scheduler_epoch=value.scheduler_epoch,
            claim_epoch=value.claim_epoch,
            output_data=value.output_data,
            output_sha256=value.output_sha256,
            claim_transition_id=value.claim_transition_id,
            claim_record_hash=value.claim_record_hash,
            claim_command_hash=value.claim_command_hash,
            claim_revision=value.claim_revision,
            claim_operation_id=value.claim_operation_id,
        )
    )
    operation_type = _required_text(
        value.canonical_operation_type,
        "canonical_operation_type",
    )
    if operation_type != terminal.operation_type.value:
        raise ValueError(
            "canonical_operation_type does not match terminal_state"
        )
    revision = _safe_integer(
        value.canonical_revision,
        "canonical_revision",
        minimum=1,
    )
    if revision != terminal.claim_revision + 1:
        raise ValueError("canonical_revision must equal claim_revision + 1")
    operation_id = _required_text(
        value.canonical_operation_id,
        "canonical_operation_id",
    )
    if operation_id != task_terminal_operation_id(terminal):
        raise ValueError("canonical_operation_id does not match claim identity")
    return replace(
        value,
        **terminal.__dict__,
        canonical_transition_id=_required_text(
            value.canonical_transition_id,
            "canonical_transition_id",
        ),
        canonical_record_hash=_required_sha256(
            value.canonical_record_hash,
            "canonical_record_hash",
        ),
        canonical_command_hash=_required_sha256(
            value.canonical_command_hash,
            "canonical_command_hash",
        ),
        canonical_revision=revision,
        canonical_operation_id=operation_id,
        canonical_operation_type=operation_type,
    )


class TaskTerminalAuthorityError(RuntimeError):
    def __init__(
        self,
        *,
        status: str,
        detail: str = "",
        canonical_commit_durable: bool = False,
    ) -> None:
        super().__init__(f"{status}: {detail}".rstrip(": "))
        self.status = status
        self.detail = detail
        self.canonical_commit_durable = canonical_commit_durable
        self.execution_allowed = False
        self.automatic_repair = False
        self.automatic_reassignment = False


@dataclass(frozen=True)
class TaskTerminalPreparedProjection:
    projection: TaskTerminalCanonicalProjectionInput
    exact_retry: bool




def _lua_path(filename: str) -> Path:
    here = Path(__file__).resolve()
    candidates = (
        here.parent.parent.parent.parent / "hfa-core" / "src" / "hfa" / "lua" / filename,
        here.parent.parent.parent / "hfa" / "lua" / filename,
    )
    for candidate in candidates:
        if candidate.exists():
            return candidate
    for parent in here.parents:
        for subdir in ("hfa-core/src/hfa/lua", "hfa/lua"):
            candidate = parent / subdir / filename
            if candidate.exists():
                return candidate
    raise FileNotFoundError(f"Lua script not found: {filename}")


@dataclass(frozen=True)
class TaskTerminalProjectionResult:
    status: str
    projected: bool
    already_projected: bool = False
    unlocked_count: int = 0
    blocked_count: int = 0


class TaskTerminalProjectionManager:
    """Apply only a durably committed canonical TASK terminal record.

    Caller-supplied terminal hashes are claims, not authority. Before the Lua
    projector can mutate runtime TASK state, this manager proves both the exact
    TASK_CLAIM predecessor and the exact TASK_COMPLETE/TASK_FAIL record+receipt
    against the existing RedisCanonicalAuthorityStore, including the current
    aggregate head. The Lua path remains an effect projector, never an alternate
    terminal authority.
    """

    def __init__(
        self,
        redis: Any,
        *,
        loader: Any | None = None,
        store: RedisCanonicalAuthorityStore | None = None,
    ) -> None:
        self._redis = redis
        self._loader = loader
        self._store = store or RedisCanonicalAuthorityStore(redis)
        self._initialised = False

    async def initialise(self) -> None:
        if self._initialised:
            return
        await self._store.initialise()
        if self._loader is None:
            self._loader = LuaScriptLoader(
                self._redis,
                _lua_path("task_complete.lua"),
            )
        await self._loader.load()
        self._initialised = True

    @staticmethod
    def _persistence_status(exc: Exception) -> str:
        if isinstance(exc, RedisAuthorityCorruptionError):
            return "CANONICAL_RECORD_CORRUPTION_CONFLICT"
        return "CONFLICT_EVIDENCE_STORE_UNAVAILABLE"

    @staticmethod
    def _require_probe_record(
        identity: CanonicalAggregateIdentity,
        probe: Any,
        *,
        role: str,
    ) -> tuple[Any, Any]:
        if probe is None or probe.canonical_store_record is None:
            raise TaskTerminalAuthorityError(
                status=TASK_TERMINAL_EVIDENCE_CONFLICT_STATUS,
                detail=f"durable canonical {role} record/receipt is missing",
            )
        record = probe.canonical_store_record
        receipt = probe.receipt
        checks = (
            record.aggregate_identity.sha256 == identity.sha256,
            record.aggregate_identity_sha256 == identity.sha256,
            receipt.operation_id == record.operation_id,
            receipt.transition_id == record.transition_id,
            receipt.canonical_command_hash == record.canonical_command_hash,
            receipt.canonical_record_hash == record.canonical_record_hash,
            receipt.aggregate_revision == record.to_revision,
            receipt.operation_type == record.operation_type,
            bool(record.verify_hash()),
        )
        if not all(checks):
            raise TaskTerminalAuthorityError(
                status=TASK_TERMINAL_EVIDENCE_CONFLICT_STATUS,
                detail=f"durable canonical {role} record/receipt mismatch",
            )
        return record, receipt

    async def _validate_durable_authority(
        self,
        projection: TaskTerminalCanonicalProjectionInput,
    ) -> None:
        identity = CanonicalAggregateIdentity(
            aggregate_type=AggregateType.TASK,
            run_id=projection.run_id,
            task_id=projection.task_id,
        )
        try:
            terminal_probe = await self._store.load_receipt_probe(
                identity,
                projection.canonical_operation_id,
            )
            claim_probe = await self._store.load_receipt_probe(
                identity,
                projection.claim_operation_id,
            )
        except Exception as exc:
            raise TaskTerminalAuthorityError(
                status=self._persistence_status(exc),
                detail=str(exc),
            ) from exc

        terminal_record, terminal_receipt = self._require_probe_record(
            identity, terminal_probe, role="terminal"
        )
        claim_record, _claim_receipt = self._require_probe_record(
            identity, claim_probe, role="TASK_CLAIM"
        )

        terminal_checks = {
            "operation_type": (
                terminal_record.operation_type,
                projection.canonical_operation_type,
            ),
            "operation_id": (
                terminal_record.operation_id,
                projection.canonical_operation_id,
            ),
            "transition_id": (
                terminal_record.transition_id,
                projection.canonical_transition_id,
            ),
            "canonical_record_hash": (
                terminal_record.canonical_record_hash,
                projection.canonical_record_hash,
            ),
            "canonical_command_hash": (
                terminal_record.canonical_command_hash,
                projection.canonical_command_hash,
            ),
            "from_revision": (
                terminal_record.from_revision,
                projection.claim_revision,
            ),
            "to_revision": (
                terminal_record.to_revision,
                projection.canonical_revision,
            ),
            "previous_state": (terminal_record.previous_state, "running"),
            "next_state": (
                terminal_record.next_state,
                projection.terminal_state,
            ),
            "causation_id": (
                terminal_record.causation_id,
                projection.claim_transition_id,
            ),
        }
        for field, (observed, expected) in terminal_checks.items():
            if observed != expected:
                raise TaskTerminalAuthorityError(
                    status=TASK_TERMINAL_EVIDENCE_CONFLICT_STATUS,
                    detail=f"durable terminal {field} mismatch",
                )

        if terminal_record.operation_type not in {
            OperationType.TASK_COMPLETE.value,
            OperationType.TASK_FAIL.value,
        }:
            raise TaskTerminalAuthorityError(
                status=TASK_TERMINAL_EVIDENCE_CONFLICT_STATUS,
                detail="durable terminal operation type is invalid",
            )

        terminal_metadata = terminal_record.authoritative_metadata_changes
        if not isinstance(terminal_metadata, Mapping):
            raise TaskTerminalAuthorityError(
                status=TASK_TERMINAL_EVIDENCE_CONFLICT_STATUS,
                detail="durable terminal metadata is invalid",
            )
        projection_metadata = {
            key: getattr(projection, key)
            for key in TaskTerminalAuthorityInput.__dataclass_fields__
        }
        for field, expected in projection_metadata.items():
            if terminal_metadata.get(field) != expected:
                raise TaskTerminalAuthorityError(
                    status=TASK_TERMINAL_EVIDENCE_CONFLICT_STATUS,
                    detail=f"durable terminal metadata.{field} mismatch",
                )

        expected_intents = (
            ("OUTPUT_PROJECTION", "DEPENDENCY_FANOUT_INTENT")
            if projection.terminal_state == "done"
            else ("DEPENDENCY_FAILURE_FANOUT_INTENT",)
        )
        observed_intents = tuple(
            str(item.get("kind") or "")
            for item in terminal_record.durable_projection_intents
            if isinstance(item, Mapping)
        )
        if observed_intents != expected_intents:
            raise TaskTerminalAuthorityError(
                status=TASK_TERMINAL_EVIDENCE_CONFLICT_STATUS,
                detail="durable terminal projection intents mismatch",
            )

        claim_checks = {
            "operation_type": (claim_record.operation_type, OperationType.TASK_CLAIM.value),
            "operation_id": (claim_record.operation_id, projection.claim_operation_id),
            "transition_id": (claim_record.transition_id, projection.claim_transition_id),
            "canonical_record_hash": (claim_record.canonical_record_hash, projection.claim_record_hash),
            "canonical_command_hash": (claim_record.canonical_command_hash, projection.claim_command_hash),
            "to_revision": (claim_record.to_revision, projection.claim_revision),
            "previous_state": (claim_record.previous_state, "scheduled"),
            "next_state": (claim_record.next_state, "running"),
        }
        for field, (observed, expected) in claim_checks.items():
            if observed != expected:
                raise TaskTerminalAuthorityError(
                    status=TASK_TERMINAL_EVIDENCE_CONFLICT_STATUS,
                    detail=f"durable TASK_CLAIM {field} mismatch",
                )

        claim_metadata = claim_record.authoritative_metadata_changes
        if not isinstance(claim_metadata, Mapping):
            raise TaskTerminalAuthorityError(
                status=TASK_TERMINAL_EVIDENCE_CONFLICT_STATUS,
                detail="durable TASK_CLAIM metadata is invalid",
            )
        expected_claim_metadata = {
            "task_id": projection.task_id,
            "run_id": projection.run_id,
            "tenant_id": projection.tenant_id,
            "worker_instance_id": projection.worker_instance_id,
            "scheduler_epoch": projection.scheduler_epoch,
            "claim_epoch": projection.claim_epoch,
        }
        for field, expected in expected_claim_metadata.items():
            if claim_metadata.get(field) != expected:
                raise TaskTerminalAuthorityError(
                    status=TASK_TERMINAL_EVIDENCE_CONFLICT_STATUS,
                    detail=f"durable TASK_CLAIM metadata.{field} mismatch",
                )

        try:
            await self._store.validate_authority_head(
                terminal_record.aggregate_identity,
                expected_operation_id=terminal_record.operation_id,
                expected_operation_digest=self._store.keyspace(
                    terminal_record.aggregate_identity_sha256
                ).operation_field(terminal_record.operation_id),
                expected_transition_id=terminal_record.transition_id,
                expected_revision=terminal_record.to_revision,
                expected_canonical_command_hash=(
                    terminal_record.canonical_command_hash
                ),
                expected_canonical_record_hash=(
                    terminal_record.canonical_record_hash
                ),
                expected_record=terminal_record,
                expected_receipt=terminal_receipt,
                expected_state=terminal_record.next_state,
                expected_projection_intents_json=canonical_json_bytes(
                    terminal_record.durable_projection_intents
                ).decode("utf-8"),
                expected_updated_at_ms=terminal_record.committed_at_ms,
            )
        except Exception as exc:
            raise TaskTerminalAuthorityError(
                status=self._persistence_status(exc),
                detail=str(exc),
            ) from exc

    async def project(
        self,
        value: TaskTerminalCanonicalProjectionInput,
    ) -> TaskTerminalProjectionResult:
        projection = normalize_task_terminal_projection(value)
        await self.initialise()
        assert self._loader is not None

        # R2 authority-proof closure: no Lua mutation is reachable until both
        # predecessor TASK_CLAIM evidence and the exact terminal record+receipt
        # are proven durable in the canonical store and equal the current head.
        await self._validate_durable_authority(projection)

        child_state_pfx = DagRedisKey.task_state_prefix()
        child_remaining_pfx = DagRedisKey.task_remaining_deps_prefix()
        child_emitted_pfx = DagRedisKey.task_ready_emitted_prefix()
        raw = await self._loader.run(
            num_keys=9,
            keys=[
                DagRedisKey.task_state(projection.task_id),
                DagRedisKey.task_meta(projection.task_id),
                DagRedisKey.task_children(projection.task_id),
                DagRedisKey.task_output(projection.task_id),
                DagRedisKey.tenant_ready_queue(projection.tenant_id),
                DagRedisKey.task_running_zset(projection.tenant_id),
                RedisKey.run_state(projection.run_id),
                RedisKey.runtime_truth_conflict_index(),
                RedisKey.runtime_truth_conflict_stream(),
            ],
            args=[
                projection.task_id,
                projection.run_id,
                projection.tenant_id,
                projection.terminal_state,
                str(projection.finished_at_ms),
                str(int(getattr(RedisTTL, "RUN_STATE", 86400))),
                str(int(getattr(RedisTTL, "RUN_META", 86400))),
                str(int(getattr(RedisTTL, "TASK_OUTPUT", 86400))),
                str(float(projection.finished_at_ms)),
                projection.reason_code,
                projection.worker_instance_id,
                projection.output_data,
                child_state_pfx,
                ":state",
                child_remaining_pfx,
                ":remaining_deps",
                child_emitted_pfx,
                ":ready_emitted",
                projection.scheduler_epoch,
                str(projection.claim_epoch),
                "1",
                projection.canonical_transition_id,
                projection.canonical_record_hash,
                projection.canonical_command_hash,
                str(projection.canonical_revision),
                projection.canonical_operation_id,
                projection.canonical_operation_type,
                projection.claim_transition_id,
                projection.claim_record_hash,
                projection.claim_command_hash,
                str(projection.claim_revision),
                projection.claim_operation_id,
                str(projection.claim_epoch),
                projection.output_sha256,
            ],
        )
        if not isinstance(raw, (list, tuple)) or len(raw) < 4:
            raise TaskTerminalAuthorityError(
                status=TASK_TERMINAL_PROJECTION_PENDING_STATUS,
                detail=f"invalid task terminal projection result: {raw!r}",
                canonical_commit_durable=True,
            )
        status = _decode(raw[1]) if len(raw) > 1 else ""
        unlocked = int(_decode(raw[2]) or 0) if len(raw) > 2 else 0
        already = bool(int(_decode(raw[3]) or 0)) if len(raw) > 3 else False
        blocked = int(_decode(raw[4]) or 0) if len(raw) > 4 else 0
        committed = bool(int(_decode(raw[0]) or 0))
        if status == TASK_TERMINAL_PROJECTED_STATUS and committed:
            return TaskTerminalProjectionResult(
                status=status,
                projected=True,
                unlocked_count=unlocked,
                blocked_count=blocked,
            )
        if status == TASK_TERMINAL_DUPLICATE_STATUS and committed and already:
            return TaskTerminalProjectionResult(
                status=status,
                projected=False,
                already_projected=True,
                unlocked_count=unlocked,
                blocked_count=blocked,
            )
        raise TaskTerminalAuthorityError(
            status=status or TASK_TERMINAL_PROJECTION_PENDING_STATUS,
            detail="canonical TASK terminal projection did not commit",
            canonical_commit_durable=True,
        )


@dataclass(frozen=True)
class TaskTerminalBindingResult:
    completed: bool
    status: str
    task_id: str
    terminal_state: str
    canonical_transition_id: str
    canonical_record_hash: str
    canonical_command_hash: str
    canonical_revision: int
    canonical_operation_id: str
    exact_retry: bool
    projection_applied: bool
    projection_already_applied: bool
    unlocked_count: int = 0
    blocked_count: int = 0

@dataclass
class TaskTerminalAuthorityBinding:
    redis: Any
    store: RedisCanonicalAuthorityStore | None = None
    projection_manager: TaskTerminalProjectionManager | None = None

    def __post_init__(self) -> None:
        if self.store is None:
            self.store = RedisCanonicalAuthorityStore(self.redis)
        if self.projection_manager is None:
            self.projection_manager = TaskTerminalProjectionManager(
                self.redis,
                store=self.store,
            )
        self._initialised = False

    async def initialise(self) -> None:
        if self._initialised:
            return
        assert self.store is not None
        try:
            await self.store.initialise()
            assert self.projection_manager is not None
            await self.projection_manager.initialise()
        except Exception as exc:
            raise TaskTerminalAuthorityError(
                status="CONFLICT_EVIDENCE_STORE_UNAVAILABLE",
                detail=str(exc),
            ) from exc
        self._initialised = True

    @staticmethod
    def _persistence_status(exc: Exception) -> str:
        if isinstance(exc, RedisAuthorityCorruptionError):
            return "CANONICAL_RECORD_CORRUPTION_CONFLICT"
        return "CONFLICT_EVIDENCE_STORE_UNAVAILABLE"

    async def _durable_conflict(
        self,
        command: AuthorityCommand,
        *,
        status: RedisAuthorityCommitStatus,
        detail_code: str,
        detail: str,
        snapshot: Any = None,
        receipt_probe: Any = None,
        observed_at_ms: int,
    ) -> None:
        assert self.store is not None
        record = (
            None
            if receipt_probe is None
            else receipt_probe.canonical_store_record
        )
        try:
            persisted = await self.store.record_authority_conflict(
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
                observed_at_ms=observed_at_ms,
                detail_code=detail_code,
                detail=detail,
            )
        except Exception as exc:
            raise TaskTerminalAuthorityError(
                status="CONFLICT_EVIDENCE_STORE_UNAVAILABLE",
                detail=str(exc),
            ) from exc
        raise TaskTerminalAuthorityError(
            status=persisted.status.value,
            detail=persisted.detail or detail,
            canonical_commit_durable=(receipt_probe is not None),
        )

    @staticmethod
    def _validate_record_receipt(
        identity: CanonicalAggregateIdentity,
        probe: Any,
    ) -> tuple[Any, Any]:
        if probe is None or probe.canonical_store_record is None:
            raise TaskTerminalAuthorityError(
                status=TASK_TERMINAL_EVIDENCE_CONFLICT_STATUS,
                detail="canonical operation record/receipt is missing",
            )
        record = probe.canonical_store_record
        receipt = probe.receipt
        checks = (
            record.aggregate_identity.sha256 == identity.sha256,
            record.aggregate_identity_sha256 == identity.sha256,
            receipt.operation_id == record.operation_id,
            receipt.transition_id == record.transition_id,
            receipt.canonical_command_hash == record.canonical_command_hash,
            receipt.canonical_record_hash == record.canonical_record_hash,
            receipt.aggregate_revision == record.to_revision,
            receipt.operation_type == record.operation_type,
            bool(record.verify_hash()),
        )
        if not all(checks):
            raise TaskTerminalAuthorityError(
                status=TASK_TERMINAL_EVIDENCE_CONFLICT_STATUS,
                detail="canonical record/receipt continuity mismatch",
            )
        return record, receipt

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
            raise TaskTerminalAuthorityError(
                status=self._persistence_status(exc),
                detail=str(exc),
            ) from exc

    async def _redis_type(self, key: str) -> str:
        method = getattr(self.redis, "type", None)
        if not callable(method):
            raise TaskTerminalAuthorityError(
                status=TASK_TERMINAL_EVIDENCE_CONFLICT_STATUS,
                detail="Redis type inspection is unavailable",
            )
        try:
            return _decode(await _maybe_await(method(key)))
        except Exception as exc:
            raise TaskTerminalAuthorityError(
                status=TASK_TERMINAL_EVIDENCE_CONFLICT_STATUS,
                detail=f"Redis type inspection failed for {key}: {exc}",
            ) from exc

    async def _read_task_meta(self, task_id: str) -> dict[str, str]:
        key = DagRedisKey.task_meta(task_id)
        kind = await self._redis_type(key)
        if kind != "hash":
            raise TaskTerminalAuthorityError(
                status=TASK_TERMINAL_EVIDENCE_CONFLICT_STATUS,
                detail=f"task metadata must be hash, observed {kind}",
            )
        try:
            raw = await _maybe_await(self.redis.hgetall(key))
        except Exception as exc:
            raise TaskTerminalAuthorityError(
                status=TASK_TERMINAL_EVIDENCE_CONFLICT_STATUS,
                detail=f"task metadata read failed: {exc}",
            ) from exc
        values = {
            _decode(key): _decode(item)
            for key, item in (raw or {}).items()
        }
        if not values:
            raise TaskTerminalAuthorityError(
                status=TASK_TERMINAL_EVIDENCE_CONFLICT_STATUS,
                detail="task metadata is missing",
            )
        return values

    async def _validate_projection_before_first_commit(
        self,
        *,
        terminal: TaskTerminalAuthorityInput,
        claim_record: Any,
        task_meta: Mapping[str, str],
    ) -> None:
        try:
            task_state = _decode(
                await _maybe_await(
                    self.redis.get(DagRedisKey.task_state(terminal.task_id))
                )
            )
            run_state = _decode(
                await _maybe_await(
                    self.redis.get(RedisKey.run_state(terminal.run_id))
                )
            )
        except Exception as exc:
            raise TaskTerminalAuthorityError(
                status=TASK_TERMINAL_EVIDENCE_CONFLICT_STATUS,
                detail=str(exc),
            ) from exc
        if task_state != "running":
            raise TaskTerminalAuthorityError(
                status=TASK_TERMINAL_EVIDENCE_CONFLICT_STATUS,
                detail=(
                    "legacy task projection is not running: "
                    f"{task_state or 'missing'}"
                ),
            )
        if run_state not in {
            "admitted",
            "queued",
            "scheduled",
            "running",
            "rescheduled",
        }:
            raise TaskTerminalAuthorityError(
                status=TASK_TERMINAL_EVIDENCE_CONFLICT_STATUS,
                detail=f"RUN truth is not nonterminal: {run_state or 'missing'}",
            )

        expected = {
            "task_id": terminal.task_id,
            "run_id": terminal.run_id,
            "tenant_id": terminal.tenant_id,
            "worker_instance_id": terminal.worker_instance_id,
            "scheduler_epoch": terminal.scheduler_epoch,
            "claim_epoch": str(terminal.claim_epoch),
            "claim_canonical_transition_id": claim_record.transition_id,
            "claim_canonical_record_hash": claim_record.canonical_record_hash,
            "claim_canonical_command_hash": claim_record.canonical_command_hash,
            "claim_canonical_revision": str(claim_record.to_revision),
            "claim_canonical_operation_id": claim_record.operation_id,
            "canonical_transition_id": claim_record.transition_id,
            "canonical_record_hash": claim_record.canonical_record_hash,
            "canonical_command_hash": claim_record.canonical_command_hash,
            "canonical_revision": str(claim_record.to_revision),
            "canonical_operation_id": claim_record.operation_id,
        }
        for field, wanted in expected.items():
            if task_meta.get(field, "") != wanted:
                raise TaskTerminalAuthorityError(
                    status=TASK_TERMINAL_EVIDENCE_CONFLICT_STATUS,
                    detail=f"task_meta.{field} mismatch",
                )

    @staticmethod
    def _claim_metadata(record: Any) -> Mapping[str, Any]:
        if record.operation_type != OperationType.TASK_CLAIM.value:
            raise TaskTerminalAuthorityError(
                status=TASK_TERMINAL_EVIDENCE_CONFLICT_STATUS,
                detail="canonical TASK predecessor is not TASK_CLAIM",
            )
        if record.previous_state != "scheduled" or record.next_state != "running":
            raise TaskTerminalAuthorityError(
                status=TASK_TERMINAL_EVIDENCE_CONFLICT_STATUS,
                detail="stored TASK_CLAIM transition is invalid",
            )
        metadata = record.authoritative_metadata_changes
        if not isinstance(metadata, Mapping):
            raise TaskTerminalAuthorityError(
                status=TASK_TERMINAL_EVIDENCE_CONFLICT_STATUS,
                detail="TASK_CLAIM metadata is invalid",
            )
        return metadata

    @staticmethod
    def _projection(
        terminal: TaskTerminalAuthorityInput,
        record: Any,
    ) -> TaskTerminalCanonicalProjectionInput:
        return normalize_task_terminal_projection(
            TaskTerminalCanonicalProjectionInput(
                **terminal.__dict__,
                canonical_transition_id=record.transition_id,
                canonical_record_hash=record.canonical_record_hash,
                canonical_command_hash=record.canonical_command_hash,
                canonical_revision=record.to_revision,
                canonical_operation_id=record.operation_id,
                canonical_operation_type=record.operation_type,
            )
        )

    @staticmethod
    def _terminal_from_record(record: Any) -> TaskTerminalAuthorityInput:
        if record.operation_type not in {
            OperationType.TASK_COMPLETE.value,
            OperationType.TASK_FAIL.value,
        }:
            raise TaskTerminalAuthorityError(
                status=TASK_TERMINAL_EVIDENCE_CONFLICT_STATUS,
                detail="stored operation is not TASK terminal",
            )
        data = record.authoritative_metadata_changes
        if not isinstance(data, Mapping):
            raise TaskTerminalAuthorityError(
                status=TASK_TERMINAL_EVIDENCE_CONFLICT_STATUS,
                detail="stored terminal metadata is invalid",
            )
        try:
            fields = {
                key: data[key]
                for key in TaskTerminalAuthorityInput.__dataclass_fields__
            }
            terminal = normalize_task_terminal_input(
                TaskTerminalAuthorityInput(**fields)
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise TaskTerminalAuthorityError(
                status=TASK_TERMINAL_EVIDENCE_CONFLICT_STATUS,
                detail=f"stored terminal metadata is invalid: {exc}",
            ) from exc

        if record.previous_state != "running":
            raise TaskTerminalAuthorityError(
                status=TASK_TERMINAL_EVIDENCE_CONFLICT_STATUS,
                detail="stored terminal previous state is not running",
            )
        if record.next_state != terminal.terminal_state:
            raise TaskTerminalAuthorityError(
                status=TASK_TERMINAL_EVIDENCE_CONFLICT_STATUS,
                detail="stored terminal next state mismatch",
            )
        if record.from_revision != terminal.claim_revision:
            raise TaskTerminalAuthorityError(
                status=TASK_TERMINAL_EVIDENCE_CONFLICT_STATUS,
                detail="stored terminal claim revision mismatch",
            )
        if record.to_revision != terminal.claim_revision + 1:
            raise TaskTerminalAuthorityError(
                status=TASK_TERMINAL_EVIDENCE_CONFLICT_STATUS,
                detail="stored terminal revision is invalid",
            )
        if record.operation_id != task_terminal_operation_id(terminal):
            raise TaskTerminalAuthorityError(
                status=TASK_TERMINAL_EVIDENCE_CONFLICT_STATUS,
                detail="stored terminal operation identity mismatch",
            )
        if record.causation_id != terminal.claim_transition_id:
            raise TaskTerminalAuthorityError(
                status=TASK_TERMINAL_EVIDENCE_CONFLICT_STATUS,
                detail="stored terminal causation does not match claim",
            )
        return terminal

    async def _load_claim_record(
        self,
        *,
        identity: CanonicalAggregateIdentity,
        terminal: TaskTerminalAuthorityInput,
    ) -> Any:
        assert self.store is not None
        try:
            probe = await self.store.load_receipt_probe(
                identity,
                terminal.claim_operation_id,
            )
        except Exception as exc:
            raise TaskTerminalAuthorityError(
                status=self._persistence_status(exc),
                detail=str(exc),
            ) from exc
        claim_record, _claim_receipt = self._validate_record_receipt(
            identity,
            probe,
        )
        claim_metadata = self._claim_metadata(claim_record)
        expected = {
            "task_id": terminal.task_id,
            "run_id": terminal.run_id,
            "tenant_id": terminal.tenant_id,
            "worker_instance_id": terminal.worker_instance_id,
            "scheduler_epoch": terminal.scheduler_epoch,
            "claim_epoch": terminal.claim_epoch,
        }
        for field, wanted in expected.items():
            if claim_metadata.get(field) != wanted:
                raise TaskTerminalAuthorityError(
                    status=TASK_TERMINAL_EVIDENCE_CONFLICT_STATUS,
                    detail=f"stored TASK_CLAIM {field} mismatch",
                )
        proof = {
            "transition_id": terminal.claim_transition_id,
            "canonical_record_hash": terminal.claim_record_hash,
            "canonical_command_hash": terminal.claim_command_hash,
            "to_revision": terminal.claim_revision,
            "operation_id": terminal.claim_operation_id,
        }
        for field, wanted in proof.items():
            if getattr(claim_record, field) != wanted:
                raise TaskTerminalAuthorityError(
                    status=TASK_TERMINAL_EVIDENCE_CONFLICT_STATUS,
                    detail=f"stored TASK_CLAIM {field} proof mismatch",
                )
        return claim_record

    @staticmethod
    def _assert_request_matches_record(
        stored: TaskTerminalAuthorityInput,
        *,
        task_id: str,
        run_id: str,
        tenant_id: str,
        terminal_state: str,
        reason_code: str,
        worker_instance_id: str,
        scheduler_epoch: str,
        claim_epoch: int,
        output_data: str,
    ) -> None:
        expected = {
            "task_id": task_id,
            "run_id": run_id,
            "tenant_id": tenant_id,
            "terminal_state": terminal_state,
            "reason_code": reason_code,
            "worker_instance_id": worker_instance_id,
            "scheduler_epoch": scheduler_epoch,
            "claim_epoch": claim_epoch,
            "output_data": output_data,
        }
        for field, wanted in expected.items():
            if getattr(stored, field) != wanted:
                raise TaskTerminalAuthorityError(
                    status=TASK_TERMINAL_EVIDENCE_CONFLICT_STATUS,
                    detail=f"stored terminal {field} mismatch",
                )

    async def _prepare_stored_terminal(
        self,
        *,
        identity: CanonicalAggregateIdentity,
        record: Any,
        receipt: Any,
        expected: Mapping[str, Any] | None = None,
    ) -> TaskTerminalPreparedProjection:
        terminal = self._terminal_from_record(record)
        if expected is not None:
            self._assert_request_matches_record(terminal, **expected)
        await self._validate_exact_head(record, receipt)
        await self._load_claim_record(identity=identity, terminal=terminal)
        return TaskTerminalPreparedProjection(
            projection=self._projection(terminal, record),
            exact_retry=True,
        )

    async def prepare_existing_terminal_projection(
        self,
        *,
        task_id: str,
        run_id: str,
        tenant_id: str,
        worker_instance_id: str,
        scheduler_epoch: str,
        claim_epoch: str | int,
    ) -> TaskTerminalPreparedProjection:
        """Recover a durable TASK terminal projection without re-execution."""
        await self.initialise()
        assert self.store is not None
        task_id = _required_text(task_id, "task_id")
        run_id = _required_text(run_id, "run_id")
        tenant_id = _required_text(tenant_id, "tenant_id")
        worker_instance_id = _required_text(
            worker_instance_id,
            "worker_instance_id",
        )
        scheduler_epoch = _required_text(
            scheduler_epoch,
            "scheduler_epoch",
        )
        claim_epoch_int = _safe_integer(
            claim_epoch,
            "claim_epoch",
            minimum=1,
        )
        identity = CanonicalAggregateIdentity(
            aggregate_type=AggregateType.TASK,
            run_id=run_id,
            task_id=task_id,
        )
        try:
            snapshot = await self.store.get_aggregate_snapshot(identity)
        except Exception as exc:
            raise TaskTerminalAuthorityError(
                status=self._persistence_status(exc),
                detail=str(exc),
            ) from exc
        if snapshot is None or snapshot.state not in {"done", "failed"}:
            raise TaskTerminalAuthorityError(
                status=TASK_TERMINAL_NOT_COMMITTED_STATUS,
                detail="canonical TASK terminal authority is not durable",
            )
        try:
            probe = await self.store.load_receipt_probe(
                identity,
                snapshot.operation_id,
            )
        except Exception as exc:
            raise TaskTerminalAuthorityError(
                status=self._persistence_status(exc),
                detail=str(exc),
            ) from exc
        record, receipt = self._validate_record_receipt(identity, probe)
        prepared = await self._prepare_stored_terminal(
            identity=identity,
            record=record,
            receipt=receipt,
        )
        terminal = normalize_task_terminal_projection(prepared.projection)
        expected = {
            "task_id": task_id,
            "run_id": run_id,
            "tenant_id": tenant_id,
            "worker_instance_id": worker_instance_id,
            "scheduler_epoch": scheduler_epoch,
            "claim_epoch": claim_epoch_int,
        }
        for field, wanted in expected.items():
            if getattr(terminal, field) != wanted:
                raise TaskTerminalAuthorityError(
                    status=TASK_TERMINAL_EVIDENCE_CONFLICT_STATUS,
                    detail=f"durable terminal {field} mismatch",
                )
        return prepared

    async def prepare_terminal(
        self,
        *,
        task_id: str,
        run_id: str,
        tenant_id: str,
        terminal_state: str,
        finished_at_ms: int,
        reason_code: str,
        worker_instance_id: str,
        output_data: str,
        scheduler_epoch: str,
        claim_epoch: str | int,
    ) -> TaskTerminalPreparedProjection:
        await self.initialise()
        assert self.store is not None

        task_id = _required_text(task_id, "task_id")
        run_id = _required_text(run_id, "run_id")
        tenant_id = _required_text(tenant_id, "tenant_id")
        terminal_state = _required_text(terminal_state, "terminal_state")
        if terminal_state not in {"done", "failed"}:
            raise TaskTerminalAuthorityError(
                status=TASK_TERMINAL_EVIDENCE_CONFLICT_STATUS,
                detail="invalid terminal_state",
            )
        reason_code = _required_text(reason_code, "reason_code")
        worker_instance_id = _required_text(
            worker_instance_id,
            "worker_instance_id",
        )
        scheduler_epoch = _required_text(
            scheduler_epoch,
            "scheduler_epoch",
        )
        if scheduler_epoch == "0":
            raise TaskTerminalAuthorityError(
                status=TASK_TERMINAL_EVIDENCE_CONFLICT_STATUS,
                detail="scheduler_epoch must represent acquired authority",
            )
        claim_epoch_int = _safe_integer(
            claim_epoch,
            "claim_epoch",
            minimum=1,
        )
        finished_at_ms = _safe_integer(
            finished_at_ms,
            "finished_at_ms",
        )
        canonical_output = _canonical_output_data(
            output_data,
            terminal_state=terminal_state,
        )
        output_sha256 = (
            _output_sha256(canonical_output)
            if terminal_state == "done"
            else ""
        )

        identity = CanonicalAggregateIdentity(
            aggregate_type=AggregateType.TASK,
            run_id=run_id,
            task_id=task_id,
        )
        operation_id = (
            f"task-terminal:v1:{identity.sha256}:claim:{claim_epoch_int}"
        )
        try:
            snapshot = await self.store.get_aggregate_snapshot(identity)
            probe = await self.store.load_receipt_probe(
                identity,
                operation_id,
            )
        except Exception as exc:
            raise TaskTerminalAuthorityError(
                status=self._persistence_status(exc),
                detail=str(exc),
            ) from exc

        if probe is not None:
            record, receipt = self._validate_record_receipt(identity, probe)
            stored = self._terminal_from_record(record)
            # The operation slot is one claim generation. Replayed wall-clock
            # observation time is not allowed to manufacture a second command;
            # every semantic terminal field is compared through the canonical
            # command hash using the durable first committed timestamp.
            incoming = normalize_task_terminal_input(
                replace(
                    stored,
                    terminal_state=terminal_state,
                    reason_code=reason_code,
                    worker_instance_id=worker_instance_id,
                    scheduler_epoch=scheduler_epoch,
                    claim_epoch=claim_epoch_int,
                    output_data=canonical_output,
                    output_sha256=(
                        _output_sha256(canonical_output)
                        if terminal_state == "done"
                        else ""
                    ),
                )
            )
            incoming_command = build_task_terminal_command(incoming)
            evaluation = evaluate_authority_commit(
                context=build_task_terminal_context(
                    incoming_command,
                    scheduler_epoch=scheduler_epoch,
                ),
                command=incoming_command,
                current_revision=(
                    record.to_revision if snapshot is None else snapshot.revision
                ),
                current_state=(
                    record.next_state if snapshot is None else snapshot.state
                ),
                receipt_probe=probe,
                committed_at_ms=stored.finished_at_ms,
                correlation_id=None,
            )
            if evaluation.decision.code is AuthorityDecisionCode.ALREADY_APPLIED:
                return await self._prepare_stored_terminal(
                    identity=identity,
                    record=record,
                    receipt=receipt,
                )
            if evaluation.decision.code is AuthorityDecisionCode.IDEMPOTENCY_CONFLICT:
                await self._durable_conflict(
                    incoming_command,
                    status=RedisAuthorityCommitStatus.IDEMPOTENCY_CONFLICT,
                    detail_code="terminal_operation_semantic_conflict",
                    detail=(
                        "one claim generation already has a different "
                        "canonical terminal truth"
                    ),
                    snapshot=snapshot,
                    receipt_probe=probe,
                    observed_at_ms=finished_at_ms,
                )
            raise TaskTerminalAuthorityError(
                status=evaluation.decision.code.value,
                detail="stored terminal receipt could not be replayed",
                canonical_commit_durable=True,
            )

        if snapshot is None or snapshot.state != "running":
            raise TaskTerminalAuthorityError(
                status=TASK_TERMINAL_EVIDENCE_CONFLICT_STATUS,
                detail="canonical TASK head is not running",
            )
        try:
            claim_probe = await self.store.load_receipt_probe(
                identity,
                snapshot.operation_id,
            )
        except Exception as exc:
            raise TaskTerminalAuthorityError(
                status=self._persistence_status(exc),
                detail=str(exc),
            ) from exc
        claim_record, claim_receipt = self._validate_record_receipt(
            identity,
            claim_probe,
        )
        claim_metadata = self._claim_metadata(claim_record)
        if (
            claim_record.to_revision != snapshot.revision
            or claim_record.transition_id != snapshot.transition_id
            or claim_record.operation_id != snapshot.operation_id
        ):
            raise TaskTerminalAuthorityError(
                status=TASK_TERMINAL_EVIDENCE_CONFLICT_STATUS,
                detail="canonical TASK head is not exact TASK_CLAIM",
            )
        await self._validate_exact_head(claim_record, claim_receipt)

        expected_claim = {
            "task_id": task_id,
            "run_id": run_id,
            "tenant_id": tenant_id,
            "worker_instance_id": worker_instance_id,
            "scheduler_epoch": scheduler_epoch,
            "claim_epoch": claim_epoch_int,
        }
        for field, wanted in expected_claim.items():
            if claim_metadata.get(field) != wanted:
                raise TaskTerminalAuthorityError(
                    status=TASK_TERMINAL_EVIDENCE_CONFLICT_STATUS,
                    detail=f"canonical TASK_CLAIM {field} mismatch",
                )

        terminal = normalize_task_terminal_input(
            TaskTerminalAuthorityInput(
                task_id=task_id,
                run_id=run_id,
                tenant_id=tenant_id,
                terminal_state=terminal_state,
                finished_at_ms=finished_at_ms,
                reason_code=reason_code,
                worker_instance_id=worker_instance_id,
                scheduler_epoch=scheduler_epoch,
                claim_epoch=claim_epoch_int,
                output_data=canonical_output,
                output_sha256=output_sha256,
                claim_transition_id=claim_record.transition_id,
                claim_record_hash=claim_record.canonical_record_hash,
                claim_command_hash=claim_record.canonical_command_hash,
                claim_revision=claim_record.to_revision,
                claim_operation_id=claim_record.operation_id,
            )
        )
        task_meta = await self._read_task_meta(task_id)
        await self._validate_projection_before_first_commit(
            terminal=terminal,
            claim_record=claim_record,
            task_meta=task_meta,
        )

        command = build_task_terminal_command(terminal)
        evaluation = evaluate_authority_commit(
            context=build_task_terminal_context(
                command,
                scheduler_epoch=scheduler_epoch,
            ),
            command=command,
            current_revision=snapshot.revision,
            current_state=snapshot.state,
            receipt_probe=None,
            committed_at_ms=terminal.finished_at_ms,
            correlation_id=None,
        )
        if (
            evaluation.decision.code is not AuthorityDecisionCode.ACCEPTED
            or evaluation.commit_plan is None
        ):
            raise TaskTerminalAuthorityError(
                status=evaluation.decision.code.value,
                detail="canonical policy rejected TASK terminal operation",
            )

        try:
            persisted = await self.store.commit(evaluation.commit_plan)
        except Exception as exc:
            raise TaskTerminalAuthorityError(
                status=self._persistence_status(exc),
                detail=str(exc),
            ) from exc

        if persisted.status is RedisAuthorityCommitStatus.COMMITTED:
            return TaskTerminalPreparedProjection(
                projection=self._projection(
                    terminal,
                    evaluation.commit_plan.record,
                ),
                exact_retry=False,
            )

        if persisted.status in {
            RedisAuthorityCommitStatus.ALREADY_APPLIED,
            RedisAuthorityCommitStatus.IDEMPOTENCY_CONFLICT,
        }:
            # A concurrent terminal contender may have won between evaluation
            # and Redis commit. Re-enter the receipt-first path so exact replay
            # recovers the winner and divergent semantics become a durable
            # idempotency conflict instead of a second terminal transition.
            return await self.prepare_terminal(
                task_id=task_id,
                run_id=run_id,
                tenant_id=tenant_id,
                terminal_state=terminal_state,
                finished_at_ms=finished_at_ms,
                reason_code=reason_code,
                worker_instance_id=worker_instance_id,
                output_data=canonical_output,
                scheduler_epoch=scheduler_epoch,
                claim_epoch=claim_epoch_int,
            )

        raise TaskTerminalAuthorityError(
            status=persisted.status.value,
            detail=persisted.detail,
        )

    async def _apply_prepared(
        self,
        prepared: TaskTerminalPreparedProjection,
    ) -> TaskTerminalBindingResult:
        assert self.projection_manager is not None
        try:
            projected = await self.projection_manager.project(
                prepared.projection
            )
        except TaskTerminalAuthorityError:
            raise
        except Exception as exc:
            raise TaskTerminalAuthorityError(
                status=TASK_TERMINAL_PROJECTION_PENDING_STATUS,
                detail=str(exc),
                canonical_commit_durable=True,
            ) from exc
        projection = prepared.projection
        return TaskTerminalBindingResult(
            completed=True,
            status=projected.status,
            task_id=projection.task_id,
            terminal_state=projection.terminal_state,
            canonical_transition_id=projection.canonical_transition_id,
            canonical_record_hash=projection.canonical_record_hash,
            canonical_command_hash=projection.canonical_command_hash,
            canonical_revision=projection.canonical_revision,
            canonical_operation_id=projection.canonical_operation_id,
            exact_retry=prepared.exact_retry,
            projection_applied=projected.projected,
            projection_already_applied=projected.already_projected,
            unlocked_count=projected.unlocked_count,
            blocked_count=projected.blocked_count,
        )

    async def complete(
        self,
        *,
        task_id: str,
        run_id: str,
        tenant_id: str,
        finished_at_ms: int,
        worker_instance_id: str,
        scheduler_epoch: str,
        claim_epoch: str | int,
        output_data: str,
        reason_code: str = "completed",
    ) -> TaskTerminalBindingResult:
        prepared = await self.prepare_terminal(
            task_id=task_id,
            run_id=run_id,
            tenant_id=tenant_id,
            terminal_state="done",
            finished_at_ms=finished_at_ms,
            reason_code=reason_code,
            worker_instance_id=worker_instance_id,
            output_data=output_data,
            scheduler_epoch=scheduler_epoch,
            claim_epoch=claim_epoch,
        )
        return await self._apply_prepared(prepared)

    async def fail(
        self,
        *,
        task_id: str,
        run_id: str,
        tenant_id: str,
        finished_at_ms: int,
        worker_instance_id: str,
        scheduler_epoch: str,
        claim_epoch: str | int,
        reason_code: str,
    ) -> TaskTerminalBindingResult:
        prepared = await self.prepare_terminal(
            task_id=task_id,
            run_id=run_id,
            tenant_id=tenant_id,
            terminal_state="failed",
            finished_at_ms=finished_at_ms,
            reason_code=reason_code,
            worker_instance_id=worker_instance_id,
            output_data="",
            scheduler_epoch=scheduler_epoch,
            claim_epoch=claim_epoch,
        )
        return await self._apply_prepared(prepared)

    async def replay_terminal_projection(
        self,
        *,
        task_id: str,
        run_id: str,
        tenant_id: str,
        worker_instance_id: str,
        scheduler_epoch: str,
        claim_epoch: str | int,
    ) -> TaskTerminalBindingResult:
        """Replay only an already-durable terminal projection.

        This manager-level recovery surface never executes task work and never
        creates a second authority transition. It exists solely for the C1
        authority-committed / projection-pending boundary.
        """
        prepared = await self.prepare_existing_terminal_projection(
            task_id=task_id,
            run_id=run_id,
            tenant_id=tenant_id,
            worker_instance_id=worker_instance_id,
            scheduler_epoch=scheduler_epoch,
            claim_epoch=claim_epoch,
        )
        return await self._apply_prepared(prepared)
