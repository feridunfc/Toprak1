"""Canonical RUN_TERMINATE authority binding for Sprint 84.5.

The binding freezes terminal aggregate TASK truth in Redis before constructing a
canonical RUN_TERMINATE command.  Canonical authority is committed before the
legacy RUN state/result/meta/event projection.  Exact retries replay from the
immutable proof plus canonical receipt and never reinterpret mutable TASK state
once the canonical transition is durable.
"""
from __future__ import annotations

import hashlib
import json
import re
import unicodedata
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, Sequence

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
from hfa.dag.schema import DagRedisKey
from hfa.governance.admission_resource_reservation import (
    AdmissionResourceReservationManager,
    AdmissionResourceSettlementInput,
    RESERVATION_STATE_SETTLED,
    RESERVATION_STATUS_ALREADY_SETTLED,
    RESERVATION_STATUS_SETTLED,
)
from hfa.lua.loader import LuaScriptLoader
from hfa_control.run_create_authority import (
    resource_reservation_from_run_create_record,
    run_create_operation_id,
)

WRITER_ID = "hfa-control/run-terminate-writer:v1"
PROOF_SCHEMA_VERSION = 1

CAPTURED_STATUS = "captured"
ALREADY_CAPTURED_STATUS = "already_captured"
NOT_READY_STATUS = "not_ready"
PROJECTED_STATUS = "canonical_run_terminate_projected"
ALREADY_PROJECTED_STATUS = "canonical_run_terminate_already_projected"
PROJECTION_CONFLICT_STATUS = "canonical_run_terminate_projection_conflict"
PROJECTION_PENDING_STATUS = "canonical_projection_pending"
AUTHORITY_CONFLICT_STATUS = "canonical_run_terminate_authority_conflict"
RESOURCE_SETTLEMENT_PENDING_STATUS = "canonical_resource_settlement_pending"

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_SAFE_INTEGER_MAX = 2**53 - 1
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


def _decode(value: Any) -> str:
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="strict")
    return "" if value is None else str(value)


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
    if _SHA256_RE.fullmatch(normalized) is None:
        raise ValueError(f"{field_name} must be lowercase SHA-256 hex")
    return normalized


def _safe_integer(value: Any, field_name: str, *, minimum: int = 0) -> int:
    if type(value) is not int:
        raise ValueError(f"{field_name} must be an exact integer")
    if value < minimum or value > _SAFE_INTEGER_MAX:
        raise ValueError(f"{field_name} is outside the safe integer domain")
    return value


def _lua_path(filename: str) -> Path:
    here = Path(__file__).resolve()
    candidates = (
        here.parent.parent.parent.parent.parent / "hfa-core" / "src" / "hfa" / "lua" / filename,
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


def run_terminate_identity(run_id: str) -> CanonicalAggregateIdentity:
    return CanonicalAggregateIdentity(
        aggregate_type=AggregateType.RUN,
        run_id=_required_text(run_id, "run_id"),
        task_id=None,
    )


def run_terminate_operation_id(run_id: str, terminal_proof_sha256: str) -> str:
    normalized_run_id = _required_text(run_id, "run_id")
    proof = _required_sha256(terminal_proof_sha256, "terminal_proof_sha256")
    digest = hashlib.sha256(
        b"RUN_TERMINATE\x00" + normalized_run_id.encode("utf-8") + b"\x00" + proof.encode("ascii")
    ).hexdigest()
    return f"run-terminate:v1:{digest}"


@dataclass(frozen=True)
class TerminalTaskEvidence:
    task_id: str
    state: str


@dataclass(frozen=True)
class TerminalAggregateProof:
    schema_version: int
    run_id: str
    tenant_id: str
    tasks: tuple[TerminalTaskEvidence, ...]
    task_count: int
    done_count: int
    failed_count: int
    skipped_count: int
    final_state: str
    proof_sha256: str
    proof_payload_json: str
    finalized_at_ms: int
    worker_instance_id: str
    trigger_task_id: str
    trigger_terminal_state: str
    canonical_expected_revision: int
    canonical_previous_state: str


@dataclass(frozen=True)
class TerminalProofCaptureResult:
    status: str
    proof: TerminalAggregateProof | None
    task_count: int = 0
    done_count: int = 0
    failed_count: int = 0
    skipped_count: int = 0


class RunTerminateAuthorityError(RuntimeError):
    def __init__(
        self,
        status: str,
        detail: str = "",
        *,
        canonical_commit_durable: bool = False,
    ) -> None:
        super().__init__(detail or status)
        self.status = status
        self.detail = detail
        self.canonical_commit_durable = canonical_commit_durable


class RunTerminateProjectionPendingError(RunTerminateAuthorityError):
    def __init__(self, detail: str) -> None:
        super().__init__(
            PROJECTION_PENDING_STATUS,
            detail,
            canonical_commit_durable=True,
        )


class TerminalAggregateProofManager:
    """Persist and validate an immutable terminal aggregate TASK proof."""

    _FIELDS = frozenset(
        {
            "schema_version",
            "run_id",
            "tenant_id",
            "proof_sha256",
            "proof_payload_json",
            "task_count",
            "done_count",
            "failed_count",
            "skipped_count",
            "final_state",
            "finalized_at_ms",
            "worker_instance_id",
            "trigger_task_id",
            "trigger_terminal_state",
            "canonical_expected_revision",
            "canonical_previous_state",
        }
    )

    def __init__(self, redis: Any, *, loader: Any | None = None) -> None:
        self._redis = redis
        self._loader = loader

    @staticmethod
    def proof_key(run_id: str) -> str:
        identity = run_terminate_identity(run_id)
        return f"{RedisKey.PREFIX}:run-terminate:proof:v1:{identity.sha256}"

    async def initialise(self) -> None:
        if self._loader is None:
            self._loader = LuaScriptLoader(
                self._redis,
                _lua_path("run_terminate_terminal_proof.lua"),
            )
        await self._loader.load()

    @staticmethod
    def _proof_from_mapping(raw: Mapping[str, Any]) -> TerminalAggregateProof:
        data = {_decode(key): _decode(value) for key, value in raw.items()}
        if set(data) != TerminalAggregateProofManager._FIELDS:
            raise RunTerminateAuthorityError(
                "terminal_proof_receipt_corrupt",
                "terminal proof receipt fields are incomplete or unexpected",
            )
        try:
            schema_version = int(data["schema_version"])
            task_count = int(data["task_count"])
            done_count = int(data["done_count"])
            failed_count = int(data["failed_count"])
            skipped_count = int(data["skipped_count"])
            finalized_at_ms = int(data["finalized_at_ms"])
            expected_revision = int(data["canonical_expected_revision"])
        except ValueError as exc:
            raise RunTerminateAuthorityError(
                "terminal_proof_receipt_corrupt",
                "terminal proof numeric field is invalid",
            ) from exc
        if schema_version != PROOF_SCHEMA_VERSION:
            raise RunTerminateAuthorityError(
                "terminal_proof_receipt_corrupt",
                "terminal proof schema version is invalid",
            )
        for value, name, minimum in (
            (task_count, "task_count", 1),
            (done_count, "done_count", 0),
            (failed_count, "failed_count", 0),
            (skipped_count, "skipped_count", 0),
            (finalized_at_ms, "finalized_at_ms", 0),
            (expected_revision, "canonical_expected_revision", 1),
        ):
            _safe_integer(value, name, minimum=minimum)
        proof_sha = _required_sha256(data["proof_sha256"], "proof_sha256")
        payload_json = data["proof_payload_json"]
        if not payload_json or payload_json != payload_json.strip():
            raise RunTerminateAuthorityError(
                "terminal_proof_receipt_corrupt",
                "terminal proof payload JSON is empty or padded",
            )
        if hashlib.sha256(payload_json.encode("utf-8")).hexdigest() != proof_sha:
            raise RunTerminateAuthorityError(
                "terminal_proof_receipt_corrupt",
                "terminal proof SHA-256 does not match immutable payload",
            )
        try:
            payload = json.loads(payload_json)
        except json.JSONDecodeError as exc:
            raise RunTerminateAuthorityError(
                "terminal_proof_receipt_corrupt",
                "terminal proof payload is not valid JSON",
            ) from exc
        expected_payload_fields = {
            "schema_version",
            "run_id",
            "tenant_id",
            "tasks",
            "task_count",
            "done_count",
            "failed_count",
            "skipped_count",
            "final_state",
        }
        if not isinstance(payload, dict) or set(payload) != expected_payload_fields:
            raise RunTerminateAuthorityError(
                "terminal_proof_receipt_corrupt",
                "terminal proof payload fields are invalid",
            )
        tasks_raw = payload.get("tasks")
        if not isinstance(tasks_raw, list) or not tasks_raw:
            raise RunTerminateAuthorityError(
                "terminal_proof_receipt_corrupt",
                "terminal proof task evidence is invalid",
            )
        tasks: list[TerminalTaskEvidence] = []
        previous_task_id = None
        observed_done = 0
        observed_failed = 0
        observed_skipped = 0
        failure_terminal_states = {
            "failed",
            "blocked_by_failure",
            "dead_lettered",
            "rejected",
            "cancelled",
        }
        for row in tasks_raw:
            if not isinstance(row, dict) or set(row) != {"task_id", "state"}:
                raise RunTerminateAuthorityError(
                    "terminal_proof_receipt_corrupt",
                    "terminal proof task row is invalid",
                )
            task_id = _required_text(row["task_id"], "task_id")
            state = _required_text(row["state"], "task_state")
            if previous_task_id is not None and task_id <= previous_task_id:
                raise RunTerminateAuthorityError(
                    "terminal_proof_receipt_corrupt",
                    "terminal proof task ordering is not strictly sorted",
                )
            previous_task_id = task_id
            if state == "done":
                observed_done += 1
            elif state == "skipped":
                observed_skipped += 1
            elif state in failure_terminal_states:
                observed_failed += 1
            else:
                raise RunTerminateAuthorityError(
                    "terminal_proof_receipt_corrupt",
                    "terminal proof contains a nonterminal or unknown TASK state",
                )
            tasks.append(TerminalTaskEvidence(task_id=task_id, state=state))
        run_id = _required_text(data["run_id"], "run_id")
        tenant_id = _required_text(data["tenant_id"], "tenant_id")
        final_state = _required_text(data["final_state"], "final_state")
        previous_state = _required_text(data["canonical_previous_state"], "canonical_previous_state")
        if final_state not in {"done", "failed"} or previous_state not in {"pending", "running"}:
            raise RunTerminateAuthorityError(
                "terminal_proof_receipt_corrupt",
                "terminal proof state contract is invalid",
            )
        payload_checks = {
            "schema_version": schema_version,
            "run_id": run_id,
            "tenant_id": tenant_id,
            "task_count": task_count,
            "done_count": done_count,
            "failed_count": failed_count,
            "skipped_count": skipped_count,
            "final_state": final_state,
        }
        for key, expected in payload_checks.items():
            if payload.get(key) != expected:
                raise RunTerminateAuthorityError(
                    "terminal_proof_receipt_corrupt",
                    f"terminal proof payload mismatch for {key}",
                )
        if len(tasks) != task_count or done_count + failed_count + skipped_count != task_count:
            raise RunTerminateAuthorityError(
                "terminal_proof_receipt_corrupt",
                "terminal proof counts are inconsistent",
            )
        return TerminalAggregateProof(
            schema_version=schema_version,
            run_id=run_id,
            tenant_id=tenant_id,
            tasks=tuple(tasks),
            task_count=task_count,
            done_count=done_count,
            failed_count=failed_count,
            skipped_count=skipped_count,
            final_state=final_state,
            proof_sha256=proof_sha,
            proof_payload_json=payload_json,
            finalized_at_ms=finalized_at_ms,
            worker_instance_id=_required_text(
                data["worker_instance_id"],
                "worker_instance_id",
                allow_empty=True,
            ),
            trigger_task_id=_required_text(
                data["trigger_task_id"],
                "trigger_task_id",
                allow_empty=True,
            ),
            trigger_terminal_state=_required_text(
                data["trigger_terminal_state"],
                "trigger_terminal_state",
                allow_empty=True,
            ),
            canonical_expected_revision=expected_revision,
            canonical_previous_state=previous_state,
        )

    async def load_existing(self, run_id: str) -> TerminalAggregateProof | None:
        key = self.proof_key(run_id)
        try:
            kind = _decode(await self._redis.type(key))
            if kind == "none":
                return None
            if kind != "hash":
                raise RunTerminateAuthorityError(
                    "terminal_proof_receipt_wrong_type",
                    "terminal proof receipt key has the wrong Redis type",
                )
            raw = await self._redis.hgetall(key)
        except RunTerminateAuthorityError:
            raise
        except Exception as exc:
            raise RunTerminateAuthorityError(
                "terminal_proof_read_failed",
                str(exc),
            ) from exc
        proof = self._proof_from_mapping(raw)
        if proof.run_id != _required_text(run_id, "run_id"):
            raise RunTerminateAuthorityError(
                "terminal_proof_receipt_corrupt",
                "terminal proof run identity mismatch",
            )
        return proof

    async def capture(
        self,
        *,
        run_id: str,
        tenant_id: str,
        finalized_at_ms: int,
        worker_instance_id: str,
        trigger_task_id: str,
        trigger_terminal_state: str,
        canonical_expected_revision: int,
        canonical_previous_state: str,
    ) -> TerminalProofCaptureResult:
        if self._loader is None:
            await self.initialise()
        assert self._loader is not None
        run_id = _required_text(run_id, "run_id")
        tenant_id = _required_text(tenant_id, "tenant_id")
        finalized_at_ms = _safe_integer(finalized_at_ms, "finalized_at_ms")
        canonical_expected_revision = _safe_integer(
            canonical_expected_revision,
            "canonical_expected_revision",
            minimum=1,
        )
        canonical_previous_state = _required_text(
            canonical_previous_state,
            "canonical_previous_state",
        )
        if canonical_previous_state not in {"pending", "running"}:
            raise ValueError("canonical_previous_state must be pending or running")
        raw = await self._loader.run(
            num_keys=4,
            keys=[
                self.proof_key(run_id),
                DagRedisKey.run_tasks(run_id),
                RedisKey.run_state(run_id),
                RedisKey.run_meta(run_id),
            ],
            args=[
                run_id,
                tenant_id,
                DagRedisKey.task_state_prefix(),
                ":state",
                DagRedisKey.task_meta_prefix(),
                ":meta",
                str(finalized_at_ms),
                _required_text(worker_instance_id, "worker_instance_id", allow_empty=True),
                _required_text(trigger_task_id, "trigger_task_id", allow_empty=True),
                _required_text(trigger_terminal_state, "trigger_terminal_state", allow_empty=True),
                str(canonical_expected_revision),
                canonical_previous_state,
            ],
        )
        if not isinstance(raw, (list, tuple)) or len(raw) < 14:
            raise RunTerminateAuthorityError(
                "terminal_proof_result_invalid",
                f"invalid terminal proof Lua result: {raw!r}",
            )
        status = _decode(raw[0])
        if status == NOT_READY_STATUS:
            return TerminalProofCaptureResult(
                status=status,
                proof=None,
                task_count=int(_decode(raw[3]) or 0),
                done_count=int(_decode(raw[4]) or 0),
                failed_count=int(_decode(raw[5]) or 0),
                skipped_count=int(_decode(raw[6]) or 0),
            )
        if status not in {CAPTURED_STATUS, ALREADY_CAPTURED_STATUS}:
            raise RunTerminateAuthorityError(status or "terminal_proof_conflict")
        proof_raw = {
            "schema_version": str(PROOF_SCHEMA_VERSION),
            "run_id": run_id,
            "tenant_id": tenant_id,
            "proof_sha256": _decode(raw[1]),
            "proof_payload_json": _decode(raw[2]),
            "task_count": _decode(raw[3]),
            "done_count": _decode(raw[4]),
            "failed_count": _decode(raw[5]),
            "skipped_count": _decode(raw[6]),
            "final_state": _decode(raw[7]),
            "finalized_at_ms": _decode(raw[8]),
            "worker_instance_id": _decode(raw[9]),
            "trigger_task_id": _decode(raw[10]),
            "trigger_terminal_state": _decode(raw[11]),
            "canonical_expected_revision": _decode(raw[12]),
            "canonical_previous_state": _decode(raw[13]),
        }
        proof = self._proof_from_mapping(proof_raw)
        return TerminalProofCaptureResult(status=status, proof=proof)


@dataclass(frozen=True)
class RunTerminateProjectionInput:
    operation_id: str
    terminal_proof_sha256: str
    canonical_transition_id: str
    canonical_record_hash: str
    canonical_command_hash: str
    canonical_revision: int
    proof: TerminalAggregateProof


@dataclass(frozen=True)
class RunTerminateProjectionResult:
    status: str
    projected: bool
    stream_entry_id: str


class RunTerminateProjectionManager:
    def __init__(self, redis: Any, *, loader: Any | None = None) -> None:
        self._redis = redis
        self._loader = loader

    @staticmethod
    def receipt_key(operation_id: str) -> str:
        operation = _required_text(operation_id, "operation_id")
        digest = hashlib.sha256(operation.encode("utf-8")).hexdigest()
        return f"{RedisKey.PREFIX}:run-terminate:projection:v1:{digest}"

    async def initialise(self) -> None:
        if self._loader is None:
            self._loader = LuaScriptLoader(
                self._redis,
                _lua_path("run_terminate_projection.lua"),
            )
        await self._loader.load()

    async def project(self, value: RunTerminateProjectionInput) -> RunTerminateProjectionResult:
        if not isinstance(value, RunTerminateProjectionInput):
            raise TypeError("projection must be RunTerminateProjectionInput")
        if self._loader is None:
            await self.initialise()
        assert self._loader is not None
        proof = value.proof
        operation_id = _required_text(value.operation_id, "operation_id")
        if operation_id != run_terminate_operation_id(proof.run_id, proof.proof_sha256):
            raise ValueError("projection operation_id does not match terminal proof")
        if _required_sha256(value.terminal_proof_sha256, "terminal_proof_sha256") != proof.proof_sha256:
            raise ValueError("projection terminal proof mismatch")
        revision = _safe_integer(value.canonical_revision, "canonical_revision", minimum=1)
        if revision != proof.canonical_expected_revision + 1:
            raise ValueError("projection canonical revision does not follow terminal proof")
        payload = {
            "task_count": proof.task_count,
            "done_count": proof.done_count,
            "failed_count": proof.failed_count,
            "skipped_count": proof.skipped_count,
            "trigger_task_id": proof.trigger_task_id,
            "trigger_terminal_state": proof.trigger_terminal_state,
        }
        payload_json = canonical_json_bytes(payload).decode("utf-8")
        payload_hash = hashlib.sha256(payload_json.encode("utf-8")).hexdigest()
        raw = await self._loader.run(
            num_keys=7,
            keys=[
                self.receipt_key(operation_id),
                RedisKey.run_terminal_event_index(),
                RedisKey.run_state(proof.run_id),
                RedisKey.run_meta(proof.run_id),
                RedisKey.run_result(proof.run_id),
                RedisKey.cp_running(),
                RedisKey.stream_results(),
            ],
            args=[
                operation_id,
                proof.proof_sha256,
                _required_text(value.canonical_transition_id, "canonical_transition_id"),
                _required_sha256(value.canonical_record_hash, "canonical_record_hash"),
                _required_sha256(value.canonical_command_hash, "canonical_command_hash"),
                str(revision),
                proof.run_id,
                proof.tenant_id,
                proof.final_state,
                str(proof.finalized_at_ms),
                proof.worker_instance_id,
                proof.trigger_task_id,
                proof.trigger_terminal_state,
                str(proof.task_count),
                str(proof.done_count),
                str(proof.failed_count),
                str(proof.skipped_count),
                str(int(getattr(RedisTTL, "RUN_STATE", 86400))),
                str(int(getattr(RedisTTL, "RUN_META", 86400))),
                str(int(getattr(RedisTTL, "RUN_RESULT", 86400))),
                str(int(getattr(RedisTTL, "STREAM_MAXLEN", 100000))),
                payload_json,
                payload_hash,
            ],
        )
        if not isinstance(raw, (list, tuple)) or len(raw) < 3:
            raise RunTerminateAuthorityError(
                PROJECTION_CONFLICT_STATUS,
                f"invalid RUN_TERMINATE projection result: {raw!r}",
                canonical_commit_durable=True,
            )
        status = _decode(raw[0])
        stream_entry_id = _decode(raw[1])
        detail = _decode(raw[2])
        if status == PROJECTED_STATUS:
            return RunTerminateProjectionResult(status=status, projected=True, stream_entry_id=stream_entry_id)
        if status == ALREADY_PROJECTED_STATUS:
            return RunTerminateProjectionResult(status=status, projected=False, stream_entry_id=stream_entry_id)
        raise RunTerminateAuthorityError(
            status or PROJECTION_CONFLICT_STATUS,
            detail,
            canonical_commit_durable=True,
        )


@dataclass(frozen=True)
class RunTerminateBindingResult:
    finalized: bool
    status: str
    run_id: str
    final_state: str = ""
    task_count: int = 0
    done_count: int = 0
    failed_count: int = 0
    skipped_count: int = 0
    already_finalized: bool = False
    ack_allowed: bool = False
    canonical_transition_id: str = ""
    canonical_record_hash: str = ""
    canonical_command_hash: str = ""
    canonical_revision: int = 0
    terminal_proof_sha256: str = ""


def build_run_terminate_command(proof: TerminalAggregateProof) -> AuthorityCommand:
    if not isinstance(proof, TerminalAggregateProof):
        raise TypeError("proof must be TerminalAggregateProof")
    identity = run_terminate_identity(proof.run_id)
    operation_id = run_terminate_operation_id(proof.run_id, proof.proof_sha256)
    task_states = [
        {"task_id": row.task_id, "state": row.state}
        for row in proof.tasks
    ]
    terminal_evidence = {
        "schema_version": proof.schema_version,
        "terminal_proof_sha256": proof.proof_sha256,
        "task_count": proof.task_count,
        "done_count": proof.done_count,
        "failed_count": proof.failed_count,
        "skipped_count": proof.skipped_count,
        "final_state": proof.final_state,
        "tasks": task_states,
    }
    metadata = {
        "run_id": proof.run_id,
        "tenant_id": proof.tenant_id,
        "terminal_evidence": terminal_evidence,
        "finalized_at_ms": proof.finalized_at_ms,
        "worker_instance_id": proof.worker_instance_id,
        "trigger_task_id": proof.trigger_task_id,
        "trigger_terminal_state": proof.trigger_terminal_state,
    }
    return AuthorityCommand(
        aggregate_identity=identity,
        operation_type=OperationType.RUN_TERMINATE,
        operation_id=operation_id,
        expected_revision=proof.canonical_expected_revision,
        intended_previous_state=proof.canonical_previous_state,
        intended_next_state=proof.final_state,
        authoritative_payload=metadata,
        authoritative_metadata_changes=metadata,
        requested_child_effects={},
        requested_projection_intents=(
            {
                "kind": "RUN_RESULT_PROJECTION",
                "terminal_proof_sha256": proof.proof_sha256,
                "terminal_state": proof.final_state,
            },
        ),
        causation_id=None,
    )


def build_run_terminate_context(command: AuthorityCommand) -> AuthorityEntryContext:
    if command.operation_type is not OperationType.RUN_TERMINATE:
        raise ValueError("RUN_TERMINATE binding rejects every other operation")
    return AuthorityEntryContext(
        authenticated_writer_id=WRITER_ID,
        allowed_operations=frozenset({OperationType.RUN_TERMINATE}),
        target_aggregate_identity_sha256=command.aggregate_identity.sha256,
        fence_required=False,
        fence_valid=True,
    )


class RunTerminateAuthorityBinding:
    def __init__(
        self,
        redis: Any,
        *,
        store: RedisCanonicalAuthorityStore | None = None,
        proof_manager: TerminalAggregateProofManager | None = None,
        projection_manager: RunTerminateProjectionManager | None = None,
        resource_manager: AdmissionResourceReservationManager | None = None,
    ) -> None:
        self.redis = redis
        self.store = store or RedisCanonicalAuthorityStore(redis)
        self.proof_manager = proof_manager or TerminalAggregateProofManager(redis)
        self.projection_manager = projection_manager or RunTerminateProjectionManager(redis)
        self.resource_manager = resource_manager

    async def initialise(self) -> None:
        await self.store.initialise()
        await self.proof_manager.initialise()
        if self.resource_manager is not None:
            await self.resource_manager.initialise()
        await self.projection_manager.initialise()

    async def _validate_exact_head(self, record: Any, receipt: Any) -> None:
        keyspace = self.store.keyspace(record.aggregate_identity_sha256)
        await self.store.validate_authority_head(
            record.aggregate_identity,
            expected_operation_id=record.operation_id,
            expected_operation_digest=keyspace.operation_field(record.operation_id),
            expected_transition_id=record.transition_id,
            expected_revision=record.to_revision,
            expected_canonical_command_hash=record.canonical_command_hash,
            expected_canonical_record_hash=record.canonical_record_hash,
            expected_record=record,
            expected_receipt=receipt,
            expected_state=record.next_state,
            expected_projection_intents_json=self.store.canonical_projection_intents_json(
                record.durable_projection_intents
            ),
            expected_updated_at_ms=record.committed_at_ms,
        )

    async def _resolve_exact_durable_commit(self, command: AuthorityCommand) -> tuple[Any, Any] | tuple[None, None]:
        try:
            probe = await self.store.load_receipt_probe(command.aggregate_identity, command.operation_id)
        except (RedisAuthorityCorruptionError, RedisAuthorityPersistenceError) as exc:
            raise RunTerminateAuthorityError(
                "canonical_run_terminate_evidence_unavailable",
                str(exc),
            ) from exc
        if probe is None:
            return None, None
        record = probe.canonical_store_record
        if record is None or probe.receipt.canonical_command_hash != command.canonical_command_hash:
            raise RunTerminateAuthorityError(
                AUTHORITY_CONFLICT_STATUS,
                "stored RUN_TERMINATE receipt does not match incoming command",
                canonical_commit_durable=True,
            )
        await self._validate_exact_head(record, probe.receipt)
        return record, probe.receipt

    async def _resource_reservation_for_terminal_record(
        self,
        *,
        terminal_record: Any,
        proof: TerminalAggregateProof,
    ):
        operation_id = run_create_operation_id(proof.run_id)
        identity = run_terminate_identity(proof.run_id)
        try:
            probe = await self.store.load_receipt_probe(identity, operation_id)
        except (RedisAuthorityCorruptionError, RedisAuthorityPersistenceError) as exc:
            raise RunTerminateAuthorityError(
                RESOURCE_SETTLEMENT_PENDING_STATUS,
                f"RUN_CREATE settlement evidence unavailable: {exc}",
                canonical_commit_durable=True,
            ) from exc
        if probe is None or probe.canonical_store_record is None:
            raise RunTerminateAuthorityError(
                RESOURCE_SETTLEMENT_PENDING_STATUS,
                "canonical RUN_CREATE record/receipt required for settlement",
                canonical_commit_durable=True,
            )
        record = probe.canonical_store_record
        receipt = probe.receipt
        checks = (
            record.aggregate_identity.sha256 == identity.sha256,
            record.aggregate_identity_sha256 == identity.sha256,
            record.operation_type == OperationType.RUN_CREATE.value,
            record.operation_id == operation_id,
            record.from_revision == 0,
            record.to_revision == 1,
            receipt.operation_id == record.operation_id,
            receipt.transition_id == record.transition_id,
            receipt.canonical_command_hash == record.canonical_command_hash,
            receipt.canonical_record_hash == record.canonical_record_hash,
            receipt.aggregate_revision == record.to_revision,
            receipt.operation_type == record.operation_type,
            bool(record.verify_hash()),
            terminal_record.from_revision == record.to_revision,
        )
        if not all(checks):
            raise RunTerminateAuthorityError(
                RESOURCE_SETTLEMENT_PENDING_STATUS,
                "canonical RUN_CREATE settlement proof continuity mismatch",
                canonical_commit_durable=True,
            )
        try:
            reservation = resource_reservation_from_run_create_record(record)
        except (TypeError, ValueError) as exc:
            raise RunTerminateAuthorityError(
                RESOURCE_SETTLEMENT_PENDING_STATUS,
                f"canonical RUN_CREATE reservation reconstruction failed: {exc}",
                canonical_commit_durable=True,
            ) from exc
        if reservation.run_id != proof.run_id or reservation.tenant_id != proof.tenant_id:
            raise RunTerminateAuthorityError(
                RESOURCE_SETTLEMENT_PENDING_STATUS,
                "RUN_CREATE settlement identity does not match terminal proof",
                canonical_commit_durable=True,
            )
        return reservation

    async def _settle_resources(
        self,
        *,
        terminal_record: Any,
        proof: TerminalAggregateProof,
    ) -> None:
        manager = self.resource_manager
        if manager is None:
            return
        reservation = await self._resource_reservation_for_terminal_record(
            terminal_record=terminal_record,
            proof=proof,
        )
        settlement = AdmissionResourceSettlementInput(
            run_create_operation_id=reservation.operation_id,
            run_id=reservation.run_id,
            tenant_id=reservation.tenant_id,
            estimated_cost_cents=reservation.estimated_cost_cents,
            run_create_reservation_proof_sha256=reservation.proof_sha256,
            run_terminate_operation_id=terminal_record.operation_id,
            terminal_proof_sha256=proof.proof_sha256,
            canonical_transition_id=terminal_record.transition_id,
            canonical_record_hash=terminal_record.canonical_record_hash,
            canonical_command_hash=terminal_record.canonical_command_hash,
            canonical_revision=terminal_record.to_revision,
            final_state=proof.final_state,
        )
        try:
            settled = await manager.settle_once(
                settlement,
                now_ms=proof.finalized_at_ms,
            )
        except Exception as exc:
            raise RunTerminateAuthorityError(
                RESOURCE_SETTLEMENT_PENDING_STATUS,
                f"resource settlement failed after durable RUN_TERMINATE: {exc}",
                canonical_commit_durable=True,
            ) from exc
        if (
            settled.status not in {
                RESERVATION_STATUS_SETTLED,
                RESERVATION_STATUS_ALREADY_SETTLED,
            }
            or settled.state != RESERVATION_STATE_SETTLED
        ):
            raise RunTerminateAuthorityError(
                RESOURCE_SETTLEMENT_PENDING_STATUS,
                f"resource settlement blocked: {settled.status}",
                canonical_commit_durable=True,
            )

    async def terminate(
        self,
        *,
        run_id: str,
        tenant_id: str,
        trigger_task_id: str,
        finalized_at_ms: int,
        worker_instance_id: str = "",
        trigger_terminal_state: str = "",
    ) -> RunTerminateBindingResult:
        run_id = _required_text(run_id, "run_id")
        tenant_id = _required_text(tenant_id, "tenant_id")
        finalized_at_ms = _safe_integer(finalized_at_ms, "finalized_at_ms")
        identity = run_terminate_identity(run_id)

        try:
            existing_proof = await self.proof_manager.load_existing(run_id)
            snapshot = await self.store.get_aggregate_snapshot(identity)
        except (RedisAuthorityCorruptionError, RedisAuthorityPersistenceError) as exc:
            raise RunTerminateAuthorityError(
                "canonical_run_terminate_evidence_unavailable",
                str(exc),
            ) from exc
        if snapshot is None:
            raise RunTerminateAuthorityError(
                "canonical_run_missing",
                "RUN_TERMINATE requires an existing canonical RUN aggregate",
            )

        proof = existing_proof
        probe = None
        command = None
        if proof is not None:
            if proof.tenant_id != tenant_id:
                raise RunTerminateAuthorityError(
                    "terminal_proof_identity_conflict",
                    "terminal proof tenant does not match caller",
                )
            command = build_run_terminate_command(proof)
            try:
                probe = await self.store.load_receipt_probe(identity, command.operation_id)
            except (RedisAuthorityCorruptionError, RedisAuthorityPersistenceError) as exc:
                raise RunTerminateAuthorityError(
                    "canonical_run_terminate_evidence_unavailable",
                    str(exc),
                ) from exc
            if probe is None:
                # Before canonical durability, a retry must prove the live TASK
                # aggregate still equals the frozen proof. Once canonical receipt
                # exists, recovery deliberately skips mutable TASK reinterpretation.
                captured = await self.proof_manager.capture(
                    run_id=run_id,
                    tenant_id=tenant_id,
                    finalized_at_ms=finalized_at_ms,
                    worker_instance_id=worker_instance_id,
                    trigger_task_id=trigger_task_id,
                    trigger_terminal_state=trigger_terminal_state,
                    canonical_expected_revision=proof.canonical_expected_revision,
                    canonical_previous_state=proof.canonical_previous_state,
                )
                if captured.proof is None:
                    raise RunTerminateAuthorityError("terminal_proof_changed_after_capture")
                proof = captured.proof
                command = build_run_terminate_command(proof)
        else:
            if snapshot.state not in {"pending", "running"}:
                raise RunTerminateAuthorityError(
                    "canonical_run_already_terminal_without_matching_proof",
                    f"canonical RUN state is {snapshot.state!r}",
                )
            captured = await self.proof_manager.capture(
                run_id=run_id,
                tenant_id=tenant_id,
                finalized_at_ms=finalized_at_ms,
                worker_instance_id=worker_instance_id,
                trigger_task_id=trigger_task_id,
                trigger_terminal_state=trigger_terminal_state,
                canonical_expected_revision=snapshot.revision,
                canonical_previous_state=snapshot.state,
            )
            if captured.status == NOT_READY_STATUS:
                return RunTerminateBindingResult(
                    finalized=False,
                    status=NOT_READY_STATUS,
                    run_id=run_id,
                    task_count=captured.task_count,
                    done_count=captured.done_count,
                    failed_count=captured.failed_count,
                    skipped_count=captured.skipped_count,
                    ack_allowed=True,
                )
            if captured.proof is None:
                raise RunTerminateAuthorityError(
                    "terminal_proof_capture_failed",
                    "terminal proof capture returned no proof",
                )
            proof = captured.proof
            command = build_run_terminate_command(proof)
            probe = await self.store.load_receipt_probe(identity, command.operation_id)

        assert proof is not None
        assert command is not None
        context = build_run_terminate_context(command)
        evaluation = evaluate_authority_commit(
            context=context,
            command=command,
            current_revision=snapshot.revision,
            current_state=snapshot.state,
            receipt_probe=probe,
            committed_at_ms=proof.finalized_at_ms,
            correlation_id=None,
        )

        record = None
        canonical_receipt = None
        if evaluation.decision.code is AuthorityDecisionCode.ALREADY_APPLIED:
            if probe is None or probe.canonical_store_record is None:
                raise RunTerminateAuthorityError(
                    "canonical_run_terminate_receipt_corrupt",
                    "duplicate RUN_TERMINATE receipt proof is incomplete",
                    canonical_commit_durable=True,
                )
            record = probe.canonical_store_record
            canonical_receipt = probe.receipt
        elif evaluation.decision.code is AuthorityDecisionCode.ACCEPTED:
            if evaluation.commit_plan is None:
                raise RunTerminateAuthorityError(
                    AUTHORITY_CONFLICT_STATUS,
                    "accepted RUN_TERMINATE has no commit plan",
                )
            try:
                persisted = await self.store.commit(evaluation.commit_plan)
            except Exception as commit_exc:
                record, canonical_receipt = await self._resolve_exact_durable_commit(command)
                if record is None:
                    raise RunTerminateAuthorityError(
                        "canonical_run_terminate_commit_failed",
                        f"canonical commit failed and exact receipt is absent: {commit_exc}",
                    ) from commit_exc
            else:
                if persisted.status is RedisAuthorityCommitStatus.COMMITTED:
                    record = evaluation.commit_plan.record
                    canonical_receipt = evaluation.commit_plan.receipt
                elif persisted.status is RedisAuthorityCommitStatus.ALREADY_APPLIED:
                    record, canonical_receipt = await self._resolve_exact_durable_commit(command)
                    if record is None or canonical_receipt is None:
                        raise RunTerminateAuthorityError(
                            "canonical_run_terminate_receipt_corrupt",
                            "ALREADY_APPLIED did not resolve to exact canonical proof",
                        )
                else:
                    detail = persisted.detail
                    durable = persisted.status not in _DEFINITIVE_NON_DURABLE_COMMIT_STATUSES
                    raise RunTerminateAuthorityError(
                        persisted.status.value,
                        detail,
                        canonical_commit_durable=durable,
                    )
        else:
            raise RunTerminateAuthorityError(
                evaluation.decision.code.value,
                "canonical authority rejected RUN_TERMINATE",
                canonical_commit_durable=probe is not None,
            )

        assert record is not None
        assert canonical_receipt is not None
        await self._validate_exact_head(record, canonical_receipt)
        if record.operation_type != OperationType.RUN_TERMINATE.value:
            raise RunTerminateProjectionPendingError("canonical record is not RUN_TERMINATE")
        if record.operation_id != command.operation_id or record.next_state != proof.final_state:
            raise RunTerminateProjectionPendingError("canonical RUN_TERMINATE proof mismatch")
        if record.canonical_command_hash != command.canonical_command_hash:
            raise RunTerminateProjectionPendingError("canonical RUN_TERMINATE command hash mismatch")

        await self._settle_resources(
            terminal_record=record,
            proof=proof,
        )

        try:
            projected = await self.projection_manager.project(
                RunTerminateProjectionInput(
                    operation_id=record.operation_id,
                    terminal_proof_sha256=proof.proof_sha256,
                    canonical_transition_id=record.transition_id,
                    canonical_record_hash=record.canonical_record_hash,
                    canonical_command_hash=record.canonical_command_hash,
                    canonical_revision=record.to_revision,
                    proof=proof,
                )
            )
        except RunTerminateAuthorityError as exc:
            if exc.canonical_commit_durable:
                raise RunTerminateProjectionPendingError(str(exc)) from exc
            raise
        except Exception as exc:
            raise RunTerminateProjectionPendingError(str(exc)) from exc

        return RunTerminateBindingResult(
            finalized=True,
            status=projected.status,
            run_id=proof.run_id,
            final_state=proof.final_state,
            task_count=proof.task_count,
            done_count=proof.done_count,
            failed_count=proof.failed_count,
            skipped_count=proof.skipped_count,
            already_finalized=not projected.projected,
            ack_allowed=True,
            canonical_transition_id=record.transition_id,
            canonical_record_hash=record.canonical_record_hash,
            canonical_command_hash=record.canonical_command_hash,
            canonical_revision=record.to_revision,
            terminal_proof_sha256=proof.proof_sha256,
        )


__all__ = [
    "ALREADY_CAPTURED_STATUS",
    "ALREADY_PROJECTED_STATUS",
    "AUTHORITY_CONFLICT_STATUS",
    "CAPTURED_STATUS",
    "NOT_READY_STATUS",
    "PROJECTED_STATUS",
    "PROJECTION_CONFLICT_STATUS",
    "PROJECTION_PENDING_STATUS",
    "RESOURCE_SETTLEMENT_PENDING_STATUS",
    "PROOF_SCHEMA_VERSION",
    "RunTerminateAuthorityBinding",
    "RunTerminateAuthorityError",
    "RunTerminateBindingResult",
    "RunTerminateProjectionInput",
    "RunTerminateProjectionManager",
    "RunTerminateProjectionPendingError",
    "RunTerminateProjectionResult",
    "TerminalAggregateProof",
    "TerminalAggregateProofManager",
    "TerminalProofCaptureResult",
    "TerminalTaskEvidence",
    "WRITER_ID",
    "build_run_terminate_command",
    "build_run_terminate_context",
    "run_terminate_identity",
    "run_terminate_operation_id",
]
