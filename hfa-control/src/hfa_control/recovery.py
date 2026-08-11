"""
hfa-control/src/hfa_control/recovery.py
---------------------------------------
Leader-owned recovery service with Sprint 82.3 TASK/RUN truth convergence.

Stale detection is read-only. A stale candidate may mutate RUN state, metadata,
the running projection and scheduler-facing events only through
``run_recovery_commit.lua`` after the RUN authority, run-task index and every
task identity/state have been checked. Contradictions produce durable
runtime-truth evidence and never trigger silent projection cleanup or automatic
repair.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Optional
from unittest.mock import Mock

from hfa.authority import AggregateType, CanonicalAggregateIdentity, OperationType
from hfa.config.keys import RedisKey, RedisTTL
from hfa.dag.schema import DagRedisKey
from hfa.events.codec import serialize_event
from hfa.events.schema import RunAdmittedEvent, RunDeadLetteredEvent
from hfa.governance.admission_resource_reservation import (
    AdmissionResourceReservationManager,
    RESERVATION_STATE_FINALIZED,
    RESERVATION_STATE_SETTLED,
)
from hfa.lua.loader import LuaScriptLoader
from hfa.runtime.tenant_utils import decrement_tenant_inflight_if_needed
from hfa_control.exceptions import DLQEntryNotFoundError, TenantMismatchError
from hfa_control.models import ControlPlaneConfig
from hfa_control.product_profile import parse_strict_bool
from hfa_control.run_create_authority import (
    FEATURE_FLAG as RUN_CREATE_FEATURE_FLAG,
    resource_reservation_from_run_create_record,
    run_create_identity,
    run_create_operation_id,
)
from hfa_control.run_terminate_authority import (
    NOT_READY_STATUS as RUN_TERMINATE_NOT_READY_STATUS,
    RunTerminateAuthorityBinding,
)
from hfa_control.state_machine import transition_state
from hfa_control.task_recovery import TaskRecoveryManager
from hfa_control.task_requeue_authority import (
    FEATURE_FLAG as TASK_REQUEUE_FEATURE_FLAG,
    parse_task_requeue_binding_flag,
)

try:
    from hfa.obs.runtime_metrics import IRONCLADMetrics as _M
except Exception:
    _M = None  # type: ignore[assignment]

try:
    from hfa.obs.tracing import get_tracer  # type: ignore

    _tracer = get_tracer("hfa.recovery")
except Exception:
    _tracer = None

logger = logging.getLogger(__name__)


RUN_RECOVERY_RESCHEDULED = "RUN_RESCHEDULED"
RUN_RECOVERY_DEAD_LETTERED = "RUN_DEAD_LETTERED"
_RUN_RECOVERY_CONFLICT_STATUSES = {
    "run_truth_missing",
    "run_truth_terminal_conflict",
    "run_truth_corruption_conflict",
    "task_truth_missing",
    "task_truth_terminal_conflict",
    "task_truth_corruption_conflict",
    "truth_conflict_evidence_store_unavailable",
}

_CANONICAL_TASK_REQUEUE_DEPENDENCY_FLAGS = (
    "HFA_CANONICAL_TASK_ADMIT_BINDING",
    "HFA_CANONICAL_TASK_DISPATCH_BINDING",
    "HFA_CANONICAL_TASK_CLAIM_BINDING",
    "HFA_CANONICAL_TASK_TERMINAL_BINDING",
    RUN_CREATE_FEATURE_FLAG,
)


def _decode(value) -> str:
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return str(value) if value is not None else ""


def _decode_mapping(raw: dict) -> dict[str, str]:
    return {_decode(key): _decode(value) for key, value in (raw or {}).items()}


def _safe_int(value, default: int = 0) -> int:
    try:
        return int(_decode(value))
    except (TypeError, ValueError):
        return default


def _is_mock_lua_result(raw) -> bool:
    if isinstance(raw, Mock):
        return True
    return bool(isinstance(raw, (list, tuple)) and raw and isinstance(raw[0], Mock))


def _lua_path(filename: str) -> Path:
    here = Path(__file__).resolve()
    candidates = [
        here.parent.parent.parent.parent / "hfa-core" / "src" / "hfa" / "lua" / filename,
        here.parent.parent.parent / "hfa" / "lua" / filename,
    ]
    for path in candidates:
        if path.exists():
            return path
    for parent in here.parents:
        for subdir in ("hfa-core/src/hfa/lua", "hfa/lua"):
            path = parent / subdir / filename
            if path.exists():
                return path
    raise FileNotFoundError(f"Lua script not found: {filename}")


@dataclass(frozen=True)
class RunRecoveryCommitResult:
    committed: bool
    status: str
    reschedule_count: int = 0
    observed_run_state: str = ""
    conflict_task_id: str = ""

    @property
    def conflict(self) -> bool:
        return self.status in _RUN_RECOVERY_CONFLICT_STATUSES


class RecoveryService:
    def __init__(self, redis, config: ControlPlaneConfig) -> None:
        self._redis = redis
        self._config = config
        self._task: Optional[asyncio.Task] = None
        self._recovery_loader: Optional[LuaScriptLoader] = None
        self._canonical_task_requeue_binding = (
            parse_task_requeue_binding_flag(
                os.getenv(TASK_REQUEUE_FEATURE_FLAG)
            )
        )
        self._task_recovery: TaskRecoveryManager | None = None
        self._resource_manager: AdmissionResourceReservationManager | None = None
        self._run_terminate_authority: RunTerminateAuthorityBinding | None = None
        self._canonical_recovery_initialised = False
        if self._canonical_task_requeue_binding:
            missing = [
                name
                for name in _CANONICAL_TASK_REQUEUE_DEPENDENCY_FLAGS
                if not parse_strict_bool(
                    os.getenv(name),
                    name=name,
                    default=False,
                )
            ]
            if missing:
                raise ValueError(
                    "canonical TASK_REQUEUE production recovery requires "
                    "the canonical TASK_ADMIT/TASK_DISPATCH/TASK_CLAIM/"
                    "TASK_TERMINAL dependency chain; disabled: "
                    + ", ".join(missing)
                )
            self._task_recovery = TaskRecoveryManager(
                redis,
                canonical_task_requeue_binding=True,
            )
            self._resource_manager = AdmissionResourceReservationManager(redis)
            self._run_terminate_authority = RunTerminateAuthorityBinding(
                redis,
                resource_manager=self._resource_manager,
            )

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> None:
        if self._task_recovery is not None:
            # Fail closed before the leader-owned background task is started.
            await self._ensure_canonical_recovery_initialised()
        loop = asyncio.get_running_loop()
        self._task = loop.create_task(self._loop(), name="recovery.sweep")
        logger.info(
            "RecoveryService started: sweep_interval=%gs",
            self._config.recovery_sweep_interval,
        )

    async def _ensure_canonical_recovery_initialised(self) -> None:
        if self._canonical_recovery_initialised:
            return
        if self._task_recovery is None:
            return
        assert self._resource_manager is not None
        assert self._run_terminate_authority is not None
        await self._task_recovery.initialise()
        await self._run_terminate_authority.initialise()
        self._canonical_recovery_initialised = True

    async def close(self) -> None:
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None
        logger.info("RecoveryService closed")

    async def _ensure_recovery_loaded(self) -> None:
        if self._recovery_loader is None:
            self._recovery_loader = LuaScriptLoader(
                self._redis,
                _lua_path("run_recovery_commit.lua"),
            )
            await self._recovery_loader.load()

    # ------------------------------------------------------------------
    # Sweep loop
    # ------------------------------------------------------------------

    async def _loop(self) -> None:
        while True:
            try:
                await asyncio.sleep(self._config.recovery_sweep_interval)
                await self._sweep()
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.error("RecoveryService._loop error: %s", exc, exc_info=True)

    async def _sweep(self) -> None:
        attempted_task_runs: set[str] = set()
        if self._task_recovery is not None:
            await self._ensure_canonical_recovery_initialised()
            attempted_task_runs = await self._sweep_stale_tasks()

        stale_runs = await self._find_stale_runs()
        rescheduled = 0
        dlq_count = 0
        conflict_count = 0
        canonical_count = 0

        if stale_runs and _M:
            _M.recovery_stale_detected_total.inc(len(stale_runs))

        for run_id in stale_runs:
            if self._task_recovery is not None:
                # A stale TASK recovery attempt owns this RUN for the whole
                # sweep, including failures. This prevents a second RUN
                # terminal attempt or a legacy fallback in the same cycle.
                if run_id in attempted_task_runs:
                    continue
                try:
                    result = await self._handle_canonical_stale_run(run_id)
                except Exception as exc:
                    conflict_count += 1
                    logger.error(
                        "RecoveryService canonical stale RUN blocked: "
                        "run=%s error=%s",
                        run_id,
                        exc,
                        exc_info=True,
                    )
                    continue
                canonical_count += 1
                if result == "conflict":
                    conflict_count += 1
                continue

            result = await self._handle_stale(run_id)
            if result == "rescheduled":
                rescheduled += 1
                if _M:
                    _M.recovery_rescheduled_total.inc()
            elif result == "dlq":
                dlq_count += 1
                if _M:
                    _M.recovery_dlq_total.inc()
            elif result == "conflict":
                conflict_count += 1

        if stale_runs:
            logger.info(
                "Recovery sweep: stale=%d rescheduled=%d dlq=%d "
                "canonical=%d conflicts=%d",
                len(stale_runs),
                rescheduled,
                dlq_count,
                canonical_count,
                conflict_count,
            )

    async def _sweep_stale_tasks(self, *, now_ms: int | None = None) -> set[str]:
        """Run canonical stale-TASK recovery from the production leader loop.

        Discovery remains the existing DAG active-tenant/running projection.
        The stale observation timestamp is the eligibility boundary for this
        invocation: a heartbeat arriving after that observation cannot revoke
        a canonical commit already in flight. If no commit becomes durable, a
        later sweep re-evaluates liveness from current heartbeat truth.
        """

        if self._task_recovery is None:
            return set()
        await self._ensure_canonical_recovery_initialised()

        observed_at_ms = int(
            now_ms if now_ms is not None else time.time() * 1000
        )
        try:
            raw_tenants = await self._redis.smembers(
                DagRedisKey.tenant_active_set()
            )
        except Exception as exc:
            logger.error(
                "RecoveryService TASK tenant discovery failed: %s",
                exc,
            )
            return set()

        tenants = sorted(
            {_decode(value) for value in raw_tenants if _decode(value)}
        )
        attempted_runs: set[str] = set()
        for tenant_id in tenants:
            stale_task_ids = await self._task_recovery.find_stale_tasks(
                tenant_id=tenant_id,
                now_ms=observed_at_ms,
            )
            for task_id in stale_task_ids:
                try:
                    run_id = _decode(
                        await self._redis.hget(
                            DagRedisKey.task_meta(task_id),
                            "run_id",
                        )
                    ).strip()
                except Exception as exc:
                    logger.error(
                        "RecoveryService TASK run identity read failed: "
                        "task=%s tenant=%s error=%s",
                        task_id,
                        tenant_id,
                        exc,
                    )
                    continue
                if not run_id:
                    logger.error(
                        "RecoveryService TASK run identity missing: "
                        "task=%s tenant=%s",
                        task_id,
                        tenant_id,
                    )
                    continue

                # Ownership begins at the stale candidate boundary. Any proof,
                # authority, projection, settlement, or RUN finalization error
                # therefore fails closed for this sweep.
                attempted_runs.add(run_id)
                try:
                    await self._validate_canonical_run_resources(
                        run_id=run_id,
                        tenant_id=tenant_id,
                        allow_settled=False,
                    )
                    result = await self._task_recovery.requeue_stale_task(
                        task_id=task_id,
                        run_id=run_id,
                        tenant_id=tenant_id,
                        expected_state="running",
                        now_ms=observed_at_ms,
                        reason_code="TASK_STALE_DETECTED",
                    )
                    if result.status in {
                        "TASK_RETRY_EXHAUSTED",
                        "TASK_TERMINAL_RECOVERED",
                    }:
                        await self._finalize_canonical_run(
                            run_id=run_id,
                            tenant_id=tenant_id,
                            preferred_trigger_task_id=task_id,
                        )
                except Exception as exc:
                    logger.error(
                        "RecoveryService canonical TASK recovery blocked: "
                        "run=%s task=%s tenant=%s error=%s",
                        run_id,
                        task_id,
                        tenant_id,
                        exc,
                        exc_info=True,
                    )
                    continue
                logger.warning(
                    "RecoveryService canonical TASK recovery: "
                    "run=%s task=%s tenant=%s status=%s requeue_count=%d",
                    run_id,
                    task_id,
                    tenant_id,
                    result.status,
                    result.requeue_count,
                )

        return attempted_runs

    async def _validate_canonical_run_resources(
        self,
        *,
        run_id: str,
        tenant_id: str,
        allow_settled: bool,
    ):
        """Prove canonical RUN_CREATE and its exact admission reservation."""

        await self._ensure_canonical_recovery_initialised()
        assert self._resource_manager is not None
        assert self._run_terminate_authority is not None
        store = self._run_terminate_authority.store
        identity = run_create_identity(run_id)
        operation_id = run_create_operation_id(run_id)
        probe = await store.load_receipt_probe(identity, operation_id)
        if probe is None or probe.canonical_store_record is None:
            raise RuntimeError(
                "canonical RUN_CREATE record/receipt required for TASK recovery"
            )
        record = probe.canonical_store_record
        authority_receipt = probe.receipt
        if (
            record.operation_type != OperationType.RUN_CREATE.value
            or record.operation_id != operation_id
            or record.from_revision != 0
            or record.to_revision != 1
            or authority_receipt.operation_id != record.operation_id
            or authority_receipt.transition_id != record.transition_id
            or authority_receipt.canonical_command_hash
            != record.canonical_command_hash
            or authority_receipt.canonical_record_hash
            != record.canonical_record_hash
            or authority_receipt.aggregate_revision != record.to_revision
            or authority_receipt.operation_type != record.operation_type
            or not record.verify_hash()
        ):
            raise RuntimeError(
                "canonical RUN_CREATE record/receipt continuity mismatch"
            )
        reservation = resource_reservation_from_run_create_record(record)
        if reservation.run_id != run_id or reservation.tenant_id != tenant_id:
            raise RuntimeError(
                "canonical RUN_CREATE reservation identity mismatch"
            )
        resource_receipt = await self._resource_manager.get_receipt(
            reservation
        )
        if resource_receipt is None:
            raise RuntimeError(
                "canonical RUN_CREATE resource reservation receipt is missing"
            )
        allowed_states = {RESERVATION_STATE_FINALIZED}
        if allow_settled:
            allowed_states.add(RESERVATION_STATE_SETTLED)
        if resource_receipt.state not in allowed_states:
            raise RuntimeError(
                "canonical RUN_CREATE resource reservation is not in an "
                f"allowed recovery state: {resource_receipt.state}"
            )
        snapshot = await store.get_aggregate_snapshot(identity)
        if snapshot is None:
            raise RuntimeError("canonical RUN aggregate is missing")
        if not allow_settled and snapshot.state not in {"pending", "running"}:
            raise RuntimeError(
                "canonical RUN is not nonterminal for TASK recovery: "
                f"{snapshot.state}"
            )
        return reservation, resource_receipt, snapshot

    async def _canonical_task_evidence(
        self, run_id: str, tenant_id: str
    ) -> list[dict]:
        """Load exact canonical TASK heads for one DAG-owned stale RUN."""

        assert self._run_terminate_authority is not None
        store = self._run_terminate_authority.store
        index_key = DagRedisKey.run_tasks(run_id)
        kind = _decode(await self._redis.type(index_key))
        if kind != "set":
            raise RuntimeError(
                f"canonical DAG run->tasks index must be set, observed {kind}"
            )
        task_ids = sorted(
            {_decode(value) for value in await self._redis.smembers(index_key) if _decode(value)}
        )
        if not task_ids:
            raise RuntimeError("canonical DAG run->tasks index is empty")

        evidence: list[dict] = []
        for task_id in task_ids:
            identity = CanonicalAggregateIdentity(
                aggregate_type=AggregateType.TASK,
                run_id=run_id,
                task_id=task_id,
            )
            snapshot = await store.get_aggregate_snapshot(identity)
            if snapshot is None:
                raise RuntimeError(
                    f"canonical TASK aggregate is missing: {task_id}"
                )
            if snapshot.state not in {
                "pending",
                "ready",
                "scheduled",
                "running",
                "done",
                "failed",
            }:
                raise RuntimeError(
                    "canonical TASK head state is unsupported: "
                    f"{task_id}={snapshot.state}"
                )
            probe = await store.load_receipt_probe(
                identity,
                snapshot.operation_id,
            )
            if probe is None or probe.canonical_store_record is None:
                raise RuntimeError(
                    f"canonical TASK head receipt is missing: {task_id}"
                )
            record = probe.canonical_store_record
            receipt = probe.receipt
            if (
                record.aggregate_identity.sha256 != identity.sha256
                or record.aggregate_identity_sha256 != identity.sha256
                or record.operation_id != snapshot.operation_id
                or record.transition_id != snapshot.transition_id
                or record.to_revision != snapshot.revision
                or record.next_state != snapshot.state
                or receipt.operation_id != record.operation_id
                or receipt.transition_id != record.transition_id
                or receipt.canonical_command_hash
                != record.canonical_command_hash
                or receipt.canonical_record_hash
                != record.canonical_record_hash
                or receipt.aggregate_revision != record.to_revision
                or receipt.operation_type != record.operation_type
                or not record.verify_hash()
            ):
                raise RuntimeError(
                    f"canonical TASK head continuity mismatch: {task_id}"
                )
            await store.validate_authority_head(
                identity,
                expected_operation_id=record.operation_id,
                expected_operation_digest=store.keyspace(
                    record.aggregate_identity_sha256
                ).operation_field(record.operation_id),
                expected_transition_id=record.transition_id,
                expected_revision=record.to_revision,
                expected_canonical_command_hash=record.canonical_command_hash,
                expected_canonical_record_hash=record.canonical_record_hash,
                expected_record=record,
                expected_receipt=receipt,
                expected_state=record.next_state,
                expected_projection_intents_json=store.canonical_projection_intents_json(
                    record.durable_projection_intents
                ),
                expected_updated_at_ms=record.committed_at_ms,
            )
            meta_kind = _decode(
                await self._redis.type(DagRedisKey.task_meta(task_id))
            )
            if meta_kind != "hash":
                raise RuntimeError(
                    f"TASK metadata projection must be hash: {task_id}"
                )
            meta = _decode_mapping(
                await self._redis.hgetall(DagRedisKey.task_meta(task_id))
            )
            if (
                meta.get("task_id") != task_id
                or meta.get("run_id") != run_id
                or meta.get("tenant_id") != tenant_id
            ):
                raise RuntimeError(
                    f"TASK metadata identity conflict: {task_id}"
                )
            mutable_state = _decode(
                await self._redis.get(DagRedisKey.task_state(task_id))
            )
            if snapshot.state in {"done", "failed"}:
                if mutable_state != snapshot.state:
                    raise RuntimeError(
                        "canonical terminal TASK projection is not proven: "
                        f"{task_id} canonical={snapshot.state} "
                        f"mutable={mutable_state or 'missing'}"
                    )
            elif mutable_state not in {
                "pending",
                "ready",
                "scheduled",
                "running",
            }:
                raise RuntimeError(
                    "canonical nonterminal TASK has contradictory mutable "
                    f"state: {task_id}={mutable_state or 'missing'}"
                )
            evidence.append(
                {
                    "task_id": task_id,
                    "tenant_id": meta["tenant_id"],
                    "snapshot": snapshot,
                    "record": record,
                    "mutable_state": mutable_state,
                }
            )
        return evidence

    async def _finalize_canonical_run(
        self,
        *,
        run_id: str,
        tenant_id: str,
        preferred_trigger_task_id: str = "",
    ):
        """Attempt/replay accepted RUN_TERMINATE after proven TASK terminality."""

        await self._validate_canonical_run_resources(
            run_id=run_id,
            tenant_id=tenant_id,
            allow_settled=True,
        )
        evidence = await self._canonical_task_evidence(run_id, tenant_id)
        nonterminal = [
            row for row in evidence
            if row["snapshot"].state not in {"done", "failed"}
        ]
        if nonterminal:
            return None

        trigger = None
        if preferred_trigger_task_id:
            trigger = next(
                (
                    row for row in evidence
                    if row["task_id"] == preferred_trigger_task_id
                ),
                None,
            )
        if trigger is None:
            trigger = max(
                evidence,
                key=lambda row: (
                    int(row["record"].committed_at_ms),
                    row["task_id"],
                ),
            )
        finalized_at_ms = max(
            int(row["record"].committed_at_ms) for row in evidence
        )
        data = trigger["record"].authoritative_metadata_changes
        if not isinstance(data, Mapping):
            raise RuntimeError("canonical terminal TASK metadata is invalid")
        assert self._run_terminate_authority is not None
        result = await self._run_terminate_authority.terminate(
            run_id=run_id,
            tenant_id=tenant_id,
            trigger_task_id=trigger["task_id"],
            finalized_at_ms=finalized_at_ms,
            worker_instance_id=str(data.get("worker_instance_id") or ""),
            trigger_terminal_state=trigger["snapshot"].state,
        )
        if result.status == RUN_TERMINATE_NOT_READY_STATUS:
            raise RuntimeError(
                "RUN_TERMINATE returned not_ready after all TASK heads were "
                "proven terminal"
            )
        return result

    async def _handle_canonical_stale_run(self, run_id: str) -> str:
        """Classify stale DAG-owned RUNs without legacy mutable retry."""

        meta_kind = _decode(await self._redis.type(RedisKey.run_meta(run_id)))
        if meta_kind != "hash":
            raise RuntimeError(
                f"RUN metadata projection must be hash, observed {meta_kind}"
            )
        meta = _decode_mapping(await self._redis.hgetall(RedisKey.run_meta(run_id)))
        tenant_id = meta.get("tenant_id", "")
        if not tenant_id or (meta.get("run_id") and meta.get("run_id") != run_id):
            raise RuntimeError("stale RUN metadata identity is missing or corrupt")

        await self._validate_canonical_run_resources(
            run_id=run_id,
            tenant_id=tenant_id,
            allow_settled=True,
        )
        evidence = await self._canonical_task_evidence(run_id, tenant_id)
        if any(
            row["snapshot"].state not in {"done", "failed"}
            for row in evidence
        ):
            # A durable TASK_REQUEUE can have completed mutable projection and
            # left the running TASK ZSET before canonical delivery succeeded.
            # The stale RUN path is then the only existing discovery surface.
            # Re-enter the receipt-first authority replay for ready requeue
            # heads; project/deliver are idempotent and no new TASK revision is
            # created. Other nonterminal heads remain suppression-only.
            assert self._task_recovery is not None
            for row in evidence:
                if (
                    row["snapshot"].state == "ready"
                    and row["record"].operation_type
                    == OperationType.TASK_REQUEUE.value
                ):
                    data = row["record"].authoritative_metadata_changes
                    if not isinstance(data, Mapping):
                        raise RuntimeError(
                            "canonical TASK_REQUEUE metadata is invalid"
                        )
                    await self._task_recovery.requeue_stale_task(
                        task_id=row["task_id"],
                        run_id=run_id,
                        tenant_id=tenant_id,
                        expected_state="running",
                        now_ms=int(row["record"].committed_at_ms),
                        reason_code=str(data.get("reason_code") or ""),
                    )
            # Case 1: TASK lifecycle remains authoritative. No RUN mutation.
            return "canonical_task_nonterminal"

        # Case 2: all TASK heads and mutable terminal projections agree. Reuse
        # the existing RUN_TERMINATE -> settlement -> projection chain.
        await self._finalize_canonical_run(
            run_id=run_id,
            tenant_id=tenant_id,
        )
        return "canonical_run_terminal"

    # ------------------------------------------------------------------
    # Stale run detection — read-only
    # ------------------------------------------------------------------

    async def _find_stale_runs(self) -> list[str]:
        """Return every stale running-projection member without mutation.

        Missing, terminal and corrupt RUN truth must reach the atomic recovery
        classifier. This method therefore never removes a ZSET member and never
        selects a caller-local winner.
        """

        cutoff = time.time() - self._config.stale_run_timeout
        try:
            stale_raw = await self._redis.zrangebyscore(
                self._config.running_zset,
                0,
                cutoff,
            )
        except Exception as exc:
            logger.error(
                "RecoveryService._find_stale_runs zrangebyscore error: %s",
                exc,
            )
            return []
        return sorted({_decode(run_id) for run_id in stale_raw if _decode(run_id)})

    # ------------------------------------------------------------------
    # Atomic recovery commit
    # ------------------------------------------------------------------

    async def _load_task_ids_for_keys(self, run_id: str) -> list[str]:
        """Pre-read task IDs only to construct dynamic Lua KEYS.

        The Lua script revalidates type, cardinality, membership and task
        identity. A missing/wrong-type index intentionally returns an empty
        pre-read so the script can classify it atomically.
        """

        key = DagRedisKey.run_tasks(run_id)
        try:
            key_type = _decode(await self._redis.type(key))
            if key_type != "set":
                return []
            raw_ids = await self._redis.smembers(key)
        except Exception:
            return []
        return sorted({_decode(task_id) for task_id in raw_ids if _decode(task_id)})

    async def _commit_recovery(
        self,
        *,
        run_id: str,
        expected_reschedule_count: int,
        requested_action: str,
        reason_code: str,
        now_ms: int | None = None,
        running_score: float | None = None,
    ) -> RunRecoveryCommitResult:
        await self._ensure_recovery_loaded()
        assert self._recovery_loader is not None

        now_ms = int(now_ms if now_ms is not None else time.time() * 1000)
        running_score = float(running_score if running_score is not None else time.time())
        task_ids = await self._load_task_ids_for_keys(run_id)

        keys = [
            RedisKey.run_state(run_id),
            RedisKey.run_meta(run_id),
            self._config.running_zset,
            DagRedisKey.run_tasks(run_id),
            RedisKey.runtime_truth_conflict_index(),
            RedisKey.runtime_truth_conflict_stream(),
            self._config.control_stream,
        ]
        for task_id in task_ids:
            keys.extend(
                [
                    DagRedisKey.task_state(task_id),
                    DagRedisKey.task_meta(task_id),
                ]
            )

        args = [
            run_id,
            str(now_ms),
            str(running_score),
            str(self._config.max_reschedule_attempts),
            str(expected_reschedule_count),
            requested_action,
            str(RedisTTL.RUN_STATE),
            str(RedisTTL.RUN_META),
            str(len(task_ids)),
            reason_code,
            str(RedisTTL.STREAM_MAXLEN),
            *task_ids,
        ]
        raw = await self._recovery_loader.run(
            num_keys=len(keys),
            keys=keys,
            args=args,
        )
        if _is_mock_lua_result(raw):
            raw = await self._commit_recovery_fallback(
                run_id=run_id,
                task_ids=task_ids,
                expected_reschedule_count=expected_reschedule_count,
                requested_action=requested_action,
                reason_code=reason_code,
                now_ms=now_ms,
                running_score=running_score,
            )

        status = _decode(raw[0]) if raw else "RUN_RECOVERY_EMPTY_RESULT"
        count = _safe_int(raw[1] if len(raw) > 1 else 0)
        observed = _decode(raw[2]) if len(raw) > 2 else ""
        conflict_task_id = _decode(raw[3]) if len(raw) > 3 else ""
        return RunRecoveryCommitResult(
            committed=status in {RUN_RECOVERY_RESCHEDULED, RUN_RECOVERY_DEAD_LETTERED},
            status=status,
            reschedule_count=count,
            observed_run_state=observed,
            conflict_task_id=conflict_task_id,
        )

    async def _commit_recovery_fallback(
        self,
        *,
        run_id: str,
        task_ids: list[str],
        expected_reschedule_count: int,
        requested_action: str,
        reason_code: str,
        now_ms: int,
        running_score: float,
    ) -> list[str]:
        """Mutation-free compatibility path.

        Recovery requires one atomic Redis/Lua boundary that includes state,
        projection and event append. A mock result cannot prove that boundary,
        so compatibility execution always fails closed and never writes a
        substitute lifecycle state.
        """

        del requested_action, reason_code, now_ms, running_score
        try:
            observed_run_state = _decode(
                await self._redis.get(RedisKey.run_state(run_id))
            )
        except Exception:
            observed_run_state = ""
        return [
            "truth_conflict_evidence_store_unavailable",
            str(expected_reschedule_count),
            observed_run_state,
            task_ids[0] if task_ids else "",
        ]

    # ------------------------------------------------------------------
    # Stale run handler
    # ------------------------------------------------------------------

    async def _handle_stale(self, run_id: str) -> str:
        """Classify a stale candidate without trusting a pre-read as authority."""

        meta_key = RedisKey.run_meta(run_id)
        try:
            meta_kind = _decode(await self._redis.type(meta_key))
            raw_meta = await self._redis.hgetall(meta_key) if meta_kind == "hash" else {}
        except Exception:
            raw_meta = {}
        meta = _decode_mapping(raw_meta)
        tenant_id = meta.get("tenant_id", "")
        agent_type = meta.get("agent_type", "")
        previous_worker = meta.get("worker_group", "")
        reschedule_count = _safe_int(meta.get("reschedule_count"), 0)

        if reschedule_count >= self._config.max_reschedule_attempts:
            return await self._dead_letter(
                run_id,
                tenant_id,
                reschedule_count,
                "max_reschedule_exceeded",
            )
        return await self._reschedule(
            run_id,
            tenant_id,
            agent_type,
            previous_worker,
            reschedule_count,
        )

    async def _reschedule(
        self,
        run_id: str,
        tenant_id: str,
        agent_type: str,
        previous_worker: str,
        reschedule_count: int,
    ) -> str:
        del agent_type
        result = await self._commit_recovery(
            run_id=run_id,
            expected_reschedule_count=reschedule_count,
            requested_action="RESCHEDULE",
            reason_code="stale_running",
        )
        if not result.committed:
            logger.warning(
                "Recovery reschedule blocked: run=%s status=%s state=%s task=%s",
                run_id,
                result.status,
                result.observed_run_state,
                result.conflict_task_id,
            )
            return "conflict" if result.conflict else "skipped"

        logger.warning(
            "Rescheduled: run=%s tenant=%s attempt=%d/%d prev_worker=%s",
            run_id,
            tenant_id,
            result.reschedule_count,
            self._config.max_reschedule_attempts,
            previous_worker,
        )
        return "rescheduled"

    async def _dead_letter(
        self,
        run_id: str,
        tenant_id: str,
        reschedule_count: int,
        reason: str,
    ) -> str:
        result = await self._commit_recovery(
            run_id=run_id,
            expected_reschedule_count=reschedule_count,
            requested_action="DEAD_LETTER",
            reason_code=reason,
        )
        if not result.committed:
            logger.warning(
                "Recovery dead-letter blocked: run=%s status=%s state=%s task=%s",
                run_id,
                result.status,
                result.observed_run_state,
                result.conflict_task_id,
            )
            return "conflict" if result.conflict else "skipped"

        dead_lettered_at = time.time()
        await self._redis.hset(
            RedisKey.cp_dlq_meta(run_id),
            mapping={
                "run_id": run_id,
                "tenant_id": tenant_id,
                "reason": reason,
                "reschedule_count": str(result.reschedule_count),
                "dead_lettered_at": str(dead_lettered_at),
            },
        )
        await self._redis.expire(
            RedisKey.cp_dlq_meta(run_id),
            RedisTTL.DLQ_META,
        )
        dead_letter_event = RunDeadLetteredEvent(
            run_id=run_id,
            tenant_id=tenant_id,
            reason=reason,
            reschedule_count=result.reschedule_count,
        )
        await self._redis.xadd(
            self._config.dlq_stream,
            serialize_event(dead_letter_event),
            maxlen=10_000,
            approximate=True,
        )
        await decrement_tenant_inflight_if_needed(self._redis, run_id)
        logger.error(
            "DLQ: run=%s tenant=%s reason=%s attempts=%d",
            run_id,
            tenant_id,
            reason,
            result.reschedule_count,
        )
        return "dlq"

    # ------------------------------------------------------------------
    # Explicit operator DLQ replay — not automatic reconciliation
    # ------------------------------------------------------------------

    async def replay_dlq_run(self, run_id: str, requesting_tenant: str) -> None:
        """Re-enqueue a DLQ run through the existing explicit operator path."""

        meta = await self._redis.hgetall(RedisKey.cp_dlq_meta(run_id))
        if not meta:
            raise DLQEntryNotFoundError(f"DLQ entry not found: {run_id!r}")
        decoded = _decode_mapping(meta)
        dlq_tenant = decoded.get("tenant_id", "")
        if dlq_tenant != requesting_tenant:
            raise TenantMismatchError(
                f"DLQ run {run_id!r} belongs to tenant {dlq_tenant!r}, "
                f"not {requesting_tenant!r}"
            )

        agent_type = decoded.get("agent_type", "")
        raw_payload = await self._redis.get(RedisKey.run_payload(run_id))
        payload: dict = {}
        if raw_payload:
            try:
                payload = json.loads(_decode(raw_payload))
            except json.JSONDecodeError:
                pass

        # Existing explicit replay behavior is preserved. It remains an
        # operator command and is not invoked by the recovery sweep.
        replay_key = RedisKey.run_state(run_id)
        await self._redis.delete(replay_key)
        replay_ok = await transition_state(
            self._redis,
            run_id,
            "admitted",
            state_key=replay_key,
            state_ttl=RedisTTL.RUN_STATE,
            expected_state=None,
        )
        if not replay_ok:
            raise DLQEntryNotFoundError(
                f"DLQ run {run_id!r} could not be re-admitted"
            )

        await self._redis.hset(
            RedisKey.run_meta(run_id),
            "reschedule_count",
            "0",
        )
        event = RunAdmittedEvent(
            run_id=run_id,
            tenant_id=dlq_tenant,
            agent_type=agent_type,
            payload=payload,
        )
        await self._redis.xadd(
            self._config.control_stream,
            serialize_event(event),
            maxlen=RedisTTL.STREAM_MAXLEN,
            approximate=True,
        )
        await self._redis.delete(RedisKey.cp_dlq_meta(run_id))
        logger.info("DLQ replay: run=%s tenant=%s", run_id, dlq_tenant)

    async def dlq_depth(self) -> int:
        try:
            return await self._redis.xlen(self._config.dlq_stream)
        except Exception:
            return 0

    async def list_dlq(self, tenant_id: str, limit: int = 50) -> list[dict]:
        """Return retained DLQ projections, optionally tenant-filtered."""

        include_all = tenant_id == "__all__"
        scan_pattern = RedisKey.cp_dlq_meta("*")
        try:
            cursor = 0
            entries: list[dict] = []
            while True:
                cursor, keys = await self._redis.scan(
                    cursor,
                    match=scan_pattern,
                    count=100,
                )
                for key in keys:
                    decoded = _decode_mapping(await self._redis.hgetall(key))
                    if not decoded:
                        continue
                    if include_all or decoded.get("tenant_id", "") == tenant_id:
                        entries.append(
                            {
                                "run_id": decoded.get("run_id", ""),
                                "tenant_id": decoded.get("tenant_id", ""),
                                "reason": decoded.get("reason", ""),
                                "reschedule_count": _safe_int(
                                    decoded.get("reschedule_count"),
                                    0,
                                ),
                                "dead_lettered_at": float(
                                    decoded.get("dead_lettered_at") or 0
                                ),
                                "original_error": decoded.get("original_error", ""),
                                "cost_cents": _safe_int(decoded.get("cost_cents"), 0),
                            }
                        )
                    if len(entries) >= limit:
                        break
                if cursor == 0 or len(entries) >= limit:
                    break
            return entries
        except Exception as exc:
            logger.error("RecoveryService.list_dlq error: %s", exc)
            return []
