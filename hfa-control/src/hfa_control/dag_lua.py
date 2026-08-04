"""
hfa_control/dag_lua.py
----------------------
Lua CAS gateway with epoch fencing and Sprint 82 runtime-truth guards.
"""
from __future__ import annotations

import logging
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from hfa.config.keys import RedisKey, RedisTTL
from hfa.dag.schema import DagRedisKey
from hfa.dag.states import DagTaskState
from hfa.lua.loader import LuaScriptLoader
from hfa_control.task_claim_authority import (
    TaskClaimCanonicalProjectionInput,
    normalize_task_claim_canonical_projection_input,
)

logger = logging.getLogger(__name__)


def _env_allows_legacy_direct_claim() -> bool:
    value = os.getenv("HFA_ALLOW_LEGACY_DIRECT_TASK_CLAIM", "")
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _legacy_direct_claim_arg(allow_legacy_direct_claim: bool | None) -> str:
    if allow_legacy_direct_claim is None:
        return "1" if _env_allows_legacy_direct_claim() else "0"
    return "1" if allow_legacy_direct_claim else "0"


def _decode_text(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return str(value)


async def _maybe_await(value):
    if hasattr(value, "__await__"):
        return await value
    return value


async def _resolve_run_id_compat(redis, *, task_id: str, run_id: str | None) -> str:
    """Compatibility-only pre-read; Lua remains the identity authority."""
    explicit = str(run_id or "").strip()
    if explicit:
        return explicit
    hget = getattr(redis, "hget", None)
    if not callable(hget):
        return ""
    try:
        raw = await _maybe_await(hget(DagRedisKey.task_meta(task_id), "run_id"))
    except Exception:
        return ""
    return _decode_text(raw).strip()


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
class TaskAdmitResult:
    admitted: bool
    ready: bool
    task_id: str
    status: str = ""


@dataclass(frozen=True)
class TaskDispatchCommitResult:
    committed: bool
    status: str
    task_id: str
    reason: str = ""


TASK_CLAIM_STATUS_TASK_CLAIMED = "task_claimed"
TASK_CLAIM_STATUS_TASK_MISSING = "task_missing"
TASK_CLAIM_STATUS_TASK_ALREADY_OWNED = "task_already_owned"
TASK_CLAIM_STATUS_TASK_STATE_CONFLICT = "task_state_conflict"
TASK_CLAIM_STATUS_RESERVATION_MISSING = "reservation_missing"
TASK_CLAIM_STATUS_RESERVATION_WORKER_MISMATCH = "reservation_worker_mismatch"
TASK_CLAIM_STATUS_RESERVATION_TASK_MISMATCH = "reservation_task_mismatch"
TASK_CLAIM_STATUS_RESERVATION_EPOCH_MISMATCH = "reservation_epoch_mismatch"
TASK_CLAIM_STATUS_MISSING_TASK_META = "missing_task_meta"
TASK_CLAIM_STATUS_IDENTITY_TASK_ID_MISSING = "identity_task_id_missing"
TASK_CLAIM_STATUS_IDENTITY_TASK_ID_MISMATCH = "identity_task_id_mismatch"
TASK_CLAIM_STATUS_IDENTITY_RUN_ID_MISSING = "identity_run_id_missing"
TASK_CLAIM_STATUS_IDENTITY_RUN_ID_MISMATCH = "identity_run_id_mismatch"
TASK_CLAIM_STATUS_RUN_TRUTH_MISSING = "run_truth_missing"
TASK_CLAIM_STATUS_RUN_TRUTH_TERMINAL_CONFLICT = "run_truth_terminal_conflict"
TASK_CLAIM_STATUS_RUN_TRUTH_CORRUPTION_CONFLICT = "run_truth_corruption_conflict"
TASK_CLAIM_STATUS_TRUTH_CONFLICT_EVIDENCE_STORE_UNAVAILABLE = (
    "truth_conflict_evidence_store_unavailable"
)
TASK_CLAIM_STATUS_CANONICAL_ALREADY_PROJECTED = (
    "canonical_claim_already_projected"
)
TASK_CLAIM_STATUS_CANONICAL_PROJECTION_CONFLICT = (
    "canonical_projection_conflict"
)

TASK_CLAIM_SUCCESS_STATUSES: frozenset[str] = frozenset({
    TASK_CLAIM_STATUS_TASK_CLAIMED,
})

TASK_CLAIM_FAILURE_STATUSES: frozenset[str] = frozenset({
    TASK_CLAIM_STATUS_TASK_MISSING,
    TASK_CLAIM_STATUS_TASK_ALREADY_OWNED,
    TASK_CLAIM_STATUS_TASK_STATE_CONFLICT,
    TASK_CLAIM_STATUS_RESERVATION_MISSING,
    TASK_CLAIM_STATUS_RESERVATION_WORKER_MISMATCH,
    TASK_CLAIM_STATUS_RESERVATION_TASK_MISMATCH,
    TASK_CLAIM_STATUS_RESERVATION_EPOCH_MISMATCH,
    TASK_CLAIM_STATUS_MISSING_TASK_META,
    TASK_CLAIM_STATUS_IDENTITY_TASK_ID_MISSING,
    TASK_CLAIM_STATUS_IDENTITY_TASK_ID_MISMATCH,
    TASK_CLAIM_STATUS_IDENTITY_RUN_ID_MISSING,
    TASK_CLAIM_STATUS_IDENTITY_RUN_ID_MISMATCH,
    TASK_CLAIM_STATUS_RUN_TRUTH_MISSING,
    TASK_CLAIM_STATUS_RUN_TRUTH_TERMINAL_CONFLICT,
    TASK_CLAIM_STATUS_RUN_TRUTH_CORRUPTION_CONFLICT,
    TASK_CLAIM_STATUS_TRUTH_CONFLICT_EVIDENCE_STORE_UNAVAILABLE,
    TASK_CLAIM_STATUS_CANONICAL_ALREADY_PROJECTED,
    TASK_CLAIM_STATUS_CANONICAL_PROJECTION_CONFLICT,
})

TASK_CLAIM_STATUSES: frozenset[str] = (
    TASK_CLAIM_SUCCESS_STATUSES | TASK_CLAIM_FAILURE_STATUSES
)


@dataclass(frozen=True)
class TaskClaimResult:
    ok: bool
    status: str
    task_id: str
    worker_id: str
    claim_epoch: str = ""
    scheduler_epoch: str = ""

    @staticmethod
    def from_lua(raw: list, task_id: str, worker_id: str) -> "TaskClaimResult":
        def _decode(value) -> str:
            return value.decode() if isinstance(value, bytes) else (str(value) if value else "")

        status = _decode(raw[0]) if raw else "unknown"
        claim_epoch = _decode(raw[1]) if len(raw) > 1 else ""
        scheduler_epoch = _decode(raw[2]) if len(raw) > 2 else ""
        return TaskClaimResult(
            ok=status in TASK_CLAIM_SUCCESS_STATUSES,
            status=status,
            task_id=task_id,
            worker_id=worker_id,
            claim_epoch=claim_epoch,
            scheduler_epoch=scheduler_epoch,
        )


@dataclass(frozen=True)
class TaskCompleteResult:
    completed: bool
    status: str
    unlocked_count: int = 0
    already_terminal: bool = False

    @staticmethod
    def from_lua(raw: list) -> "TaskCompleteResult":
        completed = bool(int(raw[0]))
        status = raw[1].decode() if isinstance(raw[1], bytes) else str(raw[1])
        unlocked = int(raw[2])
        already_terminal = bool(int(raw[3]))
        return TaskCompleteResult(
            completed=completed,
            status=status,
            unlocked_count=unlocked,
            already_terminal=already_terminal,
        )


class DagLua:
    """Lua CAS gateway for DAG task state transitions."""

    def __init__(
        self,
        redis,
        *,
        canonical_task_admit_binding: bool = False,
        canonical_task_dispatch_binding: bool = False,
    ) -> None:
        self._redis = redis
        self._canonical_task_admit_binding_enabled = bool(
            canonical_task_admit_binding
        )
        self._canonical_task_dispatch_binding_enabled = bool(
            canonical_task_dispatch_binding
        )
        if (
            self._canonical_task_dispatch_binding_enabled
            and not self._canonical_task_admit_binding_enabled
        ):
            raise ValueError(
                "canonical TASK_DISPATCH binding requires "
                "canonical TASK_ADMIT binding"
            )
        self._task_admit_authority_binding = None
        self._task_dispatch_authority_binding = None
        self._admit_loader: Optional[LuaScriptLoader] = None
        self._dispatch_loader: Optional[LuaScriptLoader] = None
        self._claim_loader: Optional[LuaScriptLoader] = None
        self._complete_loader: Optional[LuaScriptLoader] = None
        self._requeue_loader: Optional[LuaScriptLoader] = None

    async def initialise(self) -> None:
        scripts = {
            "task_admit.lua": "_admit_loader",
            "task_dispatch_commit.lua": "_dispatch_loader",
            "task_claim_start.lua": "_claim_loader",
            "task_complete.lua": "_complete_loader",
            "task_requeue.lua": "_requeue_loader",
        }
        for filename, attr in scripts.items():
            loader = LuaScriptLoader(self._redis, _lua_path(filename))
            await loader.load()
            setattr(self, attr, loader)
            sha_preview = loader.sha[:8] if loader.sha else "fallback"
            logger.info("DagLua loaded %s sha=%s…", filename, sha_preview)

    async def _ensure_initialised(self) -> None:
        if self._admit_loader is None:
            await self.initialise()

    async def task_admit(self, seed) -> TaskAdmitResult:
        if self._canonical_task_admit_binding_enabled:
            if self._task_admit_authority_binding is None:
                from hfa_control.task_admit_authority import TaskAdmitAuthorityBinding

                self._task_admit_authority_binding = TaskAdmitAuthorityBinding(
                    self._redis, self._task_admit_canonical_projection
                )
            return await self._task_admit_authority_binding.admit(seed)
        return await self._task_admit_legacy(seed)

    async def _task_admit_legacy(self, seed) -> TaskAdmitResult:
        admitted_at = float(getattr(seed, "admitted_at", 0.0) or 0.0)
        return await self._task_admit_project(
            seed,
            priority_text=str(getattr(seed, "priority", 0)),
            admitted_at_text=str(admitted_at),
            dependency_count_text=str(getattr(seed, "dependency_count", 0)),
        )

    async def _task_admit_canonical_projection(self, seed) -> TaskAdmitResult:
        return await self._task_admit_project(
            seed,
            priority_text=str(seed.priority),
            admitted_at_text=str(seed.admitted_at),
            dependency_count_text=str(seed.dependency_count),
        )

    async def _task_admit_project(
        self,
        seed,
        *,
        priority_text: str,
        admitted_at_text: str,
        dependency_count_text: str,
    ) -> TaskAdmitResult:
        await self._ensure_initialised()
        assert self._admit_loader is not None

        task_id = seed.task_id
        run_id = seed.run_id
        tenant_id = seed.tenant_id
        child_ids = list(getattr(seed, "child_task_ids", ()) or ())
        keys = [
            DagRedisKey.task_state(task_id),
            DagRedisKey.task_meta(task_id),
            DagRedisKey.task_remaining_deps(task_id),
            DagRedisKey.task_children(task_id),
            DagRedisKey.task_ready_emitted(task_id),
            DagRedisKey.tenant_ready_queue(tenant_id),
            DagRedisKey.run_tasks(run_id),
            DagRedisKey.tenant_active_set(),
        ]
        args: list[str] = [
            task_id,
            run_id,
            tenant_id,
            getattr(seed, "agent_type", "") or "",
            priority_text,
            admitted_at_text,
            getattr(seed, "payload_json", "") or "",
            getattr(seed, "trace_parent", "") or "",
            getattr(seed, "trace_state", "") or "",
            dependency_count_text,
            str(int(getattr(RedisTTL, "RUN_STATE", 86400))),
            str(int(getattr(RedisTTL, "RUN_META", 86400))),
            str(int(getattr(RedisTTL, "RUN_META", 86400))),
            getattr(seed, "region", "") or "",
            getattr(seed, "policy", "") or "",
        ] + child_ids
        raw = await self._admit_loader.run(num_keys=len(keys), keys=keys, args=args)
        status = raw[0].decode() if isinstance(raw[0], bytes) else str(raw[0])
        ready = status == "seeded_root"
        admitted = status in ("seeded_root", "seeded_waiting", "already_exists")
        return TaskAdmitResult(admitted=admitted, ready=ready, task_id=task_id, status=status)

    async def task_dispatch_commit(
        self,
        dispatch,
    ) -> TaskDispatchCommitResult:
        if self._canonical_task_dispatch_binding_enabled:
            if self._task_dispatch_authority_binding is None:
                from hfa_control.task_dispatch_authority import (
                    TaskDispatchAuthorityBinding,
                )

                self._task_dispatch_authority_binding = (
                    TaskDispatchAuthorityBinding(
                        self._redis,
                        self._task_dispatch_canonical_projection,
                    )
                )
            return (
                await self._task_dispatch_authority_binding
                .dispatch(dispatch)
            )
        return await self._task_dispatch_legacy(dispatch)

    async def _task_dispatch_legacy(
        self,
        dispatch,
    ) -> TaskDispatchCommitResult:
        return await self._task_dispatch_project(dispatch)

    async def _task_dispatch_canonical_projection(
        self,
        dispatch,
    ) -> TaskDispatchCommitResult:
        return await self._task_dispatch_project(dispatch)

    async def _task_dispatch_project(
        self,
        dispatch,
    ) -> TaskDispatchCommitResult:
        await self._ensure_initialised()
        assert self._dispatch_loader is not None

        task_id = dispatch.task_id
        tenant_id = dispatch.tenant_id
        run_id = str(
            getattr(dispatch, "run_id", "") or ""
        ).strip()
        shard = getattr(dispatch, "shard", 0)
        scheduled_at = (
            getattr(dispatch, "scheduled_at", None)
            or int(time.time() * 1000)
        )
        scheduler_epoch = str(
            getattr(dispatch, "scheduler_epoch", "")
            or ""
        ).strip()
        scheduled_zset = (
            getattr(dispatch, "scheduled_zset", "")
            or DagRedisKey.task_scheduled_zset(
                tenant_id
            )
        )
        running_zset = (
            getattr(dispatch, "running_zset", "")
            or DagRedisKey.task_running_zset(
                tenant_id
            )
        )
        control_stream = (
            getattr(dispatch, "control_stream", "")
            or RedisKey.stream_control()
        )
        shard_stream = (
            getattr(dispatch, "shard_stream", "")
            or RedisKey.stream_shard(shard)
        )
        keys = [
            DagRedisKey.task_state(task_id),
            DagRedisKey.task_meta(task_id),
            scheduled_zset,
            control_stream,
            shard_stream,
            DagRedisKey.tenant_ready_queue(
                tenant_id
            ),
            running_zset,
            RedisKey.run_state(run_id),
            RedisKey.runtime_truth_conflict_index(),
            RedisKey.runtime_truth_conflict_stream(),
        ]
        args = [
            task_id,
            run_id,
            tenant_id,
            getattr(dispatch, "agent_type", "") or "",
            getattr(dispatch, "worker_group", "") or "",
            str(shard),
            str(getattr(dispatch, "priority", 0)),
            str(
                getattr(
                    dispatch,
                    "admitted_at",
                    0.0,
                )
                or 0.0
            ),
            str(scheduled_at),
            str(
                int(
                    getattr(
                        RedisTTL,
                        "RUN_STATE",
                        86400,
                    )
                )
            ),
            str(
                int(
                    getattr(
                        RedisTTL,
                        "RUN_META",
                        86400,
                    )
                )
            ),
            "10000",
            "10000",
            getattr(dispatch, "trace_parent", "") or "",
            getattr(dispatch, "trace_state", "") or "",
            (
                getattr(dispatch, "policy", "")
                or "LEAST_LOADED"
            ),
            getattr(dispatch, "region", "") or "",
            getattr(dispatch, "payload_json", "") or "{}",
            scheduler_epoch,
            str(getattr(dispatch, "attempt", 1)),
            getattr(dispatch, "worker_id", "") or "",
            (
                getattr(
                    dispatch,
                    "canonical_transition_id",
                    "",
                )
                or ""
            ),
            (
                getattr(
                    dispatch,
                    "canonical_record_hash",
                    "",
                )
                or ""
            ),
            (
                getattr(
                    dispatch,
                    "canonical_command_hash",
                    "",
                )
                or ""
            ),
            str(
                getattr(
                    dispatch,
                    "canonical_revision",
                    0,
                )
            ),
            (
                getattr(
                    dispatch,
                    "canonical_operation_id",
                    "",
                )
                or ""
            ),
        ]
        raw = await self._dispatch_loader.run(
            num_keys=len(keys),
            keys=keys,
            args=args,
        )
        status = (
            raw[0].decode()
            if isinstance(raw[0], bytes)
            else str(raw[0])
        )
        committed = status in {
            "committed",
            "already_projected",
        }
        reason = (
            raw[1].decode()
            if len(raw) > 1
            and isinstance(raw[1], bytes)
            else (
                str(raw[1])
                if len(raw) > 1
                else ""
            )
        )
        return TaskDispatchCommitResult(
            committed=committed,
            status=status,
            task_id=task_id,
            reason=reason,
        )

    async def task_claim_start(
        self,
        *,
        task_id: str,
        tenant_id: str,
        worker_instance_id: str,
        claimed_at_ms: int,
        scheduler_epoch: str = "",
        run_id: str = "",
        allow_legacy_direct_claim: bool | None = None,
    ) -> TaskClaimResult:
        return await self._task_claim_project(
            task_id=task_id,
            tenant_id=tenant_id,
            worker_instance_id=worker_instance_id,
            claimed_at_ms=claimed_at_ms,
            scheduler_epoch=scheduler_epoch,
            run_id=run_id,
            allow_legacy_direct_claim=allow_legacy_direct_claim,
            canonical_projection=None,
        )

    async def task_claim_canonical_projection(
        self,
        projection: TaskClaimCanonicalProjectionInput,
    ) -> TaskClaimResult:
        normalized = normalize_task_claim_canonical_projection_input(
            projection
        )
        return await self._task_claim_project(
            task_id=normalized.task_id,
            tenant_id=normalized.tenant_id,
            worker_instance_id=normalized.worker_instance_id,
            claimed_at_ms=normalized.claimed_at_ms,
            scheduler_epoch=normalized.scheduler_epoch,
            run_id=normalized.run_id,
            allow_legacy_direct_claim=False,
            canonical_projection=normalized,
        )

    async def _task_claim_project(
        self,
        *,
        task_id: str,
        tenant_id: str,
        worker_instance_id: str,
        claimed_at_ms: int,
        scheduler_epoch: str,
        run_id: str,
        allow_legacy_direct_claim: bool | None,
        canonical_projection: (
            TaskClaimCanonicalProjectionInput | None
        ),
    ) -> TaskClaimResult:
        await self._ensure_initialised()
        assert self._claim_loader is not None

        resolved_run_id = await _resolve_run_id_compat(
            self._redis,
            task_id=task_id,
            run_id=run_id,
        )
        keys = [
            DagRedisKey.task_state(task_id),
            DagRedisKey.task_meta(task_id),
            DagRedisKey.task_scheduled_zset(tenant_id),
            DagRedisKey.task_running_zset(tenant_id),
            DagRedisKey.worker_reservation(worker_instance_id),
            DagRedisKey.task_reservation_owner(task_id),
            RedisKey.run_state(resolved_run_id),
            RedisKey.runtime_truth_conflict_index(),
            RedisKey.runtime_truth_conflict_stream(),
        ]
        args = [
            task_id,
            worker_instance_id,
            str(claimed_at_ms),
            str(int(getattr(RedisTTL, "RUN_STATE", 86400))),
            str(int(getattr(RedisTTL, "RUN_META", 86400))),
            str(float(claimed_at_ms)),
            scheduler_epoch,
            _legacy_direct_claim_arg(allow_legacy_direct_claim),
            resolved_run_id,
        ]

        if canonical_projection is None:
            args.extend(
                [
                    "0",
                    "",
                    "",
                    "",
                    "",
                    "0",
                    "",
                    "0",
                    "0",
                    "",
                    "",
                    "",
                    "0",
                    "",
                    "0",
                ]
            )
        else:
            args.extend(
                [
                    "1",
                    canonical_projection.tenant_id,
                    canonical_projection.canonical_transition_id,
                    canonical_projection.canonical_record_hash,
                    canonical_projection.canonical_command_hash,
                    str(canonical_projection.canonical_revision),
                    canonical_projection.canonical_operation_id,
                    str(canonical_projection.previous_claim_epoch),
                    str(canonical_projection.claim_epoch),
                    canonical_projection.dispatch_transition_id,
                    canonical_projection.dispatch_record_hash,
                    canonical_projection.dispatch_command_hash,
                    str(canonical_projection.dispatch_revision),
                    canonical_projection.dispatch_operation_id,
                    str(canonical_projection.dispatch_attempt),
                ]
            )

        raw = await self._claim_loader.run(
            num_keys=len(keys),
            keys=keys,
            args=args,
        )
        return TaskClaimResult.from_lua(
            raw,
            task_id=task_id,
            worker_id=worker_instance_id,
        )

    async def task_complete(
        self,
        *,
        task_id: str,
        run_id: str | None = None,
        tenant_id: str,
        terminal_state: str,
        finished_at_ms: int,
        reason_code: str = "completed",
        worker_instance_id: str = "",
        output_data: str | None = None,
        expected_scheduler_epoch: str = "",
        expected_claim_epoch: str = "",
    ) -> TaskCompleteResult:
        await self._ensure_initialised()
        assert self._complete_loader is not None

        resolved_run_id = await _resolve_run_id_compat(
            self._redis, task_id=task_id, run_id=run_id
        )
        child_state_pfx = DagRedisKey.task_state_prefix()
        child_rem_pfx = DagRedisKey.task_remaining_deps_prefix()
        child_emitted_pfx = DagRedisKey.task_ready_emitted_prefix()
        keys = [
            DagRedisKey.task_state(task_id),
            DagRedisKey.task_meta(task_id),
            DagRedisKey.task_children(task_id),
            DagRedisKey.task_output(task_id),
            DagRedisKey.tenant_ready_queue(tenant_id),
            DagRedisKey.task_running_zset(tenant_id),
            RedisKey.run_state(resolved_run_id),
            RedisKey.runtime_truth_conflict_index(),
            RedisKey.runtime_truth_conflict_stream(),
        ]
        args = [
            task_id,
            resolved_run_id,
            tenant_id,
            terminal_state,
            str(finished_at_ms),
            str(int(getattr(RedisTTL, "RUN_STATE", 86400))),
            str(int(getattr(RedisTTL, "RUN_META", 86400))),
            str(int(getattr(RedisTTL, "TASK_OUTPUT", 86400))),
            str(float(finished_at_ms)),
            reason_code,
            worker_instance_id or "",
            output_data or "",
            child_state_pfx,
            ":state",
            child_rem_pfx,
            ":remaining_deps",
            child_emitted_pfx,
            ":ready_emitted",
            expected_scheduler_epoch,
            expected_claim_epoch,
        ]
        raw = await self._complete_loader.run(num_keys=len(keys), keys=keys, args=args)
        return TaskCompleteResult.from_lua(raw)

    async def claim_task(
        self,
        *,
        task_id: str,
        worker_instance_id: str,
        claimed_at_ms: int,
        tenant_id: str = "",
        scheduler_epoch: str = "",
        run_id: str = "",
        allow_legacy_direct_claim: bool | None = None,
    ) -> TaskClaimResult:
        return await self.task_claim_start(
            task_id=task_id,
            tenant_id=tenant_id,
            worker_instance_id=worker_instance_id,
            claimed_at_ms=claimed_at_ms,
            scheduler_epoch=scheduler_epoch,
            run_id=run_id,
            allow_legacy_direct_claim=allow_legacy_direct_claim,
        )
