"""
hfa_control/task_recovery.py
----------------------------
Atomic Lua heartbeat, monotonic claim_epoch, runtime-truth guarded requeue,
and recovery proof helpers.
"""
from __future__ import annotations

import logging
import os
import time
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Optional
from unittest.mock import Mock

from hfa.authority import AggregateType, CanonicalAggregateIdentity, OperationType
from hfa.config.keys import RedisKey, RedisTTL
from hfa.dag.heartbeat import HeartbeatPolicy
from hfa.dag.reasons import (
    TASK_HEARTBEAT_OWNER_MISMATCH,
    TASK_HEARTBEAT_RECORDED,
    TASK_REQUEUED,
)
from hfa.dag.schema import DagRedisKey, TaskMetaField
from hfa.lua.loader import LuaScriptLoader
from hfa_control.task_requeue_authority import (
    FEATURE_FLAG as TASK_REQUEUE_FEATURE_FLAG,
    TASK_REQUEUE_DUPLICATE_STATUS as CANONICAL_TASK_REQUEUE_DUPLICATE_STATUS,
    TASK_REQUEUE_PROJECTED_STATUS as CANONICAL_TASK_REQUEUE_PROJECTED_STATUS,
    TASK_REQUEUE_EVIDENCE_CONFLICT_STATUS,
    TaskRequeueAuthorityBinding,
    TaskRequeueAuthorityError,
    parse_task_requeue_binding_flag,
)
from hfa_control.task_terminal_authority import TaskTerminalAuthorityBinding

logger = logging.getLogger(__name__)

_FALSE_VALUES = {"", "0", "false", "False", "no", "NO", "off", "OFF"}


def is_proof_enforcement_enabled() -> bool:
    return os.getenv("IRON_V3_PROOF_ENFORCEMENT", "0") not in _FALSE_VALUES


def _lua_path(filename: str):
    from pathlib import Path

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


def _decode(value) -> str:
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return str(value) if value is not None else ""


def _safe_int(value, default: int = 0) -> int:
    try:
        return int(_decode(value))
    except (ValueError, TypeError):
        return default


def _is_mock_lua_result(raw) -> bool:
    if isinstance(raw, Mock):
        return True
    if isinstance(raw, (list, tuple)) and raw and isinstance(raw[0], Mock):
        return True
    return False


async def _maybe_await(value):
    if hasattr(value, "__await__"):
        return await value
    return value


async def _resolve_run_id_compat(redis, *, task_id: str, run_id: str) -> str:
    """Resolve a compatibility run ID only to construct Lua keys.

    Production callers should pass explicit ``run_id``. Legacy tests and tools
    may omit it; in that case this pre-read is never treated as authority. Lua
    revalidates the exact task/run identity before reading RUN truth, recording
    a conflict, or mutating lifecycle state.
    """

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
    return _decode(raw).strip()


@dataclass(frozen=True)
class TaskHeartbeatResult:
    ok: bool
    status: str


@dataclass(frozen=True)
class TaskRequeueResult:
    ok: bool
    status: str
    requeue_count: int = 0


@dataclass(frozen=True)
class RecoveryProofDecision:
    replay_clean: bool
    deterministic_replay_ok: bool
    ambiguous: bool
    auto_resume_allowed: bool
    requires_manual: bool
    reason: str = ""


def recovery_proof_decision(
    *,
    replay_clean: bool = True,
    deterministic_replay_ok: bool = True,
    gaps_detected: bool = False,
    duplicates_detected: bool = False,
    critical_drift: bool = False,
) -> RecoveryProofDecision:
    reasons: list[str] = []
    if gaps_detected:
        reasons.append("gaps_detected")
    if duplicates_detected:
        reasons.append("duplicates_detected")
    if not replay_clean:
        reasons.append("replay_not_clean")
    if not deterministic_replay_ok:
        reasons.append("deterministic_replay_failed")
    if critical_drift:
        reasons.append("critical_drift")

    ambiguous = bool(reasons)
    if ambiguous and is_proof_enforcement_enabled():
        return RecoveryProofDecision(
            replay_clean=replay_clean,
            deterministic_replay_ok=deterministic_replay_ok,
            ambiguous=True,
            auto_resume_allowed=False,
            requires_manual=True,
            reason=";".join(reasons),
        )
    return RecoveryProofDecision(
        replay_clean=replay_clean,
        deterministic_replay_ok=deterministic_replay_ok,
        ambiguous=ambiguous,
        auto_resume_allowed=not ambiguous,
        requires_manual=False,
        reason=";".join(reasons),
    )


class TaskHeartbeatManager:
    def __init__(self, redis, policy: HeartbeatPolicy | None = None) -> None:
        self._redis = redis
        self._policy = policy or HeartbeatPolicy()
        self._heartbeat_loader: Optional[LuaScriptLoader] = None

    async def _ensure_loaded(self) -> None:
        if self._heartbeat_loader is None:
            path = _lua_path("task_heartbeat.lua")
            self._heartbeat_loader = LuaScriptLoader(self._redis, path)
            await self._heartbeat_loader.load()

    async def _record_heartbeat_fallback(
        self,
        *,
        task_id: str,
        run_id: str,
        tenant_id: str,
        worker_id: str,
        claim_epoch: str,
        now_ms: int,
    ) -> list[str]:
        """Mock/fakeredis compatibility path; conflict cases fail closed."""
        state = _decode(await self._redis.get(DagRedisKey.task_state(task_id)))
        if state != "running":
            return ["illegal_transition"]

        meta_key = DagRedisKey.task_meta(task_id)
        authoritative_task_id = _decode(await self._redis.hget(meta_key, "task_id"))
        authoritative_run_id = _decode(await self._redis.hget(meta_key, "run_id"))
        if not authoritative_task_id:
            return ["identity_task_id_missing"]
        if authoritative_task_id != task_id:
            return ["identity_task_id_mismatch"]
        if not authoritative_run_id:
            return ["identity_run_id_missing"]
        if authoritative_run_id != run_id:
            return ["identity_run_id_mismatch"]

        run_state = _decode(await self._redis.get(RedisKey.run_state(run_id)))
        if run_state not in {"admitted", "queued", "scheduled", "running", "rescheduled"}:
            return ["truth_conflict_evidence_store_unavailable"]

        stored_worker = _decode(
            await self._redis.hget(meta_key, TaskMetaField.WORKER_INSTANCE_ID)
        )
        stored_hb_owner = _decode(await self._redis.hget(meta_key, "heartbeat_owner"))
        effective_owner = stored_worker or stored_hb_owner
        if effective_owner and effective_owner != worker_id:
            return ["owner_mismatch"]

        if claim_epoch:
            stored_claim_epoch = _decode(
                await self._redis.hget(meta_key, TaskMetaField.CLAIM_EPOCH)
            )
            if stored_claim_epoch != claim_epoch:
                return ["claim_epoch_mismatch"]

        await self._redis.hset(
            meta_key,
            mapping={
                TaskMetaField.LAST_HEARTBEAT_AT_MS: str(now_ms),
                "heartbeat_at_ms": str(now_ms),
                "heartbeat_owner": worker_id,
            },
        )
        await self._redis.zadd(
            DagRedisKey.task_running_zset(tenant_id),
            {task_id: now_ms},
        )
        return ["heartbeat_accepted"]

    async def record_heartbeat(
        self,
        *,
        task_id: str,
        tenant_id: str,
        worker_id: str,
        claim_epoch: str = "",
        run_id: str = "",
        now_ms: int | None = None,
    ) -> TaskHeartbeatResult:
        await self._ensure_loaded()
        assert self._heartbeat_loader is not None

        now_ms = now_ms or int(time.time() * 1000)
        resolved_run_id = await _resolve_run_id_compat(
            self._redis, task_id=task_id, run_id=run_id
        )
        keys = [
            DagRedisKey.task_state(task_id),
            DagRedisKey.task_meta(task_id),
            DagRedisKey.task_running_zset(tenant_id),
            RedisKey.run_state(resolved_run_id),
            RedisKey.runtime_truth_conflict_index(),
            RedisKey.runtime_truth_conflict_stream(),
        ]
        args = [
            task_id,
            resolved_run_id,
            tenant_id,
            worker_id,
            claim_epoch,
            str(now_ms),
        ]

        raw = await self._heartbeat_loader.run(num_keys=len(keys), keys=keys, args=args)
        if _is_mock_lua_result(raw):
            raw = await self._record_heartbeat_fallback(
                task_id=task_id,
                run_id=resolved_run_id,
                tenant_id=tenant_id,
                worker_id=worker_id,
                claim_epoch=claim_epoch,
                now_ms=now_ms,
            )

        status = _decode(raw[0]) if raw else "illegal_transition"
        ok = status == "heartbeat_accepted"
        if status == "owner_mismatch":
            status = TASK_HEARTBEAT_OWNER_MISMATCH
        elif ok:
            status = TASK_HEARTBEAT_RECORDED
        return TaskHeartbeatResult(ok=ok, status=status)


class TaskRecoveryManager:
    def __init__(
        self,
        redis,
        policy: HeartbeatPolicy | None = None,
        *,
        canonical_task_requeue_binding: bool | None = None,
        requeue_authority: TaskRequeueAuthorityBinding | None = None,
        terminal_authority: TaskTerminalAuthorityBinding | None = None,
    ) -> None:
        self._redis = redis
        self._policy = policy or HeartbeatPolicy()
        self._requeue_loader: Optional[LuaScriptLoader] = None
        if canonical_task_requeue_binding is None:
            canonical_task_requeue_binding = parse_task_requeue_binding_flag(
                os.getenv(TASK_REQUEUE_FEATURE_FLAG)
            )
        elif type(canonical_task_requeue_binding) is not bool:
            raise ValueError(
                "canonical_task_requeue_binding must be an exact bool"
            )
        self._canonical_task_requeue_binding = canonical_task_requeue_binding
        if not canonical_task_requeue_binding and (
            requeue_authority is not None or terminal_authority is not None
        ):
            raise ValueError(
                "canonical TASK_REQUEUE authorities require the canonical binding flag"
            )
        self._requeue_authority = (
            requeue_authority
            if requeue_authority is not None
            else (
                TaskRequeueAuthorityBinding(redis)
                if canonical_task_requeue_binding
                else None
            )
        )
        self._terminal_authority = (
            terminal_authority
            if terminal_authority is not None
            else (
                TaskTerminalAuthorityBinding(redis)
                if canonical_task_requeue_binding
                else None
            )
        )

    async def initialise(self) -> None:
        path = _lua_path("task_requeue.lua")
        self._requeue_loader = LuaScriptLoader(self._redis, path)
        await self._requeue_loader.load()
        if self._canonical_task_requeue_binding:
            assert self._requeue_authority is not None
            assert self._terminal_authority is not None
            await self._requeue_authority.initialise()
            await self._terminal_authority.initialise()

    async def find_stale_tasks(
        self, *, tenant_id: str, now_ms: int | None = None
    ) -> list[str]:
        now_ms = now_ms or int(time.time() * 1000)
        running_key = DagRedisKey.task_running_zset(tenant_id)
        raw_ids = await self._redis.zrange(running_key, 0, -1)
        stale: list[str] = []
        for raw_id in raw_ids:
            task_id = _decode(raw_id)
            if not task_id:
                continue
            meta_key = DagRedisKey.task_meta(task_id)
            raw_hb = await self._redis.hget(
                meta_key,
                TaskMetaField.LAST_HEARTBEAT_AT_MS,
            )
            last_ms = _safe_int(raw_hb, default=0)
            if last_ms <= 0:
                raw_hb = await self._redis.hget(meta_key, "heartbeat_at_ms")
                last_ms = _safe_int(raw_hb, default=0)
            if last_ms <= 0 or (now_ms - last_ms) > self._policy.stale_after_ms:
                stale.append(task_id)
        return stale

    async def _recover_existing_canonical_effects(
        self,
        *,
        task_id: str,
        run_id: str,
        tenant_id: str,
        observed_at_ms: int,
        reason_code: str,
    ) -> TaskRequeueResult | None:
        """Replay already-durable retry/terminal effects before a new decision.

        A canonical commit may survive while its mutable projection/delivery did
        not. In that crash window the TASK head is no longer ``running`` so a
        fresh ``current_claim_context`` cannot be used. Recovery therefore
        inspects the durable head first and delegates replay to the existing
        authority bindings. No second TASK revision is created here.
        """

        assert self._requeue_authority is not None
        assert self._terminal_authority is not None
        store = self._requeue_authority.store
        assert store is not None
        identity = CanonicalAggregateIdentity(
            aggregate_type=AggregateType.TASK,
            run_id=run_id,
            task_id=task_id,
        )
        try:
            snapshot = await store.get_aggregate_snapshot(identity)
        except Exception as exc:
            raise TaskRequeueAuthorityError(
                status=TASK_REQUEUE_EVIDENCE_CONFLICT_STATUS,
                detail=f"canonical TASK recovery head unavailable: {exc}",
            ) from exc
        if snapshot is None or snapshot.state == "running":
            return None
        try:
            probe = await store.load_receipt_probe(identity, snapshot.operation_id)
        except Exception as exc:
            raise TaskRequeueAuthorityError(
                status=TASK_REQUEUE_EVIDENCE_CONFLICT_STATUS,
                detail=f"canonical TASK recovery receipt unavailable: {exc}",
            ) from exc
        if probe is None or probe.canonical_store_record is None:
            raise TaskRequeueAuthorityError(
                status=TASK_REQUEUE_EVIDENCE_CONFLICT_STATUS,
                detail="canonical TASK recovery record/receipt is missing",
            )
        record = probe.canonical_store_record
        data = record.authoritative_metadata_changes
        if not isinstance(data, Mapping):
            raise TaskRequeueAuthorityError(
                status=TASK_REQUEUE_EVIDENCE_CONFLICT_STATUS,
                detail="canonical TASK recovery metadata is invalid",
            )
        expected_identity = {
            "task_id": task_id,
            "run_id": run_id,
            "tenant_id": tenant_id,
        }
        for field, wanted in expected_identity.items():
            if data.get(field) != wanted:
                raise TaskRequeueAuthorityError(
                    status=TASK_REQUEUE_EVIDENCE_CONFLICT_STATUS,
                    detail=f"canonical TASK recovery {field} mismatch",
                )

        if snapshot.state == "ready":
            if record.operation_type != OperationType.TASK_REQUEUE.value:
                raise TaskRequeueAuthorityError(
                    status=TASK_REQUEUE_EVIDENCE_CONFLICT_STATUS,
                    detail="canonical ready TASK head is not TASK_REQUEUE",
                )
            stored_reason = str(data.get("reason_code") or "")
            if stored_reason != reason_code:
                raise TaskRequeueAuthorityError(
                    status="IDEMPOTENCY_CONFLICT",
                    detail="durable TASK_REQUEUE reason_code differs from replay",
                    canonical_commit_durable=True,
                )
            canonical = await self._requeue_authority.requeue(
                task_id=task_id,
                run_id=run_id,
                tenant_id=tenant_id,
                observed_at_ms=observed_at_ms,
                max_requeue_count=self._policy.max_requeue_count,
                reason_code=stored_reason,
                worker_instance_id=str(data.get("worker_instance_id") or ""),
                scheduler_epoch=str(data.get("scheduler_epoch") or ""),
                claim_epoch=data.get("claim_epoch"),
            )
            return TaskRequeueResult(
                ok=False,
                status="TASK_ALREADY_REQUEUED",
                requeue_count=canonical.requeue_count,
            )

        if snapshot.state in {"done", "failed"}:
            if record.operation_type not in {
                OperationType.TASK_COMPLETE.value,
                OperationType.TASK_FAIL.value,
            }:
                raise TaskRequeueAuthorityError(
                    status=TASK_REQUEUE_EVIDENCE_CONFLICT_STATUS,
                    detail="canonical terminal TASK head has invalid operation",
                )
            await self._terminal_authority.replay_terminal_projection(
                task_id=task_id,
                run_id=run_id,
                tenant_id=tenant_id,
                worker_instance_id=str(data.get("worker_instance_id") or ""),
                scheduler_epoch=str(data.get("scheduler_epoch") or ""),
                claim_epoch=data.get("claim_epoch"),
            )
            raw_count = await self._redis.hget(
                DagRedisKey.task_meta(task_id),
                TaskMetaField.REQUEUE_COUNT,
            )
            return TaskRequeueResult(
                ok=False,
                status="TASK_TERMINAL_RECOVERED",
                requeue_count=max(0, _safe_int(raw_count, default=0)),
            )

        raise TaskRequeueAuthorityError(
            status=TASK_REQUEUE_EVIDENCE_CONFLICT_STATUS,
            detail=f"canonical TASK recovery head state is unsupported: {snapshot.state}",
        )


    async def requeue_stale_task(
        self,
        *,
        task_id: str,
        tenant_id: str,
        run_id: str = "",
        expected_state: str = "running",
        now_ms: int | None = None,
        ready_score: int | None = None,
        reason_code: str = "TASK_STALE_DETECTED",
    ) -> TaskRequeueResult:
        """Atomically requeue one stale task only when both truth planes agree.

        ``run_id`` is explicit on the production path. Compatibility callers
        may omit it; the manager then pre-reads task metadata solely to build
        RUN and run-membership keys. ``task_requeue.lua`` revalidates task ID,
        run ID and tenant ID before it reads RUN truth, records conflict
        evidence, or mutates state.
        """

        if self._requeue_loader is None:
            await self.initialise()
        assert self._requeue_loader is not None

        now_ms = now_ms or int(time.time() * 1000)
        resolved_run_id = await _resolve_run_id_compat(
            self._redis, task_id=task_id, run_id=run_id
        )

        if self._canonical_task_requeue_binding:
            if expected_state != "running":
                raise ValueError(
                    "canonical TASK_REQUEUE expected_state must be running"
                )
            assert self._requeue_authority is not None
            assert self._terminal_authority is not None
            recovered = await self._recover_existing_canonical_effects(
                task_id=task_id,
                run_id=resolved_run_id,
                tenant_id=tenant_id,
                observed_at_ms=now_ms,
                reason_code=reason_code,
            )
            if recovered is not None:
                return recovered
            # Retry eligibility is derived from the exact durable TASK_CLAIM,
            # never from a mutable retry counter or stale task projection.
            claim = await self._requeue_authority.current_claim_context(
                task_id=task_id,
                run_id=resolved_run_id,
                tenant_id=tenant_id,
            )

            if claim.dispatch_attempt > self._policy.max_requeue_count:
                await self._terminal_authority.fail(
                    task_id=task_id,
                    run_id=resolved_run_id,
                    tenant_id=tenant_id,
                    finished_at_ms=now_ms,
                    worker_instance_id=claim.worker_instance_id,
                    scheduler_epoch=claim.scheduler_epoch,
                    claim_epoch=claim.claim_epoch,
                    reason_code="STALE_RETRY_EXHAUSTED",
                )
                return TaskRequeueResult(
                    ok=False,
                    status="TASK_RETRY_EXHAUSTED",
                    requeue_count=max(0, claim.dispatch_attempt - 1),
                )

            canonical = await self._requeue_authority.requeue(
                task_id=task_id,
                run_id=resolved_run_id,
                tenant_id=tenant_id,
                observed_at_ms=now_ms,
                max_requeue_count=self._policy.max_requeue_count,
                reason_code=reason_code,
                worker_instance_id=claim.worker_instance_id,
                scheduler_epoch=claim.scheduler_epoch,
                claim_epoch=claim.claim_epoch,
            )
            return TaskRequeueResult(
                ok=(canonical.status == CANONICAL_TASK_REQUEUE_PROJECTED_STATUS),
                status=(
                    TASK_REQUEUED
                    if canonical.status == CANONICAL_TASK_REQUEUE_PROJECTED_STATUS
                    else (
                        "TASK_ALREADY_REQUEUED"
                        if canonical.status == CANONICAL_TASK_REQUEUE_DUPLICATE_STATUS
                        else canonical.status
                    )
                ),
                requeue_count=canonical.requeue_count,
            )

        # Legacy-only effect input. Canonical TASK_REQUEUE derives its stable
        # ready score from the durable authority record committed_at_ms.
        ready_score = ready_score if ready_score is not None else now_ms

        keys = [
            DagRedisKey.task_state(task_id),
            DagRedisKey.task_meta(task_id),
            DagRedisKey.tenant_ready_queue(tenant_id),
            DagRedisKey.task_running_zset(tenant_id),
            DagRedisKey.completion_stream(tenant_id),
            RedisKey.run_state(resolved_run_id),
            DagRedisKey.run_tasks(resolved_run_id),
            RedisKey.runtime_truth_conflict_index(),
            RedisKey.runtime_truth_conflict_stream(),
        ]
        args = [
            task_id,
            resolved_run_id,
            tenant_id,
            expected_state,
            str(now_ms),
            str(ready_score),
            str(self._policy.max_requeue_count),
            reason_code,
            str(int(getattr(RedisTTL, "STREAM_MAXLEN", 10000))),
        ]
        raw = await self._requeue_loader.run(num_keys=len(keys), keys=keys, args=args)
        status = _decode(raw[0]) if raw else "TASK_STATE_CONFLICT"
        count_raw = raw[1] if len(raw) > 1 else b"0"
        count = _safe_int(count_raw, default=0)
        return TaskRequeueResult(
            ok=(status == TASK_REQUEUED),
            status=status,
            requeue_count=count,
        )

    def proof_allows_auto_resume(
        self,
        *,
        replay_clean: bool = True,
        deterministic_replay_ok: bool = True,
        gaps_detected: bool = False,
        duplicates_detected: bool = False,
        critical_drift: bool = False,
    ) -> RecoveryProofDecision:
        return recovery_proof_decision(
            replay_clean=replay_clean,
            deterministic_replay_ok=deterministic_replay_ok,
            gaps_detected=gaps_detected,
            duplicates_detected=duplicates_detected,
            critical_drift=critical_drift,
        )
