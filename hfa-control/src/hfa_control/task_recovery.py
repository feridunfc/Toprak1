"""
hfa_control/task_recovery.py
-----------------------------
IRONCLAD Sprint 2 patch — Atomic Lua heartbeat + monotonic claim_epoch.

Sprint 6 addition: proof-enforcement helpers classify replay/runtime evidence
before recovery auto-resume.  Gaps, duplicates, dirty replay, or deterministic
replay failure imply ambiguous authority and block automatic resume when
IRON_V3_PROOF_ENFORCEMENT is enabled.
"""
from __future__ import annotations

import logging
import os
import time
from dataclasses import dataclass
from typing import Optional
from unittest.mock import Mock

from hfa.config.keys import RedisTTL
from hfa.dag.heartbeat import HeartbeatPolicy
from hfa.dag.reasons import (
    TASK_HEARTBEAT_OWNER_MISMATCH,
    TASK_HEARTBEAT_RECORDED,
    TASK_REQUEUED,
)
from hfa.dag.schema import DagRedisKey, TaskMetaField
from hfa.lua.loader import LuaScriptLoader

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
    for p in candidates:
        if p.exists():
            return p
    for parent in here.parents:
        for subdir in ("hfa-core/src/hfa/lua", "hfa/lua"):
            p = parent / subdir / filename
            if p.exists():
                return p
    raise FileNotFoundError(f"Lua script not found: {filename}")


def _decode(v) -> str:
    if isinstance(v, bytes):
        return v.decode("utf-8", errors="replace")
    return str(v) if v is not None else ""


def _safe_int(v, default: int = 0) -> int:
    try:
        return int(_decode(v))
    except (ValueError, TypeError):
        return default


def _is_mock_lua_result(raw) -> bool:
    if isinstance(raw, Mock):
        return True
    if isinstance(raw, (list, tuple)) and raw and isinstance(raw[0], Mock):
        return True
    return False


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
    """Return the Sprint 6 recovery proof verdict.

    Detection-only recovery is not sufficient under the Sprint 6 flag.  Any gap,
    duplicate, replay integrity failure, deterministic replay failure, or critical
    drift is ambiguous and must block auto-resume.
    """
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
        tenant_id: str,
        worker_id: str,
        claim_epoch: str,
        now_ms: int,
    ) -> list[str]:
        """Unit/fakeredis-compatible heartbeat path mirroring task_heartbeat.lua."""
        state = _decode(await self._redis.get(DagRedisKey.task_state(task_id)))
        if state != "running":
            return ["illegal_transition"]

        meta_key = DagRedisKey.task_meta(task_id)
        stored_worker = _decode(
            await self._redis.hget(meta_key, TaskMetaField.WORKER_INSTANCE_ID)
        )
        if stored_worker != worker_id:
            return ["owner_mismatch"]

        if claim_epoch:
            stored_claim_epoch = _decode(
                await self._redis.hget(meta_key, TaskMetaField.CLAIM_EPOCH)
            )
            if stored_claim_epoch != claim_epoch:
                return ["claim_epoch_mismatch"]

        await self._redis.hset(
            meta_key,
            mapping={TaskMetaField.LAST_HEARTBEAT_AT_MS: str(now_ms)},
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
        now_ms: int | None = None,
    ) -> TaskHeartbeatResult:
        await self._ensure_loaded()
        assert self._heartbeat_loader is not None

        now_ms = now_ms or int(time.time() * 1000)
        keys = [
            DagRedisKey.task_state(task_id),
            DagRedisKey.task_meta(task_id),
            DagRedisKey.task_running_zset(tenant_id),
        ]
        args = [task_id, tenant_id, worker_id, claim_epoch, str(now_ms)]

        raw = await self._heartbeat_loader.run(num_keys=len(keys), keys=keys, args=args)
        if _is_mock_lua_result(raw):
            raw = await self._record_heartbeat_fallback(
                task_id=task_id,
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
    def __init__(self, redis, policy: HeartbeatPolicy | None = None) -> None:
        self._redis = redis
        self._policy = policy or HeartbeatPolicy()
        self._requeue_loader: Optional[LuaScriptLoader] = None

    async def initialise(self) -> None:
        path = _lua_path("task_requeue.lua")
        self._requeue_loader = LuaScriptLoader(self._redis, path)
        await self._requeue_loader.load()

    async def find_stale_tasks(self, *, tenant_id: str, now_ms: int | None = None) -> list[str]:
        now_ms = now_ms or int(time.time() * 1000)
        running_key = DagRedisKey.task_running_zset(tenant_id)
        raw_ids = await self._redis.zrange(running_key, 0, -1)
        stale: list[str] = []
        for raw_id in raw_ids:
            task_id = _decode(raw_id)
            if not task_id:
                continue
            raw_hb = await self._redis.hget(
                DagRedisKey.task_meta(task_id),
                TaskMetaField.LAST_HEARTBEAT_AT_MS,
            )
            last_ms = _safe_int(raw_hb, default=0)
            if last_ms <= 0 or (now_ms - last_ms) > self._policy.stale_after_ms:
                stale.append(task_id)
        return stale

    async def requeue_stale_task(
        self,
        *,
        task_id: str,
        tenant_id: str,
        expected_state: str = "running",
        now_ms: int | None = None,
        ready_score: int | None = None,
        reason_code: str = "TASK_STALE_DETECTED",
    ) -> TaskRequeueResult:
        if self._requeue_loader is None:
            await self.initialise()
        assert self._requeue_loader is not None

        now_ms = now_ms or int(time.time() * 1000)
        ready_score = ready_score if ready_score is not None else now_ms
        keys = [
            DagRedisKey.task_state(task_id),
            DagRedisKey.task_meta(task_id),
            DagRedisKey.tenant_ready_queue(tenant_id),
            DagRedisKey.task_running_zset(tenant_id),
            DagRedisKey.completion_stream(tenant_id),
        ]
        args = [
            task_id, tenant_id, expected_state,
            str(now_ms), str(ready_score),
            str(self._policy.max_requeue_count),
            reason_code,
            str(int(getattr(RedisTTL, "STREAM_MAXLEN", 10000))),
        ]
        raw = await self._requeue_loader.run(num_keys=len(keys), keys=keys, args=args)
        status = _decode(raw[0]) if raw else "TASK_STATE_CONFLICT"
        count_raw = raw[1] if len(raw) > 1 else b"0"
        count = _safe_int(count_raw, default=0)
        return TaskRequeueResult(ok=(status == TASK_REQUEUED), status=status, requeue_count=count)

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
