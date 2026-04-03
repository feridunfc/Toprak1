"""
hfa_control/task_recovery.py
-----------------------------
IRONCLAD Sprint 2 patch — Atomic Lua heartbeat + monotonic claim_epoch.

Patch changes vs Sprint 2 base:
  - TaskHeartbeatManager now loads task_heartbeat.lua and executes via EVALSHA.
    The Python read-compare-write race is gone.
  - record_heartbeat() accepts claim_epoch and passes it to the Lua fence.
  - task_requeue.lua no longer resets claim_epoch (fixed in Lua side).
    TaskRecoveryManager is unchanged — it calls the same Lua with no code change.
"""
from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import Optional

from hfa.config.keys import RedisTTL
from hfa.dag.heartbeat import HeartbeatPolicy
from hfa.dag.reasons import (
    TASK_HEARTBEAT_OWNER_MISMATCH,
    TASK_HEARTBEAT_RECORDED,
    TASK_REQUEUED,
)
from hfa.dag.schema import DagRedisKey, TaskMetaField
from hfa.dag.states import HEARTBEAT_ALLOWED_STATES
from hfa.lua.loader import LuaScriptLoader

logger = logging.getLogger(__name__)


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


# ── Result types ──────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class TaskHeartbeatResult:
    ok: bool
    status: str
    # status values:
    #   heartbeat_accepted
    #   illegal_transition
    #   owner_mismatch
    #   claim_epoch_mismatch


@dataclass(frozen=True)
class TaskRequeueResult:
    ok: bool
    status: str
    requeue_count: int = 0


# ── Heartbeat manager ─────────────────────────────────────────────────────────

class TaskHeartbeatManager:
    """
    Atomic fenced heartbeat via task_heartbeat.lua.

    Sprint 2 patch: replaces Python read-compare-write with a single Lua CAS
    that atomically checks state + owner + claim_epoch before writing.
    A zombie worker's heartbeat is now deterministically rejected even under
    concurrent re-claim races.
    """

    def __init__(self, redis, policy: HeartbeatPolicy | None = None) -> None:
        self._redis = redis
        self._policy = policy or HeartbeatPolicy()
        self._heartbeat_loader: Optional[LuaScriptLoader] = None

    async def _ensure_loaded(self) -> None:
        if self._heartbeat_loader is None:
            path = _lua_path("task_heartbeat.lua")
            self._heartbeat_loader = LuaScriptLoader(self._redis, path)
            await self._heartbeat_loader.load()

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
        args = [
            task_id,
            tenant_id,
            worker_id,
            claim_epoch,
            str(now_ms),
        ]

        raw = await self._heartbeat_loader.run(num_keys=len(keys), keys=keys, args=args)
        status = _decode(raw[0]) if raw else "illegal_transition"
        ok = status == "heartbeat_accepted"

        # Map Lua status to legacy constants for backward compat
        if status == "owner_mismatch":
            status = TASK_HEARTBEAT_OWNER_MISMATCH
        elif ok:
            status = TASK_HEARTBEAT_RECORDED

        return TaskHeartbeatResult(ok=ok, status=status)


# ── Recovery manager ──────────────────────────────────────────────────────────

class TaskRecoveryManager:
    """
    Sweeps task_running_zset for stale tasks and requeues via task_requeue.lua.

    Sprint 2 patch: task_requeue.lua now preserves claim_epoch (monotonic).
    Python side is unchanged — same call signature, same behavior.
    """

    def __init__(self, redis, policy: HeartbeatPolicy | None = None) -> None:
        self._redis = redis
        self._policy = policy or HeartbeatPolicy()
        self._requeue_loader: Optional[LuaScriptLoader] = None

    async def initialise(self) -> None:
        path = _lua_path("task_requeue.lua")
        self._requeue_loader = LuaScriptLoader(self._redis, path)
        await self._requeue_loader.load()

    async def find_stale_tasks(
        self,
        *,
        tenant_id: str,
        now_ms: int | None = None,
    ) -> list[str]:
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
        status    = _decode(raw[0]) if raw else "TASK_STATE_CONFLICT"
        count_raw = raw[1] if len(raw) > 1 else b"0"
        count     = _safe_int(count_raw, default=0)
        return TaskRequeueResult(ok=(status == TASK_REQUEUED), status=status, requeue_count=count)
