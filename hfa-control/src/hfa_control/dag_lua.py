"""
hfa_control/dag_lua.py
----------------------
IRONCLAD Sprint 2 — Lua CAS gateway with epoch fencing.

Sprint 2 changes:
  - TaskClaimResult now carries claim_epoch, scheduler_epoch, worker_instance_id
    parsed from the Lua 5-tuple return.
  - task_complete() now accepts and passes expected_scheduler_epoch and
    expected_claim_epoch so task_complete.lua can enforce the full fence.
  - task_claim_start() parses the extended return from task_claim_start.lua.
  - scheduled_at uses milliseconds (from blocker-fix patch).
"""
from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from hfa.config.keys import RedisTTL
from hfa.dag.schema import DagRedisKey
from hfa.dag.states import DagTaskState
from hfa.lua.loader import LuaScriptLoader

logger = logging.getLogger(__name__)


# ── Lua path resolution ───────────────────────────────────────────────────────

def _lua_path(filename: str) -> Path:
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


# ── Result dataclasses ────────────────────────────────────────────────────────

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


@dataclass(frozen=True)
class TaskClaimResult:
    ok: bool
    status: str
    task_id: str
    worker_id: str
    # Sprint 2: fence tuple returned from task_claim_start.lua
    claim_epoch: str = ""
    scheduler_epoch: str = ""

    @staticmethod
    def from_lua(raw: list, task_id: str, worker_id: str) -> "TaskClaimResult":
        """
        Parse the 5-element return from task_claim_start.lua:
          [status, claim_epoch, scheduler_epoch, worker_instance_id, task_id]
        """
        def _d(v) -> str:
            return v.decode() if isinstance(v, bytes) else (str(v) if v else "")

        status       = _d(raw[0]) if raw else "unknown"
        claim_epoch  = _d(raw[1]) if len(raw) > 1 else ""
        sched_epoch  = _d(raw[2]) if len(raw) > 2 else ""
        ok           = status == "task_claimed"
        return TaskClaimResult(
            ok=ok,
            status=status,
            task_id=task_id,
            worker_id=worker_id,
            claim_epoch=claim_epoch,
            scheduler_epoch=sched_epoch,
        )


@dataclass(frozen=True)
class TaskCompleteResult:
    completed: bool
    status: str
    unlocked_count: int = 0
    already_terminal: bool = False

    @staticmethod
    def from_lua(raw: list) -> "TaskCompleteResult":
        ok             = bool(int(raw[0]))
        status         = raw[1].decode() if isinstance(raw[1], bytes) else str(raw[1])
        unlocked       = int(raw[2])
        already_terminal = bool(int(raw[3]))
        return TaskCompleteResult(
            completed=ok,
            status=status,
            unlocked_count=unlocked,
            already_terminal=already_terminal,
        )


# ── DagLua gateway ────────────────────────────────────────────────────────────

class DagLua:
    """
    Lua CAS gateway for all DAG task state transitions.

    Sprint 2: claim returns a full fence tuple; complete enforces it.
    """

    def __init__(self, redis) -> None:
        self._redis = redis
        self._admit_loader:    Optional[LuaScriptLoader] = None
        self._dispatch_loader: Optional[LuaScriptLoader] = None
        self._claim_loader:    Optional[LuaScriptLoader] = None
        self._complete_loader: Optional[LuaScriptLoader] = None
        self._requeue_loader:  Optional[LuaScriptLoader] = None

    async def initialise(self) -> None:
        scripts = {
            "task_admit.lua":           "_admit_loader",
            "task_dispatch_commit.lua": "_dispatch_loader",
            "task_claim_start.lua":     "_claim_loader",
            "task_complete.lua":        "_complete_loader",
            "task_requeue.lua":         "_requeue_loader",
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

    # ── task_admit ────────────────────────────────────────────────────────────

    async def task_admit(self, seed) -> TaskAdmitResult:
        await self._ensure_initialised()
        assert self._admit_loader is not None

        task_id   = seed.task_id
        run_id    = seed.run_id
        tenant_id = seed.tenant_id
        dep_count = getattr(seed, "dependency_count", 0)
        child_ids = list(getattr(seed, "child_task_ids", ()) or ())
        admitted_at = float(getattr(seed, "admitted_at", 0.0) or 0.0)

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
            task_id, run_id, tenant_id,
            getattr(seed, "agent_type", "") or "",
            str(getattr(seed, "priority", 0)),
            str(admitted_at),
            getattr(seed, "payload_json", "") or "",
            getattr(seed, "trace_parent", "") or "",
            getattr(seed, "trace_state", "") or "",
            str(dep_count),
            str(int(getattr(RedisTTL, "RUN_STATE", 86400))),
            str(int(getattr(RedisTTL, "RUN_META", 86400))),
            str(int(getattr(RedisTTL, "RUN_META", 86400))),
            getattr(seed, "region", "") or "",
            getattr(seed, "policy", "") or "",
        ] + child_ids

        raw = await self._admit_loader.run(num_keys=len(keys), keys=keys, args=args)
        status  = raw[0].decode() if isinstance(raw[0], bytes) else str(raw[0])
        ready   = status == "seeded_root"
        admitted = status in ("seeded_root", "seeded_waiting", "already_exists")
        return TaskAdmitResult(admitted=admitted, ready=ready, task_id=task_id, status=status)

    # ── task_dispatch_commit ──────────────────────────────────────────────────

    async def task_dispatch_commit(self, dispatch) -> TaskDispatchCommitResult:
        await self._ensure_initialised()
        assert self._dispatch_loader is not None

        from hfa.config.keys import RedisKey

        task_id   = dispatch.task_id
        tenant_id = dispatch.tenant_id
        run_id    = getattr(dispatch, "run_id", task_id)
        shard     = getattr(dispatch, "shard", 0)
        # milliseconds — from blocker-fix patch
        scheduled_at = getattr(dispatch, "scheduled_at", None) or int(time.time() * 1000)

        scheduled_zset = (
            getattr(dispatch, "scheduled_zset", "") or
            DagRedisKey.task_scheduled_zset(tenant_id)
        )
        running_zset = (
            getattr(dispatch, "running_zset", "") or
            DagRedisKey.task_running_zset(tenant_id)
        )
        control_stream = getattr(dispatch, "control_stream", "") or RedisKey.stream_control()
        shard_stream   = getattr(dispatch, "shard_stream", "") or RedisKey.stream_shard(shard)

        keys = [
            DagRedisKey.task_state(task_id),
            DagRedisKey.task_meta(task_id),
            scheduled_zset,
            control_stream,
            shard_stream,
            DagRedisKey.tenant_ready_queue(tenant_id),
            running_zset,
        ]
        args = [
            task_id, run_id, tenant_id,
            getattr(dispatch, "agent_type", "") or "",
            getattr(dispatch, "worker_group", "") or "",
            str(shard),
            str(getattr(dispatch, "priority", 0)),
            str(getattr(dispatch, "admitted_at", 0.0) or 0.0),
            str(scheduled_at),
            str(int(getattr(RedisTTL, "RUN_STATE", 86400))),
            str(int(getattr(RedisTTL, "RUN_META", 86400))),
            "10000", "10000",
            getattr(dispatch, "trace_parent", "") or "",
            getattr(dispatch, "trace_state", "") or "",
            getattr(dispatch, "policy", "") or "LEAST_LOADED",
            getattr(dispatch, "region", "") or "",
            getattr(dispatch, "payload_json", "") or "{}",
        ]

        raw = await self._dispatch_loader.run(num_keys=len(keys), keys=keys, args=args)
        status    = raw[0].decode() if isinstance(raw[0], bytes) else str(raw[0])
        committed = status == "committed"
        reason    = raw[1].decode() if len(raw) > 1 and isinstance(raw[1], bytes) else (str(raw[1]) if len(raw) > 1 else "")
        return TaskDispatchCommitResult(committed=committed, status=status, task_id=task_id, reason=reason)

    # ── task_claim_start ──────────────────────────────────────────────────────

    async def task_claim_start(
        self,
        *,
        task_id: str,
        tenant_id: str,
        worker_instance_id: str,
        claimed_at_ms: int,
        scheduler_epoch: str = "",
    ) -> TaskClaimResult:
        """
        Atomically claim a scheduled task.

        Sprint 2: parses claim_epoch and scheduler_epoch from the Lua return.
        Returns TaskClaimResult with full fence tuple for downstream use.
        """
        await self._ensure_initialised()
        assert self._claim_loader is not None

        keys = [
            DagRedisKey.task_state(task_id),
            DagRedisKey.task_meta(task_id),
            DagRedisKey.task_scheduled_zset(tenant_id),
            DagRedisKey.task_running_zset(tenant_id),
            DagRedisKey.worker_reservation(worker_instance_id),
        ]
        args = [
            task_id,
            worker_instance_id,
            str(claimed_at_ms),
            str(int(getattr(RedisTTL, "RUN_STATE", 86400))),
            str(int(getattr(RedisTTL, "RUN_META", 86400))),
            str(float(claimed_at_ms)),
            scheduler_epoch,
        ]

        raw = await self._claim_loader.run(num_keys=len(keys), keys=keys, args=args)
        return TaskClaimResult.from_lua(raw, task_id=task_id, worker_id=worker_instance_id)

    # ── task_complete ─────────────────────────────────────────────────────────

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
        # Sprint 2: full fence tuple — must be supplied by worker
        expected_scheduler_epoch: str = "",
        expected_claim_epoch: str = "",
    ) -> TaskCompleteResult:
        """
        Atomically complete a task.

        Sprint 2: passes expected_scheduler_epoch and expected_claim_epoch to
        task_complete.lua.  A stale worker with an older claim_epoch is rejected.
        """
        await self._ensure_initialised()
        assert self._complete_loader is not None

        child_state_pfx   = DagRedisKey.task_state_prefix()
        child_rem_pfx     = DagRedisKey.task_remaining_deps_prefix()
        child_emitted_pfx = DagRedisKey.task_ready_emitted_prefix()

        keys = [
            DagRedisKey.task_state(task_id),
            DagRedisKey.task_meta(task_id),
            DagRedisKey.task_children(task_id),
            DagRedisKey.task_output(task_id),
            DagRedisKey.tenant_ready_queue(tenant_id),
            DagRedisKey.task_running_zset(tenant_id),
        ]
        args = [
            task_id,
            run_id or "",
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
            child_state_pfx,   ":state",
            child_rem_pfx,     ":remaining_deps",
            child_emitted_pfx, ":ready_emitted",
            # Sprint 2: fence args (ARGV[19], ARGV[20])
            expected_scheduler_epoch,
            expected_claim_epoch,
        ]

        raw = await self._complete_loader.run(num_keys=len(keys), keys=keys, args=args)
        return TaskCompleteResult.from_lua(raw)

    # ── backward-compat alias ─────────────────────────────────────────────────

    async def claim_task(
        self,
        *,
        task_id: str,
        worker_instance_id: str,
        claimed_at_ms: int,
        tenant_id: str = "",
        scheduler_epoch: str = "",
    ) -> TaskClaimResult:
        return await self.task_claim_start(
            task_id=task_id,
            tenant_id=tenant_id,
            worker_instance_id=worker_instance_id,
            claimed_at_ms=claimed_at_ms,
            scheduler_epoch=scheduler_epoch,
        )
