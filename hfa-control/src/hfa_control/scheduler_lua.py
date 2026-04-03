"""
hfa-control/src/hfa_control/scheduler_lua.py
IRONCLAD Sprint 18 — Lua script loader for scheduler atomic operations

Exposes two atomic operations:

  enqueue_admitted(...)    — Sprint 18.1: atomic queue+meta+state+index write
  dispatch_commit(...)     — Sprint 18.2: preconditioned state transition + meta + ZADD

Both use LuaScriptLoader (EVALSHA + NOSCRIPT recovery).
fakeredis fallback path preserved for unit tests.
"""

from __future__ import annotations
import json
import logging
import time
from pathlib import Path
from typing import Optional

from hfa.config.keys import RedisKey, RedisTTL
from hfa.lua.loader import LuaScriptLoader


from hfa.state import transition_state as _ts_state

logger = logging.getLogger(__name__)

# Paths relative to this package's repo location
_LUA_DIR = (
    Path(__file__).parent.parent.parent.parent.parent
    / "hfa-core"
    / "src"
    / "hfa"
    / "lua"
)

# Fallback: installed package path
_LUA_DIR_INSTALLED = Path(__file__).parent.parent.parent.parent / "hfa" / "lua"


def _lua_path(filename: str) -> Path:
    for base in (_LUA_DIR, _LUA_DIR_INSTALLED):
        p = base / filename
        if p.exists():
            return p
    # Final fallback: search upward
    here = Path(__file__).resolve()
    for parent in here.parents:
        for subdir in ("hfa-core/src/hfa/lua", "hfa/lua"):
            p = parent / subdir / filename
            if p.exists():
                return p
    raise FileNotFoundError(f"{filename} not found in known lua directories")


class DispatchCommitResult:
    """Structured dispatch commit result with requeue semantics."""

    SUCCESS = "committed"
    STATE_CONFLICT = "state_conflict"
    ALREADY_RUNNING = "already_running"
    ILLEGAL_TRANSITION = "illegal_transition"
    INTERNAL_ERROR = "internal_error"

    def __init__(self, status: str, run_id: str, metadata: Optional[dict] = None) -> None:
        self.status = status
        self.run_id = run_id
        self.metadata = metadata or {}

    @property
    def committed(self) -> bool:
        return self.status == self.SUCCESS

    @property
    def should_requeue(self) -> bool:
        return self.status in {self.INTERNAL_ERROR}


class SchedulerLua:
    """
    Loads and executes scheduler Lua scripts via EVALSHA.

    Lifecycle: call await initialise() once at scheduler startup.
    Then use enqueue_admitted() and dispatch_commit() in hot paths.
    """

    def __init__(self, redis) -> None:
        self._redis = redis
        self._enqueue_loader: Optional[LuaScriptLoader] = None
        self._commit_loader: Optional[LuaScriptLoader] = None

    async def initialise(self) -> None:
        """Load both Lua scripts into Redis. Safe to call multiple times."""
        try:
            enqueue_path = _lua_path("enqueue_admitted.lua")
            self._enqueue_loader = LuaScriptLoader(self._redis, enqueue_path)
            await self._enqueue_loader.load()
        except FileNotFoundError as exc:
            logger.warning(
                "SchedulerLua: enqueue_admitted.lua not found: %s — fallback active",
                exc,
            )
            self._enqueue_loader = None

        try:
            commit_path = _lua_path("dispatch_commit.lua")
            self._commit_loader = LuaScriptLoader(self._redis, commit_path)
            await self._commit_loader.load()
        except FileNotFoundError as exc:
            logger.warning(
                "SchedulerLua: dispatch_commit.lua not found: %s — fallback active", exc
            )
            self._commit_loader = None

        enqueue_sha = (
            self._enqueue_loader.sha[:8]
            if (self._enqueue_loader and self._enqueue_loader.sha)
            else "fallback"
        )
        commit_sha = (
            self._commit_loader.sha[:8]
            if (self._commit_loader and self._commit_loader.sha)
            else "fallback"
        )
        logger.info(
            "SchedulerLua initialised: enqueue=%s… commit=%s…", enqueue_sha, commit_sha
        )

    # ------------------------------------------------------------------
    # 18.1 Atomic enqueue
    # ------------------------------------------------------------------

    async def enqueue_admitted(
        self,
        *,
        run_id: str,
        tenant_id: str,
        agent_type: str,
        priority: int,
        preferred_region: str,
        preferred_placement: str,
        admitted_at: float,
        score: float,
        estimated_cost_cents: int = 0,
        trace_parent: str = "",
        trace_state: str = "",
        payload_json: str = "",
        max_inflight: int = 0,
        inflight_ttl: int = 86400,
    ) -> bool:
        """
        Sprint 21: Atomically check+increment inflight AND enqueue in one Lua transaction.

        Writes atomically:
          - tenant queue ZSET entry (NX)
          - run meta HASH (ALL fields: routing + cost + trace)
          - run payload STRING (if provided)
          - run state STRING = "queued" (RunState.QUEUED)
          - active tenant SET membership

        No separate non-atomic writes needed after this call.
        Returns True if newly enqueued, False if already present (idempotent).
        """
        queue_key      = RedisKey.tenant_queue(tenant_id)
        meta_key       = RedisKey.run_meta(run_id)
        active_key     = RedisKey.tenant_active_set()
        state_key      = RedisKey.run_state(run_id)
        payload_key    = RedisKey.run_payload(run_id)
        inflight_key   = RedisKey.tenant_inflight(tenant_id)

        if self._enqueue_loader is not None:
            try:
                result = await self._enqueue_loader.run(
                    num_keys=6,
                    keys=[queue_key, meta_key, active_key, state_key, payload_key, inflight_key],
                    args=[
                        score,
                        run_id,
                        tenant_id,
                        agent_type,
                        str(priority),
                        preferred_region or "",
                        preferred_placement or "LEAST_LOADED",
                        str(admitted_at),
                        RedisTTL.RUN_META,           # queue_ttl
                        RedisTTL.RUN_META,           # meta_ttl
                        RedisTTL.RUN_STATE,          # state_ttl
                        str(estimated_cost_cents),   # ARGV[12]
                        trace_parent or "",          # ARGV[13]
                        trace_state or "",           # ARGV[14]
                        payload_json or "",          # ARGV[15]
                        str(max_inflight),           # ARGV[16]
                        str(inflight_ttl),           # ARGV[17]
                    ],
                    fallback=lambda: self._enqueue_fallback(
                        queue_key, meta_key, active_key, state_key, payload_key,
                        run_id, tenant_id, agent_type, priority,
                        preferred_region, preferred_placement, admitted_at, score,
                        estimated_cost_cents, trace_parent, trace_state, payload_json,
                        max_inflight, inflight_ttl,
                    ),
                )
                if result == -1:
                    logger.warning(
                        "SchedulerLua.enqueue_admitted: inflight limit exceeded for run=%s",
                        run_id,
                    )
                    return False  # rejected by Lua inflight gate
                return bool(result)
            except Exception as exc:
                logger.warning(
                    "SchedulerLua.enqueue_admitted Lua failed: %s — using fallback", exc
                )

        # Direct fallback (fakeredis / Lua unavailable)
        return await self._enqueue_fallback(
            queue_key, meta_key, active_key, state_key, payload_key,
            run_id, tenant_id, agent_type, priority,
            preferred_region, preferred_placement, admitted_at, score,
            estimated_cost_cents, trace_parent, trace_state, payload_json,
            max_inflight, inflight_ttl,
        )

    async def _enqueue_fallback(
        self,
        queue_key: str,
        meta_key: str,
        active_key: str,
        state_key: str,
        payload_key: str,
        run_id: str,
        tenant_id: str,
        agent_type: str,
        priority: int,
        preferred_region: str,
        preferred_placement: str,
        admitted_at: float,
        score: float,
        estimated_cost_cents: int = 0,
        trace_parent: str = "",
        trace_state: str = "",
        payload_json: str = "",
        max_inflight: int = 0,
        inflight_ttl: int = 86400,
    ) -> bool:
        """Non-atomic fallback for unit tests / Lua-unavailable environments."""
        pipe = self._redis.pipeline()
        pipe.zadd(queue_key, {run_id: score}, nx=True)
        pipe.hset(
            meta_key,
            mapping={
                "run_id": run_id,
                "tenant_id": tenant_id,
                "agent_type": agent_type,
                "priority": str(priority),
                "preferred_region": preferred_region or "",
                "preferred_placement": preferred_placement or "LEAST_LOADED",
                "admitted_at": str(admitted_at),
                "queue_state": "queued",
                "estimated_cost_cents": str(estimated_cost_cents),
                "trace_parent": trace_parent or "",
                "trace_state": trace_state or "",
            },
        )
        if payload_json:
            pipe.set(payload_key, payload_json, ex=RedisTTL.RUN_META)
        pipe.sadd(active_key, tenant_id)
        pipe.expire(queue_key, RedisTTL.RUN_META)
        pipe.expire(meta_key, RedisTTL.RUN_META)
        results = await pipe.execute()
        newly_queued = bool(results[0])
        # Route state write through single authority (hfa.state)
        if newly_queued:
            await _ts_state(
                self._redis, run_id, "queued",
                state_key=state_key, state_ttl=RedisTTL.RUN_STATE,
                expected_state=None,
            )
        return newly_queued

    # ------------------------------------------------------------------
    # 18.2 Atomic dispatch commit
    # ------------------------------------------------------------------

    async def dispatch_commit(
        self,
        *,
        run_id: str,
        tenant_id: str,
        agent_type: str,
        worker_group: str,
        shard: int,
        reschedule_count: int = 0,
        admitted_at: float,
        running_zset: str,
    ) -> bool:
        result = await self.dispatch_commit_detailed(
            run_id=run_id,
            tenant_id=tenant_id,
            agent_type=agent_type,
            worker_group=worker_group,
            shard=shard,
            reschedule_count=reschedule_count,
            admitted_at=admitted_at,
            running_zset=running_zset,
        )
        return result.committed

    async def dispatch_commit_detailed(
            self,
            *,
            run_id: str,
            tenant_id: str,
            agent_type: str,
            worker_group: str,
            shard: int,
            admitted_at: int,
            running_zset: str,
            reschedule_count: int = 0,
            priority: int | None = None,
            payload: dict | None = None,
            trace_parent: str | None = None,
            trace_state: str | None = None,
            policy: str | None = None,
            region: str | None = None,
            control_stream: str | None = None,
            shard_stream: str | None = None,
    ):
        """
        Atomic dispatch commit with a structured status code.
        """
        # Default to canonical stream keys so Lua script writes to streams
        control_stream = control_stream or RedisKey.stream_control()
        shard_stream   = shard_stream   or RedisKey.stream_shard(shard)
        state_key = RedisKey.run_state(run_id)
        meta_key = RedisKey.run_meta(run_id)
        now = time.time()

        if self._commit_loader is not None:
            try:
                result = await self._commit_loader.run(
                    num_keys=3,
                    keys=[state_key, meta_key, running_zset],
                    args=[
                        run_id,
                        tenant_id,
                        agent_type,
                        worker_group,
                        str(shard),
                        str(reschedule_count),
                        str(admitted_at),
                        str(now),
                        RedisTTL.RUN_STATE,
                        RedisTTL.RUN_META,
                        str(priority or 0),
                        json.dumps(payload or {}),
                        trace_parent or "",
                        trace_state or "",
                        policy or "",
                        region or "",
                        control_stream or "",
                        shard_stream or "",
                    ],
                    fallback=lambda: self._commit_fallback(
                        state_key,
                        meta_key,
                        running_zset,
                        run_id,
                        tenant_id,
                        agent_type,
                        worker_group,
                        shard,
                        reschedule_count,
                        admitted_at,
                        now,
                        priority=priority,
                        payload=payload,
                        trace_parent=trace_parent,
                        trace_state=trace_state,
                        policy=policy,
                        region=region,
                        control_stream=control_stream,
                        shard_stream=shard_stream,
                    ),
                )
                if isinstance(result, DispatchCommitResult):
                    return result
                if isinstance(result, (list, tuple)) and result:
                    status = result[0]
                    if isinstance(status, bytes):
                        status = status.decode()
                    details = result[1] if len(result) > 1 else None
                    if isinstance(details, bytes):
                        details = details.decode()
                    return DispatchCommitResult(str(status), run_id, {"details": details})
                if bool(result):
                    return DispatchCommitResult(DispatchCommitResult.SUCCESS, run_id)
                return DispatchCommitResult(DispatchCommitResult.STATE_CONFLICT, run_id)
            except Exception as exc:
                logger.warning(
                    "SchedulerLua.dispatch_commit Lua failed: %s — using fallback", exc
                )

        return await self._commit_fallback(
            state_key,
            meta_key,
            running_zset,
            run_id,
            tenant_id,
            agent_type,
            worker_group,
            shard,
            reschedule_count,
            admitted_at,
            now,
            priority=priority,
            payload=payload,
            trace_parent=trace_parent,
            trace_state=trace_state,
            policy=policy,
            region=region,
            control_stream=control_stream,
            shard_stream=shard_stream,
        )

    async def _commit_fallback(
            self,
            state_key: str,
            meta_key: str,
            running_zset: str,
            run_id: str,
            tenant_id: str,
            agent_type: str,
            worker_group: str,
            shard: int,
            reschedule_count: int,
            admitted_at: float,
            scheduled_at: float,
            *,
            priority: int | None = None,
            payload: dict | None = None,
            trace_parent: str | None = None,
            trace_state: str | None = None,
            policy: str | None = None,
            region: str | None = None,
            control_stream: str | None = None,
            shard_stream: str | None = None,
    ) -> DispatchCommitResult:
        """
        Non-atomic fallback for Lua-unavailable environments (fakeredis/test).

        Order: CAS first, side effects only after commit eligibility confirmed.
        If CAS fails, no side effects are written.
        """
        logger.warning(
            "SchedulerLua: Lua unavailable — using Python fallback commit (non-atomic). run=%s",
            run_id,
        )

        curr = await self._redis.get(state_key)
        curr_s = curr.decode() if isinstance(curr, bytes) else curr
        if not curr_s:
            return DispatchCommitResult(
                DispatchCommitResult.STATE_CONFLICT,
                run_id,
                {"details": "missing_state"},
            )
        if curr_s not in ("admitted", "queued"):
            status = (
                DispatchCommitResult.ALREADY_RUNNING
                if curr_s in ("scheduled", "running")
                else DispatchCommitResult.ILLEGAL_TRANSITION
            )
            return DispatchCommitResult(status, run_id, {"details": curr_s})

        from hfa.state import transition_state as _ts_state

        cas_result = await _ts_state(
            self._redis,
            run_id,
            "scheduled",
            state_key=state_key,
            state_ttl=RedisTTL.RUN_STATE,
            expected_state=curr_s,
        )
        if not cas_result:
            return DispatchCommitResult(
                DispatchCommitResult.STATE_CONFLICT,
                run_id,
                {"details": f"cas_miss_from_{curr_s}"},
            )

        pipe = self._redis.pipeline()
        pipe.hset(
            meta_key,
            mapping={
                "run_id": run_id,
                "tenant_id": tenant_id,
                "agent_type": agent_type,
                "worker_group": worker_group,
                "shard": str(shard),
                "reschedule_count": str(reschedule_count),
                "admitted_at": str(admitted_at),
                "scheduled_at": str(scheduled_at),
                "queue_state": "scheduled",
                "priority": str(priority or 0),
                "payload_json": json.dumps(payload or {}),
                "trace_parent": trace_parent or "",
                "trace_state": trace_state or "",
                "policy": policy or "",
                "region": region or "",
            },
        )
        pipe.expire(meta_key, RedisTTL.RUN_META)
        pipe.zadd(running_zset, {run_id: scheduled_at})

        if control_stream:
            control_fields = {
                "event_type": "RunScheduled",
                "run_id": run_id,
                "tenant_id": tenant_id,
                "agent_type": agent_type,
                "worker_group": worker_group,
                "shard": str(shard),
                "reschedule_count": str(reschedule_count),
                "admitted_at": str(admitted_at),
                "scheduled_at": str(scheduled_at),
                "priority": str(priority or 0),
                "payload_json": json.dumps(payload or {}),
                "trace_parent": trace_parent or "",
                "trace_state": trace_state or "",
                "policy": policy or "",
                "region": region or "",
            }
            pipe.xadd(control_stream, control_fields)

        if shard_stream:
            shard_fields = {
                "event_type": "RunRequested",
                "run_id": run_id,
                "tenant_id": tenant_id,
                "agent_type": agent_type,
                "worker_group": worker_group,
                "shard": str(shard),
                "reschedule_count": str(reschedule_count),
                "admitted_at": str(admitted_at),
                "scheduled_at": str(scheduled_at),
                "priority": str(priority or 0),
                "payload_json": json.dumps(payload or {}),
                "trace_parent": trace_parent or "",
                "trace_state": trace_state or "",
                "policy": policy or "",
                "region": region or "",
            }
            pipe.xadd(shard_stream, shard_fields)

        await pipe.execute()
        return DispatchCommitResult(DispatchCommitResult.SUCCESS, run_id)

    async def _commit_fallback(
        self,
        state_key: str,
        meta_key: str,
        running_zset: str,
        run_id: str,
        tenant_id: str,
        agent_type: str,
        worker_group: str,
        shard: int,
        reschedule_count: int,
        admitted_at: float,
        scheduled_at: float,
    ) -> DispatchCommitResult:
        """
        Non-atomic fallback for Lua-unavailable environments (fakeredis/test).

        Order: CAS first, side effects only after commit eligibility confirmed.
        If CAS fails, no side effects are written.

        WARNING: This path is NOT race-safe between concurrent actors.
        It is intended for single-process test environments only.
        Production deployments must use the Lua primary path.
        """
        logger.warning(
            "SchedulerLua: Lua unavailable — using Python fallback commit (non-atomic). run=%s",
            run_id,
        )

        # Step 1: Verify preconditions FIRST (before any side effects)
        curr = await self._redis.get(state_key)
        curr_s = curr.decode() if isinstance(curr, bytes) else curr
        if not curr_s:
            return DispatchCommitResult(DispatchCommitResult.STATE_CONFLICT, run_id, {"details": "missing_state"})
        if curr_s not in ("admitted", "queued"):
            status = (DispatchCommitResult.ALREADY_RUNNING
                      if curr_s in ("scheduled", "running")
                      else DispatchCommitResult.ILLEGAL_TRANSITION)
            return DispatchCommitResult(status, run_id, {"details": curr_s})

        # Step 2: CAS state transition via single authority
        from hfa.state import transition_state as _ts_state
        cas_result = await _ts_state(
            self._redis, run_id, "scheduled",
            state_key=state_key, state_ttl=RedisTTL.RUN_STATE,
            expected_state=curr_s,
        )
        if not cas_result:
            # Another actor committed between our GET and CAS — no side effects written
            return DispatchCommitResult(DispatchCommitResult.STATE_CONFLICT, run_id,
                                        {"details": f"cas_miss_from_{curr_s}"})

        # Step 3: Side effects ONLY after CAS committed
        pipe = self._redis.pipeline()
        pipe.hset(
            meta_key,
            mapping={
                "run_id": run_id,
                "tenant_id": tenant_id,
                "agent_type": agent_type,
                "worker_group": worker_group,
                "shard": str(shard),
                "reschedule_count": str(reschedule_count),
                "admitted_at": str(admitted_at),
                "scheduled_at": str(scheduled_at),
                "queue_state": "scheduled",
                "trace_parent": trace_parent or "",
                "trace_state": trace_state or "",
            },
        )
        pipe.expire(meta_key, RedisTTL.RUN_META)
        pipe.zadd(running_zset, {run_id: scheduled_at})
        await pipe.execute()
        return DispatchCommitResult(DispatchCommitResult.SUCCESS, run_id)
