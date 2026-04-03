"""
hfa_control/dag_scheduler_bridge.py
-------------------------------------
IRONCLAD Sprint 0/1 — DAG scheduler bridge.

The critical correctness fix in this file:

  BEFORE (broken):
    task_id = await queue.dequeue(tenant_id)   # ZREM from ready queue
    # ← CRASH HERE → task lost permanently
    result  = await dag_lua.task_dispatch_commit(dispatch_input)

  AFTER (safe):
    task_id = await queue.peek(tenant_id)       # non-destructive ZRANGE
    result  = await dag_lua.task_dispatch_commit(dispatch_input)
    # task_dispatch_commit.lua atomically:
    #   1. validates state == ready
    #   2. SET state = scheduled
    #   3. ZADD task_scheduled_zset
    #   4. emits stream events
    # The ready queue entry is removed BY THE LUA SCRIPT as the last step
    # of a successful commit (caller must pass ready_queue key as KEYS[6]).

DESIGN: The ready queue (ZADD) is considered consumed only after the Lua
CAS succeeds.  If the process crashes before calling dispatch_commit, the
task remains in the ready queue and will be picked up on the next cycle.
This eliminates the dequeue-before-commit lost-work window.

NOTE: task_dispatch_commit.lua is extended in this PR to also ZREM the
ready-queue entry atomically (see KEYS[6] in the updated Lua contract).
"""
from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Optional

from hfa.dag.schema import DagRedisKey, DagTaskDispatchInput
from hfa.config.keys import RedisKey


# ── Data transfer objects ─────────────────────────────────────────────────────

@dataclass(frozen=True)
class DagReadyTask:
    task_id: str
    run_id: str
    tenant_id: str
    agent_type: str
    priority: int
    admitted_at: float
    payload_json: str
    preferred_region: str
    preferred_placement: str
    trace_parent: str
    trace_state: str


# ── Ready queue helper ────────────────────────────────────────────────────────

class DagReadyQueue:
    """
    Non-destructive peek at the tenant ready queue.

    The actual removal of a task from the ready queue happens atomically
    inside task_dispatch_commit.lua upon successful CAS commit.
    Python-side ZREM before commit is prohibited — it creates a lost-work
    window if the process crashes between ZREM and state transition.
    """

    def __init__(self, redis) -> None:
        self._redis = redis

    async def peek(self, tenant_id: str) -> Optional[str]:
        """Return the highest-priority (lowest-score) task_id without removing it."""
        rows = await self._redis.zrange(
            DagRedisKey.task_ready_queue(tenant_id), 0, 0
        )
        if not rows:
            return None
        task_id = rows[0]
        return task_id.decode() if isinstance(task_id, bytes) else task_id

    async def rebuild_dispatch_input(
        self,
        task_id: str,
        *,
        tenant_id: str,
        worker_group: str,
        shard: int,
        control_stream: str = "",
        shard_stream: str = "",
        region: str = "",
    ) -> Optional[DagTaskDispatchInput]:
        """
        Build a DagTaskDispatchInput by reading task metadata from Redis.

        Uses DagRedisKey builders exclusively — no ad-hoc key strings.
        Returns None if the task metadata hash is missing.
        """
        raw = await self._redis.hgetall(DagRedisKey.task_meta(task_id))
        if not raw:
            return None

        def _s(k: str) -> str:
            v = raw.get(k.encode()) or raw.get(k)
            if isinstance(v, bytes):
                return v.decode("utf-8", errors="replace")
            return str(v) if v is not None else ""

        def _i(k: str, default: int = 0) -> int:
            try:
                return int(_s(k))
            except (ValueError, TypeError):
                return default

        def _f(k: str, default: float = 0.0) -> float:
            try:
                return float(_s(k))
            except (ValueError, TypeError):
                return default

        resolved_tenant = _s("tenant_id") or tenant_id
        now = int(time.time() * 1000)

        return DagTaskDispatchInput(
            task_id=task_id,
            run_id=_s("run_id"),
            tenant_id=resolved_tenant,
            agent_type=_s("agent_type"),
            worker_group=worker_group,
            shard=shard,
            priority=_i("priority", 5),
            admitted_at=_f("admitted_at", float(now)),
            scheduled_at=float(now),
            # Use canonical scheduled_zset field (replaces old running_zset).
            scheduled_zset=DagRedisKey.task_scheduled_zset(resolved_tenant),
            control_stream=control_stream or RedisKey.stream_control(),
            shard_stream=shard_stream or RedisKey.stream_shard(shard),
            region=region,
            policy=_s("policy") or _s("preferred_placement") or "LEAST_LOADED",
            trace_parent=_s("trace_parent"),
            trace_state=_s("trace_state"),
            payload_json=_s("payload_json") or "{}",
        )
