from __future__ import annotations

import json
import time
from dataclasses import dataclass

import pytest

from hfa.config.keys import RedisKey, RedisTTL
from hfa.events.schema import RunAdmittedEvent
from hfa_control.models import ControlPlaneConfig
from hfa_control.scheduler_loop import SchedulerLoop
from hfa_control.scheduler_lua import SchedulerLua

pytestmark = [pytest.mark.asyncio, pytest.mark.integration]


@dataclass
class _Permit:
    allowed: bool = True
    max_dispatches: int = 1


@dataclass
class _TenantCandidate:
    tenant_id: str
    dispatchable: bool = True
    queued_depth: int = 1


@dataclass
class _Worker:
    schedulable: bool = True
    load_factor: float = 0.1
    capacity: int = 4


@dataclass
class _CapacitySnapshot:
    workers: list[_Worker]
    dispatch_allowed: bool = True
    blocked_reason: str | None = None
    total_available_slots: int = 4


@dataclass
class _Selection:
    chosen: str | None
    reason: str


class _TenantQueue:
    def __init__(self, tenant_id: str, run_id: str) -> None:
        self.tenant_id = tenant_id
        self.run_id = run_id
        self.dequeued = False
        self.enqueued: list[tuple[str, str]] = []

    async def all_depths(self):
        return {self.tenant_id: 1 if not self.dequeued else 0}

    async def peek(self, tenant_id: str):
        return self.run_id if tenant_id == self.tenant_id and not self.dequeued else None

    async def dequeue(self, tenant_id: str):
        if tenant_id == self.tenant_id and not self.dequeued:
            self.dequeued = True
            return self.run_id
        return None

    async def enqueue(self, tenant_id: str, run_id: str, priority: int, now: float):
        self.enqueued.append((tenant_id, run_id))
        self.dequeued = False

    async def remove(self, tenant_id: str, run_id: str):
        self.dequeued = True


class _TenantFairness:
    def __init__(self, tenant_id: str) -> None:
        self.tenant_id = tenant_id
        self.updated: list[tuple[str, float]] = []

    def reset(self):
        pass

    def pick_next(self, eligible):
        return self.tenant_id if self.tenant_id in eligible else None

    def get(self, tenant_id: str) -> float:
        return 0.0

    def update_on_dispatch(self, tenant_id: str, cost: float):
        self.updated.append((tenant_id, cost))


class _SnapshotBuilder:
    async def list_candidate_tenants(self):
        return [_TenantCandidate("tenant-e2e")]

    async def build_capacity_snapshot(self):
        return _CapacitySnapshot(workers=[_Worker()])


class _DispatchController:
    def __init__(self) -> None:
        self.successes = 0
        self.failures: list[str] = []

    async def initialise(self):
        return None

    async def current_permit(self):
        return _Permit()

    async def try_consume(self, n: int):
        return True

    async def refund(self, n: int):
        return None

    async def on_dispatch_failure(self, reason: str):
        self.failures.append(reason)

    async def on_dispatch_success(self):
        self.successes += 1


class _WorkerScorer:
    def select_worker_group(self, workers, *, agent_type, preferred_region, policy):
        return _Selection(chosen="grp-a", reason="selected")

    def observe_dispatch_success(self, worker_group: str):
        return None

    def observe_dispatch_failure(self, worker_group: str, reason: str):
        return None


class _Shards:
    async def shard_for_group(self, worker_group: str, run_id: str) -> int:
        return 0


async def _lua(redis_client):
    lua = SchedulerLua(redis_client)
    await lua.initialise()
    return lua


@pytest.mark.integration
async def test_scheduler_loop_run_cycle_end_to_end(redis_client):
    run_id = "loop-e2e-001"
    tenant_id = "tenant-e2e"
    admitted_at = time.time()

    await redis_client.hset(
        RedisKey.run_meta(run_id),
        mapping={
            "run_id": run_id,
            "tenant_id": tenant_id,
            "agent_type": "default",
            "priority": "5",
            "preferred_region": "",
            "preferred_placement": "LEAST_LOADED",
            "admitted_at": str(admitted_at),
            "estimated_cost_cents": "7",
            "trace_parent": "00-loop-parent-01",
            "trace_state": "loop=1",
        },
    )
    await redis_client.expire(RedisKey.run_meta(run_id), RedisTTL.RUN_META)
    await redis_client.set(RedisKey.run_payload(run_id), json.dumps({"hello": "loop"}), ex=RedisTTL.RUN_META)
    await redis_client.set(RedisKey.run_state(run_id), "admitted", ex=RedisTTL.RUN_STATE)

    queue = _TenantQueue(tenant_id, run_id)
    fairness = _TenantFairness(tenant_id)
    controller = _DispatchController()
    config = ControlPlaneConfig(strict_cas_mode=True)
    loop = SchedulerLoop(
        redis_client,
        registry=None,
        shards=_Shards(),
        tenant_queue=queue,
        tenant_fairness=fairness,
        lua=await _lua(redis_client),
        snapshot_builder=_SnapshotBuilder(),
        dispatch_controller=controller,
        worker_scorer=_WorkerScorer(),
        config=config,
    )

    committed = await loop.run_cycle(max_dispatches=1)
    assert committed == 1
    assert controller.successes == 1
    assert fairness.updated and fairness.updated[0][0] == tenant_id

    state = await redis_client.get(RedisKey.run_state(run_id))
    assert state == "scheduled"

    scheduled = [e for e in await redis_client.xrange(RedisKey.stream_control(), "-", "+", 20) if e[1].get("run_id") == run_id]
    requested = [e for e in await redis_client.xrange(RedisKey.stream_shard(0), "-", "+", 20) if e[1].get("run_id") == run_id]
    assert len(scheduled) == 1
    assert len(requested) == 1

    fields = scheduled[0][1]
    assert fields["event_type"] == "RunScheduled"
    assert fields["worker_group"] == "grp-a"
    assert fields["policy"] == "LEAST_LOADED"
