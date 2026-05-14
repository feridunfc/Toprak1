from unittest.mock import AsyncMock, MagicMock

import pytest

from hfa_control.models import WorkerStatus
from hfa_control.scheduler_snapshot import SchedulerSnapshotBuilder


class MockWorkerProfile:
    def __init__(
        self,
        w_id,
        group,
        region,
        cap,
        inflight,
        status,
        caps=None,
        agents=None,
        last_heartbeat_at=12345.0,
        latency_ewma_ms=0.0,
        failure_penalty=0.0,
        dispatch_reject_penalty=0.0,
    ):
        self.worker_id = w_id
        self.worker_group = group
        self.region = region
        self.capacity = cap
        self.inflight = inflight
        self.status = status
        self.capabilities = caps
        self.agent_types = agents
        self.last_heartbeat_at = last_heartbeat_at
        self.latency_ewma_ms = latency_ewma_ms
        self.failure_penalty = failure_penalty
        self.dispatch_reject_penalty = dispatch_reject_penalty


@pytest.mark.asyncio
async def test_canonical_worker_snapshot_comprehensive():
    w1 = MockWorkerProfile(
        "w-c",
        "group-B",
        "us-east",
        10,
        2,
        WorkerStatus.HEALTHY,
        [" chat "],
        ["vision"],
    )
    w2 = MockWorkerProfile(
        "w-a",
        "group-B",
        "eu-west",
        0,
        0,
        WorkerStatus.HEALTHY,
        None,
        None,
    )
    w3 = MockWorkerProfile(
        "w-b",
        "group-A",
        "us-east",
        5,
        5,
        WorkerStatus.HEALTHY,
        ["audio"],
        None,
    )
    w4 = MockWorkerProfile(
        "w-d",
        "group-A",
        "us-east",
        10,
        1,
        WorkerStatus.DRAINING,
        ["audio"],
        None,
    )

    mock_registry = AsyncMock()
    mock_registry.list_all_workers.return_value = [w1, w2, w3, w4]

    builder = SchedulerSnapshotBuilder(
        redis=AsyncMock(),
        registry=mock_registry,
        tenant_queue=AsyncMock(),
        tenant_fairness=MagicMock(),
        config=MagicMock(scheduler_loop_max_dispatches=32),
    )

    snapshot = await builder.build_capacity_snapshot()

    assert len(snapshot.workers) == 4

    w_out = snapshot.workers
    assert w_out[0].worker_id == "w-b"
    assert w_out[1].worker_id == "w-d"
    assert w_out[2].worker_id == "w-a"
    assert w_out[3].worker_id == "w-c"

    assert w_out[3].capabilities == ("chat", "vision")
    assert w_out[2].capabilities == ()

    assert w_out[2].blocked_reason == "capacity_zero"
    assert w_out[2].schedulable is False
    assert w_out[2].load_factor == 1.0

    assert w_out[0].blocked_reason == "at_capacity"
    assert w_out[0].schedulable is False

    assert w_out[1].blocked_reason == "draining"
    assert w_out[1].schedulable is False

    assert w_out[3].blocked_reason is None
    assert w_out[3].schedulable is True

    assert snapshot.total_available_slots == 8
    assert snapshot.dispatch_allowed is True
    assert snapshot.max_dispatches_this_cycle == 8


@pytest.mark.asyncio
async def test_duplicate_worker_id_is_resolved_deterministically(caplog):
    # Same worker_id appears twice with different topology.
    # Builder must always pick canonical minimum:
    # (worker_group, region, worker_id, last_seen, capacity, inflight)
    older = MockWorkerProfile(
        "w-x",
        "group-Z",
        "us-west",
        50,
        0,
        WorkerStatus.HEALTHY,
        ["vision"],
        None,
        last_heartbeat_at=99999.0,
    )
    canonical = MockWorkerProfile(
        "w-x",
        "group-A",
        "eu-central",
        10,
        1,
        WorkerStatus.HEALTHY,
        ["chat"],
        None,
        last_heartbeat_at=100.0,
    )

    mock_registry = AsyncMock()
    mock_registry.list_all_workers.return_value = [older, canonical]

    builder = SchedulerSnapshotBuilder(
        redis=AsyncMock(),
        registry=mock_registry,
        tenant_queue=AsyncMock(),
        tenant_fairness=MagicMock(),
        config=MagicMock(scheduler_loop_max_dispatches=32),
    )

    snapshot = await builder.build_capacity_snapshot()

    assert len(snapshot.workers) == 1
    assert snapshot.workers[0].worker_id == "w-x"
    assert snapshot.workers[0].worker_group == "group-A"
    assert snapshot.workers[0].region == "eu-central"
    assert snapshot.workers[0].capabilities == ("chat",)

    assert "Duplicate worker_id detected" in caplog.text


@pytest.mark.asyncio
async def test_unhealthy_worker_is_visible_and_blocked():
    unhealthy = MockWorkerProfile(
        "w-u",
        "group-A",
        "us-east",
        8,
        1,
        "offline",  # string geliyor, enum değil
        ["audio"],
        None,
    )

    mock_registry = AsyncMock()
    mock_registry.list_all_workers.return_value = [unhealthy]

    builder = SchedulerSnapshotBuilder(
        redis=AsyncMock(),
        registry=mock_registry,
        tenant_queue=AsyncMock(),
        tenant_fairness=MagicMock(),
        config=MagicMock(scheduler_loop_max_dispatches=32),
    )

    snapshot = await builder.build_capacity_snapshot()

    assert len(snapshot.workers) == 1
    assert snapshot.workers[0].worker_id == "w-u"
    assert snapshot.workers[0].blocked_reason == "unhealthy"
    assert snapshot.workers[0].schedulable is False
    assert snapshot.total_available_slots == 0
    assert snapshot.dispatch_allowed is False
    assert snapshot.blocked_reason == "worker_pool_empty"


@pytest.mark.asyncio
async def test_empty_worker_ids_are_skipped_safely():
    bad = MockWorkerProfile(
        "   ",
        "group-A",
        "us-east",
        10,
        0,
        WorkerStatus.HEALTHY,
        ["chat"],
        None,
    )
    good = MockWorkerProfile(
        "w-ok",
        "group-A",
        "us-east",
        10,
        2,
        WorkerStatus.HEALTHY,
        ["chat"],
        None,
    )

    mock_registry = AsyncMock()
    mock_registry.list_all_workers.return_value = [bad, good]

    builder = SchedulerSnapshotBuilder(
        redis=AsyncMock(),
        registry=mock_registry,
        tenant_queue=AsyncMock(),
        tenant_fairness=MagicMock(),
        config=MagicMock(scheduler_loop_max_dispatches=32),
    )

    snapshot = await builder.build_capacity_snapshot()

    assert len(snapshot.workers) == 1
    assert snapshot.workers[0].worker_id == "w-ok"
    assert snapshot.total_available_slots == 8


@pytest.mark.asyncio
async def test_candidate_tenants_are_sorted_and_sanitized():
    redis = AsyncMock()
    tenant_queue = AsyncMock()
    tenant_queue.active_tenants.return_value = ["  tenant-b ", "", "tenant-a"]
    tenant_queue.depth.side_effect = lambda tenant_id: {"tenant-a": 2, "tenant-b": 1}[tenant_id]
    tenant_queue.peek.side_effect = lambda tenant_id: {"tenant-a": "run-a", "tenant-b": "run-b"}[tenant_id]

    redis.get.side_effect = lambda key: {
        "hfa:tenant:tenant-a:inflight": b"3",
        "hfa:tenant:tenant-b:inflight": b"1",
    }.get(key)

    redis.hget.side_effect = lambda key, field: b"1000.0"

    tenant_fairness = MagicMock()
    tenant_fairness.get.side_effect = lambda tenant_id: {"tenant-a": 2.5, "tenant-b": 1.0}[tenant_id]

    builder = SchedulerSnapshotBuilder(
        redis=redis,
        registry=AsyncMock(),
        tenant_queue=tenant_queue,
        tenant_fairness=tenant_fairness,
        config=MagicMock(),
    )

    result = await builder.list_candidate_tenants()

    assert [t.tenant_id for t in result] == ["tenant-a", "tenant-b"]
    assert result[0].head_run_id == "run-a"
    assert result[0].inflight == 3
    assert result[0].vruntime == 2.5
    assert result[0].dispatchable is True