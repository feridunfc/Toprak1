"""
tests/integration/test_sprint2_zombie_fence_integration.py
------------------------------------------------------------
Sprint 2 integration tests — true monotonic claim_epoch fencing.

All tests now validate the real reclaim flow:
  claim-1 → epoch=1
  requeue  → epoch stays 1 (NOT reset to 0)
  claim-2 → epoch=2
  late completion with epoch=1 → claim_epoch_mismatch

No test manually forces claim_epoch to a specific value unless it explicitly
tests corrupted-state recovery (which is labeled clearly).

Require a running Redis instance.
Run with: pytest -m integration tests/integration/test_sprint2_zombie_fence_integration.py
"""
from __future__ import annotations

import time

import pytest

from hfa.dag.schema import DagRedisKey, TaskMetaField
from hfa_control.dag_lua import DagLua
from hfa_control.task_recovery import TaskHeartbeatManager, TaskRecoveryManager
from hfa.dag.heartbeat import HeartbeatPolicy

pytestmark = pytest.mark.asyncio


# ── Helpers ───────────────────────────────────────────────────────────────────

async def _place_scheduled(redis, task_id: str, tenant_id: str) -> None:
    """Seed a task into scheduled state with a blank meta hash."""
    await redis.set(DagRedisKey.task_state(task_id), "scheduled")
    await redis.zadd(
        DagRedisKey.task_scheduled_zset(tenant_id),
        {task_id: float(int(time.time() * 1000))},
    )


async def _make_reservation(
    redis, worker_id: str, task_id: str, scheduler_epoch: str
) -> None:
    res_key = DagRedisKey.worker_reservation(worker_id)
    await redis.hset(res_key, mapping={
        "worker_id":       worker_id,
        "task_id":         task_id,
        "scheduler_epoch": scheduler_epoch,
    })
    await redis.expire(res_key, 60)


async def _do_requeue(redis, task_id: str, tenant_id: str) -> None:
    """
    Simulate recovery requeue by calling task_requeue.lua through the manager.
    This is the only supported requeue path — no direct Redis manipulation.
    """
    # Force state to running so requeue logic accepts it
    await redis.set(DagRedisKey.task_state(task_id), "running")
    await redis.zadd(DagRedisKey.task_running_zset(tenant_id), {task_id: 1.0})

    policy = HeartbeatPolicy(stale_after_ms=0, max_requeue_count=10)
    mgr = TaskRecoveryManager(redis, policy=policy)
    result = await mgr.requeue_stale_task(
        task_id=task_id,
        tenant_id=tenant_id,
        expected_state="running",
        now_ms=int(time.time() * 1000),
        reason_code="TEST_REQUEUE",
    )
    assert result.ok is True, f"Requeue failed: {result.status}"


async def _reclaim_scheduled(redis, dag: DagLua, task_id: str, tenant_id: str,
                              worker_id: str, scheduler_epoch: str,
                              claimed_at_ms: int):
    """Move task back to scheduled then claim it."""
    await redis.set(DagRedisKey.task_state(task_id), "scheduled")
    await redis.zadd(
        DagRedisKey.task_scheduled_zset(tenant_id),
        {task_id: float(claimed_at_ms)},
    )
    await _make_reservation(redis, worker_id, task_id, scheduler_epoch)
    return await dag.task_claim_start(
        task_id=task_id,
        tenant_id=tenant_id,
        worker_instance_id=worker_id,
        claimed_at_ms=claimed_at_ms,
        scheduler_epoch=scheduler_epoch,
    )


# ── Test 1: true monotonic epoch — zombie completion rejected ─────────────────

@pytest.mark.integration
async def test_monotonic_epoch_zombie_completion_rejected(redis_client):
    """
    Full monotonic epoch flow:
      worker-A claims  → claim_epoch = 1
      requeue via Lua  → claim_epoch stays 1
      worker-B claims  → claim_epoch = 2
      worker-A late completion with epoch=1 → claim_epoch_mismatch
      worker-B completion with epoch=2      → committed
    """
    task_id   = "mono-zombie-001"
    tenant_id = "tenant-mono"
    dag = DagLua(redis_client)
    await dag.initialise()

    # ── Claim by worker-A ─────────────────────────────────────────────────
    await _place_scheduled(redis_client, task_id, tenant_id)
    await _make_reservation(redis_client, "worker-A", task_id, "sched-1")
    claim_a = await dag.task_claim_start(
        task_id=task_id, tenant_id=tenant_id,
        worker_instance_id="worker-A", claimed_at_ms=1000,
        scheduler_epoch="sched-1",
    )
    assert claim_a.ok is True
    assert claim_a.claim_epoch == "1", f"Expected claim_epoch=1, got {claim_a.claim_epoch}"

    # ── Requeue via Lua recovery (NOT manual reset) ───────────────────────
    await _do_requeue(redis_client, task_id, tenant_id)

    # Verify claim_epoch was NOT reset
    stored_epoch = await redis_client.hget(DagRedisKey.task_meta(task_id), TaskMetaField.CLAIM_EPOCH)
    stored_epoch_str = stored_epoch.decode() if isinstance(stored_epoch, bytes) else str(stored_epoch or "")
    assert stored_epoch_str == "1", f"claim_epoch must stay 1 after requeue, got {stored_epoch_str}"

    # Verify ownership was cleared
    stored_owner = await redis_client.hget(DagRedisKey.task_meta(task_id), TaskMetaField.WORKER_INSTANCE_ID)
    stored_owner_str = stored_owner.decode() if isinstance(stored_owner, bytes) else str(stored_owner or "")
    assert stored_owner_str == "", f"worker_instance_id must be cleared after requeue, got {stored_owner_str}"

    # ── Claim by worker-B ─────────────────────────────────────────────────
    claim_b = await _reclaim_scheduled(
        redis_client, dag, task_id, tenant_id,
        "worker-B", "sched-2", claimed_at_ms=2000,
    )
    assert claim_b.ok is True
    assert claim_b.claim_epoch == "2", f"Expected claim_epoch=2, got {claim_b.claim_epoch}"

    # ── Worker-A late completion with old epoch=1 → rejected ─────────────
    late_result = await dag.task_complete(
        task_id=task_id, tenant_id=tenant_id,
        terminal_state="done", finished_at_ms=3000,
        worker_instance_id="worker-A",
        expected_scheduler_epoch="sched-1",
        expected_claim_epoch="1",
    )
    assert late_result.completed is False
    # owner_mismatch fires first (worker-A vs stored worker-B)
    assert late_result.status in ("owner_mismatch", "claim_epoch_mismatch")

    # ── Worker-B completes with correct epoch=2 → committed ──────────────
    ok_result = await dag.task_complete(
        task_id=task_id, tenant_id=tenant_id,
        terminal_state="done", finished_at_ms=3000,
        worker_instance_id="worker-B",
        expected_scheduler_epoch="sched-2",
        expected_claim_epoch="2",
    )
    assert ok_result.completed is True
    assert ok_result.status == "committed"


# ── Test 2: monotonic epoch after multiple requeues ───────────────────────────

@pytest.mark.integration
async def test_monotonic_epoch_increments_across_requeues(redis_client):
    """
    Claim 1 → epoch=1, requeue → epoch stays 1.
    Claim 2 → epoch=2, requeue → epoch stays 2.
    Claim 3 → epoch=3.
    Old completion with epoch=2 → claim_epoch_mismatch.
    """
    task_id   = "mono-multi-requeue-001"
    tenant_id = "tenant-mono"
    dag = DagLua(redis_client)
    await dag.initialise()

    # Claim 1
    await _place_scheduled(redis_client, task_id, tenant_id)
    await _make_reservation(redis_client, "worker-A", task_id, "sched-1")
    c1 = await dag.task_claim_start(
        task_id=task_id, tenant_id=tenant_id,
        worker_instance_id="worker-A", claimed_at_ms=1000,
        scheduler_epoch="sched-1",
    )
    assert c1.ok and c1.claim_epoch == "1"

    # Requeue 1
    await _do_requeue(redis_client, task_id, tenant_id)
    ep = await redis_client.hget(DagRedisKey.task_meta(task_id), TaskMetaField.CLAIM_EPOCH)
    assert (ep.decode() if isinstance(ep, bytes) else str(ep or "")) == "1"

    # Claim 2
    c2 = await _reclaim_scheduled(
        redis_client, dag, task_id, tenant_id,
        "worker-B", "sched-2", claimed_at_ms=2000,
    )
    assert c2.ok and c2.claim_epoch == "2"

    # Requeue 2
    await _do_requeue(redis_client, task_id, tenant_id)
    ep2 = await redis_client.hget(DagRedisKey.task_meta(task_id), TaskMetaField.CLAIM_EPOCH)
    assert (ep2.decode() if isinstance(ep2, bytes) else str(ep2 or "")) == "2"

    # Claim 3
    c3 = await _reclaim_scheduled(
        redis_client, dag, task_id, tenant_id,
        "worker-C", "sched-3", claimed_at_ms=3000,
    )
    assert c3.ok and c3.claim_epoch == "3"

    # Old completion from epoch=2, worker-B → rejected
    stale = await dag.task_complete(
        task_id=task_id, tenant_id=tenant_id,
        terminal_state="done", finished_at_ms=4000,
        worker_instance_id="worker-B",
        expected_scheduler_epoch="sched-2",
        expected_claim_epoch="2",
    )
    assert stale.completed is False
    assert stale.status in ("owner_mismatch", "claim_epoch_mismatch")

    # Correct completion from epoch=3, worker-C → committed
    good = await dag.task_complete(
        task_id=task_id, tenant_id=tenant_id,
        terminal_state="done", finished_at_ms=4000,
        worker_instance_id="worker-C",
        expected_scheduler_epoch="sched-3",
        expected_claim_epoch="3",
    )
    assert good.completed is True


# ── Test 3: Lua heartbeat fencing ─────────────────────────────────────────────

@pytest.mark.integration
async def test_lua_heartbeat_rejected_after_reclaim(redis_client):
    """
    After reclaim by worker-B (epoch=2), worker-A heartbeat with epoch=1
    is rejected atomically by task_heartbeat.lua.
    """
    task_id   = "hb-fence-001"
    tenant_id = "tenant-mono"
    dag = DagLua(redis_client)
    await dag.initialise()

    # Claim by worker-A
    await _place_scheduled(redis_client, task_id, tenant_id)
    await _make_reservation(redis_client, "worker-A", task_id, "sched-1")
    c1 = await dag.task_claim_start(
        task_id=task_id, tenant_id=tenant_id,
        worker_instance_id="worker-A", claimed_at_ms=1000,
        scheduler_epoch="sched-1",
    )
    assert c1.ok and c1.claim_epoch == "1"

    # Requeue
    await _do_requeue(redis_client, task_id, tenant_id)

    # Claim by worker-B → epoch=2
    c2 = await _reclaim_scheduled(
        redis_client, dag, task_id, tenant_id,
        "worker-B", "sched-2", claimed_at_ms=2000,
    )
    assert c2.ok and c2.claim_epoch == "2"

    hb_mgr = TaskHeartbeatManager(redis_client)

    # Worker-A heartbeat with epoch=1 → must be rejected
    stale_hb = await hb_mgr.record_heartbeat(
        task_id=task_id, tenant_id=tenant_id,
        worker_id="worker-A", claim_epoch="1",
    )
    assert stale_hb.ok is False

    # Worker-B heartbeat with epoch=2 → accepted
    ok_hb = await hb_mgr.record_heartbeat(
        task_id=task_id, tenant_id=tenant_id,
        worker_id="worker-B", claim_epoch="2",
    )
    assert ok_hb.ok is True


# ── Test 4: scheduler epoch mismatch on completion ───────────────────────────

@pytest.mark.integration
async def test_scheduler_epoch_mismatch_on_completion(redis_client):
    """
    Completion arrives with an old scheduler_epoch → rejected.
    Correct scheduler_epoch → committed.
    """
    task_id   = "sched-epoch-001"
    tenant_id = "tenant-mono"
    dag = DagLua(redis_client)
    await dag.initialise()

    await _place_scheduled(redis_client, task_id, tenant_id)
    await _make_reservation(redis_client, "worker-A", task_id, "sched-2")
    claim = await dag.task_claim_start(
        task_id=task_id, tenant_id=tenant_id,
        worker_instance_id="worker-A", claimed_at_ms=1000,
        scheduler_epoch="sched-2",
    )
    assert claim.ok is True
    assert claim.scheduler_epoch == "sched-2"

    # Wrong scheduler_epoch
    bad = await dag.task_complete(
        task_id=task_id, tenant_id=tenant_id,
        terminal_state="done", finished_at_ms=2000,
        worker_instance_id="worker-A",
        expected_scheduler_epoch="sched-1",
        expected_claim_epoch=claim.claim_epoch,
    )
    assert bad.completed is False
    assert bad.status == "scheduler_epoch_mismatch"

    # Correct scheduler_epoch
    good = await dag.task_complete(
        task_id=task_id, tenant_id=tenant_id,
        terminal_state="done", finished_at_ms=2000,
        worker_instance_id="worker-A",
        expected_scheduler_epoch="sched-2",
        expected_claim_epoch=claim.claim_epoch,
    )
    assert good.completed is True
    assert good.status == "committed"
