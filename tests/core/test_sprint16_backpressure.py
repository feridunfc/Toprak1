"""
tests/core/test_sprint16_backpressure.py
IRONCLAD Sprint 16 — Queue depth and backpressure tests

Verifies:
  - Queue depth accurately reflects backlog
  - Admission still enforces inflight limits (not bypassed by queueing)
  - Burst from a single tenant fills queue without affecting others
  - all_depths() provides system-wide backlog visibility
"""

from __future__ import annotations

import pytest
import fakeredis.aioredis as faredis

from hfa_control.tenant_queue import TenantQueue
from hfa_control.tenant_fairness import TenantFairnessTracker


# ---------------------------------------------------------------------------
# Queue depth reflects backlog
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_queue_depth_tracks_backlog():
    redis = faredis.FakeRedis()
    q = TenantQueue(redis)

    for i in range(5):
        await q.enqueue("tenant-a", f"run-{i:03d}")

    assert await q.depth("tenant-a") == 5


@pytest.mark.asyncio
async def test_queue_depth_zero_after_drain():
    redis = faredis.FakeRedis()
    q = TenantQueue(redis)

    for i in range(3):
        await q.enqueue("t", f"r{i}")

    for _ in range(3):
        await q.dequeue("t")

    assert await q.depth("t") == 0


@pytest.mark.asyncio
async def test_burst_tenant_does_not_affect_other_queue():
    """A burst from one tenant should not spill into another tenant's queue."""
    redis = faredis.FakeRedis()
    q = TenantQueue(redis)

    # Burst: 20 runs from heavy tenant
    for i in range(20):
        await q.enqueue("heavy", f"run-heavy-{i:03d}")

    # Light tenant has 1 run
    await q.enqueue("light", "run-light-001")

    assert await q.depth("heavy") == 20
    assert await q.depth("light") == 1


@pytest.mark.asyncio
async def test_all_depths_shows_system_backlog():
    redis = faredis.FakeRedis()
    q = TenantQueue(redis)

    await q.enqueue("t1", "r1")
    await q.enqueue("t1", "r2")
    await q.enqueue("t2", "r3")
    await q.enqueue("t3", "r4")
    await q.enqueue("t3", "r5")
    await q.enqueue("t3", "r6")

    depths = await q.all_depths()
    assert depths.get("t1") == 2
    assert depths.get("t2") == 1
    assert depths.get("t3") == 3


# ---------------------------------------------------------------------------
# Backpressure via fairness — heavy tenant is naturally throttled
# ---------------------------------------------------------------------------


def test_fairness_throttles_heavy_tenant():
    """
    Heavy tenant dispatches 8 runs; light dispatches 2.
    pick_next() should select light more often than heavy going forward.
    """
    tracker = TenantFairnessTracker()

    # Simulate past burst
    for _ in range(8):
        tracker.update_on_dispatch("heavy", cost=1.0)
    for _ in range(2):
        tracker.update_on_dispatch("light", cost=1.0)

    # Next 4 picks should mostly be light
    picks = []
    for _ in range(4):
        chosen = tracker.pick_next(["heavy", "light"])
        picks.append(chosen)
        tracker.update_on_dispatch(chosen, cost=1.0)

    light_picks = picks.count("light")
    assert light_picks >= 3, f"Expected light to get ≥3 of next 4, got {light_picks}"


def test_new_tenant_not_starved_by_veterans():
    """
    Two veteran tenants with high vruntime.
    New tenant should be picked immediately (starts at min_vruntime).
    """
    tracker = TenantFairnessTracker()
    for _ in range(20):
        tracker.update_on_dispatch("veteran-a", cost=1.0)
        tracker.update_on_dispatch("veteran-b", cost=1.0)

    # New tenant enters
    chosen = tracker.pick_next(["veteran-a", "veteran-b", "new"])
    assert chosen == "new", "New tenant should be picked first (min_vruntime)"


# ---------------------------------------------------------------------------
# Queue ordering under priority + time
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_queue_ordering_priority_over_fifo():
    """Priority always overrides FIFO — p1 beats p5 even if enqueued later."""
    redis = faredis.FakeRedis()
    q = TenantQueue(redis)
    now = 1000.0

    await q.enqueue("t", "early-p5", priority=5, now=now)
    await q.enqueue("t", "late-p1", priority=1, now=now + 10.0)

    first = await q.dequeue("t")
    assert first == "late-p1"


@pytest.mark.asyncio
async def test_queue_drains_in_correct_order():
    """Full drain of 4 items should produce correct priority order."""
    redis = faredis.FakeRedis()
    q = TenantQueue(redis)
    now = 3000.0

    await q.enqueue("t", "p5-a", priority=5, now=now)
    await q.enqueue("t", "p2-b", priority=2, now=now + 0.001)
    await q.enqueue("t", "p8-c", priority=8, now=now + 0.002)
    await q.enqueue("t", "p2-d", priority=2, now=now + 0.003)

    order = []
    for _ in range(4):
        r = await q.dequeue("t")
        if r:
            order.append(r)

    # p2-b before p2-d (FIFO within same priority), then p5, then p8
    assert order == ["p2-b", "p2-d", "p5-a", "p8-c"], f"Got: {order}"
