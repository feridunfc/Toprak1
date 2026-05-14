"""
tests/core/test_sprint16_tenant_queue.py
IRONCLAD Sprint 16 — TenantQueue unit tests

Verifies:
  - enqueue/dequeue ordering (priority + FIFO within same priority)
  - dequeue returns None on empty queue
  - active_tenants() tracks correctly
  - depth() is accurate
  - remove() works
  - idempotent enqueue (NX)
  - multi-tenant isolation
"""

from __future__ import annotations

import pytest
import fakeredis.aioredis as faredis

from hfa_control.tenant_queue import TenantQueue


# ---------------------------------------------------------------------------
# Basic enqueue / dequeue
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_dequeue_empty_returns_none():
    redis = faredis.FakeRedis()
    q = TenantQueue(redis)
    result = await q.dequeue("tenant-a")
    assert result is None


@pytest.mark.asyncio
async def test_enqueue_dequeue_single():
    redis = faredis.FakeRedis()
    q = TenantQueue(redis)
    await q.enqueue("tenant-a", "run-001", priority=5)
    result = await q.dequeue("tenant-a")
    assert result == "run-001"


@pytest.mark.asyncio
async def test_dequeue_empty_after_last_item():
    redis = faredis.FakeRedis()
    q = TenantQueue(redis)
    await q.enqueue("tenant-a", "run-001")
    await q.dequeue("tenant-a")
    result = await q.dequeue("tenant-a")
    assert result is None


# ---------------------------------------------------------------------------
# Priority ordering
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_higher_priority_dequeued_first():
    redis = faredis.FakeRedis()
    q = TenantQueue(redis)
    now = 1000.0

    # Enqueue lower priority first, then higher
    await q.enqueue("t", "run-low", priority=9, now=now)
    await q.enqueue("t", "run-high", priority=1, now=now + 0.01)

    first = await q.dequeue("t")
    assert first == "run-high", f"Expected high priority first, got {first}"


@pytest.mark.asyncio
async def test_same_priority_fifo_order():
    """Same priority runs should be dispatched in admission order (FIFO)."""
    redis = faredis.FakeRedis()
    q = TenantQueue(redis)

    await q.enqueue("t", "run-first", priority=5, now=1000.0)
    await q.enqueue("t", "run-second", priority=5, now=1000.001)
    await q.enqueue("t", "run-third", priority=5, now=1000.002)

    order = []
    for _ in range(3):
        r = await q.dequeue("t")
        if r:
            order.append(r)

    assert order == ["run-first", "run-second", "run-third"]


@pytest.mark.asyncio
async def test_mixed_priority_correct_order():
    redis = faredis.FakeRedis()
    q = TenantQueue(redis)
    now = 2000.0

    await q.enqueue("t", "run-p5", priority=5, now=now)
    await q.enqueue("t", "run-p1", priority=1, now=now + 0.001)
    await q.enqueue("t", "run-p9", priority=9, now=now + 0.002)
    await q.enqueue("t", "run-p3", priority=3, now=now + 0.003)

    order = []
    for _ in range(4):
        r = await q.dequeue("t")
        if r:
            order.append(r)

    assert order == ["run-p1", "run-p3", "run-p5", "run-p9"]


# ---------------------------------------------------------------------------
# Idempotency
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_enqueue_same_run_id_twice_is_noop():
    """Re-enqueueing the same run_id should not create a duplicate."""
    redis = faredis.FakeRedis()
    q = TenantQueue(redis)

    await q.enqueue("t", "run-dup", priority=5, now=1000.0)
    await q.enqueue("t", "run-dup", priority=1, now=1000.1)  # NX — ignored

    depth = await q.depth("t")
    assert depth == 1


# ---------------------------------------------------------------------------
# active_tenants
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_active_tenants_includes_tenant_after_enqueue():
    redis = faredis.FakeRedis()
    q = TenantQueue(redis)
    await q.enqueue("tenant-x", "run-001")
    active = await q.active_tenants()
    assert "tenant-x" in active


@pytest.mark.asyncio
async def test_active_tenants_removes_tenant_after_queue_empty():
    redis = faredis.FakeRedis()
    q = TenantQueue(redis)
    await q.enqueue("tenant-y", "run-001")
    await q.dequeue("tenant-y")
    active = await q.active_tenants()
    assert "tenant-y" not in active


@pytest.mark.asyncio
async def test_active_tenants_multi_tenant():
    redis = faredis.FakeRedis()
    q = TenantQueue(redis)
    await q.enqueue("t1", "run-a")
    await q.enqueue("t2", "run-b")
    await q.enqueue("t3", "run-c")
    active = await q.active_tenants()
    assert set(active) == {"t1", "t2", "t3"}


# ---------------------------------------------------------------------------
# depth / peek
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_depth_increases_on_enqueue():
    redis = faredis.FakeRedis()
    q = TenantQueue(redis)
    assert await q.depth("t") == 0
    await q.enqueue("t", "r1")
    assert await q.depth("t") == 1
    await q.enqueue("t", "r2")
    assert await q.depth("t") == 2


@pytest.mark.asyncio
async def test_depth_decreases_on_dequeue():
    redis = faredis.FakeRedis()
    q = TenantQueue(redis)
    await q.enqueue("t", "r1")
    await q.enqueue("t", "r2")
    await q.dequeue("t")
    assert await q.depth("t") == 1


@pytest.mark.asyncio
async def test_peek_returns_next_without_removing():
    redis = faredis.FakeRedis()
    q = TenantQueue(redis)
    await q.enqueue("t", "run-peek", priority=5)
    peeked = await q.peek("t")
    assert peeked == "run-peek"
    assert await q.depth("t") == 1  # still in queue


@pytest.mark.asyncio
async def test_peek_returns_none_on_empty():
    redis = faredis.FakeRedis()
    q = TenantQueue(redis)
    assert await q.peek("t") is None


# ---------------------------------------------------------------------------
# remove
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_remove_existing_run():
    redis = faredis.FakeRedis()
    q = TenantQueue(redis)
    await q.enqueue("t", "run-del")
    removed = await q.remove("t", "run-del")
    assert removed is True
    assert await q.depth("t") == 0


@pytest.mark.asyncio
async def test_remove_nonexistent_run_returns_false():
    redis = faredis.FakeRedis()
    q = TenantQueue(redis)
    removed = await q.remove("t", "ghost-run")
    assert removed is False


# ---------------------------------------------------------------------------
# Multi-tenant isolation
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_dequeue_is_isolated_per_tenant():
    redis = faredis.FakeRedis()
    q = TenantQueue(redis)
    await q.enqueue("tenant-a", "run-a1")
    await q.enqueue("tenant-b", "run-b1")

    r = await q.dequeue("tenant-a")
    assert r == "run-a1"

    # tenant-b queue unaffected
    assert await q.depth("tenant-b") == 1


@pytest.mark.asyncio
async def test_all_depths_returns_per_tenant_map():
    redis = faredis.FakeRedis()
    q = TenantQueue(redis)
    await q.enqueue("t1", "r1")
    await q.enqueue("t1", "r2")
    await q.enqueue("t2", "r3")
    depths = await q.all_depths()
    assert depths["t1"] == 2
    assert depths["t2"] == 1
