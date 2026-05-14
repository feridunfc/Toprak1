"""
hfa-semantic/tests/test_production_hardening.py

IRONCLAD — Production Hardening Tests

Tests for distributed correctness, race conditions, and pressure scenarios.
"""

import asyncio
import pytest

from hfa_semantic.runtime import (
    DedupStore,
    EvictionPolicy,
    RedisStateStore,
    Watermark,
)


# =========================================================================
# CRITICAL: Redis Atomic Update Tests
# =========================================================================


@pytest.mark.asyncio
async def test_redis_atomic_update_no_race(redis_async_client):
    """Test that Redis Lua update prevents race conditions."""
    store = RedisStateStore(redis_async_client)

    # Concurrent puts from two "workers"
    async def worker(worker_id: int, partition: str):
        for i in range(10):
            await store.put(
                "rule1",
                partition,
                {"worker": worker_id, "iter": i},
                ttl_ms=60_000,
            )
            await asyncio.sleep(0.001)

    # Run two workers concurrently
    await asyncio.gather(
        worker(1, "partition_a"),
        worker(2, "partition_a"),
    )

    # Final state should be consistent (no corruption)
    result = await store.get("rule1", "partition_a")
    assert result is not None
    assert "worker" in result.state
    assert "iter" in result.state

    await store.close()


# =========================================================================
# CRITICAL: Global Dedup Tests (Multi-Worker)
# =========================================================================


@pytest.mark.asyncio
async def test_global_dedup_setnx_mutual_exclusion(redis_async_client):
    """Test that SETNX ensures only one worker can process an event."""
    dedup = DedupStore(redis_async_client)

    event_id = "race_test_event_12345"
    winners = []

    async def worker(worker_id: int):
        """Try to mark event as processed."""
        success = await dedup.mark_processed(
            event_id,
            event_time_ms=1000.0,
            ttl_ms=3_600_000,
        )
        if success:
            winners.append(worker_id)

    # 10 workers race to process same event
    await asyncio.gather(*[worker(i) for i in range(10)])

    # Only ONE should have won (SETNX guarantee)
    assert len(winners) == 1, f"Expected 1 winner, got {len(winners)}"

    # All subsequent checks should see duplicate
    for i in range(10):
        is_dup = await dedup.is_duplicate(event_id)
        assert is_dup

    await dedup.close()


@pytest.mark.asyncio
async def test_global_dedup_duplicate_detection(redis_async_client):
    """Test that duplicate detection works across workers."""
    dedup = DedupStore(redis_async_client)

    event_id = "event_dup_test_99"

    # Worker 1 marks first
    success1 = await dedup.mark_processed(
        event_id,
        event_time_ms=1000.0,
        ttl_ms=3_600_000,
    )
    assert success1

    # Worker 2 sees it as duplicate
    success2 = await dedup.mark_processed(
        event_id,
        event_time_ms=1000.0,
        ttl_ms=3_600_000,
    )
    assert not success2  # Lost race

    # Worker 1 check
    is_dup = await dedup.is_duplicate(event_id)
    assert is_dup

    await dedup.close()


# =========================================================================
# CRITICAL: Watermark Global Consistency Tests
# =========================================================================


@pytest.mark.asyncio
async def test_watermark_redis_consistency(redis_async_client):
    """Test that watermark state syncs with Redis."""
    wm = Watermark(
        allowed_lateness_ms=10_000,
        redis_client=redis_async_client,
        rule_id="rule_wm_test",
    )

    now_ms = 1000.0

    # Observe event
    is_late, _ = await wm.observe(now_ms)
    assert not is_late

    # Create new instance (simulates different worker)
    wm2 = Watermark(
        allowed_lateness_ms=10_000,
        redis_client=redis_async_client,
        rule_id="rule_wm_test",
    )

    # Should see same watermark
    await wm2._sync_from_redis()
    assert wm2.watermark_ms == wm.watermark_ms


@pytest.mark.asyncio
async def test_watermark_late_event_detection(redis_async_client):
    """Test lateness detection across workers."""
    wm = Watermark(
        allowed_lateness_ms=10_000,
        redis_client=redis_async_client,
        rule_id="rule_late_test",
    )

    now_ms = 1000.0

    # Observe recent event
    is_late, _ = await wm.observe(now_ms)
    assert not is_late

    # Observe event 20 seconds in past (late)
    late_ms = now_ms - 20_000
    is_late, _ = await wm.observe(late_ms)
    assert is_late


# =========================================================================
# CRITICAL: Partition Pressure Control Tests
# =========================================================================


def test_eviction_policy_partition_pressure():
    """Test partition pressure detection."""
    policy = EvictionPolicy(
        max_partitions_per_rule=1000,
        partition_pressure_threshold=0.8,
    )

    # Empty
    policy.update_partition_count(0)
    assert policy.partition_pressure == 0.0
    assert not policy.under_pressure

    # 50%
    policy.update_partition_count(500)
    assert policy.partition_pressure == 0.5
    assert not policy.under_pressure

    # 85% (above threshold)
    policy.update_partition_count(850)
    assert policy.partition_pressure == 0.85
    assert policy.under_pressure  # Over threshold

    # Emergency (95%)
    policy.update_partition_count(950)
    assert policy.aggressive_eviction_needed()


def test_eviction_policy_adaptive_batch_size():
    """Test batch size adapts to pressure."""
    policy = EvictionPolicy(
        max_partitions_per_rule=1000,
        partition_pressure_threshold=0.8,
    )

    # Normal: 1% of max
    policy.update_partition_count(100)
    batch = policy.get_eviction_batch_size()
    assert batch == 10  # 1% of 1000

    # Under pressure: 10% of max
    policy.update_partition_count(850)
    batch = policy.get_eviction_batch_size()
    assert batch >= 100  # 10% of 1000

    # Emergency: 20% of max
    policy.update_partition_count(950)
    batch = policy.get_eviction_batch_size()
    assert batch >= 200  # 20% of 1000


# =========================================================================
# HIGH LOAD Tests
# =========================================================================


@pytest.mark.asyncio
async def test_dedup_high_throughput(redis_async_client):
    """Test dedup under high event rate."""
    dedup = DedupStore(redis_async_client)

    # 100 events, some duplicates
    event_ids = [f"event_{i % 50}" for i in range(100)]

    processed = 0
    duplicates = 0

    for event_id in event_ids:
        success = await dedup.mark_processed(
            event_id,
            event_time_ms=1000.0,
            ttl_ms=3_600_000,
        )
        if success:
            processed += 1
        else:
            duplicates += 1

    # Should see exactly 50 unique events
    assert processed == 50
    assert duplicates == 50

    await dedup.close()


@pytest.mark.asyncio
async def test_redis_state_store_high_partition_count(redis_async_client):
    """Test state store with many partitions."""
    store = RedisStateStore(redis_async_client)

    # Create 100 partitions
    for i in range(100):
        await store.put(
            "rule1",
            f"partition_{i}",
            {"value": i},
            ttl_ms=60_000,
        )

    # Retrieve all
    partitions = await store.list_partitions("rule1")
    assert len(partitions) == 100

    # Evict to 50
    evicted = await store.evict("rule1", max_partitions=50)
    assert evicted == 50

    # Check new count
    partitions = await store.list_partitions("rule1")
    assert len(partitions) == 50

    await store.close()


# =========================================================================
# CORRECTNESS Under Concurrency
# =========================================================================


@pytest.mark.asyncio
async def test_concurrent_state_updates_consistency(redis_async_client):
    """Test that concurrent state updates don't corrupt state."""
    store = RedisStateStore(redis_async_client)

    async def update_loop(partition_id: int):
        for iter_i in range(20):
            await store.put(
                "rule_concurrent_test",
                f"partition_{partition_id}",
                {
                    "partition": partition_id,
                    "iter": iter_i,
                    "iterations_done": iter_i + 1,
                },
                ttl_ms=60_000,
            )

    # 10 partitions, each updated 20 times concurrently
    await asyncio.gather(*[update_loop(i) for i in range(10)])

    # All should be consistent
    for i in range(10):
        result = await store.get("rule_concurrent_test", f"partition_{i}")
        assert result is not None
        # Last write should have iterations_done == 20
        assert result.state.get("iterations_done") == 20

    await store.close()

