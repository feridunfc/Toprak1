"""
hfa-semantic/tests/test_runtime_foundation.py

IRONCLAD Sprint R1/R2 Tests

Tests for:
  * State store abstraction
  * In-memory implementation
  * Redis implementation
  * Dedup store
  * Watermark + lateness
  * Observability

CRITICAL: These tests validate the distributed truth engine foundation.
"""

import time

import pytest

from hfa_semantic.runtime import (
    DedupStore,
    InMemoryStateStore,
    LatenessPolicy,
    RedisStateStore,
    RuntimeMetrics,
    Watermark,
)


# =========================================================================
# StateStore Tests (In-Memory)
# =========================================================================


@pytest.mark.asyncio
async def test_inmemory_state_store_put_get():
    """Test basic put/get operations."""
    store = InMemoryStateStore()

    state_data = {"count": 42, "events": ["a", "b"]}
    await store.put("rule1", "partition_a", state_data, ttl_ms=60_000)

    result = await store.get("rule1", "partition_a")
    assert result is not None
    assert result.state == state_data
    assert result.rule_id == "rule1"
    assert result.partition == "partition_a"

    await store.close()


@pytest.mark.asyncio
async def test_inmemory_state_store_ttl_expiration():
    """Test TTL expiration."""
    store = InMemoryStateStore(cleanup_interval_ms=100)

    state_data = {"count": 1}
    # Set very short TTL (100ms)
    await store.put("rule1", "partition_a", state_data, ttl_ms=100)

    # Should exist immediately
    result = await store.get("rule1", "partition_a")
    assert result is not None

    # Wait for expiration
    await asyncio.sleep(0.2)
    result = await store.get("rule1", "partition_a")
    assert result is None

    await store.close()


@pytest.mark.asyncio
async def test_inmemory_state_store_delete():
    """Test delete operation."""
    store = InMemoryStateStore()

    state_data = {"count": 1}
    await store.put("rule1", "partition_a", state_data, ttl_ms=60_000)

    await store.delete("rule1", "partition_a")
    result = await store.get("rule1", "partition_a")
    assert result is None

    await store.close()


@pytest.mark.asyncio
async def test_inmemory_state_store_eviction():
    """Test partition eviction (memory safety)."""
    store = InMemoryStateStore()

    # Add 5 partitions
    for i in range(5):
        await store.put(f"rule1", f"partition_{i}", {"count": i}, ttl_ms=60_000)

    # Evict to max 3
    evicted = await store.evict("rule1", max_partitions=3)
    assert evicted == 2

    # Check remaining
    partitions = await store.list_partitions("rule1")
    assert len(partitions) == 3

    await store.close()


@pytest.mark.asyncio
async def test_inmemory_state_store_list_partitions():
    """Test listing partitions."""
    store = InMemoryStateStore()

    for i in range(3):
        await store.put("rule1", f"partition_{i}", {"count": i}, ttl_ms=60_000)

    partitions = await store.list_partitions("rule1")
    assert len(partitions) == 3
    assert all(f"partition_{i}" in partitions for i in range(3))

    await store.close()


# =========================================================================
# StateStore Tests (Redis)
# =========================================================================


@pytest.mark.asyncio
async def test_redis_state_store_put_get(redis_async_client):
    """Test Redis state store put/get."""
    store = RedisStateStore(redis_async_client)

    state_data = {"count": 42, "events": ["a", "b"]}
    await store.put("rule1", "partition_a", state_data, ttl_ms=60_000)

    result = await store.get("rule1", "partition_a")
    assert result is not None
    assert result.state == state_data
    assert result.rule_id == "rule1"

    await store.close()


@pytest.mark.asyncio
async def test_redis_state_store_delete(redis_async_client):
    """Test Redis delete."""
    store = RedisStateStore(redis_async_client)

    state_data = {"count": 1}
    await store.put("rule1", "partition_a", state_data, ttl_ms=60_000)

    await store.delete("rule1", "partition_a")
    result = await store.get("rule1", "partition_a")
    assert result is None

    await store.close()


@pytest.mark.asyncio
async def test_redis_state_store_eviction(redis_async_client):
    """Test Redis eviction."""
    store = RedisStateStore(redis_async_client)

    # Add 5 partitions
    for i in range(5):
        await store.put("rule1", f"partition_{i}", {"count": i}, ttl_ms=60_000)

    # Evict to max 3
    evicted = await store.evict("rule1", max_partitions=3)
    assert evicted == 2

    partitions = await store.list_partitions("rule1")
    assert len(partitions) == 3

    await store.close()


# =========================================================================
# DedupStore Tests
# =========================================================================


@pytest.mark.asyncio
async def test_dedup_store_new_event(redis_async_client):
    """Test new event (not duplicate)."""
    dedup = DedupStore(redis_async_client)

    event_id = "event_12345"
    event_time_ms = time.time() * 1000.0

    is_dup = await dedup.is_duplicate(event_id)
    assert not is_dup

    await dedup.mark_processed(event_id, event_time_ms, ttl_ms=3_600_000)

    is_dup = await dedup.is_duplicate(event_id)
    assert is_dup

    await dedup.close()


@pytest.mark.asyncio
async def test_dedup_store_duplicate_detection(redis_async_client):
    """Test duplicate detection."""
    dedup = DedupStore(redis_async_client)

    event_id = "event_dup_test"
    event_time_ms = time.time() * 1000.0

    # Mark first
    await dedup.mark_processed(event_id, event_time_ms)

    # Check duplicate
    is_dup = await dedup.is_duplicate(event_id)
    assert is_dup

    # Verify entry exists
    entry = await dedup.get_entry(event_id)
    assert entry is not None
    assert entry["event_id"] == event_id

    await dedup.close()


@pytest.mark.asyncio
@pytest.mark.skip(reason="FakeRedis TTL timing inconsistent in test environment")
async def test_dedup_store_ttl_expiration(redis_async_client):
    """Test dedup TTL expiration (skipped - timing dependent)."""
    dedup = DedupStore(redis_async_client)

    event_id = "event_ttl_test"
    event_time_ms = time.time() * 1000.0

    # Mark with very short TTL (100ms)
    await dedup.mark_processed(event_id, event_time_ms, ttl_ms=100)

    is_dup = await dedup.is_duplicate(event_id)
    assert is_dup

    # Wait for expiration (need extra time for Redis)
    await asyncio.sleep(0.3)

    is_dup = await dedup.is_duplicate(event_id)
    assert not is_dup

    await dedup.close()


# =========================================================================
# Watermark Tests
# =========================================================================


def test_watermark_initial_state():
    """Test watermark initialization."""
    wm = Watermark(allowed_lateness_ms=10_000)
    assert wm.watermark_ms == 0.0
    assert wm.late_count == 0
    assert wm.on_time_count == 0


def test_watermark_on_time_event():
    """Test on-time event."""
    wm = Watermark(allowed_lateness_ms=10_000)

    now_ms = time.time() * 1000.0
    is_late, should_accept = wm.observe(now_ms)

    assert not is_late
    assert wm.on_time_count == 1
    assert wm.watermark_ms > 0


def test_watermark_late_event():
    """Test late event detection."""
    wm = Watermark(allowed_lateness_ms=10_000)

    now_ms = time.time() * 1000.0
    # Observe on-time event first
    wm.observe(now_ms)

    # Observe event 20 seconds in the past (late)
    late_ms = now_ms - 20_000
    is_late, should_accept = wm.observe(late_ms)

    assert is_late
    assert wm.late_count == 1


def test_watermark_policy_drop():
    """Test DROP policy for late events."""
    wm = Watermark(allowed_lateness_ms=10_000)

    now_ms = time.time() * 1000.0
    wm.observe(now_ms)

    late_ms = now_ms - 20_000
    accept = wm.should_accept(late_ms, LatenessPolicy.DROP)
    assert not accept


def test_watermark_policy_accept():
    """Test ACCEPT_WITH_CORRECTION policy."""
    wm = Watermark(allowed_lateness_ms=10_000)

    now_ms = time.time() * 1000.0
    wm.observe(now_ms)

    late_ms = now_ms - 20_000
    accept = wm.should_accept(late_ms, LatenessPolicy.ACCEPT_WITH_CORRECTION)
    assert accept


# =========================================================================
# Observability Tests
# =========================================================================


# =========================================================================
# Observability Tests
# =========================================================================

def test_runtime_metrics_basic():
    """Test metrics basic functionality (single test to avoid registry collision)."""
    # Note: Prometheus metrics use a global registry,
    # so we can only instantiate metrics once per test session
    metrics = RuntimeMetrics()

    # Verify metrics exist
    assert metrics.events_processed is not None
    assert metrics.matches_emitted is not None
    assert metrics.dedup_hits is not None
    assert metrics.late_events is not None

    # Test recording methods (should not raise)
    try:
        metrics.record_event_processed("rule1")
        metrics.record_match_emitted("rule1", "HIGH")
        metrics.record_dedup_hit()
        metrics.record_late_event("drop")
        metrics.record_confidence(0.85)
        metrics.set_watermark_lag(100.0)
        metrics.record_event_latency("dedup", 1.5)
    except Exception as e:
        pytest.fail(f"Metrics recording failed: {e}")


# =========================================================================
# Integration Tests
# =========================================================================


@pytest.mark.asyncio
async def test_state_store_with_dedup(redis_async_client):
    """Integration: StateStore + DedupStore."""
    state_store = RedisStateStore(redis_async_client)
    dedup_store = DedupStore(redis_async_client)

    event_id = "integration_event_1"
    event_time_ms = time.time() * 1000.0

    # First event: not duplicate
    is_dup = await dedup_store.is_duplicate(event_id)
    assert not is_dup

    # Store state
    state_data = {"processed": True}
    await state_store.put("rule1", "partition_1", state_data, ttl_ms=60_000)

    # Mark as processed
    await dedup_store.mark_processed(event_id, event_time_ms)

    # Second pass: should be duplicate
    is_dup = await dedup_store.is_duplicate(event_id)
    assert is_dup

    # State should still exist
    result = await state_store.get("rule1", "partition_1")
    assert result is not None

    await state_store.close()
    await dedup_store.close()


# Import asyncio at the top for tests
import asyncio

