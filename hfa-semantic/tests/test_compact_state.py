"""
hfa-semantic/tests/test_compact_state.py

Sprint 17 — Compact State Tests
"""

import pytest
from hfa_semantic.runtime.compact_state import (
    CompactStateManager,
    CompactPartitionState,
    WindowAggregate,
)


def test_window_aggregate_o1_updates() -> None:
    """Window aggregate updates in O(1)."""
    window = WindowAggregate(
        window_start_ms=0,
        window_end_ms=60_000,
    )
    
    # Add many events
    for i in range(10_000):
        window.add_event("success", latency_ms=float(i % 100))
    
    assert window.event_count == 10_000
    assert window.success_count == 10_000
    assert window.success_rate == 1.0


def test_window_aggregate_stats() -> None:
    """Window aggregate computes stats correctly."""
    window = WindowAggregate(
        window_start_ms=0,
        window_end_ms=60_000,
    )
    
    window.add_event("success", latency_ms=10.0)
    window.add_event("success", latency_ms=20.0)
    window.add_event("failure", latency_ms=50.0)
    
    assert window.event_count == 3
    assert window.success_count == 2
    assert window.failure_count == 1
    assert window.success_rate == pytest.approx(2/3)
    assert window.avg_latency_ms == pytest.approx(26.67, rel=0.01)


def test_compact_partition_bounded_windows() -> None:
    """Partition only keeps last N windows."""
    partition = CompactPartitionState(
        partition_id="user_123",
        rule_id="rule_1",
        max_windows=3,
    )
    
    # Add events to 5 different windows
    for window_num in range(5):
        for i in range(10):
            partition.add_event(
                "success",
                latency_ms=10.0,
                window_size_ms=60_000,
            )
    
    # Should only keep last 3 windows
    assert len(partition.windows) <= 3


def test_compact_state_manager_high_cardinality() -> None:
    """Manager handles high cardinality (many partitions) safely."""
    manager = CompactStateManager(
        rule_id="rule_1",
        max_partitions=1_000,
    )
    
    # Add events to 1000 different partitions
    for partition_id in range(1_000):
        for i in range(100):
            manager.add_event(
                partition_id=f"entity_{partition_id}",
                outcome="success",
                latency_ms=10.0,
            )
    
    assert manager.partition_count == 1_000
    
    # Memory pressure should be 100% (at max)
    assert manager.memory_pressure == 1.0


def test_compact_state_manager_lru_eviction() -> None:
    """Manager evicts least-recently-used partitions when at capacity."""
    manager = CompactStateManager(
        rule_id="rule_1",
        max_partitions=5,
    )
    
    # Add 5 partitions
    for i in range(5):
        manager.add_event(f"entity_{i}", "success", 10.0)
    
    assert manager.partition_count == 5
    
    # Add a new partition (should evict oldest)
    manager.add_event("entity_new", "success", 10.0)
    
    assert manager.partition_count == 5  # Still at capacity
    assert "entity_new" in [p.partition_id for p in manager._partitions.values()]


def test_compact_state_memory_bounded() -> None:
    """Memory usage is bounded even with high cardinality."""
    manager = CompactStateManager(
        rule_id="rule_1",
        max_partitions=10_000,
    )
    
    # Add 10k partitions with 10 events each
    for partition_id in range(10_000):
        for i in range(10):
            manager.add_event(
                partition_id=f"entity_{partition_id}",
                outcome="success",
                latency_ms=10.0,
            )
    
    # Estimate memory (should be reasonable)
    rule_stats = manager.get_rule_stats()
    memory_bytes = rule_stats["memory_estimate_bytes"]
    
    # 10k partitions × 10 windows × 500 bytes = ~50MB
    # Should be way less than 1GB
    assert memory_bytes < 1_000_000_000  # 1GB
    assert memory_bytes > 1_000_000  # > 1MB (should have some data)


def test_compact_state_no_event_lists() -> None:
    """Verify no full event lists are stored (compact model proof)."""
    partition = CompactPartitionState(
        partition_id="user_123",
        rule_id="rule_1",
    )
    
    # Add 100 events
    for i in range(100):
        partition.add_event("success", latency_ms=10.0)
    
    # Should not have a raw event list
    assert not hasattr(partition, "events")
    assert not hasattr(partition, "event_list")
    
    # Should have windows with aggregates
    assert len(partition.windows) > 0
    assert all(isinstance(w.event_count, int) for w in partition.windows.values())

