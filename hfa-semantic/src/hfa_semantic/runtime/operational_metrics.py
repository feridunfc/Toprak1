"""
hfa-semantic/src/hfa_semantic/runtime/operational_metrics.py

IRONCLAD v6 — Operational Metrics (not just counters)

CRITICAL: These are the metrics that keep production alive.
Not vanity metrics, but system survival indicators.
"""

from prometheus_client import Counter, Gauge, Histogram
import time


class OperationalMetrics:
    """
    Production metrics for semantic runtime observability.
    
    These are:
      ✔️ Latency histograms (RTT, merge time)
      ✔️ Backpressure signals (partition pressure)
      ✔️ Consistency metrics (watermark lag)
      ✔️ Failure rates (late events, dedup misses)
    """

    def __init__(self, namespace: str = "semantic_op"):
        self.namespace = namespace

        # ===== LATENCY (Critical for production)
        self.state_store_get_latency = Histogram(
            f"{namespace}_state_store_get_latency_ms",
            "State store GET latency",
            buckets=(0.1, 0.5, 1, 2, 5, 10, 25, 50, 100),
        )

        self.state_store_put_latency = Histogram(
            f"{namespace}_state_store_put_latency_ms",
            "State store PUT (merge) latency",
            buckets=(0.1, 0.5, 1, 2, 5, 10, 25, 50, 100, 250),
        )

        self.dedup_latency = Histogram(
            f"{namespace}_dedup_latency_ms",
            "Dedup try_accept latency",
            buckets=(0.1, 0.25, 0.5, 1, 2, 5, 10),
        )

        self.watermark_latency = Histogram(
            f"{namespace}_watermark_latency_ms",
            "Watermark observe latency",
            buckets=(0.1, 0.25, 0.5, 1, 2, 5, 10),
        )

        # ===== BACKPRESSURE (System health)
        self.partition_pressure = Gauge(
            f"{namespace}_partition_pressure",
            "Partition count / max (0.0-1.0)",
            ["rule_id"],
        )

        self.active_partitions = Gauge(
            f"{namespace}_active_partitions",
            "Current active partitions",
            ["rule_id"],
        )

        self.backpressure_active = Counter(
            f"{namespace}_backpressure_active_total",
            "Backpressure activation count",
            ["rule_id", "level"],
        )

        # ===== CONSISTENCY (Ordering correctness)
        self.watermark_lag_ms = Gauge(
            f"{namespace}_watermark_lag_ms",
            "Current time - watermark time (milliseconds)",
        )

        self.late_event_rate = Gauge(
            f"{namespace}_late_event_ratio",
            "Ratio of late events to total",
        )

        self.watermark_progression = Gauge(
            f"{namespace}_watermark_progression_ms_per_sec",
            "Watermark advancement rate",
        )

        # ===== FAILURE RATES
        self.dedup_accuracy = Gauge(
            f"{namespace}_dedup_hit_ratio",
            "Ratio of duplicate events caught",
        )

        self.eviction_throughput = Counter(
            f"{namespace}_evictions_total",
            "Total partitions evicted",
            ["rule_id"],
        )

        self.merge_conflicts = Counter(
            f"{namespace}_merge_conflicts_total",
            "State merge conflicts/retries",
            ["rule_id"],
        )

        # ===== RUNTIME STATE
        self.state_store_size_estimate = Gauge(
            f"{namespace}_state_store_size_bytes",
            "Estimated state store size",
            ["rule_id"],
        )

        self.redis_fallback_count = Counter(
            f"{namespace}_redis_fallback_total",
            "Times fallback mode was used",
        )

        # ===== LATENCY TRACKING (internal)
        self._last_watermark_ms = 0
        self._last_watermark_check = time.time() * 1000

    def record_state_get(self, latency_ms: float) -> None:
        """Record state store GET."""
        self.state_store_get_latency.observe(latency_ms)

    def record_state_put(self, latency_ms: float) -> None:
        """Record state store PUT."""
        self.state_store_put_latency.observe(latency_ms)

    def record_dedup(self, latency_ms: float) -> None:
        """Record dedup latency."""
        self.dedup_latency.observe(latency_ms)

    def record_watermark(self, latency_ms: float) -> None:
        """Record watermark latency."""
        self.watermark_latency.observe(latency_ms)

    def set_partition_pressure(
        self, rule_id: str, current: int, max_allowed: int
    ) -> None:
        """Update partition pressure gauge."""
        pressure = min(1.0, max(0.0, current / max_allowed if max_allowed > 0 else 0))
        self.partition_pressure.labels(rule_id=rule_id).set(pressure)
        self.active_partitions.labels(rule_id=rule_id).set(current)

    def signal_backpressure(self, rule_id: str, level: str) -> None:
        """Signal backpressure (WARN or CRITICAL)."""
        self.backpressure_active.labels(rule_id=rule_id, level=level).inc()

    def set_watermark_lag(self, lag_ms: float) -> None:
        """Update watermark lag gauge."""
        self.watermark_lag_ms.set(max(0.0, lag_ms))

    def update_late_ratio(self, late_count: int, on_time_count: int) -> None:
        """Update late event ratio."""
        total = late_count + on_time_count
        ratio = late_count / total if total > 0 else 0.0
        self.late_event_rate.set(ratio)

    def update_watermark_progression(self, current_watermark_ms: float) -> None:
        """Calculate and update watermark progression rate."""
        now_ms = time.time() * 1000
        if self._last_watermark_ms > 0:
            delta_ms = current_watermark_ms - self._last_watermark_ms
            delta_time_sec = (now_ms - self._last_watermark_check) / 1000

            if delta_time_sec > 0:
                rate = delta_ms / delta_time_sec
                self.watermark_progression.set(max(0, rate))

        self._last_watermark_ms = current_watermark_ms
        self._last_watermark_check = now_ms

    def set_dedup_accuracy(self, dedup_hits: int, total_events: int) -> None:
        """Update dedup hit ratio."""
        ratio = dedup_hits / total_events if total_events > 0 else 0.0
        self.dedup_accuracy.set(min(1.0, max(0.0, ratio)))

    def record_eviction(self, rule_id: str, count: int) -> None:
        """Record eviction."""
        self.eviction_throughput.labels(rule_id=rule_id).inc(count)

    def record_merge_conflict(self, rule_id: str) -> None:
        """Record merge conflict/retry."""
        self.merge_conflicts.labels(rule_id=rule_id).inc()

    def set_state_size(self, rule_id: str, size_bytes: int) -> None:
        """Update state size estimate."""
        self.state_store_size_estimate.labels(rule_id=rule_id).set(size_bytes)

    def record_redis_fallback(self) -> None:
        """Record fallback to non-Lua mode."""
        self.redis_fallback_count.inc()


# Global operational metrics instance
_op_metrics: OperationalMetrics | None = None


def get_operational_metrics() -> OperationalMetrics:
    """Get or create global instance."""
    global _op_metrics
    if _op_metrics is None:
        _op_metrics = OperationalMetrics()
    return _op_metrics

