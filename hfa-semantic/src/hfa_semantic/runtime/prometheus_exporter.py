"""
hfa-semantic/src/hfa_semantic/runtime/prometheus_exporter.py

IRONCLAD — Production Prometheus Export

Exposes semantic runtime metrics on /metrics endpoint.
Integrates with existing Prometheus scrape jobs.

CRITICAL METRICS:
  * events_processed_total
  * partition_pressure (0.0-1.0)
  * eviction_rate
  * dedup_hits
  * watermark_lag
  * state_size
"""

from typing import Optional

from prometheus_client import Counter, Gauge, Histogram, generate_latest


class PrometheusExporter:
    """
    Prometheus metric export for semantic runtime.
    
    Handles:
      * Counter metrics (totals)
      * Gauge metrics (current state)
      * Histogram metrics (latency/size)
    """

    def __init__(self, namespace: str = "semantic"):
        self.namespace = namespace
        
        # Counters
        self.events_processed = Counter(
            f"{namespace}_events_processed_total",
            "Total events processed",
            ["rule_id"],
        )
        
        self.matches_emitted = Counter(
            f"{namespace}_matches_emitted_total",
            "Reasoning matches emitted",
            ["rule_id", "severity"],
        )
        
        self.dedup_hits = Counter(
            f"{namespace}_dedup_hits_total",
            "Duplicate events dropped",
        )
        
        self.late_events = Counter(
            f"{namespace}_late_events_total",
            "Late events detected",
            ["policy"],
        )
        
        self.evictions = Counter(
            f"{namespace}_evictions_total",
            "Partition evictions",
            ["rule_id", "strategy"],
        )
        
        # Gauges
        self.partition_pressure = Gauge(
            f"{namespace}_partition_pressure",
            "Partition count pressure (0.0-1.0)",
            ["rule_id"],
        )
        
        self.state_size = Gauge(
            f"{namespace}_state_size_bytes",
            "Estimated state store size",
            ["rule_id"],
        )
        
        self.active_partitions = Gauge(
            f"{namespace}_active_partitions",
            "Active partitions per rule",
            ["rule_id"],
        )
        
        self.watermark_lag = Gauge(
            f"{namespace}_watermark_lag_ms",
            "Watermark lag (current time - watermark)",
        )
        
        # Histograms
        self.event_latency = Histogram(
            f"{namespace}_event_latency_ms",
            "Event processing latency",
            ["stage"],
            buckets=(1, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000),
        )
        
        self.match_confidence = Histogram(
            f"{namespace}_match_confidence",
            "Match confidence distribution",
            buckets=(0.1, 0.25, 0.5, 0.75, 0.9, 0.95, 0.99),
        )

    def record_event_processed(self, rule_id: str) -> None:
        """Record event processing."""
        self.events_processed.labels(rule_id=rule_id).inc()

    def record_match_emitted(self, rule_id: str, severity: str) -> None:
        """Record match emission."""
        self.matches_emitted.labels(rule_id=rule_id, severity=severity).inc()

    def record_dedup_hit(self) -> None:
        """Record duplicate event."""
        self.dedup_hits.inc()

    def record_late_event(self, policy: str) -> None:
        """Record late event."""
        self.late_events.labels(policy=policy).inc()

    def record_eviction(self, rule_id: str, strategy: str, count: int = 1) -> None:
        """Record partition eviction."""
        self.evictions.labels(rule_id=rule_id, strategy=strategy).inc(count)

    def set_partition_pressure(self, rule_id: str, pressure: float) -> None:
        """Set partition pressure (0.0-1.0)."""
        self.partition_pressure.labels(rule_id=rule_id).set(min(1.0, max(0.0, pressure)))

    def set_state_size(self, rule_id: str, size_bytes: int) -> None:
        """Set estimated state size."""
        self.state_size.labels(rule_id=rule_id).set(size_bytes)

    def set_active_partitions(self, rule_id: str, count: int) -> None:
        """Set active partition count."""
        self.active_partitions.labels(rule_id=rule_id).set(count)

    def set_watermark_lag(self, lag_ms: float) -> None:
        """Set watermark lag."""
        self.watermark_lag.set(max(0.0, lag_ms))

    def record_event_latency(self, stage: str, latency_ms: float) -> None:
        """Record event processing latency."""
        self.event_latency.labels(stage=stage).observe(latency_ms)

    def record_match_confidence(self, confidence: float) -> None:
        """Record match confidence score."""
        self.match_confidence.observe(max(0.0, min(1.0, confidence)))

    def export_text(self) -> bytes:
        """Export metrics in Prometheus text format."""
        return generate_latest()


# Global exporter instance
_exporter: Optional[PrometheusExporter] = None


def get_exporter() -> PrometheusExporter:
    """Get or create global exporter instance."""
    global _exporter
    if _exporter is None:
        _exporter = PrometheusExporter()
    return _exporter


def export_metrics() -> bytes:
    """Export all metrics in Prometheus format."""
    return get_exporter().export_text()

