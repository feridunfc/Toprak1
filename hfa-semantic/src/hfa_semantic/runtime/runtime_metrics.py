"""
hfa-semantic/src/hfa_semantic/runtime/runtime_metrics.py

IRONCLAD Sprint 6.1 — Runtime Metrics + Observability

Sprint 6.1 additions:
  * inc_degraded()    — semantic pipeline running in degraded mode
  * inc_bridge_pass() — event passed through by SemanticBridge
  * inc_bridge_skip() — event dropped by SemanticBridge (dup/late)
  * get_global_metrics() — returns the module-level singleton
  * aggregate_snapshot() — merges cognitive + feedback + semantic metrics
    into one dict for log emission or health endpoints

All existing inc_* methods and aliases are unchanged.
"""

from __future__ import annotations

import time
from typing import Optional


class SemanticMetrics:
    """
    Lightweight in-process counters and gauges for semantic runtime observability.

    Sprint 6.1 additions:
      inc_degraded()    — pipeline running without Redis/semantic
      inc_bridge_pass() — SemanticBridge passed event through (enriched)
      inc_bridge_skip() — SemanticBridge dropped event (dedup/late/filtered)
      snapshot()        — point-in-time dict of all counters
    """

    def __init__(self) -> None:
        # ── Existing counters (unchanged) ──────────────────────────────────
        self.events_processed:    int = 0
        self.dedup_hits:          int = 0
        self.late_events_dropped: int = 0
        self.matches_emitted:     int = 0
        self.evictions:           int = 0

        # ── State size gauge ────────────────────────────────────────────────
        self.state_size: int = 0

        # ── Processing latency ──────────────────────────────────────────────
        self.last_processing_latency_ms: float = 0.0
        self._total_latency_ms:          float = 0.0
        self._latency_samples:           int = 0

        # ── Sprint 6.1: degraded + bridge counters ──────────────────────────
        self.degraded_mode_runs: int = 0   # times semantic ran without pipeline
        self.bridge_pass:        int = 0   # SemanticBridge enriched+passed
        self.bridge_skip:        int = 0   # SemanticBridge dropped (dup/late)

        self._start_time_ms: float = time.time() * 1000.0

    # ── Existing inc_* interface (unchanged) ──────────────────────────────────

    def inc_processed(self)    -> None: self.events_processed += 1
    def inc_dedup(self)        -> None: self.dedup_hits += 1
    def inc_late_dropped(self) -> None: self.late_events_dropped += 1
    def inc_matches(self)      -> None: self.matches_emitted += 1
    def inc_eviction(self)     -> None: self.evictions += 1


    @property
    def late_events(self) -> int:
        """Alias for late_events_dropped (test backward compat)."""
        return self.late_events_dropped

    # ── Sprint 6.1: new counters ──────────────────────────────────────────────

    def inc_degraded(self)     -> None: self.degraded_mode_runs += 1
    def inc_bridge_pass(self)  -> None: self.bridge_pass += 1
    def inc_bridge_skip(self)  -> None: self.bridge_skip += 1

    # ── State size ────────────────────────────────────────────────────────────

    def set_state_size(self, n: int) -> None:
        self.state_size = max(0, n)

    # ── Latency ───────────────────────────────────────────────────────────────

    def record_processing_latency(self, latency_ms: float) -> None:
        self.last_processing_latency_ms = latency_ms
        self._total_latency_ms += latency_ms
        self._latency_samples += 1

    @property
    def avg_processing_latency_ms(self) -> float:
        if self._latency_samples == 0:
            return 0.0
        return self._total_latency_ms / self._latency_samples

    # ── Eviction rate ─────────────────────────────────────────────────────────

    @property
    def eviction_rate(self) -> float:
        elapsed_sec = (time.time() * 1000.0 - self._start_time_ms) / 1000.0
        if elapsed_sec < 1.0:
            return 0.0
        return self.evictions / elapsed_sec

    # ── Snapshot ──────────────────────────────────────────────────────────────

    def snapshot(self) -> dict:
        return {
            "events_processed":          self.events_processed,
            "dedup_hits":                self.dedup_hits,
            "late_events_dropped":       self.late_events_dropped,
            "matches_emitted":           self.matches_emitted,
            "evictions":                 self.evictions,
            "state_size":                self.state_size,
            "last_processing_latency_ms": self.last_processing_latency_ms,
            "avg_processing_latency_ms":  round(self.avg_processing_latency_ms, 2),
            "eviction_rate_per_sec":      round(self.eviction_rate, 4),
            "degraded_mode_runs":         self.degraded_mode_runs,
            "bridge_pass":                self.bridge_pass,
            "bridge_skip":                self.bridge_skip,
        }


    # ── High-level record_* API (test + observability compat) ────────────────

    def record_event_processed(self, rule_id: str = "") -> None:
        self.events_processed += 1

    def record_match_emitted(self, rule_id: str = "", severity: str = "") -> None:
        self.matches_emitted += 1

    def record_dedup_hit(self) -> None:
        self.dedup_hits += 1

    def record_late_event(self, policy: str = "") -> None:
        self.late_events_dropped += 1

    def record_confidence(self, confidence: float) -> None:
        """No-op gauge placeholder — confidence tracked externally."""
        pass

    def set_watermark_lag(self, lag_ms: float) -> None:
        """No-op gauge placeholder — watermark lag is a gauge metric."""
        pass

    def record_event_latency(self, stage: str = "", latency_ms: float = 0.0) -> None:
        self.record_processing_latency(latency_ms)

    def reset(self) -> None:
        """Reset all counters. For testing only."""
        self.__init__()  # type: ignore[misc]


# ── Module-level singleton ────────────────────────────────────────────────────

_global_metrics: Optional[SemanticMetrics] = None


def get_global_metrics() -> SemanticMetrics:
    """Return the module-level SemanticMetrics singleton (lazy init)."""
    global _global_metrics
    if _global_metrics is None:
        _global_metrics = SemanticMetrics()
    return _global_metrics


def aggregate_snapshot() -> dict:
    """
    Aggregate metrics from semantic + cognitive + feedback into one dict.

    Safe to call even if cognitive/feedback packages are not installed —
    their metrics are simply omitted from the result.
    """
    result: dict = {"semantic": get_global_metrics().snapshot()}

    try:
        from hfa_worker.cognitive_executor import get_cognitive_metrics
        result["cognitive"] = get_cognitive_metrics().snapshot()
    except ImportError:
        pass

    try:
        from hfa_worker.feedback_writer import get_feedback_metrics
        result["feedback"] = get_feedback_metrics().snapshot()
    except ImportError:
        pass

    result["collected_at_ms"] = int(time.time() * 1000)
    return result


# Alias — __init__.py exports RuntimeMetrics
RuntimeMetrics = SemanticMetrics
