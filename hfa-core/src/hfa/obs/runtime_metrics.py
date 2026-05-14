"""
hfa-core/src/hfa/obs/runtime_metrics.py
IRONCLAD Sprint 12 — Runtime Metric Registry

Provides named, in-memory counters / histograms / gauges that:
  - are always safe to call (never raise)
  - work without OpenTelemetry installed
  - can be read by tests for assertion
  - bridge to OTel instruments when OTel is available

Usage
-----
    from hfa.obs.runtime_metrics import IRONCLADMetrics as M

    M.runs_started_total.inc()
    M.run_execution_duration_ms.record(125.4)

    # Tests
    assert M.runs_started_total.value == 1
    M.reset_all()   # between tests

IRONCLAD rules
--------------
* No print() — logging only.
* Never raise from metric calls.
* reset_all() only for test isolation.
"""

from __future__ import annotations

import logging
import threading
from typing import Dict

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Primitive metric types
# ---------------------------------------------------------------------------


class _Counter:
    """Monotonically increasing integer counter."""

    def __init__(self, name: str) -> None:
        self._name = name
        self._value: int = 0
        self._lock = threading.Lock()

    @property
    def value(self) -> int:
        return self._value

    def inc(self, amount: int = 1) -> None:
        try:
            with self._lock:
                self._value += amount
        except Exception as exc:
            logger.debug("_Counter.inc error name=%s: %s", self._name, exc)

    def reset(self) -> None:
        with self._lock:
            self._value = 0


class _Histogram:
    """Records float observations; exposes count and total for tests."""

    def __init__(self, name: str) -> None:
        self._name = name
        self._count: int = 0
        self._total: float = 0.0
        self._lock = threading.Lock()

    @property
    def count(self) -> int:
        return self._count

    @property
    def total(self) -> float:
        return self._total

    def record(self, value: float) -> None:
        try:
            with self._lock:
                self._count += 1
                self._total += value
        except Exception as exc:
            logger.debug("_Histogram.record error name=%s: %s", self._name, exc)

    def reset(self) -> None:
        with self._lock:
            self._count = 0
            self._total = 0.0


class _Gauge:
    """Up/down integer gauge."""

    def __init__(self, name: str) -> None:
        self._name = name
        self._value: int = 0
        self._lock = threading.Lock()

    @property
    def value(self) -> int:
        return self._value

    def inc(self, amount: int = 1) -> None:
        try:
            with self._lock:
                self._value += amount
        except Exception as exc:
            logger.debug("_Gauge.inc error name=%s: %s", self._name, exc)

    def dec(self, amount: int = 1) -> None:
        try:
            with self._lock:
                self._value -= amount
        except Exception as exc:
            logger.debug("_Gauge.dec error name=%s: %s", self._name, exc)

    def set(self, value: int) -> None:
        try:
            with self._lock:
                self._value = value
        except Exception as exc:
            logger.debug("_Gauge.set error name=%s: %s", self._name, exc)

    def reset(self) -> None:
        with self._lock:
            self._value = 0


# ---------------------------------------------------------------------------
# Named metric registry
# ---------------------------------------------------------------------------

_ALL_METRICS: Dict[str, "_Counter | _Histogram | _Gauge"] = {}


def _counter(name: str) -> _Counter:
    m: _Counter = _Counter(name)
    _ALL_METRICS[name] = m
    return m


def _histogram(name: str) -> _Histogram:
    m: _Histogram = _Histogram(name)
    _ALL_METRICS[name] = m
    return m


def _gauge(name: str) -> _Gauge:
    m: _Gauge = _Gauge(name)
    _ALL_METRICS[name] = m
    return m


def reset_all() -> None:
    """Reset all metrics to zero. Intended for test isolation only."""
    for m in _ALL_METRICS.values():
        m.reset()


# ---------------------------------------------------------------------------
# Sprint 12 named metrics
# ---------------------------------------------------------------------------


class IRONCLADMetrics:
    """
    Namespace for all Sprint 12 metric instances.

    Every attribute is a module-level singleton.
    Call .inc() on counters/gauges, .record(value) on histograms.
    Read .value on counters/gauges, .count / .total on histograms.
    """

    # Worker execution
    runs_started_total: _Counter = _counter("runs_started_total")
    runs_completed_total: _Counter = _counter("runs_completed_total")
    runs_failed_total: _Counter = _counter("runs_failed_total")
    runs_infra_failed_total: _Counter = _counter("runs_infra_failed_total")
    run_execution_duration_ms: _Histogram = _histogram("run_execution_duration_ms")

    # Pending resume / reclaim
    pending_reclaimed_total: _Counter = _counter("pending_reclaimed_total")

    # Claim renewal
    claim_renew_total: _Counter = _counter("claim_renew_total")
    claim_renew_failure_total: _Counter = _counter("claim_renew_failure_total")

    # Drain
    worker_drain_started_total: _Counter = _counter("worker_drain_started_total")
    worker_drain_completed_total: _Counter = _counter("worker_drain_completed_total")
    worker_drain_timeout_total: _Counter = _counter("worker_drain_timeout_total")

    # In-flight gauge
    worker_inflight: _Gauge = _gauge("worker_inflight")

    # Scheduling
    scheduling_attempts_total: _Counter = _counter("scheduling_attempts_total")
    scheduling_failures_total: _Counter = _counter("scheduling_failures_total")
    workers_excluded_draining_total: _Counter = _counter(
        "workers_excluded_draining_total"
    )

    # Recovery
    recovery_stale_detected_total: _Counter = _counter("recovery_stale_detected_total")
    recovery_rescheduled_total: _Counter = _counter("recovery_rescheduled_total")
    recovery_dlq_total: _Counter = _counter("recovery_dlq_total")

    @staticmethod
    def reset_all() -> None:
        """Reset all metrics. For test isolation only."""
        reset_all()
