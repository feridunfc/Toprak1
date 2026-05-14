"""
hfa-core/src/hfa/obs/metrics.py
IRONCLAD Sprint 7 / Sprint 12 — Telemetry Baseline + Worker/Control Metrics

Goals
-----
- Centralized metric helper API
- No-op safe when OpenTelemetry is unavailable
- Fire-and-forget instrumentation
- Minimal, stable contract for Sprint 7 Mini3

Notes
-----
- This module does NOT bootstrap exporters/providers.
- Export/bootstrap belongs to a later hardening step.
"""

from __future__ import annotations

import logging
from typing import Optional

logger = logging.getLogger(__name__)

try:
    from opentelemetry import metrics as _otel_metrics

    _OTEL_AVAILABLE = True
except ImportError:
    _OTEL_AVAILABLE = False
    logger.info("OpenTelemetry not installed; HFAMetrics running in no-op mode")


class _Instruments:
    """Lazy singleton container for OTel instruments."""

    _instance: Optional["_Instruments"] = None

    def __init__(self) -> None:
        self._ready = False

        if not _OTEL_AVAILABLE:
            return

        try:
            meter = _otel_metrics.get_meter("hfa.core")

            self.runs_total = meter.create_counter(
                name="hfa_runs_total",
                description="Total number of orchestration runs completed",
                unit="1",
            )
            self.run_latency = meter.create_histogram(
                name="hfa_run_latency_ms",
                description="Run duration in milliseconds",
                unit="ms",
            )
            self.queue_depth = meter.create_up_down_counter(
                name="hfa_queue_depth",
                description="Current orchestrator queue depth",
                unit="1",
            )
            self.sandbox_slots = meter.create_up_down_counter(
                name="hfa_sandbox_slots_active",
                description="Current number of active sandbox slots",
                unit="1",
            )

            # Sprint 12 — worker lifecycle counters
            self.runs_started = meter.create_counter(
                name="hfa_runs_started_total",
                description="Total runs started by workers",
                unit="1",
            )
            self.runs_completed = meter.create_counter(
                name="hfa_runs_completed_total",
                description="Total runs successfully completed",
                unit="1",
            )
            self.runs_failed = meter.create_counter(
                name="hfa_runs_failed_total",
                description="Total runs that reached terminal failure",
                unit="1",
            )
            self.runs_infra_failed = meter.create_counter(
                name="hfa_runs_infra_failed_total",
                description="Total runs that hit infrastructure errors (no ACK)",
                unit="1",
            )
            self.run_execution_duration = meter.create_histogram(
                name="hfa_run_execution_duration_ms",
                description="Run execution wall-clock duration in milliseconds",
                unit="ms",
            )
            self.pending_reclaimed = meter.create_counter(
                name="hfa_pending_reclaimed_total",
                description="Total pending stream messages reclaimed on resume",
                unit="1",
            )
            self.claim_renew = meter.create_counter(
                name="hfa_claim_renew_total",
                description="Successful claim renewal cycles",
                unit="1",
            )
            self.claim_renew_failure = meter.create_counter(
                name="hfa_claim_renew_failure_total",
                description="Failed claim renewal attempts",
                unit="1",
            )
            self.worker_drain_started = meter.create_counter(
                name="hfa_worker_drain_started_total",
                description="Worker drain sequences initiated",
                unit="1",
            )
            self.worker_drain_completed = meter.create_counter(
                name="hfa_worker_drain_completed_total",
                description="Worker drains that completed cleanly",
                unit="1",
            )
            self.worker_drain_timeout = meter.create_counter(
                name="hfa_worker_drain_timeout_total",
                description="Worker drains that exceeded timeout",
                unit="1",
            )
            self.recovery_stale_detected = meter.create_counter(
                name="hfa_recovery_stale_detected_total",
                description="Stale runs detected by recovery sweep",
                unit="1",
            )
            self.recovery_rescheduled = meter.create_counter(
                name="hfa_recovery_rescheduled_total",
                description="Stale runs rescheduled by recovery",
                unit="1",
            )
            self.recovery_dlq = meter.create_counter(
                name="hfa_recovery_dlq_total",
                description="Runs moved to DLQ by recovery",
                unit="1",
            )

            self._ready = True
        except Exception as exc:
            logger.warning("HFAMetrics instrument init failed: %s", exc)

    @classmethod
    def get(cls) -> "_Instruments":
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    @classmethod
    def reset(cls) -> None:
        cls._instance = None


class HFAMetrics:
    """
    Fire-and-forget metric helpers.

    Design constraints:
    - must never break business logic
    - must be safe when OTel is absent
    - must keep label cardinality low
    """

    @staticmethod
    def inc_runs_total(tenant_id: str, status: str) -> None:
        try:
            inst = _Instruments.get()
            if inst._ready:
                inst.runs_total.add(1, {"tenant_id": tenant_id, "status": status})
        except Exception as exc:
            logger.debug("HFAMetrics.inc_runs_total failed: %s", exc)

    @staticmethod
    def record_run_latency(tenant_id: str, duration_ms: float) -> None:
        try:
            inst = _Instruments.get()
            if inst._ready:
                inst.run_latency.record(duration_ms, {"tenant_id": tenant_id})
        except Exception as exc:
            logger.debug("HFAMetrics.record_run_latency failed: %s", exc)

    @staticmethod
    def inc_queue_depth(tenant_id: str) -> None:
        try:
            inst = _Instruments.get()
            if inst._ready:
                inst.queue_depth.add(1, {"tenant_id": tenant_id})
        except Exception as exc:
            logger.debug("HFAMetrics.inc_queue_depth failed: %s", exc)

    @staticmethod
    def dec_queue_depth(tenant_id: str) -> None:
        try:
            inst = _Instruments.get()
            if inst._ready:
                inst.queue_depth.add(-1, {"tenant_id": tenant_id})
        except Exception as exc:
            logger.debug("HFAMetrics.dec_queue_depth failed: %s", exc)

    @staticmethod
    def inc_sandbox_slots(node_id: str) -> None:
        try:
            inst = _Instruments.get()
            if inst._ready:
                inst.sandbox_slots.add(1, {"node_id": node_id})
        except Exception as exc:
            logger.debug("HFAMetrics.inc_sandbox_slots failed: %s", exc)

    @staticmethod
    def dec_sandbox_slots(node_id: str) -> None:
        try:
            inst = _Instruments.get()
            if inst._ready:
                inst.sandbox_slots.add(-1, {"node_id": node_id})
        except Exception as exc:
            logger.debug("HFAMetrics.dec_sandbox_slots failed: %s", exc)

    # ------------------------------------------------------------------
    # Sprint 12 — worker lifecycle helpers
    # ------------------------------------------------------------------

    @staticmethod
    def inc_runs_started(worker_group: str = "") -> None:
        try:
            inst = _Instruments.get()
            if inst._ready:
                inst.runs_started.add(1, {"worker_group": worker_group})
        except Exception as exc:
            logger.debug("HFAMetrics.inc_runs_started failed: %s", exc)

    @staticmethod
    def inc_runs_completed(worker_group: str = "") -> None:
        try:
            inst = _Instruments.get()
            if inst._ready:
                inst.runs_completed.add(1, {"worker_group": worker_group})
        except Exception as exc:
            logger.debug("HFAMetrics.inc_runs_completed failed: %s", exc)

    @staticmethod
    def inc_runs_failed(worker_group: str = "") -> None:
        try:
            inst = _Instruments.get()
            if inst._ready:
                inst.runs_failed.add(1, {"worker_group": worker_group})
        except Exception as exc:
            logger.debug("HFAMetrics.inc_runs_failed failed: %s", exc)

    @staticmethod
    def inc_runs_infra_failed(worker_group: str = "") -> None:
        try:
            inst = _Instruments.get()
            if inst._ready:
                inst.runs_infra_failed.add(1, {"worker_group": worker_group})
        except Exception as exc:
            logger.debug("HFAMetrics.inc_runs_infra_failed failed: %s", exc)

    @staticmethod
    def record_execution_duration(duration_ms: float, worker_group: str = "") -> None:
        try:
            inst = _Instruments.get()
            if inst._ready:
                inst.run_execution_duration.record(
                    duration_ms, {"worker_group": worker_group}
                )
        except Exception as exc:
            logger.debug("HFAMetrics.record_execution_duration failed: %s", exc)

    @staticmethod
    def inc_pending_reclaimed(count: int = 1) -> None:
        try:
            inst = _Instruments.get()
            if inst._ready:
                inst.pending_reclaimed.add(count)
        except Exception as exc:
            logger.debug("HFAMetrics.inc_pending_reclaimed failed: %s", exc)

    @staticmethod
    def inc_claim_renew() -> None:
        try:
            inst = _Instruments.get()
            if inst._ready:
                inst.claim_renew.add(1)
        except Exception as exc:
            logger.debug("HFAMetrics.inc_claim_renew failed: %s", exc)

    @staticmethod
    def inc_claim_renew_failure() -> None:
        try:
            inst = _Instruments.get()
            if inst._ready:
                inst.claim_renew_failure.add(1)
        except Exception as exc:
            logger.debug("HFAMetrics.inc_claim_renew_failure failed: %s", exc)

    @staticmethod
    def inc_worker_drain_started() -> None:
        try:
            inst = _Instruments.get()
            if inst._ready:
                inst.worker_drain_started.add(1)
        except Exception as exc:
            logger.debug("HFAMetrics.inc_worker_drain_started failed: %s", exc)

    @staticmethod
    def inc_worker_drain_completed() -> None:
        try:
            inst = _Instruments.get()
            if inst._ready:
                inst.worker_drain_completed.add(1)
        except Exception as exc:
            logger.debug("HFAMetrics.inc_worker_drain_completed failed: %s", exc)

    @staticmethod
    def inc_worker_drain_timeout() -> None:
        try:
            inst = _Instruments.get()
            if inst._ready:
                inst.worker_drain_timeout.add(1)
        except Exception as exc:
            logger.debug("HFAMetrics.inc_worker_drain_timeout failed: %s", exc)

    @staticmethod
    def inc_recovery_stale_detected(count: int = 1) -> None:
        try:
            inst = _Instruments.get()
            if inst._ready:
                inst.recovery_stale_detected.add(count)
        except Exception as exc:
            logger.debug("HFAMetrics.inc_recovery_stale_detected failed: %s", exc)

    @staticmethod
    def inc_recovery_rescheduled() -> None:
        try:
            inst = _Instruments.get()
            if inst._ready:
                inst.recovery_rescheduled.add(1)
        except Exception as exc:
            logger.debug("HFAMetrics.inc_recovery_rescheduled failed: %s", exc)

    @staticmethod
    def inc_recovery_dlq() -> None:
        try:
            inst = _Instruments.get()
            if inst._ready:
                inst.recovery_dlq.add(1)
        except Exception as exc:
            logger.debug("HFAMetrics.inc_recovery_dlq failed: %s", exc)
