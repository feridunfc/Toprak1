"""
hfa-worker/src/hfa_worker/feedback_writer.py

Sprint 9.4 + 9.5 + 10.6 + 10.7 — Feedback Writer

Sprint 10.6: trace_id consistency
  * trace_id presence validated before write
  * Mismatch between run_id and trace_id logged as warning
  * Log: trace_id_missing (if absent), trace_consistency_ok

Sprint 10.7: distributed safety signals
  * feedback_rejected (existing, now explicit for all rejection paths)
  * duplicate_event_detected (when idempotent memory rejects duplicate)
  * memory_write_skipped_duplicate (emitted from memory layer, echoed here)
  * All structured logs carry trace_id

No behavior changes — no blocking, no exception propagation.
"""


from __future__ import annotations

# Sprint 23 semantic advisory contract marker.
# This module may emit advisory/feedback/validation signals only.
# It must not directly mutate canonical runtime truth.
ADVISORY_ONLY_SURFACE = True
CANONICAL_AUTHORITY_WRITES_ALLOWED = False


import logging
import time
from dataclasses import dataclass
from typing import Any

logger = logging.getLogger("hfa.feedback_writer")

_MIN_CONFIDENCE = 0.70
_WRITE_STATUSES = frozenset({"success"})
_MAX_FEEDBACK_OUTPUT_KEYS = 100
_MAX_REASONING_TRACE_ITEMS = 3


@dataclass(frozen=True)
class FeedbackGovernanceDecision:
    accepted: bool
    reason: str
    confidence: float

    @property
    def rejected(self) -> bool:
        return not self.accepted


def validate_feedback_governance(
    *,
    result: Any,
    task_id: str,
    run_id: str,
    tenant_id: str,
    trace_id: str,
    min_confidence: float = _MIN_CONFIDENCE,
) -> FeedbackGovernanceDecision:
    """Hard local governance gate for non-authoritative feedback writes.

    This gate protects the feedback/memory write path only. It must not mutate
    canonical runtime truth and must not promote feedback into authority.
    """
    confidence = float(getattr(result, "confidence", 0.0) or 0.0)
    status = str(getattr(result, "status", "") or "")

    if not task_id:
        return FeedbackGovernanceDecision(False, "missing_task_id", confidence)
    if not run_id:
        return FeedbackGovernanceDecision(False, "missing_run_id", confidence)
    if not tenant_id:
        return FeedbackGovernanceDecision(False, "missing_tenant_id", confidence)
    if not trace_id:
        return FeedbackGovernanceDecision(False, "missing_trace_id", confidence)
    if status not in _WRITE_STATUSES:
        return FeedbackGovernanceDecision(False, f"non_accepted_status:{status or '?'}", confidence)
    if confidence < min_confidence:
        return FeedbackGovernanceDecision(False, f"confidence_below_threshold:{confidence:.3f}", confidence)
    if getattr(result, "requires_hitl", False):
        return FeedbackGovernanceDecision(False, "requires_hitl_pending", confidence)

    output_data = getattr(result, "output_data", None)
    if output_data is None:
        output_data = {}
    if not isinstance(output_data, dict):
        return FeedbackGovernanceDecision(False, "output_data_not_mapping", confidence)
    if len(output_data.keys()) > _MAX_FEEDBACK_OUTPUT_KEYS:
        return FeedbackGovernanceDecision(False, "output_keys_limit_exceeded", confidence)

    reasoning_trace = getattr(result, "reasoning_trace", None)
    if reasoning_trace is None:
        reasoning_trace = []
    if not isinstance(reasoning_trace, list):
        return FeedbackGovernanceDecision(False, "reasoning_trace_not_list", confidence)

    return FeedbackGovernanceDecision(True, "all_checks_passed", confidence)


# ── In-process metrics ────────────────────────────────────────────────────────

class FeedbackMetrics:
    """Lightweight counters for feedback write path."""

    def __init__(self) -> None:
        self.attempted:  int = 0
        self.written:    int = 0
        self.skipped:    int = 0
        self.failed:     int = 0
        self.accepted:   int = 0
        self.rejected:   int = 0
        self._total_latency_ms: float = 0.0
        self._latency_samples:  int = 0

    def inc_attempted(self) -> None: self.attempted += 1
    def inc_written(self)   -> None: self.written += 1
    def inc_skipped(self)   -> None: self.skipped += 1
    def inc_failed(self)    -> None: self.failed += 1
    def inc_accepted(self)  -> None: self.accepted += 1
    def inc_rejected(self)  -> None: self.rejected += 1

    def record_latency(self, ms: float) -> None:
        self._total_latency_ms += ms
        self._latency_samples += 1

    @property
    def avg_latency_ms(self) -> float:
        return (
            self._total_latency_ms / self._latency_samples
            if self._latency_samples > 0 else 0.0
        )

    def snapshot(self) -> dict:
        return {
            "feedback_attempted":       self.attempted,
            "feedback_written":         self.written,
            "feedback_skipped":         self.skipped,
            "feedback_failed":          self.failed,
            "feedback_accepted":        self.accepted,
            "feedback_rejected":        self.rejected,
            "feedback_avg_latency_ms":  round(self.avg_latency_ms, 2),
        }


_metrics = FeedbackMetrics()


def get_feedback_metrics() -> FeedbackMetrics:
    return _metrics


# ── FeedbackWriter ────────────────────────────────────────────────────────────

class FeedbackWriter:
    """
    Validates and persists execution outcomes into hfa-semantic memory.

    Called via asyncio.create_task() — never awaited on the critical path.
    Never raises. All errors are caught and logged.

    Sprint 10.6: trace_id consistency validated on every write.
    Sprint 10.7: structured distributed safety logs on all paths.
    """

    def __init__(
        self,
        semantic_pipeline: Any | None = None,
        min_confidence: float = _MIN_CONFIDENCE,
    ) -> None:
        self._pipeline       = semantic_pipeline
        self._min_confidence = min_confidence
        self._validator      = None

    def _get_validator(self):
        if self._validator is None:
            try:
                from hfa_semantic.validation.feedback_validator import FeedbackValidator
                self._validator = FeedbackValidator(min_confidence=self._min_confidence)
            except ImportError:
                self._validator = _FallbackValidator(self._min_confidence)
        return self._validator

    async def write(
        self,
        execution_result: Any,
        task_id: str,
        run_id: str,
        tenant_id: str,
    ) -> None:
        """Write validated outcome. Never raises."""
        _metrics.inc_attempted()
        trace_id = (
            (getattr(execution_result, "payload", {}) or {}).get("trace_id")
            or run_id
        )
        try:
            await self._write_safe(execution_result, task_id, run_id, tenant_id, trace_id)
        except Exception as exc:
            _metrics.inc_failed()
            logger.warning(
                "FeedbackWriter.write_error task=%s run=%s trace_id=%s error=%s",
                task_id, run_id, trace_id, exc,
            )

    async def _write_safe(
        self,
        result: Any,
        task_id: str,
        run_id: str,
        tenant_id: str,
        trace_id: str,
    ) -> None:
        # ── Sprint 10.6: trace_id consistency check ───────────────────────
        if not trace_id or trace_id == "":
            logger.warning(
                "trace_id_missing task=%s run=%s — feedback write continuing",
                task_id, run_id,
            )
        else:
            logger.debug(
                "trace_consistency_ok task=%s run=%s trace_id=%s",
                task_id, run_id, trace_id,
            )

        # Guard: no pipeline
        if self._pipeline is None:
            logger.debug(
                "FeedbackWriter.skip task=%s trace_id=%s reason=no_pipeline",
                task_id, trace_id,
            )
            _metrics.inc_skipped()
            return

        # Sprint 24: hard local governance gate for feedback writes.
        governance_decision = validate_feedback_governance(
            result=result,
            task_id=task_id,
            run_id=run_id,
            tenant_id=tenant_id,
            trace_id=trace_id,
            min_confidence=self._min_confidence,
        )
        if governance_decision.rejected:
            _metrics.inc_rejected()
            logger.debug(
                "feedback_rejected task=%s run=%s trace_id=%s "
                "reason=%s confidence=%.2f",
                task_id,
                run_id,
                trace_id,
                governance_decision.reason,
                governance_decision.confidence,
            )
            _metrics.inc_skipped()
            return

        # FeedbackValidator gate
        validator = self._get_validator()
        decision  = validator.validate(result)

        if decision.rejected:
            _metrics.inc_rejected()
            logger.debug(
                "feedback_rejected task=%s run=%s trace_id=%s "
                "reason=%s confidence=%.2f",
                task_id, run_id, trace_id, decision.reason, decision.confidence,
            )
            _metrics.inc_skipped()
            return

        _metrics.inc_accepted()
        logger.info(
            "feedback_accepted task=%s run=%s trace_id=%s confidence=%.2f",
            task_id, run_id, trace_id, decision.confidence,
        )

        feedback = {
            "task_id":         task_id,
            "run_id":          run_id,
            "tenant_id":       tenant_id,
            "trace_id":        trace_id,
            "status":          result.status,
            "confidence":      result.confidence,
            "cost_cents":      getattr(result, "cost_cents", 0),
            "output_keys":     list(result.output_data.keys()),
            "reasoning_trace": result.reasoning_trace[-3:],
            "validated_at_ms": int(time.time() * 1000),
        }

        t0 = time.perf_counter()
        try:
            memory = getattr(self._pipeline, "_memory", None)
            if memory is None and hasattr(self._pipeline, "semantic_memory"):
                memory = self._pipeline.semantic_memory

            if memory is not None:
                written = await self._do_write(memory, feedback, task_id, trace_id)
                # Sprint 10.7: detect duplicate
                if not written:
                    logger.info(
                        "duplicate_event_detected task=%s run=%s trace_id=%s",
                        task_id, run_id, trace_id,
                    )
            else:
                if hasattr(self._pipeline, "validate_outcome"):
                    await self._pipeline.validate_outcome(
                        event_id=task_id, outcome=feedback
                    )

            write_ms = round((time.perf_counter() - t0) * 1000)
            _metrics.inc_written()
            _metrics.record_latency(write_ms)
            logger.info(
                "FeedbackWriter.write_ok task=%s run=%s trace_id=%s write_ms=%d",
                task_id, run_id, trace_id, write_ms,
            )

        except Exception as exc:
            write_ms = round((time.perf_counter() - t0) * 1000)
            _metrics.inc_failed()
            logger.warning(
                "FeedbackWriter.write_fail task=%s run=%s trace_id=%s "
                "write_ms=%d error=%s",
                task_id, run_id, trace_id, write_ms, exc,
            )

    async def _do_write(
        self, memory: Any, feedback: dict, task_id: str, trace_id: str
    ) -> bool:
        """Write to memory. Returns True if written, False if duplicate."""
        if hasattr(memory, "record_outcome"):
            result = memory.record_outcome(feedback)
            if hasattr(result, "__await__"):
                result = await result
            return result if isinstance(result, bool) else True
        elif hasattr(memory, "append"):
            result = memory.append(feedback)
            if hasattr(result, "__await__"):
                result = await result
            return result if isinstance(result, bool) else True
        elif hasattr(memory, "write"):
            result = memory.write(feedback)
            if hasattr(result, "__await__"):
                result = await result
            return True
        else:
            logger.debug(
                "FeedbackWriter: memory interface not recognized task=%s trace_id=%s",
                task_id, trace_id,
            )
            return True


# ── Fallback validator ────────────────────────────────────────────────────────

class _FallbackValidator:
    def __init__(self, min_confidence: float) -> None:
        self._min = min_confidence

    def validate(self, result: Any):
        from dataclasses import dataclass

        @dataclass(frozen=True)
        class _D:
            accepted:   bool
            reason:     str
            confidence: float
            @property
            def rejected(self): return not self.accepted

        conf = float(getattr(result, "confidence", 0.0))
        if getattr(result, "status", "") not in ("success",):
            return _D(False, f"non_accepted_status:{getattr(result,'status','?')}", conf)
        if conf < self._min:
            return _D(False, f"confidence_below_threshold:{conf:.3f}", conf)
        if getattr(result, "requires_hitl", False):
            return _D(False, "requires_hitl_pending", conf)
        return _D(True, "all_checks_passed", conf)
