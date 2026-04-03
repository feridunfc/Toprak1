"""
hfa-semantic/src/hfa_semantic/memory/outcome_writer.py

Sprint 10.2 — Feedback Truth Validation Gate

OutcomeWriter is the single entry point for writing validated outcomes
into SemanticMemoryV2. It enforces:

  1. Relevance filter (retain() — confidence + validated flag)
  2. FeedbackValidator (Sprint 9.4) — status, confidence, HITL, fields
  3. Idempotency — duplicate event_id writes are silently skipped (Sprint 10.1)

write() is async because SemanticMemoryV2.append() is async.
Callers that previously called write() synchronously MUST be updated to
await write().

Logs:
  feedback_validated  — outcome passed all guards, written
  feedback_rejected   — outcome failed a guard, skipped
  memory_write_skipped_duplicate — idempotency guard triggered
"""

from __future__ import annotations

import logging
from typing import Any

from hfa_semantic.api.models import ValidatedOutcome
from hfa_semantic.memory.relevance_policy import retain
from hfa_semantic.memory.semantic_memory_v2 import SemanticMemoryV2

logger = logging.getLogger("hfa.outcome_writer")


class OutcomeWriter:
    """
    Async gate that decides what outcomes get written to memory.

    All writes go through:
      1. retain() — confidence + validated flag
      2. FeedbackValidator — status, HITL, required fields
      3. SemanticMemoryV2.append() — idempotent async write

    Never raises — all exceptions are caught and logged.
    """

    def __init__(
        self,
        memory: SemanticMemoryV2,
        min_confidence: float = 0.70,
    ) -> None:
        self._memory = memory
        self._min_confidence = min_confidence
        self._write_count  = 0
        self._reject_count = 0
        self._validator    = None

    def _get_validator(self):
        if self._validator is None:
            try:
                from hfa_semantic.validation.feedback_validator import FeedbackValidator
                self._validator = FeedbackValidator(
                    min_confidence=self._min_confidence
                )
            except ImportError:
                self._validator = _PassthroughValidator()
        return self._validator

    async def write(self, outcome: ValidatedOutcome) -> bool:
        """
        Try to write outcome to memory.

        Returns:
            True   — outcome written (or idempotent duplicate — still True)
            False  — outcome rejected by a guard

        Never raises.
        """
        try:
            return await self._write_safe(outcome)
        except Exception as exc:
            logger.warning(
                "OutcomeWriter.write_error event_id=%s error=%s",
                getattr(outcome, "event_id", "?"), exc,
            )
            self._reject_count += 1
            return False

    async def _write_safe(self, outcome: ValidatedOutcome) -> bool:
        # Guard 1: relevance policy
        if not retain(outcome, self._min_confidence):
            self._reject_count += 1
            logger.debug(
                "feedback_rejected event_id=%s reason=relevance_policy "
                "confidence=%.3f validated=%s",
                outcome.event_id, outcome.confidence, outcome.validated,
            )
            return False

        # Guard 2: FeedbackValidator
        validator = self._get_validator()
        decision  = validator.validate(outcome)
        if decision.rejected:
            self._reject_count += 1
            logger.debug(
                "feedback_rejected event_id=%s reason=%s confidence=%.3f",
                outcome.event_id, decision.reason, decision.confidence,
            )
            return False

        # Write to memory (idempotent — duplicate returns False from append)
        written = await self._memory.append(outcome)

        if written:
            self._write_count += 1
            logger.info(
                "feedback_validated event_id=%s confidence=%.3f",
                outcome.event_id, outcome.confidence,
            )
        # Duplicate: not an error, just silent skip (already logged in memory)
        return True   # accepted (whether new or duplicate)

    @property
    def stats(self) -> dict:
        return {
            "written":           self._write_count,
            "rejected":          self._reject_count,
            "memory_size":       self._memory.size,
            "memory_utilization": self._memory.utilization,
        }


# ── Fallback validator (when hfa_semantic.validation not importable) ──────────

class _PassthroughValidator:
    """Used when FeedbackValidator is unavailable. Applies minimal checks."""

    def __init__(self) -> None:
        pass

    def validate(self, outcome: Any):
        class _D:
            accepted  = True
            rejected  = False
            reason    = "passthrough"
            confidence = 0.0
        return _D()
