"""Feedback integration client for Sprint 5 worker/effect closure."""
from __future__ import annotations

from typing import Any

from hfa_control.feedback.handle_result import CorrectionFeedbackDecision, handle_result


class FeedbackClient:
    """Small adapter that normalizes feedback through the single control path."""

    def __init__(self, sink: Any | None = None) -> None:
        self._sink = sink

    async def report_result(self, result: Any, *, strict_mode: bool = False) -> CorrectionFeedbackDecision:
        decision = handle_result(result, strict_mode=strict_mode)
        if self._sink is not None and decision.should_record_feedback:
            record = getattr(self._sink, "record", None) or getattr(self._sink, "write", None)
            if callable(record):
                maybe = record(decision)
                if hasattr(maybe, "__await__"):
                    await maybe
        return decision
