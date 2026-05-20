"""
hfa-agents/src/hfa_agents/integration/semantic_bridge.py

Semantic Bridge

Default behavior remains advisory/degraded pass-through. Sprint 7 adds an
explicit gate path that is separate from advisory enrichment. Advisory may fail
open; gate must fail closed and return an audit/replay-visible verdict.
"""


# Sprint 23 semantic advisory contract marker.
# This module may emit advisory/feedback/validation signals only.
# It must not directly mutate canonical runtime truth.
ADVISORY_ONLY_SURFACE = True
CANONICAL_AUTHORITY_WRITES_ALLOWED = False

from __future__ import annotations

import inspect
import logging
import uuid
from typing import Any, Dict, Optional

try:
    from hfa_semantic.runtime.semantic_hook import (
        SemanticVerdict,
        evaluate_advisory_semantics,
        evaluate_gate_semantics,
    )
except Exception:  # pragma: no cover - compatibility when semantic package absent
    SemanticVerdict = None  # type: ignore[assignment]
    evaluate_advisory_semantics = None  # type: ignore[assignment]
    evaluate_gate_semantics = None  # type: ignore[assignment]

logger = logging.getLogger(__name__)


def _get_or_generate_trace_id(raw_event: Dict[str, Any]) -> str:
    """Extract trace_id from event or generate a fresh UUID."""

    trace_id = raw_event.get("trace_id") or raw_event.get("run_id") or ""
    if not trace_id:
        trace_id = uuid.uuid4().hex[:16]
    return trace_id


class SemanticBridge:
    """
    Translates a raw event dict into a semantically enriched dict.

    Advisory enrichment remains fail-open. Gate evaluation is explicit via
    ``evaluate_gate`` and must not be collapsed back into enrichment.
    """

    def __init__(self, semantic_pipeline: Any = None, gate_evaluator: Any = None) -> None:
        self.semantic_pipeline = semantic_pipeline
        self.gate_evaluator = gate_evaluator if gate_evaluator is not None else semantic_pipeline

    async def enrich_event(
        self,
        raw_event: Dict[str, Any],
        workflow_id: str = "",
        execution_id: str = "",
    ) -> Optional[Dict[str, Any]]:
        """
        Advisory enrich event. Returns enriched dict or None (filtered). Never raises.
        """

        trace_id = _get_or_generate_trace_id(raw_event)

        if self.semantic_pipeline is None:
            logger.debug(
                "SemanticBridge: degraded advisory mode (no pipeline) trace_id=%s", trace_id
            )
            return self._pass_through(raw_event, workflow_id, execution_id, trace_id)

        if getattr(self.semantic_pipeline, "is_degraded", False):
            logger.debug("SemanticBridge: DegradedPipeline trace_id=%s", trace_id)
            return self._pass_through(raw_event, workflow_id, execution_id, trace_id)

        try:
            return await self._enrich_strict(raw_event, workflow_id, execution_id, trace_id)
        except Exception as exc:
            logger.warning(
                "SemanticBridge: advisory enrichment failed, degraded mode trace_id=%s error=%s",
                trace_id,
                exc,
            )
            return self._pass_through(raw_event, workflow_id, execution_id, trace_id)

    async def evaluate_advisory(self, raw_event: Dict[str, Any]) -> Any:
        """Return an advisory semantic verdict; failures may fail open."""

        if evaluate_advisory_semantics is None:
            return {"mode": "advisory", "allowed": True, "reason": "semantic_hook_unavailable"}
        return await evaluate_advisory_semantics(self.semantic_pipeline, raw_event)

    async def evaluate_gate(self, raw_event: Dict[str, Any]) -> Any:
        """Return a gate semantic verdict; v3 gate mode fails closed."""

        if evaluate_gate_semantics is None:
            return {
                "mode": "gate",
                "allowed": False,
                "reason": "semantic_hook_unavailable",
                "replay_visible": True,
                "audit_visible": True,
            }
        return await evaluate_gate_semantics(self.gate_evaluator, raw_event)

    # ── Strict advisory enrichment path ───────────────────────────────────────

    async def _enrich_strict(
        self,
        raw_event: Dict[str, Any],
        workflow_id: str,
        execution_id: str,
        trace_id: str,
    ) -> Optional[Dict[str, Any]]:
        pipeline = self.semantic_pipeline

        missing = [
            attr for attr in ("dedup_store", "watermark", "metrics")
            if not hasattr(pipeline, attr)
        ]
        if missing:
            logger.error(
                "semantic_contract_violation: pipeline=%s missing=%s trace_id=%s",
                type(pipeline).__name__,
                missing,
                trace_id,
            )
            return self._pass_through(raw_event, workflow_id, execution_id, trace_id)

        logger.debug(
            "semantic_pipeline_validated: pipeline=%s trace_id=%s",
            type(pipeline).__name__,
            trace_id,
        )

        dedup_store = pipeline.dedup_store
        watermark = pipeline.watermark
        metrics = pipeline.metrics

        event_id = raw_event.get("event_id", "")
        event_time_ms = float(raw_event.get("timestamp_ms", 0) or 0)

        try:
            is_dup = await dedup_store.is_duplicate(event_id)
        except Exception as exc:
            logger.warning("SemanticBridge: dedup check failed trace_id=%s: %s", trace_id, exc)
            is_dup = False

        if is_dup:
            logger.info("duplicate_event_detected event_id=%s trace_id=%s", event_id, trace_id)
            try:
                metrics.inc_dedup()
                metrics.inc_bridge_skip()
            except Exception:
                pass
            return None

        if event_time_ms > 0:
            try:
                observe_result = watermark.observe(event_time_ms)
                if inspect.isawaitable(observe_result):
                    observe_result = await observe_result

                if isinstance(observe_result, (list, tuple)) and len(observe_result) >= 2:
                    is_late = observe_result[0]
                else:
                    is_late = bool(observe_result)

                if is_late:
                    logger.warning(
                        "SemanticBridge: late event dropped event_id=%s trace_id=%s",
                        event_id,
                        trace_id,
                    )
                    try:
                        metrics.inc_late_dropped()
                        metrics.inc_bridge_skip()
                    except Exception:
                        pass
                    return None
            except Exception as exc:
                logger.warning(
                    "SemanticBridge: watermark check failed (continuing) trace_id=%s: %s",
                    trace_id,
                    exc,
                )

        try:
            mark = getattr(dedup_store, "mark_processed", None)
            if mark is not None:
                await mark(event_id, event_time_ms or None)
        except Exception as exc:
            logger.warning(
                "SemanticBridge: mark_processed failed (non-fatal) trace_id=%s: %s",
                trace_id,
                exc,
            )

        try:
            metrics.inc_processed()
            metrics.inc_bridge_pass()
        except Exception:
            pass

        enriched = raw_event.copy()
        enriched["semantic_enriched"] = True
        enriched["semantic_mode"] = "advisory"
        enriched["workflow_id"] = workflow_id
        enriched["execution_id"] = execution_id
        enriched["trace_id"] = trace_id

        logger.info(
            "SemanticBridge: enriched event_id=%s trace_id=%s (dedup OK, watermark OK)",
            event_id,
            trace_id,
        )
        return enriched

    @staticmethod
    def _pass_through(
        raw_event: Dict[str, Any],
        workflow_id: str,
        execution_id: str,
        trace_id: str,
    ) -> Dict[str, Any]:
        enriched = raw_event.copy()
        enriched["semantic_enriched"] = False
        enriched["semantic_mode"] = "advisory"
        enriched["workflow_id"] = workflow_id
        enriched["execution_id"] = execution_id
        enriched["trace_id"] = trace_id
        return enriched
