"""
hfa-agents/src/hfa_agents/integration/semantic_bridge.py

IRONCLAD Sprint 9.2 + 9.5 — Semantic Bridge (Strict Contract + Trace)

Sprint 9.2: strict contract enforcement
  * Replaces getattr(pipeline, "dedup_store", None) silent fallback
  * SemanticPipeline type is validated upfront
  * DegradedPipeline bypasses validation (is_degraded=True)
  * Invalid pipeline type → "semantic_contract_violation" log + pass-through

Sprint 9.5: trace propagation
  * trace_id is extracted from raw_event or generated once as UUID
  * trace_id is attached to all enriched events and all structured logs
  * trace_id flows: cognitive_executor → semantic_bridge → enriched_dict → workflow

Contract:
  STRICT path (pipeline is SemanticPipeline, not degraded):
    * pipeline.dedup_store MUST exist
    * pipeline.watermark   MUST exist
    * pipeline.metrics     MUST exist
    * Missing attribute → contract violation log + pass-through (fail-open)

  DEGRADED path (pipeline.is_degraded=True OR pipeline=None):
    * No validation — pass-through directly
"""

from __future__ import annotations

import inspect
import logging
import uuid
from typing import Any, Dict, Optional

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

    Sprint 9 contract:
      * pipeline=None         → degraded pass-through
      * pipeline.is_degraded  → degraded pass-through
      * SemanticPipeline      → strict attribute validation + enrichment
      * Contract violation    → log + fail-open pass-through (never crash)

    Trace:
      * trace_id extracted from event or generated
      * Propagated in enriched dict and all log calls
    """

    def __init__(self, semantic_pipeline: Any = None) -> None:
        self.semantic_pipeline = semantic_pipeline

    async def enrich_event(
        self,
        raw_event: Dict[str, Any],
        workflow_id: str = "",
        execution_id: str = "",
    ) -> Optional[Dict[str, Any]]:
        """
        Enrich event. Returns enriched dict or None (filtered). Never raises.
        """
        trace_id = _get_or_generate_trace_id(raw_event)

        if self.semantic_pipeline is None:
            logger.debug(
                "SemanticBridge: degraded mode (no pipeline) trace_id=%s", trace_id
            )
            return self._pass_through(raw_event, workflow_id, execution_id, trace_id)

        # DegradedPipeline — skip validation, pass-through
        if getattr(self.semantic_pipeline, "is_degraded", False):
            logger.debug(
                "SemanticBridge: DegradedPipeline trace_id=%s", trace_id
            )
            return self._pass_through(raw_event, workflow_id, execution_id, trace_id)

        try:
            return await self._enrich_strict(
                raw_event, workflow_id, execution_id, trace_id
            )
        except Exception as exc:
            logger.warning(
                "SemanticBridge: enrichment failed, degraded mode trace_id=%s error=%s",
                trace_id, exc,
            )
            return self._pass_through(raw_event, workflow_id, execution_id, trace_id)

    # ── Strict enrichment path ────────────────────────────────────────────────

    async def _enrich_strict(
        self,
        raw_event: Dict[str, Any],
        workflow_id: str,
        execution_id: str,
        trace_id: str,
    ) -> Optional[Dict[str, Any]]:
        """
        Sprint 9.2: strict contract validation.
        SemanticPipeline attributes are required — no silent None fallback.
        """
        pipeline = self.semantic_pipeline

        # ── Contract validation ───────────────────────────────────────────
        missing = [
            attr for attr in ("dedup_store", "watermark", "metrics")
            if not hasattr(pipeline, attr)
        ]
        if missing:
            logger.error(
                "semantic_contract_violation: pipeline=%s missing=%s trace_id=%s",
                type(pipeline).__name__, missing, trace_id,
            )
            # Fail-open: pass through rather than crash
            return self._pass_through(raw_event, workflow_id, execution_id, trace_id)

        logger.debug(
            "semantic_pipeline_validated: pipeline=%s trace_id=%s",
            type(pipeline).__name__, trace_id,
        )

        dedup_store = pipeline.dedup_store
        watermark   = pipeline.watermark
        metrics     = pipeline.metrics

        event_id      = raw_event.get("event_id", "")
        event_time_ms = float(raw_event.get("timestamp_ms", 0) or 0)

        # ── 1. Dedup check ────────────────────────────────────────────────
        try:
            is_dup = await dedup_store.is_duplicate(event_id)
        except Exception as exc:
            logger.warning(
                "SemanticBridge: dedup check failed trace_id=%s: %s", trace_id, exc
            )
            is_dup = False

        if is_dup:
            logger.info(
                "duplicate_event_detected event_id=%s trace_id=%s",
                event_id, trace_id,
            )
            try:
                metrics.inc_dedup()
                metrics.inc_bridge_skip()
            except Exception:
                pass
            return None

        # ── 2. Watermark check ────────────────────────────────────────────
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
                        event_id, trace_id,
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
                    trace_id, exc,
                )

        # ── 3. Mark as processed ──────────────────────────────────────────
        try:
            mark = getattr(dedup_store, "mark_processed", None)
            if mark is not None:
                await mark(event_id, event_time_ms or None)
        except Exception as exc:
            logger.warning(
                "SemanticBridge: mark_processed failed (non-fatal) trace_id=%s: %s",
                trace_id, exc,
            )

        # ── 4. Metrics ────────────────────────────────────────────────────
        try:
            metrics.inc_processed()
            metrics.inc_bridge_pass()
        except Exception:
            pass

        # ── 5. Enrich ─────────────────────────────────────────────────────
        enriched = raw_event.copy()
        enriched["semantic_enriched"] = True
        enriched["workflow_id"]       = workflow_id
        enriched["execution_id"]      = execution_id
        enriched["trace_id"]          = trace_id  # Sprint 9.5: propagate trace

        logger.info(
            "SemanticBridge: enriched event_id=%s trace_id=%s (dedup OK, watermark OK)",
            event_id, trace_id,
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
        enriched["workflow_id"]       = workflow_id
        enriched["execution_id"]      = execution_id
        enriched["trace_id"]          = trace_id  # Sprint 9.5: propagate even in degraded
        return enriched
