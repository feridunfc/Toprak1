from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any, Dict, Optional

from hfa_semantic.api.models import RawEvent
from hfa_semantic.runtime.dedup_store import DedupStore
from hfa_semantic.runtime.eviction import CardinalityGuard
from hfa_semantic.runtime.lateness_policy import LatenessAction, LatenessPolicy
from hfa_semantic.runtime.partitioning import partition_key
from hfa_semantic.runtime.runtime_metrics import SemanticMetrics
from hfa_semantic.runtime.state_store import StateStore
from hfa_semantic.runtime.watermark import WatermarkManager

try:
    from hfa_semantic.validation.safety_verdict import SafetyVerdict
except Exception:  # pragma: no cover - old packaging compatibility
    SafetyVerdict = None  # type: ignore[assignment]


@dataclass(slots=True)
class SemanticEventContext:
    event_id: str
    partition_key: str
    event_time_ms: int
    processing_time_ms: int
    is_late: bool = False
    lateness_action: str = "drop"


class SemanticRuntimeEngine:
    def __init__(
        self,
        state_store: StateStore,
        dedup_store: DedupStore,
        watermark: WatermarkManager,
        metrics: SemanticMetrics,
        cardinality_guard: CardinalityGuard | None = None,
        lateness_policy: LatenessPolicy | None = None,
        merge_engine: Any | None = None,
    ) -> None:
        self._state = state_store
        self._dedup = dedup_store
        self._watermark = watermark
        self._metrics = metrics
        self._cardinality = cardinality_guard or CardinalityGuard()
        self._lateness = lateness_policy or LatenessPolicy()
        self.merge_engine = merge_engine

    async def process_event(self, raw: dict | RawEvent) -> tuple[bool, Optional[Dict[str, Any]]]:
        started = time.perf_counter()
        evt = raw if isinstance(raw, RawEvent) else RawEvent.model_validate(raw)
        ctx = SemanticEventContext(
            event_id=evt.event_id,
            partition_key=partition_key(evt),
            event_time_ms=evt.timestamp_ms,
            processing_time_ms=int(time.time() * 1000),
        )

        if await self._dedup.is_duplicate(ctx.event_id):
            self._metrics.inc_dedup()
            return False, None

        is_late, action = self._watermark.evaluate(ctx.event_time_ms)
        ctx.is_late = is_late
        ctx.lateness_action = action.value

        if is_late:
            self._metrics.inc_late_dropped()
            if action == LatenessAction.DROP:
                return False, None

        self._watermark.observe(ctx.event_time_ms)

        if not self._cardinality.is_safe_to_allocate("global"):
            self._metrics.inc_eviction()
            return False, None

        self._metrics.inc_processed()
        result = {
            "raw": evt.model_dump(),
            "context": ctx.__dict__,
            "partition_key": ctx.partition_key,
            "watermark_ms": self._watermark.current_watermark_ms,
            "latency_ms": (time.perf_counter() - started) * 1000,
            "semantic_matches": [],
        }

        verdict_payload = self._extract_safety_verdict_payload(raw)
        if verdict_payload is not None:
            result["safety_verdict"] = verdict_payload
            result["audit_visible"] = bool(verdict_payload.get("audit_visible", True))
            result["replay_visible"] = bool(verdict_payload.get("replay_visible", True))

        return True, result

    @staticmethod
    def _extract_safety_verdict_payload(raw: dict | RawEvent) -> dict[str, Any] | None:
        """Carry policy/safety verdicts into semantic outputs.

        This keeps Sprint 7 verdicts visible to audit/replay surfaces without
        changing the main event-processing contract.
        """

        if isinstance(raw, RawEvent):
            raw_map = raw.model_dump()
        else:
            raw_map = dict(raw)

        verdict = raw_map.get("safety_verdict") or raw_map.get("semantic_verdict")
        if verdict is None:
            return None
        if SafetyVerdict is not None and isinstance(verdict, SafetyVerdict):
            return verdict.to_event_payload()
        if hasattr(verdict, "to_event_payload"):
            return dict(verdict.to_event_payload())
        if isinstance(verdict, dict):
            payload = dict(verdict)
            payload.setdefault("audit_visible", True)
            payload.setdefault("replay_visible", True)
            return payload
        return {
            "allowed": bool(verdict),
            "reason": "boolean_semantic_verdict",
            "audit_visible": True,
            "replay_visible": True,
        }
