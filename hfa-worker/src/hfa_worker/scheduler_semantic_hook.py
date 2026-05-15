"""
hfa-worker/src/hfa_worker/scheduler_semantic_hook.py

Scheduler/worker semantic hook utilities.

The prewarm hook is advisory and fire-and-forget. Sprint 7 adds a separate gate
helper for explicit semantic gate checks. Do not use the prewarm hook as an
authoritative gate.
"""
from __future__ import annotations

import asyncio
import logging
import time
from typing import Any

try:
    from hfa_semantic.runtime.semantic_hook import evaluate_gate_semantics
except Exception:  # pragma: no cover - semantic package optional in worker tests
    evaluate_gate_semantics = None  # type: ignore[assignment]

logger = logging.getLogger("hfa.scheduler_semantic_hook")


def fire_semantic_prewarm(
    *,
    run_id: str,
    tenant_id: str,
    agent_type: str,
    pipeline: Any | None,
) -> None:
    """
    Advisory fire-and-forget: inform hfa-semantic that a task was scheduled.

    This must never become an authoritative gate. Failure is logged at debug
    level and does not affect scheduler/worker progress.
    """

    if pipeline is None:
        return

    async def _prewarm() -> None:
        try:
            event = {
                "event_id": f"sched:{run_id}",
                "event_type": f"scheduled:{agent_type}",
                "tenant_id": tenant_id,
                "run_id": run_id,
                "timestamp_ms": int(time.time() * 1000),
                "goal": "",
            }
            await pipeline.process(event=event, rule_id="scheduler_prewarm")
        except Exception as exc:
            logger.debug("semantic advisory prewarm failed run=%s: %s", run_id, exc)

    try:
        asyncio.create_task(_prewarm())
    except RuntimeError:
        pass


async def evaluate_scheduler_semantic_gate(
    *,
    run_id: str,
    tenant_id: str,
    agent_type: str,
    evaluator: Any | None,
    payload: dict[str, Any] | None = None,
) -> Any:
    """Evaluate an explicit semantic gate before execution/dispatch coupling.

    Missing semantic hook support is fail-closed because this helper is only for
    gate mode. Advisory callers must continue to use ``fire_semantic_prewarm``.
    """

    event = {
        "event_id": f"gate:{run_id}",
        "event_type": f"semantic_gate:{agent_type}",
        "tenant_id": tenant_id,
        "run_id": run_id,
        "timestamp_ms": int(time.time() * 1000),
        **(payload or {}),
    }
    if evaluate_gate_semantics is None:
        return {
            "mode": "gate",
            "allowed": False,
            "reason": "semantic_gate_unavailable",
            "replay_visible": True,
            "audit_visible": True,
        }
    return await evaluate_gate_semantics(evaluator, event)
