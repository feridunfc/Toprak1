"""
hfa-control/src/hfa_control/scheduler_semantic_hook.py
IRONCLAD OS — Scheduler → Semantic Pre-warming Hook

PURPOSE
-------
Before a task enters the dispatch queue, this hook fires an async
background signal to hfa-semantic so the semantic pipeline can:
  - Pre-warm window state for tenant
  - Start watermark tracking for the event stream
  - Record the admission event for ordering guarantees

CRITICAL RULES
--------------
1. This is ALWAYS fire-and-forget (asyncio.create_task)
2. NEVER awaited inside SchedulerLoop
3. Never raises — exceptions are swallowed
4. Adds ZERO latency to the scheduler tick

HOW TO WIRE (one line in scheduler_loop.py or dispatch_controller.py)
----------------------------------------------------------------------
  from hfa_control.scheduler_semantic_hook import fire_semantic_prewarm

  # After successful Lua dispatch commit:
  fire_semantic_prewarm(run_id=run_id, tenant_id=tenant_id,
                        agent_type=agent_type, pipeline=self._semantic_pipeline)

The semantic_pipeline is injected at startup via DI — see worker_main.py patch.
"""
from __future__ import annotations

import asyncio
import logging
import time
from typing import Any, Optional

logger = logging.getLogger("hfa.scheduler_semantic_hook")


def fire_semantic_prewarm(
    *,
    run_id: str,
    tenant_id: str,
    agent_type: str,
    pipeline: Any | None,
) -> None:
    """
    Fire-and-forget: inform hfa-semantic that a task was just scheduled.

    This is called INSIDE the scheduler but never awaited.
    Failure is silent — semantic is a sidecar, not critical path.
    """
    if pipeline is None:
        return

    async def _prewarm() -> None:
        try:
            event = {
                "event_id":     f"sched:{run_id}",
                "event_type":   f"scheduled:{agent_type}",
                "tenant_id":    tenant_id,
                "run_id":       run_id,
                "timestamp_ms": int(time.time() * 1000),
                "goal":         "",  # goal unknown at scheduling time
            }
            await pipeline.process(event=event, rule_id="scheduler_prewarm")
        except Exception as exc:
            logger.debug("semantic prewarm failed run=%s: %s", run_id, exc)

    try:
        asyncio.create_task(_prewarm())
    except RuntimeError:
        pass   # No event loop running (e.g. during tests)
