"""
hfa-worker/src/hfa_worker/worker_main.py  (PATCH — replaces or extends existing)
IRONCLAD OS — Worker Entry Point with Cognitive Stack

PURPOSE
-------
Shows the minimal wiring to boot the full cognitive stack alongside
the existing IRONCLAD worker. This is the ONLY file that constructs
CognitiveExecutor and wires it into the existing TaskConsumer.

KEY PRINCIPLE: TaskConsumer is unchanged. We only swap the executor.
"""
from __future__ import annotations

import asyncio
import logging
import os

import redis.asyncio as aioredis

logger = logging.getLogger("hfa.worker_main")


async def build_cognitive_stack(redis_url: str | None = None):
    """
    Build the full cognitive stack: Redis → Semantic → CognitiveExecutor.

    Returns (redis_client, cognitive_executor).
    """
    url = redis_url or os.environ.get("REDIS_URL", "redis://localhost:6379")
    redis_client = aioredis.from_url(url, decode_responses=False)

    # ── hfa-semantic pipeline ───────────────────────────────────────────
    semantic_pipeline = None
    try:
        from hfa_semantic.runtime.factory import build_pipeline
        semantic_pipeline = build_pipeline(redis_client=redis_client)
        logger.info("worker_main: hfa-semantic pipeline ready")
    except ImportError:
        logger.warning("worker_main: hfa-semantic not installed — running without")

    # ── CognitiveExecutor ───────────────────────────────────────────────
    from hfa_worker.cognitive_executor import CognitiveExecutor
    executor = CognitiveExecutor(
        semantic_pipeline=semantic_pipeline,
        max_budget_cents=int(os.environ.get("COGNITIVE_BUDGET_CENTS", "2000")),
    )

    return redis_client, executor


async def main() -> None:
    """
    Minimal worker main showing cognitive stack integration.

    In the real worker, integrate this into your existing worker loop.
    The key change is: pass `executor=cognitive_executor` to TaskConsumer.
    """
    from hfa_control.task_claim import TaskClaimManager
    from hfa_worker.task_consumer import TaskConsumer

    redis_client, executor = await build_cognitive_stack()

    # Build IRONCLAD components (unchanged)
    claim_manager = TaskClaimManager(redis_client)

    # ← THE ONLY CHANGE: pass CognitiveExecutor instead of TaskExecutor stub
    consumer = TaskConsumer(
        claim_manager=claim_manager,
        executor=executor,          # was: TaskExecutor()
        worker_capabilities=os.environ.get("WORKER_CAPABILITIES", "cognitive").split(","),
    )

    logger.info("worker_main: cognitive worker started")

    # Worker poll loop (simplified — production uses your existing loop)
    while True:
        try:
            # TaskContext is obtained from your existing task poll mechanism
            # consumer.consume_once(ctx, claimed_at_ms=...) — unchanged call
            await asyncio.sleep(1)
        except KeyboardInterrupt:
            break

    await redis_client.aclose()


# ── Integration points summary ──────────────────────────────────────────────
#
# FILES CHANGED (minimal):
# ─────────────────────────────────────────────────────────────────────────────
# 1. hfa-worker/src/hfa_worker/worker_main.py    ← THIS FILE (new or patched)
# 2. hfa-worker/src/hfa_worker/cognitive_executor.py  ← NEW
# 3. hfa-worker/src/hfa_worker/feedback_writer.py     ← NEW
#
# FILES UNCHANGED (sealed):
# ─────────────────────────────────────────────────────────────────────────────
# hfa-worker/src/hfa_worker/task_consumer.py     ← UNTOUCHED
# hfa-worker/src/hfa_worker/task_context.py      ← UNTOUCHED
# hfa-control/ (entire package)                  ← UNTOUCHED
# hfa-core/ (entire package)                     ← UNTOUCHED
#
# ENV VARS ADDED:
# ─────────────────────────────────────────────────────────────────────────────
# REDIS_URL               = redis://redis:6379
# COGNITIVE_BUDGET_CENTS  = 2000
# SEMANTIC_ENABLED        = true
# SEMANTIC_MAX_PARTITIONS = 10000
# ANTHROPIC_API_KEY       = ...
# OPENAI_API_KEY          = ...   (fallback)
