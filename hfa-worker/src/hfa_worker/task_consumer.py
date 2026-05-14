"""
hfa_worker/task_consumer.py
----------------------------
IRONCLAD Sprint 2 — Task consumer with fence tuple propagation.

Sprint 2 change: after a successful claim_start(), the fence tuple
(claim_epoch, scheduler_epoch) is extracted from the TaskClaimResult and:
  1. Injected into the HeartbeatLoop so heartbeats carry claim_epoch.
  2. Available via ConsumedTaskResult so the execution plane can pass all
     three fence values to task_complete().
"""
from __future__ import annotations

from dataclasses import dataclass

from hfa.dag.capabilities import TaskCapabilitySpec, WorkerCapabilitySpec
from hfa_control.capability_router import CapabilityRouter
from hfa_control.task_claim import TaskClaimManager, TaskClaimResult
from hfa_control.task_recovery import TaskHeartbeatManager
from hfa_worker.task_context import TaskContext
from hfa_worker.task_executor import TaskExecutor, TaskExecutionResult
from hfa_worker.task_heartbeat import HeartbeatLoop


@dataclass(frozen=True)
class ConsumedTaskResult:
    claimed: TaskClaimResult | None = None
    executed: TaskExecutionResult | None = None
    rejected_reason: str = ""


class TaskConsumer:
    def __init__(
        self,
        claim_manager: TaskClaimManager,
        executor: TaskExecutor,
        *,
        worker_capabilities: list[str] | None = None,
        heartbeat_manager: TaskHeartbeatManager | None = None,
        heartbeat_interval_ms: int = 5_000,
    ) -> None:
        self._claim_manager = claim_manager
        self._executor = executor
        self._worker_capabilities = worker_capabilities or []
        self._heartbeat_manager = heartbeat_manager
        self._heartbeat_interval_ms = heartbeat_interval_ms

    async def consume_once(
        self, ctx: TaskContext, *, claimed_at_ms: int
    ) -> ConsumedTaskResult:
        match = CapabilityRouter.matches(
            TaskCapabilitySpec(required_capabilities=ctx.required_capabilities or []),
            WorkerCapabilitySpec(
                worker_id=ctx.worker_instance_id,
                capabilities=self._worker_capabilities,
            ),
        )
        if not match.ok:
            return ConsumedTaskResult(
                rejected_reason="missing_capabilities:" + ",".join(match.missing_capabilities),
            )

        claim = await self._claim_manager.claim_start(
            task_id=ctx.task_id,
            tenant_id=ctx.tenant_id,
            worker_instance_id=ctx.worker_instance_id,
            claimed_at_ms=claimed_at_ms,
            # Pass scheduler_epoch from context if present (set during dispatch)
            scheduler_epoch=ctx.scheduler_epoch,
        )
        if not claim.ok:
            return ConsumedTaskResult(claimed=claim)

        # Sprint 2: start heartbeat loop with the claim_epoch just issued.
        loop = None
        if self._heartbeat_manager is not None:
            loop = HeartbeatLoop(
                heartbeat_manager=self._heartbeat_manager,
                task_id=ctx.task_id,
                tenant_id=ctx.tenant_id,
                worker_instance_id=ctx.worker_instance_id,
                interval_ms=self._heartbeat_interval_ms,
                claim_epoch=claim.claim_epoch,   # Sprint 2: fence token
            )
            await loop.start()

        try:
            executed = await self._executor.execute(ctx)
            return ConsumedTaskResult(claimed=claim, executed=executed)
        finally:
            if loop is not None:
                await loop.stop()
