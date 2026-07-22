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

import asyncio
import json
import time
from dataclasses import dataclass
from typing import Any

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
    completed: Any | None = None
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
        completion_manager: Any | None = None,
    ) -> None:
        self._claim_manager = claim_manager
        self._executor = executor
        self._worker_capabilities = worker_capabilities or []
        self._heartbeat_manager = heartbeat_manager
        self._heartbeat_interval_ms = heartbeat_interval_ms
        self._completion_manager = completion_manager

    async def _complete_with_fence(
        self,
        ctx: TaskContext,
        claim: TaskClaimResult,
        executed: TaskExecutionResult,
    ) -> Any:
        """
        Complete the task through the authoritative Lua completion fence.

        Sprint 63 scope:
        - pass task_id, worker_instance_id, scheduler_epoch, and claim_epoch
        - preserve default TaskConsumer behavior when completion_manager is None
        - do not redesign WorkerConsumer legacy completion, ack, retry, or reclaim
        """
        if self._completion_manager is None:
            return None

        terminal_state = "done" if executed.ok else "failed"
        reason_code = "completed" if executed.ok else (executed.error or "failed")
        output_data = json.dumps(
            executed.output or {},
            sort_keys=True,
            separators=(",", ":"),
        )

        scheduler_epoch = claim.scheduler_epoch or ctx.scheduler_epoch

        return await self._completion_manager.task_complete(
            task_id=ctx.task_id,
            run_id=ctx.run_id,
            tenant_id=ctx.tenant_id,
            terminal_state=terminal_state,
            finished_at_ms=int(time.time() * 1000),
            reason_code=reason_code,
            worker_instance_id=ctx.worker_instance_id,
            output_data=output_data,
            expected_scheduler_epoch=scheduler_epoch,
            expected_claim_epoch=claim.claim_epoch,
        )

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

        execution_task: asyncio.Task | None = None
        ownership_task: asyncio.Task | None = None
        try:
            if loop is None:
                executed = await self._executor.execute(ctx)
            else:
                execution_task = asyncio.create_task(
                    self._executor.execute(ctx),
                    name=f"task-execution:{ctx.task_id}",
                )
                ownership_task = asyncio.create_task(
                    loop.wait_for_ownership_loss(),
                    name=f"task-ownership:{ctx.task_id}",
                )
                done, _pending = await asyncio.wait(
                    {execution_task, ownership_task},
                    return_when=asyncio.FIRST_COMPLETED,
                )

                if ownership_task in done:
                    status = ownership_task.result()
                    execution_task.cancel()
                    await asyncio.gather(
                        execution_task,
                        return_exceptions=True,
                    )
                    raise RuntimeError(
                        "Task ownership lost during execution: "
                        f"task_id={ctx.task_id} status={status}"
                    )

                ownership_task.cancel()
                await asyncio.gather(
                    ownership_task,
                    return_exceptions=True,
                )
                executed = execution_task.result()

            completed = await self._complete_with_fence(ctx, claim, executed)
            return ConsumedTaskResult(
                claimed=claim,
                executed=executed,
                completed=completed,
            )
        finally:
            if ownership_task is not None and not ownership_task.done():
                ownership_task.cancel()
                await asyncio.gather(
                    ownership_task,
                    return_exceptions=True,
                )
            if execution_task is not None and not execution_task.done():
                execution_task.cancel()
                await asyncio.gather(
                    execution_task,
                    return_exceptions=True,
                )
            if loop is not None:
                await loop.stop()
