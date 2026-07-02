
from __future__ import annotations

from dataclasses import dataclass

import pytest

from hfa_control.task_claim import TaskClaimResult
from hfa_worker.task_consumer import TaskConsumer
from hfa_worker.task_context import TaskContext
from hfa_worker.task_executor import TaskExecutionResult


class FakeClaimManager:
    async def claim_start(
        self,
        *,
        task_id: str,
        tenant_id: str,
        worker_instance_id: str,
        claimed_at_ms: int,
        scheduler_epoch: str = "",
    ) -> TaskClaimResult:
        return TaskClaimResult(
            ok=True,
            status="claimed",
            task_id=task_id,
            worker_id=worker_instance_id,
            claim_epoch="claim-63",
            scheduler_epoch=scheduler_epoch or "scheduler-from-claim",
        )


class RecordingExecutor:
    async def execute(self, ctx: TaskContext) -> TaskExecutionResult:
        return TaskExecutionResult(
            ok=True,
            output={"task_id": ctx.task_id, "answer": 42},
        )


class FailingExecutor:
    async def execute(self, ctx: TaskContext) -> TaskExecutionResult:
        return TaskExecutionResult(
            ok=False,
            output={"task_id": ctx.task_id, "failed": True},
            error="executor_failed",
        )


@dataclass(frozen=True)
class CompletionResult:
    ok: bool
    status: str


class RecordingCompletionManager:
    def __init__(self) -> None:
        self.calls: list[dict] = []

    async def task_complete(self, **kwargs):
        self.calls.append(kwargs)
        return CompletionResult(ok=True, status="OK")


@pytest.mark.asyncio
async def test_task_consumer_completes_with_claim_fence_tuple() -> None:
    completion = RecordingCompletionManager()
    consumer = TaskConsumer(
        claim_manager=FakeClaimManager(),
        executor=RecordingExecutor(),
        completion_manager=completion,
    )

    ctx = TaskContext(
        task_id="task-63",
        run_id="run-63",
        tenant_id="tenant-a",
        agent_type="agent-a",
        worker_group="group-a",
        worker_instance_id="worker-a",
        payload={"prompt": "hello"},
        scheduler_epoch="scheduler-63",
    )

    result = await consumer.consume_once(ctx, claimed_at_ms=123456)

    assert result.claimed is not None
    assert result.claimed.ok is True
    assert result.executed is not None
    assert result.executed.ok is True
    assert result.completed is not None
    assert result.completed.ok is True

    assert len(completion.calls) == 1
    call = completion.calls[0]

    assert call["task_id"] == "task-63"
    assert call["run_id"] == "run-63"
    assert call["tenant_id"] == "tenant-a"
    assert call["terminal_state"] == "done"
    assert call["reason_code"] == "completed"
    assert call["worker_instance_id"] == "worker-a"
    assert call["expected_scheduler_epoch"] == "scheduler-63"
    assert call["expected_claim_epoch"] == "claim-63"
    assert call["output_data"] == '{"answer":42,"task_id":"task-63"}'


@pytest.mark.asyncio
async def test_task_consumer_completion_uses_claim_scheduler_epoch_when_claim_returns_one() -> None:
    class ClaimManagerOverridesScheduler(FakeClaimManager):
        async def claim_start(self, **kwargs) -> TaskClaimResult:
            return TaskClaimResult(
                ok=True,
                status="claimed",
                task_id=kwargs["task_id"],
                worker_id=kwargs["worker_instance_id"],
                claim_epoch="claim-override",
                scheduler_epoch="scheduler-from-claim",
            )

    completion = RecordingCompletionManager()
    consumer = TaskConsumer(
        claim_manager=ClaimManagerOverridesScheduler(),
        executor=RecordingExecutor(),
        completion_manager=completion,
    )

    ctx = TaskContext(
        task_id="task-override",
        run_id="run-override",
        tenant_id="tenant-a",
        agent_type="agent-a",
        worker_group="group-a",
        worker_instance_id="worker-a",
        payload={},
        scheduler_epoch="scheduler-from-context",
    )

    await consumer.consume_once(ctx, claimed_at_ms=123456)

    assert completion.calls[0]["expected_scheduler_epoch"] == "scheduler-from-claim"
    assert completion.calls[0]["expected_claim_epoch"] == "claim-override"


@pytest.mark.asyncio
async def test_task_consumer_completion_maps_failed_execution_to_failed_terminal_state() -> None:
    completion = RecordingCompletionManager()
    consumer = TaskConsumer(
        claim_manager=FakeClaimManager(),
        executor=FailingExecutor(),
        completion_manager=completion,
    )

    ctx = TaskContext(
        task_id="task-failed",
        run_id="run-failed",
        tenant_id="tenant-a",
        agent_type="agent-a",
        worker_group="group-a",
        worker_instance_id="worker-a",
        payload={},
        scheduler_epoch="scheduler-failed",
    )

    result = await consumer.consume_once(ctx, claimed_at_ms=123456)

    assert result.executed is not None
    assert result.executed.ok is False
    assert result.completed is not None

    call = completion.calls[0]
    assert call["terminal_state"] == "failed"
    assert call["reason_code"] == "executor_failed"
    assert call["expected_claim_epoch"] == "claim-63"


@pytest.mark.asyncio
async def test_task_consumer_without_completion_manager_preserves_previous_behavior() -> None:
    consumer = TaskConsumer(
        claim_manager=FakeClaimManager(),
        executor=RecordingExecutor(),
    )

    ctx = TaskContext(
        task_id="task-no-completion",
        run_id="run-no-completion",
        tenant_id="tenant-a",
        agent_type="agent-a",
        worker_group="group-a",
        worker_instance_id="worker-a",
        payload={},
        scheduler_epoch="scheduler-no-completion",
    )

    result = await consumer.consume_once(ctx, claimed_at_ms=123456)

    assert result.claimed is not None
    assert result.executed is not None
    assert result.completed is None
