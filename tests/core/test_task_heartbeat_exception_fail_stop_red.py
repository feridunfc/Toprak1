from __future__ import annotations

import asyncio

import pytest

from hfa_control.dag_lua import TaskClaimResult
from hfa_worker.task_consumer import TaskConsumer
from hfa_worker.task_context import TaskContext


class ClaimManagerProbe:
    async def claim_start(self, **kwargs):
        return TaskClaimResult(
            ok=True,
            status="task_claimed",
            task_id=kwargs["task_id"],
            worker_id=kwargs["worker_instance_id"],
            claim_epoch="claim-exception-79-10",
            scheduler_epoch=kwargs["scheduler_epoch"],
        )


class FailingHeartbeatManager:
    def __init__(self) -> None:
        self.failed = asyncio.Event()

    async def record_heartbeat(self, **kwargs):
        self.failed.set()
        raise RuntimeError("heartbeat-redis-failure-79-10")


class LongRunningExecutor:
    def __init__(self) -> None:
        self.started = asyncio.Event()
        self.stopped = asyncio.Event()
        self.active = False

    async def execute(self, ctx):
        self.active = True
        self.started.set()
        try:
            await asyncio.Event().wait()
        finally:
            self.active = False
            self.stopped.set()


async def _wait_until(
    predicate,
    *,
    timeout: float = 0.35,
    interval: float = 0.01,
) -> bool:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while loop.time() < deadline:
        if predicate():
            return True
        await asyncio.sleep(interval)
    return bool(predicate())


@pytest.mark.asyncio
async def test_heartbeat_exception_cannot_leave_executor_running() -> None:
    heartbeat = FailingHeartbeatManager()
    executor = LongRunningExecutor()
    consumer = TaskConsumer(
        claim_manager=ClaimManagerProbe(),
        executor=executor,
        heartbeat_manager=heartbeat,
        heartbeat_interval_ms=10,
    )
    ctx = TaskContext(
        task_id="task-heartbeat-exception-79-10",
        run_id="run-heartbeat-exception-79-10",
        tenant_id="tenant-heartbeat-exception-79-10",
        agent_type="fake",
        worker_group="group-heartbeat-exception-79-10",
        worker_instance_id="worker-heartbeat-exception-79-10",
        payload={},
        scheduler_epoch="scheduler-exception-79-10",
    )

    consume_task = asyncio.create_task(
        consumer.consume_once(ctx, claimed_at_ms=1)
    )
    try:
        await asyncio.wait_for(executor.started.wait(), timeout=0.2)
        await asyncio.wait_for(heartbeat.failed.wait(), timeout=0.2)

        finished = await _wait_until(consume_task.done)
        assert finished is True, (
            "A task-heartbeat infrastructure exception must either recover "
            "within a bounded policy or fail-stop the active execution. The "
            "heartbeat task must not die while the executor remains active."
        )
        assert executor.active is False
        assert executor.stopped.is_set()
    finally:
        if not consume_task.done():
            consume_task.cancel()
        await asyncio.gather(consume_task, return_exceptions=True)
