from __future__ import annotations

import asyncio

import pytest

from hfa_control.dag_lua import TaskClaimResult
from hfa_control.task_recovery import TaskHeartbeatResult
from hfa_worker.task_consumer import TaskConsumer
from hfa_worker.task_context import TaskContext


class ClaimManagerProbe:
    async def claim_start(self, **kwargs):
        return TaskClaimResult(
            ok=True,
            status="task_claimed",
            task_id=kwargs["task_id"],
            worker_id=kwargs["worker_instance_id"],
            claim_epoch="claim-79-9",
            scheduler_epoch=kwargs["scheduler_epoch"],
        )


class RejectingHeartbeatManager:
    def __init__(self) -> None:
        self.rejected = asyncio.Event()

    async def record_heartbeat(self, **kwargs):
        self.rejected.set()
        return TaskHeartbeatResult(
            ok=False,
            status="claim_epoch_mismatch",
        )


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
    timeout: float = 0.4,
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
async def test_rejected_claim_heartbeat_stops_long_running_executor() -> None:
    heartbeat = RejectingHeartbeatManager()
    executor = LongRunningExecutor()
    consumer = TaskConsumer(
        claim_manager=ClaimManagerProbe(),
        executor=executor,
        heartbeat_manager=heartbeat,
        heartbeat_interval_ms=10,
    )
    ctx = TaskContext(
        task_id="task-heartbeat-loss-79-9",
        run_id="run-heartbeat-loss-79-9",
        tenant_id="tenant-heartbeat-loss-79-9",
        agent_type="fake",
        worker_group="group-heartbeat-loss-79-9",
        worker_instance_id="worker-heartbeat-loss-79-9",
        payload={},
        scheduler_epoch="scheduler-79-9",
    )

    consume_task = asyncio.create_task(
        consumer.consume_once(ctx, claimed_at_ms=1)
    )
    try:
        await asyncio.wait_for(executor.started.wait(), timeout=0.2)
        await asyncio.wait_for(heartbeat.rejected.wait(), timeout=0.2)

        finished = await _wait_until(consume_task.done)
        assert finished is True, (
            "A rejected fenced heartbeat means this worker no longer owns the "
            "task. TaskConsumer must fail-stop/cancel the active executor "
            "instead of allowing stale external side effects to continue."
        )
        assert executor.active is False
        assert executor.stopped.is_set()
    finally:
        if not consume_task.done():
            consume_task.cancel()
        await asyncio.gather(consume_task, return_exceptions=True)
