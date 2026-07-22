from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest

from hfa_worker.consumer import WorkerConsumer


class ExecutorProbe:
    async def execute(self, event):
        return SimpleNamespace(
            status="done",
            payload={},
            cost_cents=0,
            tokens_used=0,
            error="",
        )


class AssignedPendingRedis:
    """Minimal stream model for an already-running target consumer."""

    def __init__(self) -> None:
        self.assigned = False
        self.assigned_delivered = False
        self.pending_reads = 0
        self.new_reads = 0

    async def xgroup_create(self, *args, **kwargs):
        return True

    async def xpending_range(self, *args, **kwargs):
        return []

    async def xreadgroup(
        self,
        *,
        groupname,
        consumername,
        streams,
        count,
        block=None,
    ):
        stream, cursor = next(iter(streams.items()))
        if cursor != ">":
            self.pending_reads += 1
            if (
                cursor in {"0", "0-0"}
                and self.assigned
                and not self.assigned_delivered
            ):
                self.assigned_delivered = True
                return [
                    (
                        stream,
                        [
                            (
                                "79-9-0",
                                {
                                    b"event_type": b"TaskRequested",
                                    b"task_id": b"task-steady-pel-79-9",
                                },
                            )
                        ],
                    )
                ]
            return []

        assert cursor == ">"
        self.new_reads += 1
        await asyncio.sleep(0.01)
        return []


async def _wait_until(
    predicate,
    *,
    timeout: float = 0.5,
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
async def test_running_worker_consumes_pending_assigned_after_startup() -> None:
    redis = AssignedPendingRedis()
    consumer = WorkerConsumer(
        redis=redis,
        worker_id="worker-steady-pel-79-9",
        worker_group="group-steady-pel-79-9",
        shards=[0],
        executor=ExecutorProbe(),
    )

    processed = asyncio.Event()

    async def process_message(msg_id, data, stream, shard):
        assert msg_id == "79-9-0"
        processed.set()

    consumer._process_message = process_message

    await consumer.start()
    try:
        steady = await _wait_until(
            lambda: redis.pending_reads >= 1 and redis.new_reads >= 1
        )
        assert steady is True

        # Simulate another same-group worker transferring a reservation-owned
        # message into this already-running worker's PEL.
        redis.assigned = True

        handled = await _wait_until(processed.is_set, timeout=0.35)
        assert handled is True, (
            "A worker that is already in its normal '>' read loop must also "
            "notice messages assigned to its PEL after startup. A startup-only "
            "pending scan strands steady-state same-group handoffs."
        )
    finally:
        await consumer.close()
