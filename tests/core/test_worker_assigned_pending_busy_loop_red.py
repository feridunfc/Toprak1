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


class BusyAssignedPendingRedis:
    """Keep the new-message path continuously busy after startup."""

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
                                "79-10-pel-0",
                                {
                                    b"event_type": b"TaskRequested",
                                    b"task_id": b"task-busy-pel-79-10",
                                },
                            )
                        ],
                    )
                ]
            return []

        assert cursor == ">"
        self.new_reads += 1
        await asyncio.sleep(0.005)
        return [
            (
                stream,
                [
                    (
                        f"79-10-new-{self.new_reads}",
                        {
                            b"event_type": b"RunRequested",
                            b"run_id": f"run-{self.new_reads}".encode(),
                        },
                    )
                ],
            )
        ]


async def _wait_until(
    predicate,
    *,
    timeout: float = 0.4,
    interval: float = 0.005,
) -> bool:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while loop.time() < deadline:
        if predicate():
            return True
        await asyncio.sleep(interval)
    return bool(predicate())


@pytest.mark.asyncio
async def test_busy_running_worker_does_not_starve_assigned_pending() -> None:
    redis = BusyAssignedPendingRedis()
    consumer = WorkerConsumer(
        redis=redis,
        worker_id="worker-busy-pel-79-10",
        worker_group="group-busy-pel-79-10",
        shards=[0],
        executor=ExecutorProbe(),
    )

    assigned_processed = asyncio.Event()

    async def process_message(msg_id, data, stream, shard):
        if msg_id == "79-10-pel-0":
            assigned_processed.set()

    consumer._process_message = process_message

    await consumer.start()
    try:
        busy = await _wait_until(
            lambda: redis.pending_reads >= 1 and redis.new_reads >= 3
        )
        assert busy is True

        redis.assigned = True

        handled = await _wait_until(
            assigned_processed.is_set,
            timeout=0.3,
        )
        assert handled is True, (
            "Assigned PEL work must be checked on a bounded cadence even while "
            "the normal '>' stream continuously returns new messages. Scanning "
            "only when XREADGROUP returns no messages permits indefinite "
            "same-group handoff starvation."
        )
    finally:
        await consumer.close()
