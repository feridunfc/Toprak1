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


def _consumer(redis, worker_id: str) -> WorkerConsumer:
    return WorkerConsumer(
        redis=redis,
        worker_id=worker_id,
        worker_group="group-drain-barrier-79-12",
        shards=[0],
        executor=ExecutorProbe(),
        reclaim_idle_ms=1,
    )


class AssignedThenReclaimRedis:
    def __init__(self) -> None:
        self.assigned_delivered = False
        self.reclaim_reads = 0

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
        if cursor == ">":
            return []
        if not self.assigned_delivered:
            self.assigned_delivered = True
            return [
                (
                    stream,
                    [
                        (
                            "79-12-assigned-1",
                            {
                                b"event_type": b"TaskRequested",
                                b"task_id": b"task-assigned-1",
                            },
                        )
                    ],
                )
            ]
        return []

    async def xpending_range(self, *args, **kwargs):
        self.reclaim_reads += 1
        return [
            {
                "message_id": "79-12-reclaim-1",
                "time_since_delivered": 10_000,
            }
        ]

    async def xclaim(self, stream, group, consumer, idle, ids):
        return [
            (
                "79-12-reclaim-1",
                {
                    b"event_type": b"TaskRequested",
                    b"task_id": b"task-reclaim-1",
                },
            )
        ]


@pytest.mark.asyncio
async def test_stop_during_assigned_scan_blocks_later_reclaim_phase() -> None:
    redis = AssignedThenReclaimRedis()
    consumer = _consumer(redis, "worker-drain-phase-79-12")
    processed: list[str] = []

    async def process_message(msg_id, data, stream, shard):
        processed.append(str(msg_id))
        consumer.stop_pulling()

    consumer._process_message = process_message
    consumer._running = True
    consumer._pulling = True

    await consumer._main_lifecycle()

    assert processed == ["79-12-assigned-1"], (
        "stop_pulling() during assigned-PEL processing must terminate the "
        "entire intake lifecycle. _main_lifecycle must not continue into "
        "generic reclaim after the drain boundary is crossed."
    )
    assert redis.reclaim_reads == 0, (
        "Generic reclaim was entered after stop_pulling()."
    )


class ReclaimBatchRedis:
    async def xpending_range(self, *args, **kwargs):
        return [
            {
                "message_id": "79-12-reclaim-a",
                "time_since_delivered": 10_000,
            },
            {
                "message_id": "79-12-reclaim-b",
                "time_since_delivered": 10_000,
            },
        ]

    async def xclaim(self, stream, group, consumer, idle, ids):
        return [
            (
                "79-12-reclaim-a",
                {
                    b"event_type": b"TaskRequested",
                    b"task_id": b"task-reclaim-a",
                },
            ),
            (
                "79-12-reclaim-b",
                {
                    b"event_type": b"TaskRequested",
                    b"task_id": b"task-reclaim-b",
                },
            ),
        ]


@pytest.mark.asyncio
async def test_reclaim_batch_rechecks_drain_before_each_entry() -> None:
    consumer = _consumer(
        ReclaimBatchRedis(),
        "worker-reclaim-batch-79-12",
    )
    processed: list[str] = []

    async def process_message(msg_id, data, stream, shard):
        processed.append(str(msg_id))
        if len(processed) == 1:
            consumer.stop_pulling()

    consumer._process_message = process_message
    consumer._running = True
    consumer._pulling = True

    await consumer._reclaim_pending_messages()

    assert processed == ["79-12-reclaim-a"], (
        "A reclaimed batch must re-check the pulling state before each entry. "
        "The second task must not begin after the first task starts drain."
    )


class BlockingNewMessageRedis:
    def __init__(self) -> None:
        self.read_started = asyncio.Event()
        self.release_read = asyncio.Event()

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
            return []

        self.read_started.set()
        await self.release_read.wait()
        return [
            (
                stream,
                [
                    (
                        "79-12-new-after-stop",
                        {
                            b"event_type": b"TaskRequested",
                            b"task_id": b"task-new-after-stop",
                        },
                    )
                ],
            )
        ]


@pytest.mark.asyncio
async def test_blocked_new_message_read_cannot_cross_drain_boundary() -> None:
    redis = BlockingNewMessageRedis()
    consumer = _consumer(redis, "worker-blocked-read-79-12")
    processed: list[str] = []

    async def process_message(msg_id, data, stream, shard):
        processed.append(str(msg_id))

    consumer._process_message = process_message
    consumer._running = True
    consumer._pulling = True

    task = asyncio.create_task(consumer._consume_loop())
    await asyncio.wait_for(redis.read_started.wait(), timeout=1.0)

    consumer.stop_pulling()
    redis.release_read.set()
    await asyncio.wait_for(task, timeout=1.0)

    assert processed == [], (
        "A blocking XREADGROUP may return after stop_pulling(). The consumer "
        "must re-check its drain state after the await and before processing "
        "the returned message."
    )


class TwoNewMessagesRedis:
    def __init__(self) -> None:
        self.delivered = False

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
        if cursor != ">" or self.delivered:
            return []

        self.delivered = True
        return [
            (
                stream,
                [
                    (
                        "79-12-new-a",
                        {
                            b"event_type": b"TaskRequested",
                            b"task_id": b"task-new-a",
                        },
                    ),
                    (
                        "79-12-new-b",
                        {
                            b"event_type": b"TaskRequested",
                            b"task_id": b"task-new-b",
                        },
                    ),
                ],
            )
        ]


@pytest.mark.asyncio
async def test_new_message_batch_rechecks_drain_before_each_entry() -> None:
    consumer = _consumer(
        TwoNewMessagesRedis(),
        "worker-new-batch-79-12",
    )
    processed: list[str] = []

    async def process_message(msg_id, data, stream, shard):
        processed.append(str(msg_id))
        if len(processed) == 1:
            consumer.stop_pulling()

    consumer._process_message = process_message
    consumer._running = True
    consumer._pulling = True

    await consumer._consume_loop()

    assert processed == ["79-12-new-a"], (
        "Normal stream batches must re-check the pulling state before each "
        "entry. The second task must not begin after drain starts."
    )
