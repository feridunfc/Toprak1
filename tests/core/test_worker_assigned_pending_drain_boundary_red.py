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


class TwoMessagePendingRedis:
    def __init__(self) -> None:
        self.returned = False

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
        if cursor == ">" or self.returned:
            await asyncio.sleep(0)
            return []

        self.returned = True
        return [
            (
                stream,
                [
                    (
                        "79-11-drain-1",
                        {
                            b"event_type": b"TaskRequested",
                            b"task_id": b"task-drain-boundary-1",
                        },
                    ),
                    (
                        "79-11-drain-2",
                        {
                            b"event_type": b"TaskRequested",
                            b"task_id": b"task-drain-boundary-2",
                        },
                    ),
                ],
            )
        ]


@pytest.mark.asyncio
async def test_assigned_pending_scan_stops_at_drain_boundary() -> None:
    redis = TwoMessagePendingRedis()
    consumer = WorkerConsumer(
        redis=redis,
        worker_id="worker-pel-drain-79-11",
        worker_group="group-pel-drain-79-11",
        shards=[0],
        executor=ExecutorProbe(),
    )

    processed: list[str] = []

    async def process_message(msg_id, data, stream, shard):
        processed.append(str(msg_id))
        if len(processed) == 1:
            consumer.stop_pulling()

    consumer._process_message = process_message
    consumer._running = True
    consumer._pulling = True

    await consumer._consume_assigned_pending_messages()

    assert processed == ["79-11-drain-1"], (
        "stop_pulling() is the no-new-work boundary. Assigned PEL scanning "
        "must re-check pulling state between entries and must not begin a "
        "second pending task after drain starts."
    )
