from __future__ import annotations

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


class LargeBlockedPendingRedis:
    """Model 101 unacked entries owned by one consumer."""

    def __init__(self) -> None:
        self.ids = [f"{index}-0" for index in range(1, 102)]
        self.cursors: list[str] = []

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
        cursor_text = str(cursor)
        self.cursors.append(cursor_text)

        if cursor_text in {"0", "0-0"}:
            minimum = 0
        else:
            minimum = int(cursor_text.split("-", 1)[0])

        selected = [
            msg_id
            for msg_id in self.ids
            if int(msg_id.split("-", 1)[0]) > minimum
        ][:count]

        if not selected:
            return []

        return [
            (
                stream,
                [
                    (
                        msg_id,
                        {
                            b"event_type": b"TaskRequested",
                            b"task_id": f"task-{msg_id}".encode(),
                        },
                    )
                    for msg_id in selected
                ],
            )
        ]


@pytest.mark.asyncio
async def test_assigned_pending_scan_paginates_beyond_first_blocked_batch() -> None:
    redis = LargeBlockedPendingRedis()
    consumer = WorkerConsumer(
        redis=redis,
        worker_id="worker-pel-page-79-11",
        worker_group="group-pel-page-79-11",
        shards=[0],
        executor=ExecutorProbe(),
    )

    processed: list[str] = []

    async def process_message(msg_id, data, stream, shard):
        # Deliberately do not ACK: rejected/mismatched canonical tasks can
        # legitimately remain pending while later transferred work exists.
        processed.append(str(msg_id))

    consumer._process_message = process_message
    consumer._running = True
    consumer._pulling = True

    await consumer._consume_assigned_pending_messages()

    assert "101-0" in processed, (
        "Assigned PEL traversal must advance its stream cursor. Re-reading "
        "the fixed '0' cursor means the first 100 unacked entries permanently "
        "hide later reservation-owned handoffs."
    )
    assert any(cursor not in {"0", "0-0"} for cursor in redis.cursors[1:]), (
        "The pending scan never advanced beyond its initial cursor."
    )
