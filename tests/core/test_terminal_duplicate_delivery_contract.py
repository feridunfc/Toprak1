from __future__ import annotations

from pathlib import Path

import pytest

from hfa.dag.schema import DagRedisKey
from hfa_worker.runtime.terminal_duplicate_delivery import (
    NOT_TERMINAL_DUPLICATE_DELIVERY,
    TERMINAL_DUPLICATE_DELIVERY,
    classify_terminal_duplicate_delivery,
)
from hfa_worker.task_context import TaskContext


class FakeRedis:
    def __init__(self) -> None:
        self.values: dict[str, object] = {}
        self.mutations: list[str] = []

    async def get(self, key: str) -> object:
        return self.values.get(key)

    async def xack(self, *args: object, **kwargs: object) -> None:
        self.mutations.append("xack")

    async def xclaim(self, *args: object, **kwargs: object) -> None:
        self.mutations.append("xclaim")

    async def set(self, *args: object, **kwargs: object) -> None:
        self.mutations.append("set")


def _ctx(task_id: str = "task-1") -> TaskContext:
    return TaskContext(
        task_id=task_id,
        run_id=task_id,
        tenant_id="tenant-1",
        agent_type="test",
        worker_group="worker_consumers",
        worker_instance_id="worker-1",
        payload={},
        shard=0,
        scheduler_epoch="1",
    )


@pytest.mark.asyncio
async def test_terminal_duplicate_delivery_classification_suppresses_without_ack() -> None:
    redis = FakeRedis()
    ctx = _ctx()
    redis.values[DagRedisKey.task_state(ctx.task_id)] = b"done"

    decision = await classify_terminal_duplicate_delivery(redis, ctx)

    assert decision.status == TERMINAL_DUPLICATE_DELIVERY
    assert decision.state == "done"
    assert decision.terminal is True
    assert decision.suppress_claim is True
    assert decision.suppress_execution is True
    assert decision.suppress_completion is True
    assert decision.ack_allowed is False
    assert decision.reason == "task_already_terminal_before_claim"
    assert redis.mutations == []


@pytest.mark.asyncio
async def test_non_terminal_delivery_allows_normal_bridge_path() -> None:
    redis = FakeRedis()
    ctx = _ctx("task-scheduled")
    redis.values[DagRedisKey.task_state(ctx.task_id)] = b"scheduled"

    decision = await classify_terminal_duplicate_delivery(redis, ctx)

    assert decision.status == NOT_TERMINAL_DUPLICATE_DELIVERY
    assert decision.state == "scheduled"
    assert decision.terminal is False
    assert decision.suppress_claim is False
    assert decision.suppress_execution is False
    assert decision.suppress_completion is False
    assert decision.ack_allowed is False
    assert redis.mutations == []


def test_terminal_duplicate_delivery_module_is_read_only_and_pre_claim_contract() -> None:
    source = Path("hfa-worker/src/hfa_worker/runtime/terminal_duplicate_delivery.py").read_text(
        encoding="utf-8"
    )

    forbidden = [
        ".xack(",
        ".xclaim(",
        ".xadd(",
        ".set(",
        ".hset(",
        ".delete(",
        ".expire(",
        "claim_start(",
        "task_complete(",
        "task_requeue(",
        "runtime_repair(",
    ]
    for token in forbidden:
        assert token not in source

    assert "await self._task_consumer.consume_once(" not in source
    assert "await self._task_consumer" not in source


def test_worker_consumer_bridge_classifies_before_consume_once() -> None:
    source = Path("hfa-worker/src/hfa_worker/consumer.py").read_text(encoding="utf-8")
    marker = "    async def _process_message_via_task_consumer("
    start = source.index(marker)
    bridge_body = source[start : source.index("    async def _process_message(", start)]

    assert "classify_terminal_duplicate_delivery(self._redis, ctx)" in bridge_body
    assert "TERMINAL_DUPLICATE_DELIVERY" in bridge_body
    assert "await self._task_consumer.consume_once(" in bridge_body
    assert bridge_body.index("classify_terminal_duplicate_delivery(self._redis, ctx)") < bridge_body.index(
        "await self._task_consumer.consume_once("
    )
