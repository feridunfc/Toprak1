from __future__ import annotations

from pathlib import Path

import pytest

from hfa.dag.schema import DagRedisKey
from hfa_worker.runtime.terminal_duplicate_delivery import (
    ACK_POLICY_ACK_EXPLICIT_TASK_RUN_TERMINAL_EVIDENCE,
    ACK_POLICY_NO_ACK_WITHOUT_EXPLICIT_TASK_IDENTITY,
    ACK_POLICY_NO_ACK_WITHOUT_TERMINAL_EVIDENCE,
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

    async def hgetall(self, key: str) -> dict[object, object]:
        value = self.values.get(key)
        if isinstance(value, dict):
            return value
        return {}

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
    assert decision.ack_policy == ACK_POLICY_NO_ACK_WITHOUT_EXPLICIT_TASK_IDENTITY
    assert decision.message_identity_verified is False
    assert decision.terminal_evidence_verified is False
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


@pytest.mark.asyncio
async def test_terminal_duplicate_delivery_ack_allowed_only_with_explicit_identity_and_evidence() -> None:
    redis = FakeRedis()
    ctx = _ctx("task-explicit-ack")
    redis.values[DagRedisKey.task_state(ctx.task_id)] = b"done"
    redis.values[DagRedisKey.task_meta(ctx.task_id)] = {b"run_id": b"task-explicit-ack"}

    decision = await classify_terminal_duplicate_delivery(
        redis,
        ctx,
        message_task_id=ctx.task_id,
        message_run_id=ctx.run_id,
    )

    assert decision.status == TERMINAL_DUPLICATE_DELIVERY
    assert decision.ack_allowed is True
    assert decision.ack_policy == ACK_POLICY_ACK_EXPLICIT_TASK_RUN_TERMINAL_EVIDENCE
    assert decision.message_identity_verified is True
    assert decision.terminal_evidence_verified is True
    assert decision.evidence_run_id == ctx.run_id
    assert redis.mutations == []


@pytest.mark.asyncio
async def test_terminal_duplicate_delivery_ack_denied_when_terminal_evidence_missing() -> None:
    redis = FakeRedis()
    ctx = _ctx("task-no-terminal-evidence")
    redis.values[DagRedisKey.task_state(ctx.task_id)] = b"done"

    decision = await classify_terminal_duplicate_delivery(
        redis,
        ctx,
        message_task_id=ctx.task_id,
        message_run_id=ctx.run_id,
    )

    assert decision.status == TERMINAL_DUPLICATE_DELIVERY
    assert decision.ack_allowed is False
    assert decision.ack_policy == ACK_POLICY_NO_ACK_WITHOUT_TERMINAL_EVIDENCE
    assert decision.message_identity_verified is True
    assert decision.terminal_evidence_verified is False
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

    # Sprint 69.2: the module may read task_meta evidence, but must not mutate.
    assert 'getattr(redis, "hgetall", None)' in source
    assert "await hgetall(DagRedisKey.task_meta(task_id))" in source
    assert "ACK_POLICY_ACK_EXPLICIT_TASK_RUN_TERMINAL_EVIDENCE" in source


def test_worker_consumer_bridge_classifies_before_consume_once() -> None:
    source = Path("hfa-worker/src/hfa_worker/consumer.py").read_text(encoding="utf-8")
    marker = "    async def _process_message_via_task_consumer("
    start = source.index(marker)
    bridge_body = source[start : source.index("    async def _process_message(", start)]

    assert "duplicate_delivery = await classify_terminal_duplicate_delivery(" in bridge_body
    assert "self._redis," in bridge_body
    assert "ctx," in bridge_body
    assert "message_task_id=" in bridge_body
    assert "message_run_id=" in bridge_body
    assert "TERMINAL_DUPLICATE_DELIVERY" in bridge_body
    assert "await self._task_consumer.consume_once(" in bridge_body
    assert bridge_body.index("duplicate_delivery = await classify_terminal_duplicate_delivery(") < bridge_body.index(
        "await self._task_consumer.consume_once("
    )

def test_worker_consumer_acks_terminal_duplicate_only_when_policy_allows() -> None:
    source = Path("hfa-worker/src/hfa_worker/consumer.py").read_text(encoding="utf-8")
    marker = "    async def _process_message_via_task_consumer("
    start = source.index(marker)
    bridge_body = source[start : source.index("    async def _process_message(", start)]

    assert "message_task_id=str(getattr(event, \"task_id\", \"\") or \"\")" in bridge_body
    assert "message_run_id=str(getattr(event, \"run_id\", \"\") or \"\")" in bridge_body
    assert "if duplicate_delivery.ack_allowed:" in bridge_body
    assert "await ack_message(self._redis, stream, CONSUMER_GROUP, msg_id)" in bridge_body
    assert bridge_body.index("if duplicate_delivery.ack_allowed:") < bridge_body.index(
        "await ack_message(self._redis, stream, CONSUMER_GROUP, msg_id)"
    )
