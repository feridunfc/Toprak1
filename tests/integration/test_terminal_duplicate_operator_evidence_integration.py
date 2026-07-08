from __future__ import annotations

from uuid import uuid4

import pytest

from hfa.config.keys import RedisKey
from hfa.dag.schema import DagRedisKey
from hfa_control.terminal_duplicate_operator_evidence import (
    ACK_POLICY_ACK_EXPLICIT_TASK_RUN_TERMINAL_EVIDENCE,
    ACK_POLICY_NO_ACK_WITHOUT_EXPLICIT_TASK_IDENTITY,
    EXPLICIT_TERMINAL_DUPLICATE_CLEANUP_CANDIDATE,
    NO_ACK_WITHOUT_EXPLICIT_TASK_IDENTITY,
    read_terminal_duplicate_operator_evidence,
)
from hfa_worker.consumer import CONSUMER_GROUP

pytestmark = [pytest.mark.asyncio, pytest.mark.integration]


def _pending_count(summary: object) -> int:
    if isinstance(summary, dict):
        if "pending" in summary:
            return int(summary.get("pending") or 0)
        if "count" in summary:
            return int(summary.get("count") or 0)
    if isinstance(summary, (list, tuple)) and summary:
        return int(summary[0] or 0)
    return 0


async def _create_pending_message(
    redis_client,
    *,
    stream: str,
    consumer_name: str,
    fields: dict[str, str],
) -> str:
    await redis_client.xgroup_create(stream, CONSUMER_GROUP, id="0", mkstream=True)
    message_id = await redis_client.xadd(stream, fields)

    delivered = await redis_client.xreadgroup(
        CONSUMER_GROUP,
        consumer_name,
        {stream: ">"},
        count=1,
        block=1000,
    )
    assert delivered
    assert _pending_count(await redis_client.xpending(stream, CONSUMER_GROUP)) == 1

    return message_id


@pytest.mark.integration
async def test_terminal_duplicate_operator_evidence_real_redis_fallback_identity_is_not_cleanup_candidate(
    redis_client,
) -> None:
    task_id = f"sprint-70-fallback-{uuid4().hex}"
    stream = f"hfa:sprint70:operator-evidence:fallback:{uuid4().hex}"
    consumer_name = "worker-sprint-70-fallback"

    await redis_client.set(DagRedisKey.task_state(task_id), "done")
    await redis_client.hset(DagRedisKey.task_meta(task_id), mapping={"run_id": task_id})

    await _create_pending_message(
        redis_client,
        stream=stream,
        consumer_name=consumer_name,
        fields={
            "run_id": task_id,
            "tenant_id": "tenant-sprint-70",
            "agent_type": "operator-evidence",
        },
    )

    before = _pending_count(await redis_client.xpending(stream, CONSUMER_GROUP))

    evidence = await read_terminal_duplicate_operator_evidence(
        redis_client,
        task_id=task_id,
        stream_key=stream,
        consumer_group=CONSUMER_GROUP,
    )

    after = _pending_count(await redis_client.xpending(stream, CONSUMER_GROUP))

    assert evidence.task_id == task_id
    assert evidence.run_id == task_id
    assert evidence.task_state == "done"
    assert evidence.terminal is True
    assert evidence.stream_pending is True
    assert evidence.message_task_id == ""
    assert evidence.message_run_id == task_id
    assert evidence.task_meta_run_id == task_id

    assert evidence.ack_allowed is False
    assert evidence.ack_policy == ACK_POLICY_NO_ACK_WITHOUT_EXPLICIT_TASK_IDENTITY
    assert evidence.cleanup_candidate is False
    assert evidence.cleanup_done is False
    assert evidence.cleanup_executed is False
    assert evidence.operator_action_required is True
    assert evidence.reason == NO_ACK_WITHOUT_EXPLICIT_TASK_IDENTITY

    assert evidence.read_only is True
    assert evidence.mutation_allowed is False
    assert evidence.production_ready_claim is False

    # Evidence read must not ACK or clean the pending stream message.
    assert before == 1
    assert after == before


@pytest.mark.integration
async def test_terminal_duplicate_operator_evidence_real_redis_explicit_identity_is_cleanup_candidate_without_cleanup(
    redis_client,
) -> None:
    task_id = f"sprint-70-explicit-task-{uuid4().hex}"
    run_id = f"sprint-70-explicit-run-{uuid4().hex}"
    stream = f"hfa:sprint70:operator-evidence:explicit:{uuid4().hex}"
    consumer_name = "worker-sprint-70-explicit"

    await redis_client.set(DagRedisKey.task_state(task_id), "done")
    await redis_client.hset(DagRedisKey.task_meta(task_id), mapping={"run_id": run_id})

    message_id = await _create_pending_message(
        redis_client,
        stream=stream,
        consumer_name=consumer_name,
        fields={
            "task_id": task_id,
            "run_id": run_id,
            "tenant_id": "tenant-sprint-70",
            "agent_type": "operator-evidence",
        },
    )

    before = _pending_count(await redis_client.xpending(stream, CONSUMER_GROUP))

    evidence = await read_terminal_duplicate_operator_evidence(
        redis_client,
        task_id=task_id,
        stream_key=stream,
        consumer_group=CONSUMER_GROUP,
    )

    after = _pending_count(await redis_client.xpending(stream, CONSUMER_GROUP))
    remaining_entry = await redis_client.xrange(stream, message_id, message_id)

    assert evidence.task_id == task_id
    assert evidence.run_id == run_id
    assert evidence.task_state == "done"
    assert evidence.terminal is True
    assert evidence.stream_pending is True
    assert evidence.pending_message_id == message_id
    assert evidence.message_task_id == task_id
    assert evidence.message_run_id == run_id
    assert evidence.task_meta_run_id == run_id

    assert evidence.message_identity_verified is True
    assert evidence.terminal_evidence_verified is True
    assert evidence.ack_allowed is True
    assert evidence.ack_policy == ACK_POLICY_ACK_EXPLICIT_TASK_RUN_TERMINAL_EVIDENCE
    assert evidence.cleanup_candidate is True
    assert evidence.cleanup_done is False
    assert evidence.cleanup_executed is False
    assert evidence.operator_action_required is False
    assert evidence.reason == EXPLICIT_TERMINAL_DUPLICATE_CLEANUP_CANDIDATE
    assert evidence.evidence_status == "cleanup_candidate"

    assert evidence.read_only is True
    assert evidence.mutation_allowed is False
    assert evidence.production_ready_claim is False

    # This endpoint/read-model only identifies a cleanup candidate.
    # It must not perform cleanup or ACK.
    assert before == 1
    assert after == before
    assert remaining_entry


@pytest.mark.integration
async def test_terminal_duplicate_operator_evidence_real_redis_no_pending_message_is_not_candidate(
    redis_client,
) -> None:
    task_id = f"sprint-70-no-pending-{uuid4().hex}"
    run_id = f"sprint-70-no-pending-run-{uuid4().hex}"
    stream = f"hfa:sprint70:operator-evidence:no-pending:{uuid4().hex}"

    await redis_client.xgroup_create(stream, CONSUMER_GROUP, id="0", mkstream=True)
    await redis_client.set(DagRedisKey.task_state(task_id), "done")
    await redis_client.hset(DagRedisKey.task_meta(task_id), mapping={"run_id": run_id})

    evidence = await read_terminal_duplicate_operator_evidence(
        redis_client,
        task_id=task_id,
        stream_key=stream,
        consumer_group=CONSUMER_GROUP,
    )

    assert evidence.task_id == task_id
    assert evidence.run_id == run_id
    assert evidence.task_state == "done"
    assert evidence.terminal is True
    assert evidence.stream_pending is False
    assert evidence.ack_allowed is False
    assert evidence.cleanup_candidate is False
    assert evidence.cleanup_done is False
    assert evidence.cleanup_executed is False
    assert evidence.operator_action_required is False
    assert evidence.reason == "NO_PENDING_STREAM_MESSAGE"
    assert evidence.evidence_status == "no_pending_message"
    assert evidence.read_only is True
    assert evidence.mutation_allowed is False
    assert evidence.production_ready_claim is False
