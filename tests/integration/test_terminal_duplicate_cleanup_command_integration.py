from uuid import uuid4

import pytest

from hfa.dag.schema import DagRedisKey
from hfa_control.terminal_duplicate_cleanup_command import (
    CLEANED,
    DENIED_EXECUTE_REASON_REQUIRED,
    DENIED_FALLBACK_IDENTITY,
    DENIED_NO_PENDING_STREAM_MESSAGE,
    DENIED_NOT_CLEANUP_CANDIDATE,
    DENIED_PENDING_MESSAGE_ID_REQUIRED,
    DRY_RUN_CLEANUP_CANDIDATE,
    execute_terminal_duplicate_cleanup_command,
)


pytestmark = [pytest.mark.asyncio, pytest.mark.integration]

CONSUMER_GROUP = "worker_consumers"


def _pending_count(summary) -> int:
    if isinstance(summary, dict):
        return int(
            summary.get("pending")
            or summary.get("pending_messages")
            or summary.get("count")
            or 0
        )

    if isinstance(summary, (list, tuple)) and summary:
        return int(summary[0] or 0)

    return 0


async def _create_pending_message(
    redis_client,
    *,
    stream: str,
    group: str,
    consumer_name: str,
    fields: dict[str, str],
) -> str:
    await redis_client.xgroup_create(stream, group, id="0", mkstream=True)
    message_id = await redis_client.xadd(stream, fields)

    delivered = await redis_client.xreadgroup(
        group,
        consumer_name,
        {stream: ">"},
        count=1,
        block=1000,
    )
    assert delivered

    summary = await redis_client.xpending(stream, group)
    assert _pending_count(summary) == 1

    return str(message_id)


async def _setup_explicit_candidate(redis_client):
    unique = uuid4().hex
    task_id = f"sprint-71-cleanup-task-{unique}"
    run_id = f"sprint-71-cleanup-run-{unique}"
    stream = f"hfa:sprint71:terminal-duplicate-cleanup:{unique}"

    await redis_client.set(DagRedisKey.task_state(task_id), "done")
    await redis_client.hset(DagRedisKey.task_meta(task_id), mapping={"run_id": run_id})
    await redis_client.set(DagRedisKey.task_output(task_id), "original-output")

    message_id = await _create_pending_message(
        redis_client,
        stream=stream,
        group=CONSUMER_GROUP,
        consumer_name=f"consumer-{unique}",
        fields={
            "task_id": task_id,
            "run_id": run_id,
            "tenant_id": "tenant-1",
            "agent_type": "tester",
        },
    )

    return task_id, run_id, stream, message_id


async def test_terminal_duplicate_cleanup_real_redis_dry_run_candidate_does_not_xack(
    redis_client,
):
    task_id, run_id, stream, message_id = await _setup_explicit_candidate(redis_client)

    result = await execute_terminal_duplicate_cleanup_command(
        redis_client,
        task_id=task_id,
        stream_key=stream,
        consumer_group=CONSUMER_GROUP,
        dry_run=True,
        execute=False,
        pending_limit=100,
    )

    assert result.status == DRY_RUN_CLEANUP_CANDIDATE
    assert result.task_id == task_id
    assert result.run_id == run_id
    assert result.pending_message_id == message_id
    assert result.cleanup_candidate is True
    assert result.ack_allowed is True
    assert result.cleanup_executed is False
    assert result.ack_executed is False
    assert result.ack_count == 0
    assert result.mutation_allowed is False
    assert result.production_ready_claim is False

    summary = await redis_client.xpending(stream, CONSUMER_GROUP)
    assert _pending_count(summary) == 1


async def test_terminal_duplicate_cleanup_real_redis_fallback_identity_execute_is_denied(
    redis_client,
):
    unique = uuid4().hex
    task_id = f"sprint-71-fallback-task-{unique}"
    run_id = f"sprint-71-fallback-run-{unique}"
    stream = f"hfa:sprint71:terminal-duplicate-cleanup:fallback:{unique}"

    await redis_client.set(DagRedisKey.task_state(task_id), "done")
    await redis_client.hset(DagRedisKey.task_meta(task_id), mapping={"run_id": run_id})

    message_id = await _create_pending_message(
        redis_client,
        stream=stream,
        group=CONSUMER_GROUP,
        consumer_name=f"consumer-{unique}",
        fields={
            "run_id": run_id,
            "tenant_id": "tenant-1",
        },
    )

    result = await execute_terminal_duplicate_cleanup_command(
        redis_client,
        task_id=task_id,
        stream_key=stream,
        consumer_group=CONSUMER_GROUP,
        pending_message_id=message_id,
        dry_run=False,
        execute=True,
        reason="operator_cleanup_terminal_duplicate_pending_message",
        pending_limit=100,
    )

    assert result.status == DENIED_FALLBACK_IDENTITY
    assert result.cleanup_candidate is False
    assert result.ack_allowed is False
    assert result.cleanup_executed is False
    assert result.ack_executed is False
    assert result.ack_count == 0
    assert result.production_ready_claim is False

    summary = await redis_client.xpending(stream, CONSUMER_GROUP)
    assert _pending_count(summary) == 1


async def test_terminal_duplicate_cleanup_real_redis_execute_requires_reason(
    redis_client,
):
    task_id, _run_id, stream, message_id = await _setup_explicit_candidate(redis_client)

    result = await execute_terminal_duplicate_cleanup_command(
        redis_client,
        task_id=task_id,
        stream_key=stream,
        consumer_group=CONSUMER_GROUP,
        pending_message_id=message_id,
        dry_run=False,
        execute=True,
        reason="",
        pending_limit=100,
    )

    assert result.status == DENIED_EXECUTE_REASON_REQUIRED
    assert result.cleanup_executed is False
    assert result.ack_executed is False
    assert result.ack_count == 0

    summary = await redis_client.xpending(stream, CONSUMER_GROUP)
    assert _pending_count(summary) == 1


async def test_terminal_duplicate_cleanup_real_redis_execute_requires_pending_message_id(
    redis_client,
):
    task_id, _run_id, stream, _message_id = await _setup_explicit_candidate(redis_client)

    result = await execute_terminal_duplicate_cleanup_command(
        redis_client,
        task_id=task_id,
        stream_key=stream,
        consumer_group=CONSUMER_GROUP,
        pending_message_id="",
        dry_run=False,
        execute=True,
        reason="operator_cleanup_terminal_duplicate_pending_message",
        pending_limit=100,
    )

    assert result.status == DENIED_PENDING_MESSAGE_ID_REQUIRED
    assert result.cleanup_executed is False
    assert result.ack_executed is False
    assert result.ack_count == 0

    summary = await redis_client.xpending(stream, CONSUMER_GROUP)
    assert _pending_count(summary) == 1


async def test_terminal_duplicate_cleanup_real_redis_wrong_pending_message_id_is_denied(
    redis_client,
):
    task_id, _run_id, stream, _message_id = await _setup_explicit_candidate(redis_client)

    result = await execute_terminal_duplicate_cleanup_command(
        redis_client,
        task_id=task_id,
        stream_key=stream,
        consumer_group=CONSUMER_GROUP,
        pending_message_id="9999999999999-0",
        dry_run=False,
        execute=True,
        reason="operator_cleanup_terminal_duplicate_pending_message",
        pending_limit=100,
    )

    assert result.status == DENIED_NOT_CLEANUP_CANDIDATE
    assert result.denial_reason == "pending_message_id_does_not_match_evidence"
    assert result.cleanup_executed is False
    assert result.ack_executed is False
    assert result.ack_count == 0

    summary = await redis_client.xpending(stream, CONSUMER_GROUP)
    assert _pending_count(summary) == 1


async def test_terminal_duplicate_cleanup_real_redis_explicit_identity_executes_single_xack_only(
    redis_client,
):
    task_id, run_id, stream, message_id = await _setup_explicit_candidate(redis_client)

    before_state = await redis_client.get(DagRedisKey.task_state(task_id))
    before_meta = await redis_client.hgetall(DagRedisKey.task_meta(task_id))
    before_output = await redis_client.get(DagRedisKey.task_output(task_id))

    result = await execute_terminal_duplicate_cleanup_command(
        redis_client,
        task_id=task_id,
        stream_key=stream,
        consumer_group=CONSUMER_GROUP,
        pending_message_id=message_id,
        dry_run=False,
        execute=True,
        reason="operator_cleanup_terminal_duplicate_pending_message",
        pending_limit=100,
    )

    assert result.status == CLEANED
    assert result.task_id == task_id
    assert result.run_id == run_id
    assert result.pending_message_id == message_id
    assert result.cleanup_candidate is True
    assert result.ack_allowed is True
    assert result.cleanup_executed is True
    assert result.ack_executed is True
    assert result.ack_count == 1
    assert result.mutation_allowed is True
    assert result.mutation_type == "xack_terminal_duplicate_cleanup"
    assert result.production_ready_claim is False

    summary = await redis_client.xpending(stream, CONSUMER_GROUP)
    assert _pending_count(summary) == 0

    # XACK removes the message from the PEL, not from the stream itself.
    assert await redis_client.xrange(stream, message_id, message_id)

    assert await redis_client.get(DagRedisKey.task_state(task_id)) == before_state
    assert await redis_client.hgetall(DagRedisKey.task_meta(task_id)) == before_meta
    assert await redis_client.get(DagRedisKey.task_output(task_id)) == before_output


async def test_terminal_duplicate_cleanup_real_redis_second_execute_after_cleanup_does_not_mutate_again(
    redis_client,
):
    task_id, _run_id, stream, message_id = await _setup_explicit_candidate(redis_client)

    first = await execute_terminal_duplicate_cleanup_command(
        redis_client,
        task_id=task_id,
        stream_key=stream,
        consumer_group=CONSUMER_GROUP,
        pending_message_id=message_id,
        dry_run=False,
        execute=True,
        reason="operator_cleanup_terminal_duplicate_pending_message",
        pending_limit=100,
    )

    assert first.status == CLEANED
    assert first.ack_count == 1

    second = await execute_terminal_duplicate_cleanup_command(
        redis_client,
        task_id=task_id,
        stream_key=stream,
        consumer_group=CONSUMER_GROUP,
        pending_message_id=message_id,
        dry_run=False,
        execute=True,
        reason="operator_cleanup_terminal_duplicate_pending_message",
        pending_limit=100,
    )

    assert second.status == DENIED_NO_PENDING_STREAM_MESSAGE
    assert second.cleanup_executed is False
    assert second.ack_executed is False
    assert second.ack_count == 0

    summary = await redis_client.xpending(stream, CONSUMER_GROUP)
    assert _pending_count(summary) == 0
