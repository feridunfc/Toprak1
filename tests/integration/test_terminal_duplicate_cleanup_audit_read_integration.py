from uuid import uuid4

import pytest

from hfa.dag.schema import DagRedisKey
from hfa_control.terminal_duplicate_cleanup_audit import (
    AUDIT_PHASE_INTENT,
    AUDIT_PHASE_OUTCOME,
)
from hfa_control.terminal_duplicate_cleanup_audit_read_model import (
    AUDIT_READ_OK,
    read_terminal_duplicate_cleanup_audit,
)
from hfa_control.terminal_duplicate_cleanup_command import (
    CLEANED,
    DENIED_EXECUTE_REASON_REQUIRED,
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
    task_id = f"sprint-74-audit-read-task-{unique}"
    run_id = f"sprint-74-audit-read-run-{unique}"
    stream = f"hfa:sprint74:terminal-duplicate-cleanup-audit-read:{unique}"

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


async def test_terminal_duplicate_cleanup_audit_read_real_redis_execute_returns_intent_and_outcome(
    redis_client,
):
    task_id, run_id, stream, message_id = await _setup_explicit_candidate(redis_client)
    reason = "operator_cleanup_terminal_duplicate_pending_message_secret_text"

    before_state = await redis_client.get(DagRedisKey.task_state(task_id))
    before_meta = await redis_client.hgetall(DagRedisKey.task_meta(task_id))
    before_output = await redis_client.get(DagRedisKey.task_output(task_id))

    command = await execute_terminal_duplicate_cleanup_command(
        redis_client,
        task_id=task_id,
        stream_key=stream,
        consumer_group=CONSUMER_GROUP,
        pending_message_id=message_id,
        dry_run=False,
        execute=True,
        reason=reason,
        pending_limit=100,
    )

    assert command.status == CLEANED
    assert command.audit_intent_written is True
    assert command.audit_outcome_written is True
    assert command.cleanup_executed is True
    assert command.ack_executed is True
    assert command.ack_count == 1

    read = await read_terminal_duplicate_cleanup_audit(
        redis_client,
        task_id=task_id,
        limit=100,
        scan_limit=5000,
    )

    assert read.read_status == AUDIT_READ_OK
    assert read.task_id == task_id
    assert read.entry_count == 2
    assert len(read.entries) == 2
    assert read.command_attempt_count == 1
    assert read.latest_command_attempt_id == command.command_attempt_id
    assert read.latest_status == CLEANED
    assert read.latest_cleanup_executed is True
    assert read.latest_ack_executed is True
    assert read.latest_ack_count == 1
    assert read.has_intent_without_outcome is False
    assert read.has_operator_reason_text_exposure is False
    assert read.production_ready_claim is False

    intent, outcome = read.entries

    assert intent.event_phase == AUDIT_PHASE_INTENT
    assert outcome.event_phase == AUDIT_PHASE_OUTCOME

    assert intent.command_attempt_id == command.command_attempt_id
    assert outcome.command_attempt_id == command.command_attempt_id

    assert intent.task_id == task_id
    assert outcome.task_id == task_id
    assert intent.run_id == run_id
    assert outcome.run_id == run_id
    assert intent.pending_message_id == message_id
    assert outcome.pending_message_id == message_id

    assert intent.operator_reason_present is True
    assert outcome.operator_reason_present is True
    assert intent.operator_reason_length == len(reason)
    assert outcome.operator_reason_length == len(reason)

    read_repr = repr(read)

    assert reason not in read_repr
    assert "operator_reason=" not in read_repr
    assert "operator_reason_text=" not in read_repr
    assert "operator_reason_full_text=" not in read_repr
    assert "full_operator_reason=" not in read_repr
    assert "reason_text=" not in read_repr

    assert "ACK recommended" not in read.operator_summary
    assert "Run cleanup" not in read.operator_summary

    summary = await redis_client.xpending(stream, CONSUMER_GROUP)
    assert _pending_count(summary) == 0

    # XACK removes the message from the PEL, not from the runtime stream.
    assert await redis_client.xrange(stream, message_id, message_id)

    assert await redis_client.get(DagRedisKey.task_state(task_id)) == before_state
    assert await redis_client.hgetall(DagRedisKey.task_meta(task_id)) == before_meta
    assert await redis_client.get(DagRedisKey.task_output(task_id)) == before_output


async def test_terminal_duplicate_cleanup_audit_read_real_redis_dry_run_returns_outcome_only(
    redis_client,
):
    task_id, run_id, stream, message_id = await _setup_explicit_candidate(redis_client)

    command = await execute_terminal_duplicate_cleanup_command(
        redis_client,
        task_id=task_id,
        stream_key=stream,
        consumer_group=CONSUMER_GROUP,
        dry_run=True,
        execute=False,
        pending_limit=100,
    )

    assert command.status == DRY_RUN_CLEANUP_CANDIDATE
    assert command.audit_intent_written is False
    assert command.audit_outcome_written is True
    assert command.cleanup_executed is False
    assert command.ack_executed is False

    read = await read_terminal_duplicate_cleanup_audit(
        redis_client,
        task_id=task_id,
        limit=100,
        scan_limit=5000,
    )

    assert read.read_status == AUDIT_READ_OK
    assert read.entry_count == 1
    assert len(read.entries) == 1
    assert read.command_attempt_count == 1
    assert read.latest_command_attempt_id == command.command_attempt_id
    assert read.latest_status == DRY_RUN_CLEANUP_CANDIDATE
    assert read.latest_cleanup_executed is False
    assert read.latest_ack_executed is False
    assert read.latest_ack_count == 0
    assert read.has_intent_without_outcome is False

    entry = read.entries[0]
    assert entry.event_phase == AUDIT_PHASE_OUTCOME
    assert entry.task_id == task_id
    assert entry.run_id == run_id
    assert entry.pending_message_id == message_id
    assert entry.operator_reason_present is False
    assert entry.production_ready_claim is False

    summary = await redis_client.xpending(stream, CONSUMER_GROUP)
    assert _pending_count(summary) == 1


async def test_terminal_duplicate_cleanup_audit_read_real_redis_denied_execute_returns_denied_outcome(
    redis_client,
):
    task_id, run_id, stream, message_id = await _setup_explicit_candidate(redis_client)

    command = await execute_terminal_duplicate_cleanup_command(
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

    assert command.status == DENIED_EXECUTE_REASON_REQUIRED
    assert command.audit_intent_written is False
    assert command.audit_outcome_written is True
    assert command.cleanup_executed is False
    assert command.ack_executed is False

    read = await read_terminal_duplicate_cleanup_audit(
        redis_client,
        task_id=task_id,
        limit=100,
        scan_limit=5000,
    )

    assert read.read_status == AUDIT_READ_OK
    assert read.entry_count == 1
    assert len(read.entries) == 1
    assert read.command_attempt_count == 1
    assert read.latest_command_attempt_id == command.command_attempt_id
    assert read.latest_status == DENIED_EXECUTE_REASON_REQUIRED
    assert read.latest_cleanup_executed is False
    assert read.latest_ack_executed is False
    assert read.latest_ack_count == 0
    assert read.has_intent_without_outcome is False

    entry = read.entries[0]
    assert entry.event_phase == AUDIT_PHASE_OUTCOME
    assert entry.task_id == task_id
    assert entry.run_id == run_id
    assert entry.pending_message_id == message_id
    assert entry.operator_reason_present is False
    assert entry.production_ready_claim is False

    summary = await redis_client.xpending(stream, CONSUMER_GROUP)
    assert _pending_count(summary) == 1


async def test_terminal_duplicate_cleanup_audit_read_real_redis_include_entries_false_preserves_summary(
    redis_client,
):
    task_id, _run_id, stream, message_id = await _setup_explicit_candidate(redis_client)

    command = await execute_terminal_duplicate_cleanup_command(
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

    assert command.status == CLEANED

    read = await read_terminal_duplicate_cleanup_audit(
        redis_client,
        task_id=task_id,
        limit=100,
        scan_limit=5000,
        include_entries=False,
    )

    assert read.read_status == AUDIT_READ_OK
    assert read.entries == ()
    assert read.entry_count == 2
    assert read.command_attempt_count == 1
    assert read.latest_command_attempt_id == command.command_attempt_id
    assert read.latest_status == CLEANED
    assert read.latest_ack_count == 1
    assert read.operator_summary
    assert read.production_ready_claim is False


async def test_terminal_duplicate_cleanup_audit_read_real_redis_filters_unrelated_task_entries(
    redis_client,
):
    task_id, _run_id, stream, message_id = await _setup_explicit_candidate(redis_client)
    other_task_id, _other_run_id, other_stream, other_message_id = await _setup_explicit_candidate(redis_client)

    other = await execute_terminal_duplicate_cleanup_command(
        redis_client,
        task_id=other_task_id,
        stream_key=other_stream,
        consumer_group=CONSUMER_GROUP,
        pending_message_id=other_message_id,
        dry_run=False,
        execute=True,
        reason="operator_cleanup_terminal_duplicate_pending_message",
        pending_limit=100,
    )
    assert other.status == CLEANED

    command = await execute_terminal_duplicate_cleanup_command(
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
    assert command.status == CLEANED

    read = await read_terminal_duplicate_cleanup_audit(
        redis_client,
        task_id=task_id,
        limit=100,
        scan_limit=5000,
    )

    assert read.read_status == AUDIT_READ_OK
    assert read.entry_count == 2
    assert {entry.task_id for entry in read.entries} == {task_id}
    assert {entry.command_attempt_id for entry in read.entries} == {
        command.command_attempt_id
    }
    assert other.command_attempt_id not in {
        entry.command_attempt_id for entry in read.entries
    }
    assert read.latest_status == CLEANED
    assert read.production_ready_claim is False
