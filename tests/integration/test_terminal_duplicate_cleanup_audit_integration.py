from uuid import uuid4

import pytest

from hfa.dag.schema import DagRedisKey
from hfa_control.terminal_duplicate_cleanup_audit import (
    AUDIT_PHASE_INTENT,
    AUDIT_PHASE_OUTCOME,
    TERMINAL_DUPLICATE_CLEANUP_AUDIT_STREAM,
)
from hfa_control.terminal_duplicate_cleanup_command import (
    CLEANED,
    DENIED_EXECUTE_REASON_REQUIRED,
    DRY_RUN_CLEANUP_CANDIDATE,
    execute_terminal_duplicate_cleanup_command,
)


pytestmark = [pytest.mark.asyncio, pytest.mark.integration]

CONSUMER_GROUP = "worker_consumers"


def _decode(value) -> str:
    if value is None:
        return ""
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return str(value)


def _normalize_fields(fields) -> dict[str, str]:
    if isinstance(fields, dict):
        return {_decode(key): _decode(value) for key, value in fields.items()}

    if isinstance(fields, (list, tuple)):
        items = list(fields)
        out: dict[str, str] = {}
        for index in range(0, len(items) - 1, 2):
            out[_decode(items[index])] = _decode(items[index + 1])
        return out

    return {}


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


def _stream_id_tuple(message_id: str) -> tuple[int, int]:
    left, right = str(message_id).split("-", 1)
    return int(left), int(right)


async def _read_audit_event(redis_client, audit_id: str) -> dict[str, str]:
    raw = await redis_client.xrange(
        TERMINAL_DUPLICATE_CLEANUP_AUDIT_STREAM,
        audit_id,
        audit_id,
    )
    assert raw, f"audit event {audit_id!r} not found"

    first = raw[0]
    if isinstance(first, dict):
        return _normalize_fields(first)

    if isinstance(first, (list, tuple)) and len(first) >= 2:
        return _normalize_fields(first[1])

    raise AssertionError(f"unexpected audit event shape: {first!r}")


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
    task_id = f"sprint-73-audit-task-{unique}"
    run_id = f"sprint-73-audit-run-{unique}"
    stream = f"hfa:sprint73:terminal-duplicate-cleanup-audit:{unique}"

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


async def test_terminal_duplicate_cleanup_audit_real_redis_dry_run_writes_outcome_only(
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
    assert result.command_attempt_id.startswith("tdc-")
    assert result.audit_intent_written is False
    assert result.audit_intent_id == ""
    assert result.audit_outcome_written is True
    assert result.audit_outcome_id
    assert result.cleanup_executed is False
    assert result.ack_executed is False
    assert result.ack_count == 0
    assert result.command_safety["append_only_audit_attempted"] is True
    assert result.command_safety["runtime_stream_audit_attempted"] is False
    assert result.production_ready_claim is False

    outcome = await _read_audit_event(redis_client, result.audit_outcome_id)

    assert outcome["event_type"] == "terminal_duplicate_cleanup_command"
    assert outcome["event_phase"] == AUDIT_PHASE_OUTCOME
    assert outcome["command_attempt_id"] == result.command_attempt_id
    assert outcome["task_id"] == task_id
    assert outcome["run_id"] == run_id
    assert outcome["pending_message_id"] == message_id
    assert outcome["status"] == DRY_RUN_CLEANUP_CANDIDATE
    assert outcome["dry_run"] == "true"
    assert outcome["execute_requested"] == "false"
    assert outcome["cleanup_executed"] == "false"
    assert outcome["ack_executed"] == "false"
    assert outcome["ack_count"] == "0"
    assert outcome["operator_reason_present"] == "false"
    assert outcome["production_ready_claim"] == "false"
    assert "operator_reason" not in outcome

    summary = await redis_client.xpending(stream, CONSUMER_GROUP)
    assert _pending_count(summary) == 1


async def test_terminal_duplicate_cleanup_audit_real_redis_execute_writes_intent_then_outcome_and_xacks(
    redis_client,
):
    task_id, run_id, stream, message_id = await _setup_explicit_candidate(redis_client)
    reason = "operator_cleanup_terminal_duplicate_pending_message_secret_text"

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
        reason=reason,
        pending_limit=100,
    )

    assert result.status == CLEANED
    assert result.command_attempt_id.startswith("tdc-")
    assert result.audit_intent_written is True
    assert result.audit_intent_id
    assert result.audit_outcome_written is True
    assert result.audit_outcome_id
    assert result.audit_required_for_execute is True
    assert result.cleanup_executed is True
    assert result.ack_executed is True
    assert result.ack_count == 1
    assert result.production_ready_claim is False

    assert _stream_id_tuple(result.audit_intent_id) <= _stream_id_tuple(
        result.audit_outcome_id
    )

    intent = await _read_audit_event(redis_client, result.audit_intent_id)
    outcome = await _read_audit_event(redis_client, result.audit_outcome_id)

    assert intent["event_phase"] == AUDIT_PHASE_INTENT
    assert outcome["event_phase"] == AUDIT_PHASE_OUTCOME

    assert intent["command_attempt_id"] == result.command_attempt_id
    assert outcome["command_attempt_id"] == result.command_attempt_id

    assert intent["task_id"] == task_id
    assert outcome["task_id"] == task_id
    assert intent["run_id"] == run_id
    assert outcome["run_id"] == run_id
    assert intent["pending_message_id"] == message_id
    assert outcome["pending_message_id"] == message_id

    assert intent["status"] == "PENDING_EXECUTION"
    assert outcome["status"] == CLEANED

    assert intent["operator_reason_present"] == "true"
    assert outcome["operator_reason_present"] == "true"
    assert intent["operator_reason_length"] == str(len(reason))
    assert outcome["operator_reason_length"] == str(len(reason))
    assert "operator_reason" not in intent
    assert "operator_reason" not in outcome
    assert reason not in intent.values()
    assert reason not in outcome.values()

    assert intent["cleanup_executed"] == "false"
    assert intent["ack_executed"] == "false"
    assert intent["ack_count"] == "0"

    assert outcome["cleanup_executed"] == "true"
    assert outcome["ack_executed"] == "true"
    assert outcome["ack_count"] == "1"

    assert intent["production_ready_claim"] == "false"
    assert outcome["production_ready_claim"] == "false"

    summary = await redis_client.xpending(stream, CONSUMER_GROUP)
    assert _pending_count(summary) == 0

    # XACK removes the message from the PEL, not from the runtime stream.
    runtime_entry = await redis_client.xrange(stream, message_id, message_id)
    assert runtime_entry

    assert await redis_client.get(DagRedisKey.task_state(task_id)) == before_state
    assert await redis_client.hgetall(DagRedisKey.task_meta(task_id)) == before_meta
    assert await redis_client.get(DagRedisKey.task_output(task_id)) == before_output


async def test_terminal_duplicate_cleanup_audit_real_redis_denied_command_writes_best_effort_outcome(
    redis_client,
):
    task_id, run_id, stream, message_id = await _setup_explicit_candidate(redis_client)

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
    assert result.audit_intent_written is False
    assert result.audit_outcome_written is True
    assert result.cleanup_executed is False
    assert result.ack_executed is False
    assert result.ack_count == 0
    assert result.production_ready_claim is False

    outcome = await _read_audit_event(redis_client, result.audit_outcome_id)

    assert outcome["event_phase"] == AUDIT_PHASE_OUTCOME
    assert outcome["command_attempt_id"] == result.command_attempt_id
    assert outcome["task_id"] == task_id
    assert outcome["run_id"] == run_id
    assert outcome["pending_message_id"] == message_id
    assert outcome["status"] == DENIED_EXECUTE_REASON_REQUIRED
    assert outcome["operator_reason_present"] == "false"
    assert "operator_reason" not in outcome
    assert outcome["production_ready_claim"] == "false"

    summary = await redis_client.xpending(stream, CONSUMER_GROUP)
    assert _pending_count(summary) == 1
