from types import SimpleNamespace

import pytest

from hfa_control.terminal_duplicate_cleanup_audit import (
    AUDIT_BEST_EFFORT_WRITE_FAILED,
    AUDIT_INTENT_WRITE_FAILED,
    AUDIT_OUTCOME_WRITE_FAILED,
    AUDIT_PHASE_INTENT,
    AUDIT_PHASE_OUTCOME,
    TERMINAL_DUPLICATE_CLEANUP_AUDIT_STREAM,
)
from hfa_control.terminal_duplicate_cleanup_command import (
    CLEANED,
    CLEANED_AUDIT_OUTCOME_WRITE_FAILED,
    DENIED_AUDIT_INTENT_WRITE_FAILED,
    DRY_RUN_CLEANUP_CANDIDATE,
    execute_terminal_duplicate_cleanup_command,
)


def _candidate_evidence() -> SimpleNamespace:
    return SimpleNamespace(
        task_id="task-1",
        run_id="run-1",
        task_state="done",
        terminal=True,
        pending_message_id="1-0",
        message_task_id="task-1",
        message_run_id="run-1",
        task_meta_run_id="run-1",
        reason="EXPLICIT_TERMINAL_DUPLICATE_CLEANUP_CANDIDATE",
        evidence_status="cleanup_candidate",
        ack_policy="ack_explicit_task_run_terminal_evidence",
        ack_allowed=True,
        cleanup_candidate=True,
        message_identity_verified=True,
        terminal_evidence_verified=True,
        operator_action_required=False,
    )


async def _candidate_reader(redis, **kwargs):
    return _candidate_evidence()


class Redis:
    def __init__(self, *, fail_intent=False, fail_outcome=False, ack_count=1):
        self.fail_intent = fail_intent
        self.fail_outcome = fail_outcome
        self.ack_count = ack_count
        self.xadd_count = 0
        self.calls = []

    async def xadd(self, stream, fields, maxlen=None, approximate=True):
        self.xadd_count += 1
        self.calls.append(("xadd", stream, dict(fields)))
        if self.xadd_count == 1 and self.fail_intent:
            raise RuntimeError("intent audit unavailable")
        if self.xadd_count >= 2 and self.fail_outcome:
            raise RuntimeError("outcome audit unavailable")
        return f"audit-{self.xadd_count}-0"

    async def xpending_range(self, stream, group, start, end, count):
        self.calls.append(("xpending_range", stream, group, count))
        return [{"message_id": "1-0"}]

    async def xrange(self, stream, start, end):
        self.calls.append(("xrange", stream, start, end))
        return [("1-0", {"task_id": "task-1", "run_id": "run-1"})]

    async def xack(self, stream, group, message_id):
        self.calls.append(("xack", stream, group, message_id))
        return self.ack_count


@pytest.mark.asyncio
async def test_dry_run_writes_best_effort_outcome_audit_without_xack():
    redis = Redis()

    result = await execute_terminal_duplicate_cleanup_command(
        redis,
        task_id="task-1",
        stream_key="hfa:stream:runs:0",
        consumer_group="worker_consumers",
        dry_run=True,
        execute=False,
        evidence_reader=_candidate_reader,
    )

    assert result.status == DRY_RUN_CLEANUP_CANDIDATE
    assert result.command_attempt_id.startswith("tdc-")
    assert result.audit_intent_written is False
    assert result.audit_outcome_written is True
    assert result.audit_outcome_id == "audit-1-0"
    assert result.audit_error == ""
    assert result.command_safety["append_only_audit_attempted"] is True
    assert result.command_safety["runtime_stream_audit_attempted"] is False
    assert result.production_ready_claim is False

    assert [call[0] for call in redis.calls] == ["xadd"]
    stream, fields = redis.calls[0][1], redis.calls[0][2]
    assert stream == TERMINAL_DUPLICATE_CLEANUP_AUDIT_STREAM
    assert fields["event_phase"] == AUDIT_PHASE_OUTCOME
    assert fields["command_attempt_id"] == result.command_attempt_id
    assert "operator_reason" not in fields


@pytest.mark.asyncio
async def test_execute_candidate_writes_intent_before_xack_and_outcome_after():
    redis = Redis()

    result = await execute_terminal_duplicate_cleanup_command(
        redis,
        task_id="task-1",
        stream_key="hfa:stream:runs:0",
        consumer_group="worker_consumers",
        pending_message_id="1-0",
        dry_run=False,
        execute=True,
        reason="operator cleanup approved",
        evidence_reader=_candidate_reader,
    )

    assert result.status == CLEANED
    assert result.cleanup_executed is True
    assert result.ack_executed is True
    assert result.ack_count == 1
    assert result.audit_intent_written is True
    assert result.audit_intent_id == "audit-1-0"
    assert result.audit_outcome_written is True
    assert result.audit_outcome_id == "audit-2-0"
    assert result.audit_required_for_execute is True
    assert result.audit_error == ""
    assert result.command_safety["append_only_audit_attempted"] is True
    assert result.command_safety["runtime_stream_audit_attempted"] is False

    call_names = [call[0] for call in redis.calls]
    assert call_names.index("xadd") < call_names.index("xack")
    assert call_names[-1] == "xadd"

    intent_fields = redis.calls[0][2]
    outcome_fields = redis.calls[-1][2]

    assert intent_fields["event_phase"] == AUDIT_PHASE_INTENT
    assert outcome_fields["event_phase"] == AUDIT_PHASE_OUTCOME
    assert intent_fields["command_attempt_id"] == result.command_attempt_id
    assert outcome_fields["command_attempt_id"] == result.command_attempt_id
    assert intent_fields["operator_reason_present"] == "true"
    assert "operator_reason" not in intent_fields
    assert "operator_reason" not in outcome_fields


@pytest.mark.asyncio
async def test_audit_intent_failure_blocks_execute_xack():
    redis = Redis(fail_intent=True)

    result = await execute_terminal_duplicate_cleanup_command(
        redis,
        task_id="task-1",
        stream_key="hfa:stream:runs:0",
        consumer_group="worker_consumers",
        pending_message_id="1-0",
        dry_run=False,
        execute=True,
        reason="operator cleanup approved",
        evidence_reader=_candidate_reader,
    )

    assert result.status == DENIED_AUDIT_INTENT_WRITE_FAILED
    assert result.cleanup_executed is False
    assert result.ack_executed is False
    assert result.audit_intent_written is False
    assert result.audit_intent_id == ""
    assert result.audit_status == AUDIT_INTENT_WRITE_FAILED
    assert "intent audit unavailable" in result.audit_error
    assert result.production_ready_claim is False

    assert [call[0] for call in redis.calls] == ["xadd"]


@pytest.mark.asyncio
async def test_audit_outcome_failure_after_xack_preserves_cleanup_truth():
    redis = Redis(fail_outcome=True)

    result = await execute_terminal_duplicate_cleanup_command(
        redis,
        task_id="task-1",
        stream_key="hfa:stream:runs:0",
        consumer_group="worker_consumers",
        pending_message_id="1-0",
        dry_run=False,
        execute=True,
        reason="operator cleanup approved",
        evidence_reader=_candidate_reader,
    )

    assert result.status == CLEANED_AUDIT_OUTCOME_WRITE_FAILED
    assert result.cleanup_executed is True
    assert result.ack_executed is True
    assert result.ack_count == 1
    assert result.audit_intent_written is True
    assert result.audit_outcome_written is False
    assert result.audit_status == AUDIT_OUTCOME_WRITE_FAILED
    assert "outcome audit unavailable" in result.audit_error

    call_names = [call[0] for call in redis.calls]
    assert "xack" in call_names
    assert call_names[-1] == "xadd"


@pytest.mark.asyncio
async def test_dry_run_audit_failure_is_best_effort_and_does_not_ack():
    redis = Redis(fail_intent=True)

    result = await execute_terminal_duplicate_cleanup_command(
        redis,
        task_id="task-1",
        stream_key="hfa:stream:runs:0",
        consumer_group="worker_consumers",
        dry_run=True,
        execute=False,
        evidence_reader=_candidate_reader,
    )

    assert result.status == DRY_RUN_CLEANUP_CANDIDATE
    assert result.cleanup_executed is False
    assert result.ack_executed is False
    assert result.audit_outcome_written is False
    assert result.audit_status == AUDIT_BEST_EFFORT_WRITE_FAILED
    assert result.command_safety["append_only_audit_attempted"] is True

    assert [call[0] for call in redis.calls] == ["xadd"]


def test_command_service_uses_audit_helper_without_direct_xadd():
    source = open(
        "hfa-control/src/hfa_control/terminal_duplicate_cleanup_command.py",
        encoding="utf-8",
    ).read().lower()

    assert "append_terminal_duplicate_cleanup_audit_event" in source
    assert ".xack(" in source
    assert ".xadd(" not in source
    assert ".xclaim(" not in source
    assert "rediskey.stream_shard" not in source
    assert "persistent_audit_attempted" not in source
