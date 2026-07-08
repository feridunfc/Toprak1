from pathlib import Path

import pytest

from hfa_control.terminal_duplicate_cleanup_audit import (
    AUDIT_BEST_EFFORT_WRITE_FAILED,
    AUDIT_INTENT_WRITE_FAILED,
    AUDIT_INTENT_WRITTEN,
    AUDIT_OUTCOME_WRITTEN,
    AUDIT_PHASE_INTENT,
    AUDIT_PHASE_OUTCOME,
    TERMINAL_DUPLICATE_CLEANUP_AUDIT_EVENT_TYPE,
    TERMINAL_DUPLICATE_CLEANUP_AUDIT_STREAM,
    TerminalDuplicateCleanupAuditEvent,
    append_terminal_duplicate_cleanup_audit_event,
    new_cleanup_command_attempt_id,
    terminal_duplicate_cleanup_audit_fields,
)


class Redis:
    def __init__(self, *, fail: bool = False):
        self.fail = fail
        self.calls = []

    async def xadd(self, stream, fields, maxlen=None, approximate=True):
        self.calls.append((stream, fields, maxlen, approximate))
        if self.fail:
            raise RuntimeError("audit unavailable")
        return "123-0"


def test_new_cleanup_command_attempt_id_is_prefixed_and_unique():
    first = new_cleanup_command_attempt_id()
    second = new_cleanup_command_attempt_id()

    assert first.startswith("tdc-")
    assert second.startswith("tdc-")
    assert first != second


def test_audit_fields_are_string_safe_and_exclude_operator_reason_text():
    event = TerminalDuplicateCleanupAuditEvent(
        command_attempt_id="tdc-1",
        event_phase=AUDIT_PHASE_INTENT,
        task_id="task-1",
        run_id="run-1",
        stream="hfa:stream:runs:0",
        group="worker_consumers",
        pending_message_id="1-0",
        dry_run=False,
        execute_requested=True,
        status="PENDING_EXECUTION",
        operator_reason_present=True,
        operator_reason_length=52,
        evidence_reason="EXPLICIT_TERMINAL_DUPLICATE_CLEANUP_CANDIDATE",
        evidence_status="cleanup_candidate",
        ack_policy="ack_explicit_task_run_terminal_evidence",
        ack_allowed=True,
        cleanup_candidate=True,
        metadata={"source": "unit"},
    )

    fields = terminal_duplicate_cleanup_audit_fields(event)

    assert fields["event_type"] == TERMINAL_DUPLICATE_CLEANUP_AUDIT_EVENT_TYPE
    assert fields["event_phase"] == AUDIT_PHASE_INTENT
    assert fields["command_attempt_id"] == "tdc-1"
    assert fields["task_id"] == "task-1"
    assert fields["run_id"] == "run-1"
    assert fields["pending_message_id"] == "1-0"
    assert fields["dry_run"] == "false"
    assert fields["execute_requested"] == "true"
    assert fields["operator_reason_present"] == "true"
    assert fields["operator_reason_length"] == "52"
    assert fields["cleanup_candidate"] == "true"
    assert fields["ack_allowed"] == "true"
    assert fields["production_ready_claim"] == "false"
    assert fields["metadata_source"] == "unit"

    assert "operator_reason" not in fields
    assert all(isinstance(key, str) for key in fields)
    assert all(isinstance(value, str) for value in fields.values())


@pytest.mark.asyncio
async def test_append_writes_dedicated_audit_stream_with_xadd():
    redis = Redis()
    event = TerminalDuplicateCleanupAuditEvent(
        command_attempt_id="tdc-1",
        event_phase=AUDIT_PHASE_INTENT,
        task_id="task-1",
        run_id="run-1",
        pending_message_id="1-0",
        dry_run=False,
        execute_requested=True,
        status="PENDING_EXECUTION",
    )

    result = await append_terminal_duplicate_cleanup_audit_event(
        redis,
        event,
        required_for_execute=True,
    )

    assert result.written is True
    assert result.audit_stream == TERMINAL_DUPLICATE_CLEANUP_AUDIT_STREAM
    assert result.audit_id == "123-0"
    assert result.audit_status == AUDIT_INTENT_WRITTEN
    assert result.audit_error == ""
    assert result.production_ready_claim is False

    assert len(redis.calls) == 1
    stream, fields, maxlen, approximate = redis.calls[0]
    assert stream == TERMINAL_DUPLICATE_CLEANUP_AUDIT_STREAM
    assert fields["command_attempt_id"] == "tdc-1"
    assert fields["event_phase"] == AUDIT_PHASE_INTENT
    assert maxlen == 10_000
    assert approximate is True


@pytest.mark.asyncio
async def test_append_failure_for_required_execute_intent_is_reported_as_intent_failure():
    redis = Redis(fail=True)
    event = TerminalDuplicateCleanupAuditEvent(
        command_attempt_id="tdc-1",
        event_phase=AUDIT_PHASE_INTENT,
        task_id="task-1",
        status="PENDING_EXECUTION",
    )

    result = await append_terminal_duplicate_cleanup_audit_event(
        redis,
        event,
        required_for_execute=True,
    )

    assert result.written is False
    assert result.audit_id == ""
    assert result.audit_status == AUDIT_INTENT_WRITE_FAILED
    assert "audit unavailable" in result.audit_error
    assert result.production_ready_claim is False


@pytest.mark.asyncio
async def test_append_failure_for_best_effort_outcome_is_reported_without_raising():
    redis = Redis(fail=True)
    event = TerminalDuplicateCleanupAuditEvent(
        command_attempt_id="tdc-1",
        event_phase=AUDIT_PHASE_OUTCOME,
        task_id="task-1",
        status="DRY_RUN_CLEANUP_CANDIDATE",
    )

    result = await append_terminal_duplicate_cleanup_audit_event(
        redis,
        event,
        required_for_execute=False,
    )

    assert result.written is False
    assert result.audit_status == AUDIT_BEST_EFFORT_WRITE_FAILED
    assert "audit unavailable" in result.audit_error


@pytest.mark.asyncio
async def test_append_success_for_outcome_reports_outcome_written():
    redis = Redis()
    event = TerminalDuplicateCleanupAuditEvent(
        command_attempt_id="tdc-1",
        event_phase=AUDIT_PHASE_OUTCOME,
        task_id="task-1",
        status="CLEANED",
    )

    result = await append_terminal_duplicate_cleanup_audit_event(redis, event)

    assert result.written is True
    assert result.audit_status == AUDIT_OUTCOME_WRITTEN


def test_audit_module_static_contract_allows_only_dedicated_xadd_append():
    source = Path(
        "hfa-control/src/hfa_control/terminal_duplicate_cleanup_audit.py"
    ).read_text(encoding="utf-8").lower()

    assert ".xadd(" in source
    assert "hfa:control:audit:terminal_duplicate_cleanup" in source
    assert "hfa:stream:runs" not in source
    assert "rediskey.stream_shard" not in source

    forbidden_tokens = [
        ".xack(",
        ".xclaim(",
        ".xpending",
        ".xrange(",
        ".hset(",
        ".set(",
        ".delete(",
        ".expire(",
        "requeue(",
        "repair(",
        "task_complete(",
        "claim_start(",
    ]

    for token in forbidden_tokens:
        assert token not in source
