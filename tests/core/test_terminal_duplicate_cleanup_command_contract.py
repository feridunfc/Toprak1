from pathlib import Path

import pytest

from hfa_control.terminal_duplicate_cleanup_command import (
    ACK_NOT_APPLIED_PENDING_MISSING,
    CLEANED,
    DENIED_CONFLICTING_EXECUTION_FLAGS,
    DENIED_EXECUTE_NOT_EXPLICIT,
    DENIED_EXECUTE_REASON_REQUIRED,
    DENIED_FALLBACK_IDENTITY,
    DENIED_PENDING_MESSAGE_ID_REQUIRED,
    DRY_RUN_CLEANUP_CANDIDATE,
    DRY_RUN_NOT_CANDIDATE,
    MUTATION_TYPE_XACK_TERMINAL_DUPLICATE_CLEANUP,
    execute_terminal_duplicate_cleanup_command,
)
from hfa_control.terminal_duplicate_operator_evidence import (
    EXPLICIT_TERMINAL_DUPLICATE_CLEANUP_CANDIDATE,
    IDENTITY_EXPLICIT_TASK_RUN,
    IDENTITY_MISSING_TASK_ID,
    NO_ACK_WITHOUT_EXPLICIT_TASK_IDENTITY,
    TerminalDuplicateOperatorEvidence,
)


def _evidence_candidate() -> TerminalDuplicateOperatorEvidence:
    return TerminalDuplicateOperatorEvidence(
        task_id="task-1",
        run_id="run-1",
        task_state="done",
        terminal=True,
        stream_pending=True,
        pending_message_id="1-0",
        message_task_id="task-1",
        message_run_id="run-1",
        task_meta_run_id="run-1",
        message_identity_verified=True,
        terminal_evidence_verified=True,
        ack_allowed=True,
        ack_policy="ack_explicit_task_run_terminal_evidence",
        cleanup_candidate=True,
        cleanup_done=False,
        cleanup_executed=False,
        operator_action_required=False,
        reason=EXPLICIT_TERMINAL_DUPLICATE_CLEANUP_CANDIDATE,
        evidence_status="cleanup_candidate",
        identity_status=IDENTITY_EXPLICIT_TASK_RUN,
        identity_reason="explicit_task_id_and_run_id_match",
        canonical_identity_confirmed=True,
        fallback_identity_detected=False,
        read_only=True,
        mutation_allowed=False,
        production_ready_claim=False,
    )


def _evidence_missing_task_id() -> TerminalDuplicateOperatorEvidence:
    return TerminalDuplicateOperatorEvidence(
        task_id="task-1",
        run_id="run-1",
        task_state="done",
        terminal=True,
        stream_pending=True,
        pending_message_id="1-0",
        message_task_id="",
        message_run_id="run-1",
        task_meta_run_id="run-1",
        message_identity_verified=False,
        terminal_evidence_verified=True,
        ack_allowed=False,
        ack_policy="no_ack_without_explicit_task_identity",
        cleanup_candidate=False,
        cleanup_done=False,
        cleanup_executed=False,
        operator_action_required=True,
        reason=NO_ACK_WITHOUT_EXPLICIT_TASK_IDENTITY,
        evidence_status="operator_attention_required",
        identity_status=IDENTITY_MISSING_TASK_ID,
        identity_reason="message_task_id_missing",
        canonical_identity_confirmed=False,
        fallback_identity_detected=False,
        read_only=True,
        mutation_allowed=False,
        production_ready_claim=False,
    )


class Redis:
    def __init__(self, *, ack_count: int = 1):
        self.calls = []
        self.ack_count = ack_count
        self.pending = [{"message_id": "1-0"}]
        self.messages = {"1-0": {"task_id": "task-1", "run_id": "run-1"}}

    async def xpending_range(self, stream, group, start, end, limit):
        self.calls.append(("xpending_range", stream, group, start, end, limit))
        return list(self.pending)

    async def xrange(self, stream, start, end):
        self.calls.append(("xrange", stream, start, end))
        fields = self.messages.get(start)
        if not fields:
            return []
        return [(start, fields)]

    async def xadd(self, stream, fields, maxlen=None, approximate=True):
        return "audit-0"

    async def xack(self, stream, group, message_id):
        self.calls.append(("xack", stream, group, message_id))
        if self.ack_count:
            self.pending = [
                entry for entry in self.pending if entry.get("message_id") != message_id
            ]
        return self.ack_count


async def _candidate_reader(redis, **kwargs):
    return _evidence_candidate()


async def _missing_task_id_reader(redis, **kwargs):
    return _evidence_missing_task_id()


@pytest.mark.asyncio
async def test_dry_run_candidate_does_not_ack():
    redis = Redis()

    result = await execute_terminal_duplicate_cleanup_command(
        redis,
        task_id="task-1",
        stream_key="stream",
        consumer_group="group",
        dry_run=True,
        execute=False,
        evidence_reader=_candidate_reader,
    )

    assert result.status == DRY_RUN_CLEANUP_CANDIDATE
    assert result.cleanup_candidate is True
    assert result.ack_allowed is True
    assert result.identity_status == IDENTITY_EXPLICIT_TASK_RUN
    assert result.canonical_identity_confirmed is True
    assert result.fallback_identity_detected is False
    assert result.evidence_snapshot["identity_status"] == IDENTITY_EXPLICIT_TASK_RUN
    assert result.cleanup_executed is False
    assert result.ack_executed is False
    assert result.ack_count == 0
    assert result.mutation_allowed is False
    assert [call[0] for call in redis.calls] == []


@pytest.mark.asyncio
async def test_dry_run_not_candidate_does_not_ack():
    redis = Redis()

    result = await execute_terminal_duplicate_cleanup_command(
        redis,
        task_id="task-1",
        stream_key="stream",
        consumer_group="group",
        dry_run=True,
        execute=False,
        evidence_reader=_missing_task_id_reader,
    )

    assert result.status == DRY_RUN_NOT_CANDIDATE
    assert result.cleanup_candidate is False
    assert result.ack_allowed is False
    assert result.identity_status == IDENTITY_MISSING_TASK_ID
    assert result.canonical_identity_confirmed is False
    assert result.fallback_identity_detected is False
    assert result.evidence_snapshot["identity_status"] == IDENTITY_MISSING_TASK_ID
    assert result.cleanup_executed is False
    assert result.ack_executed is False
    assert [call[0] for call in redis.calls] == []


@pytest.mark.asyncio
async def test_conflicting_dry_run_and_execute_is_denied():
    redis = Redis()

    result = await execute_terminal_duplicate_cleanup_command(
        redis,
        task_id="task-1",
        stream_key="stream",
        consumer_group="group",
        dry_run=True,
        execute=True,
        pending_message_id="1-0",
        reason="operator_cleanup",
        evidence_reader=_candidate_reader,
    )

    assert result.status == DENIED_CONFLICTING_EXECUTION_FLAGS
    assert result.cleanup_executed is False
    assert result.ack_executed is False
    assert [call[0] for call in redis.calls] == []


@pytest.mark.asyncio
async def test_execute_requires_explicit_execute_when_dry_run_false():
    redis = Redis()

    result = await execute_terminal_duplicate_cleanup_command(
        redis,
        task_id="task-1",
        stream_key="stream",
        consumer_group="group",
        dry_run=False,
        execute=False,
        pending_message_id="1-0",
        reason="operator_cleanup",
        evidence_reader=_candidate_reader,
    )

    assert result.status == DENIED_EXECUTE_NOT_EXPLICIT
    assert result.ack_executed is False
    assert [call[0] for call in redis.calls] == []


@pytest.mark.asyncio
async def test_execute_requires_operator_reason():
    redis = Redis()

    result = await execute_terminal_duplicate_cleanup_command(
        redis,
        task_id="task-1",
        stream_key="stream",
        consumer_group="group",
        dry_run=False,
        execute=True,
        pending_message_id="1-0",
        reason="",
        evidence_reader=_candidate_reader,
    )

    assert result.status == DENIED_EXECUTE_REASON_REQUIRED
    assert result.ack_executed is False
    assert [call[0] for call in redis.calls] == []


@pytest.mark.asyncio
async def test_execute_requires_pending_message_id():
    redis = Redis()

    result = await execute_terminal_duplicate_cleanup_command(
        redis,
        task_id="task-1",
        stream_key="stream",
        consumer_group="group",
        dry_run=False,
        execute=True,
        pending_message_id="",
        reason="operator_cleanup",
        evidence_reader=_candidate_reader,
    )

    assert result.status == DENIED_PENDING_MESSAGE_ID_REQUIRED
    assert result.ack_executed is False
    assert [call[0] for call in redis.calls] == []


@pytest.mark.asyncio
async def test_missing_task_id_is_denied_before_xack():
    redis = Redis()

    result = await execute_terminal_duplicate_cleanup_command(
        redis,
        task_id="task-1",
        stream_key="stream",
        consumer_group="group",
        dry_run=False,
        execute=True,
        pending_message_id="1-0",
        reason="operator_cleanup",
        evidence_reader=_missing_task_id_reader,
    )

    assert result.status == DENIED_FALLBACK_IDENTITY
    assert result.identity_status == IDENTITY_MISSING_TASK_ID
    assert result.canonical_identity_confirmed is False
    assert result.fallback_identity_detected is False
    assert result.evidence_snapshot["identity_status"] == IDENTITY_MISSING_TASK_ID
    assert result.cleanup_executed is False
    assert result.ack_executed is False
    assert [call[0] for call in redis.calls] == []


@pytest.mark.asyncio
async def test_explicit_candidate_executes_single_xack_only():
    redis = Redis()

    result = await execute_terminal_duplicate_cleanup_command(
        redis,
        task_id="task-1",
        stream_key="stream",
        consumer_group="group",
        dry_run=False,
        execute=True,
        pending_message_id="1-0",
        reason="operator_cleanup_terminal_duplicate_pending_message",
        evidence_reader=_candidate_reader,
    )

    assert result.status == CLEANED
    assert result.cleanup_executed is True
    assert result.ack_executed is True
    assert result.ack_count == 1
    assert result.mutation_allowed is True
    assert result.mutation_type == MUTATION_TYPE_XACK_TERMINAL_DUPLICATE_CLEANUP
    assert result.identity_status == IDENTITY_EXPLICIT_TASK_RUN
    assert result.canonical_identity_confirmed is True
    assert result.fallback_identity_detected is False
    assert result.evidence_snapshot["identity_status"] == IDENTITY_EXPLICIT_TASK_RUN
    assert result.production_ready_claim is False

    assert [call[0] for call in redis.calls] == [
        "xpending_range",
        "xrange",
        "xpending_range",
        "xrange",
        "xack",
    ]


@pytest.mark.asyncio
async def test_xack_zero_is_not_reported_as_ack_failed():
    redis = Redis(ack_count=0)

    result = await execute_terminal_duplicate_cleanup_command(
        redis,
        task_id="task-1",
        stream_key="stream",
        consumer_group="group",
        dry_run=False,
        execute=True,
        pending_message_id="1-0",
        reason="operator_cleanup_terminal_duplicate_pending_message",
        evidence_reader=_candidate_reader,
    )

    assert result.status == ACK_NOT_APPLIED_PENDING_MISSING
    assert result.cleanup_executed is False
    assert result.ack_executed is False
    assert result.ack_count == 0
    assert result.denial_reason == "xack_returned_zero"


def test_command_service_contains_only_xack_mutation_and_no_worker_import():
    source = Path(
        "hfa-control/src/hfa_control/terminal_duplicate_cleanup_command.py"
    ).read_text(encoding="utf-8").lower()

    assert "hfa_worker" not in source
    assert ".xack(" in source

    forbidden_tokens = [
        ".xclaim(",
        ".xadd(",
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
