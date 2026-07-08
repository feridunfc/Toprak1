from dataclasses import asdict

import pytest

from hfa_control.terminal_duplicate_cleanup_command import (
    ACK_NOT_APPLIED_PENDING_MISSING,
    CLEANED,
    DENIED_EXECUTE_REASON_REQUIRED,
    DRY_RUN_CLEANUP_CANDIDATE,
    execute_terminal_duplicate_cleanup_command,
)
from hfa_control.terminal_duplicate_operator_evidence import (
    EXPLICIT_TERMINAL_DUPLICATE_CLEANUP_CANDIDATE,
    TerminalDuplicateOperatorEvidence,
)


def _candidate_evidence() -> TerminalDuplicateOperatorEvidence:
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
        read_only=True,
        mutation_allowed=False,
        production_ready_claim=False,
    )


class Redis:
    def __init__(self, *, ack_count: int = 1):
        self.ack_count = ack_count
        self.pending = [{"message_id": "1-0"}]
        self.messages = {"1-0": {"task_id": "task-1", "run_id": "run-1"}}

    async def xpending_range(self, stream, group, start, end, limit):
        return list(self.pending)

    async def xrange(self, stream, start, end):
        fields = self.messages.get(start)
        if not fields:
            return []
        return [(start, fields)]

    async def xack(self, stream, group, message_id):
        if self.ack_count:
            self.pending = [
                entry for entry in self.pending if entry.get("message_id") != message_id
            ]
        return self.ack_count


async def _candidate_reader(redis, **kwargs):
    return _candidate_evidence()


def _assert_result_surface(result, *, expected_status: str) -> None:
    payload = asdict(result)

    assert payload["status"] == expected_status
    assert payload["production_ready_claim"] is False

    assert isinstance(payload["operator_summary"], str)
    assert payload["operator_summary"].strip()

    evidence_snapshot = payload["evidence_snapshot"]
    assert evidence_snapshot["reason"] == result.evidence_reason
    assert evidence_snapshot["status"] == result.evidence_status
    assert evidence_snapshot["ack_policy"] == result.ack_policy
    assert evidence_snapshot["ack_allowed"] == result.ack_allowed
    assert evidence_snapshot["cleanup_candidate"] == result.cleanup_candidate
    assert evidence_snapshot["message_identity_verified"] is True
    assert evidence_snapshot["terminal_evidence_verified"] is True
    assert evidence_snapshot["production_ready_claim"] is False

    command_decision = payload["command_decision"]
    assert command_decision["dry_run"] == result.dry_run
    assert command_decision["execute_requested"] == result.execute_requested
    assert "pending_message_id_required" in command_decision
    assert "pending_message_id_present" in command_decision
    assert "reason_required" in command_decision
    assert "reason_present" in command_decision
    assert "pel_reread_required" in command_decision
    assert "xrange_reread_required" in command_decision
    assert command_decision["production_ready_claim"] is False

    command_safety = payload["command_safety"]
    assert command_safety["mutation_boundary"] == "xack_only"
    assert command_safety["xclaim_attempted"] is False
    assert command_safety["xadd_attempted"] is False
    assert command_safety["state_write_attempted"] is False
    assert command_safety["meta_write_attempted"] is False
    assert command_safety["output_write_attempted"] is False
    assert command_safety["repair_attempted"] is False
    assert command_safety["requeue_attempted"] is False
    assert command_safety["persistent_audit_attempted"] is False
    assert command_safety["production_ready_claim"] is False


@pytest.mark.asyncio
async def test_dry_run_candidate_result_surface_is_operator_readable():
    result = await execute_terminal_duplicate_cleanup_command(
        Redis(),
        task_id="task-1",
        stream_key="stream",
        consumer_group="group",
        dry_run=True,
        execute=False,
        evidence_reader=_candidate_reader,
    )

    _assert_result_surface(result, expected_status=DRY_RUN_CLEANUP_CANDIDATE)
    assert "Dry run only" in result.operator_summary
    assert result.command_decision["pending_message_id_required"] is False
    assert result.command_safety["mutation_executed"] is False
    assert result.command_safety["xack_attempted"] is False


@pytest.mark.asyncio
async def test_denied_result_surface_explains_missing_operator_reason():
    result = await execute_terminal_duplicate_cleanup_command(
        Redis(),
        task_id="task-1",
        stream_key="stream",
        consumer_group="group",
        pending_message_id="1-0",
        dry_run=False,
        execute=True,
        reason="",
        evidence_reader=_candidate_reader,
    )

    _assert_result_surface(result, expected_status=DENIED_EXECUTE_REASON_REQUIRED)
    assert "operator reason" in result.operator_summary
    assert result.command_decision["reason_required"] is True
    assert result.command_decision["reason_present"] is False
    assert result.command_safety["mutation_executed"] is False
    assert result.command_safety["xack_attempted"] is False


@pytest.mark.asyncio
async def test_cleaned_result_surface_reports_single_xack_boundary():
    result = await execute_terminal_duplicate_cleanup_command(
        Redis(),
        task_id="task-1",
        stream_key="stream",
        consumer_group="group",
        pending_message_id="1-0",
        dry_run=False,
        execute=True,
        reason="operator_cleanup_terminal_duplicate_pending_message",
        evidence_reader=_candidate_reader,
    )

    _assert_result_surface(result, expected_status=CLEANED)
    assert "Cleanup executed" in result.operator_summary
    assert result.command_decision["pending_message_id_required"] is True
    assert result.command_decision["pending_message_id_present"] is True
    assert result.command_decision["reason_required"] is True
    assert result.command_decision["reason_present"] is True
    assert result.command_decision["single_xack_allowed"] is True
    assert result.command_safety["mutation_executed"] is True
    assert result.command_safety["xack_attempted"] is True


@pytest.mark.asyncio
async def test_xack_zero_result_surface_is_not_reported_as_successful_cleanup():
    result = await execute_terminal_duplicate_cleanup_command(
        Redis(ack_count=0),
        task_id="task-1",
        stream_key="stream",
        consumer_group="group",
        pending_message_id="1-0",
        dry_run=False,
        execute=True,
        reason="operator_cleanup_terminal_duplicate_pending_message",
        evidence_reader=_candidate_reader,
    )

    _assert_result_surface(result, expected_status=ACK_NOT_APPLIED_PENDING_MISSING)
    assert "ACK not applied" in result.operator_summary
    assert result.command_safety["mutation_executed"] is False
    assert result.command_safety["xack_attempted"] is True
