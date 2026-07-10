import pytest

from hfa_control.terminal_duplicate_cleanup_audit import (
    TerminalDuplicateCleanupAuditEvent,
    terminal_duplicate_cleanup_audit_fields,
)
from hfa_control.terminal_duplicate_cleanup_command import (
    DENIED_FALLBACK_IDENTITY,
    DENIED_RUN_ID_MISMATCH,
    DENIED_TASK_ID_MISMATCH,
    DENIED_TASK_META_RUN_ID_MISMATCH,
    execute_terminal_duplicate_cleanup_command,
)
from hfa_control.terminal_duplicate_operator_evidence import (
    ACK_POLICY_ACK_EXPLICIT_TASK_RUN_TERMINAL_EVIDENCE,
    EXPLICIT_TERMINAL_DUPLICATE_CLEANUP_CANDIDATE,
    IDENTITY_EXPLICIT_TASK_RUN,
    IDENTITY_FALLBACK_RUN_ONLY,
    IDENTITY_MISSING_RUN_ID,
    IDENTITY_MISSING_TASK_ID,
    IDENTITY_TASK_RUN_MISMATCH,
    PendingTerminalDuplicateMessageEvidence,
    evaluate_terminal_duplicate_operator_evidence,
)


pytestmark = pytest.mark.asyncio


class AuditOnlyRedis:
    def __init__(self):
        self.xadd_calls = []
        self.xack_calls = []

    async def xadd(self, stream, fields, maxlen=None, approximate=True):
        self.xadd_calls.append((stream, fields, maxlen, approximate))
        return f"audit-{len(self.xadd_calls)}-0"

    async def xack(self, *args, **kwargs):
        self.xack_calls.append((args, kwargs))
        raise AssertionError("xack must not be attempted for non-canonical identity")


def _pending(
    *,
    message_task_id: str = "task-1",
    message_run_id: str = "run-1",
) -> PendingTerminalDuplicateMessageEvidence:
    return PendingTerminalDuplicateMessageEvidence(
        message_id="1-0",
        message_task_id=message_task_id,
        message_run_id=message_run_id,
        consumer="consumer-1",
        idle_ms=1,
        deliveries=1,
        raw_fields_found=True,
    )


def _evidence(
    *,
    task_id: str = "task-1",
    task_state: str = "done",
    task_meta_run_id: str = "run-1",
    message_task_id: str = "task-1",
    message_run_id: str = "run-1",
):
    return evaluate_terminal_duplicate_operator_evidence(
        task_id=task_id,
        task_state=task_state,
        task_meta_run_id=task_meta_run_id,
        pending_message=_pending(
            message_task_id=message_task_id,
            message_run_id=message_run_id,
        ),
    )


async def test_explicit_task_run_identity_is_canonical_cleanup_candidate():
    evidence = _evidence()

    assert evidence.identity_status == IDENTITY_EXPLICIT_TASK_RUN
    assert evidence.identity_reason == "explicit_task_id_and_run_id_match"
    assert evidence.canonical_identity_confirmed is True
    assert evidence.fallback_identity_detected is False
    assert evidence.message_identity_verified is True
    assert evidence.terminal_evidence_verified is True
    assert evidence.cleanup_candidate is True
    assert evidence.ack_allowed is True
    assert evidence.ack_policy == ACK_POLICY_ACK_EXPLICIT_TASK_RUN_TERMINAL_EVIDENCE
    assert evidence.reason == EXPLICIT_TERMINAL_DUPLICATE_CLEANUP_CANDIDATE


async def test_fallback_run_only_identity_is_degraded_and_not_cleanup_candidate():
    evidence = _evidence(
        task_id="legacy-run-as-task",
        task_meta_run_id="",
        message_task_id="",
        message_run_id="legacy-run-as-task",
    )

    assert evidence.identity_status == IDENTITY_FALLBACK_RUN_ONLY
    assert evidence.identity_reason == "message_run_id_matches_task_id_without_explicit_task_id"
    assert evidence.canonical_identity_confirmed is False
    assert evidence.fallback_identity_detected is True
    assert evidence.cleanup_candidate is False
    assert evidence.ack_allowed is False
    assert evidence.message_identity_verified is False
    assert evidence.terminal_evidence_verified is False


async def test_missing_task_id_is_not_canonical_and_not_cleanup_candidate():
    evidence = _evidence(
        task_id="task-1",
        task_meta_run_id="run-1",
        message_task_id="",
        message_run_id="run-1",
    )

    assert evidence.identity_status == IDENTITY_MISSING_TASK_ID
    assert evidence.identity_reason == "message_task_id_missing"
    assert evidence.canonical_identity_confirmed is False
    assert evidence.fallback_identity_detected is False
    assert evidence.cleanup_candidate is False
    assert evidence.ack_allowed is False


async def test_missing_run_id_is_not_canonical_and_not_cleanup_candidate():
    evidence = _evidence(
        task_id="task-1",
        task_meta_run_id="run-1",
        message_task_id="task-1",
        message_run_id="",
    )

    assert evidence.identity_status == IDENTITY_MISSING_RUN_ID
    assert evidence.identity_reason == "message_run_id_missing"
    assert evidence.canonical_identity_confirmed is False
    assert evidence.cleanup_candidate is False
    assert evidence.ack_allowed is False


async def test_task_run_mismatch_is_not_canonical_and_not_cleanup_candidate():
    evidence = _evidence(
        task_id="task-1",
        task_meta_run_id="run-1",
        message_task_id="task-1",
        message_run_id="run-2",
    )

    assert evidence.identity_status == IDENTITY_TASK_RUN_MISMATCH
    assert evidence.identity_reason == "message_run_id_mismatch"
    assert evidence.canonical_identity_confirmed is False
    assert evidence.cleanup_candidate is False
    assert evidence.ack_allowed is False


async def test_cleanup_command_mirrors_fallback_identity_and_does_not_xack():
    redis = AuditOnlyRedis()
    evidence = _evidence(
        task_id="legacy-run-as-task",
        task_meta_run_id="",
        message_task_id="",
        message_run_id="legacy-run-as-task",
    )

    async def evidence_reader(*args, **kwargs):
        return evidence

    result = await execute_terminal_duplicate_cleanup_command(
        redis,
        task_id="legacy-run-as-task",
        stream_key="stream-1",
        consumer_group="worker_consumers",
        pending_message_id="1-0",
        dry_run=False,
        execute=True,
        reason="operator_cleanup_terminal_duplicate_pending_message",
        evidence_reader=evidence_reader,
    )

    assert result.status == DENIED_FALLBACK_IDENTITY
    assert result.identity_status == IDENTITY_FALLBACK_RUN_ONLY
    assert result.identity_reason == "message_run_id_matches_task_id_without_explicit_task_id"
    assert result.canonical_identity_confirmed is False
    assert result.fallback_identity_detected is True
    assert result.cleanup_candidate is False
    assert result.ack_allowed is False
    assert result.cleanup_executed is False
    assert result.ack_executed is False
    assert result.ack_count == 0
    assert result.evidence_snapshot["identity_status"] == IDENTITY_FALLBACK_RUN_ONLY
    assert redis.xack_calls == []


async def test_cleanup_command_mirrors_task_and_run_mismatch_denials():
    redis = AuditOnlyRedis()

    async def task_mismatch_reader(*args, **kwargs):
        return _evidence(
            task_id="task-1",
            task_meta_run_id="run-1",
            message_task_id="task-2",
            message_run_id="run-1",
        )

    task_result = await execute_terminal_duplicate_cleanup_command(
        redis,
        task_id="task-1",
        stream_key="stream-1",
        consumer_group="worker_consumers",
        pending_message_id="1-0",
        dry_run=False,
        execute=True,
        reason="operator_cleanup_terminal_duplicate_pending_message",
        evidence_reader=task_mismatch_reader,
    )

    assert task_result.status == DENIED_TASK_ID_MISMATCH
    assert task_result.identity_status == IDENTITY_TASK_RUN_MISMATCH
    assert task_result.canonical_identity_confirmed is False
    assert task_result.cleanup_executed is False

    async def run_mismatch_reader(*args, **kwargs):
        return _evidence(
            task_id="task-1",
            task_meta_run_id="run-1",
            message_task_id="task-1",
            message_run_id="run-2",
        )

    run_result = await execute_terminal_duplicate_cleanup_command(
        redis,
        task_id="task-1",
        stream_key="stream-1",
        consumer_group="worker_consumers",
        pending_message_id="1-0",
        dry_run=False,
        execute=True,
        reason="operator_cleanup_terminal_duplicate_pending_message",
        evidence_reader=run_mismatch_reader,
    )

    assert run_result.status == DENIED_TASK_META_RUN_ID_MISMATCH
    assert run_result.identity_status == IDENTITY_TASK_RUN_MISMATCH
    assert run_result.canonical_identity_confirmed is False
    assert run_result.cleanup_executed is False
    assert redis.xack_calls == []


def test_audit_event_preserves_task_id_and_run_id_as_separate_fields():
    fields = terminal_duplicate_cleanup_audit_fields(
        TerminalDuplicateCleanupAuditEvent(
            command_attempt_id="tdc-1",
            event_phase="outcome",
            task_id="task-1",
            run_id="run-1",
            status="DRY_RUN_CLEANUP_CANDIDATE",
        )
    )

    assert fields["task_id"] == "task-1"
    assert fields["run_id"] == "run-1"
    assert fields["task_id"] != fields["run_id"]
    assert fields["production_ready_claim"] == "false"
