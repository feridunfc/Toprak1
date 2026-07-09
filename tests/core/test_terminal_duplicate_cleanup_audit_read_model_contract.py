import inspect

import pytest

from hfa_control.terminal_duplicate_cleanup_audit import (
    AUDIT_PHASE_INTENT,
    AUDIT_PHASE_OUTCOME,
    TERMINAL_DUPLICATE_CLEANUP_AUDIT_EVENT_TYPE,
    TERMINAL_DUPLICATE_CLEANUP_AUDIT_STREAM,
)
from hfa_control.terminal_duplicate_cleanup_audit_read_model import (
    AUDIT_READ_DEGRADED,
    AUDIT_READ_EMPTY,
    AUDIT_READ_OK,
    AUDIT_READ_SAFETY_VIOLATION,
    MAX_AUDIT_READ_LIMIT,
    MAX_AUDIT_SCAN_LIMIT,
    read_terminal_duplicate_cleanup_audit,
)


pytestmark = pytest.mark.asyncio


class Redis:
    def __init__(self, rows=None, *, fail=False):
        self.rows = list(rows or [])
        self.fail = fail
        self.calls = []

    async def xrevrange(self, stream, max="+", min="-", count=None):
        self.calls.append(("xrevrange", stream, max, min, count))
        if self.fail:
            raise RuntimeError("synthetic audit read failure")
        return self.rows[:count]


def _event(
    audit_id: str,
    *,
    task_id: str = "task-1",
    command_attempt_id: str = "tdc-1",
    event_phase: str = AUDIT_PHASE_OUTCOME,
    status: str = "DRY_RUN_CLEANUP_CANDIDATE",
    cleanup_executed: str = "false",
    ack_executed: str = "false",
    ack_count: str = "0",
    extra=None,
):
    fields = {
        "event_type": TERMINAL_DUPLICATE_CLEANUP_AUDIT_EVENT_TYPE,
        "event_phase": event_phase,
        "command_attempt_id": command_attempt_id,
        "task_id": task_id,
        "run_id": "run-1",
        "stream": "runtime-stream",
        "group": "worker_consumers",
        "pending_message_id": "1-0",
        "dry_run": "false" if event_phase == AUDIT_PHASE_INTENT else "true",
        "execute_requested": "true" if event_phase == AUDIT_PHASE_INTENT else "false",
        "status": status,
        "operator_reason_present": "false",
        "operator_reason_length": "0",
        "evidence_reason": "terminal_duplicate_explicit_identity",
        "evidence_status": "RESOLVED",
        "ack_policy": "TERMINAL_DUPLICATE_EXPLICIT_IDENTITY",
        "ack_allowed": "true",
        "cleanup_candidate": "true",
        "cleanup_executed": cleanup_executed,
        "ack_executed": ack_executed,
        "ack_count": ack_count,
        "audit_status": "AUDIT_OUTCOME_WRITTEN",
        "production_ready_claim": "false",
    }
    fields.update(extra or {})
    return (audit_id, fields)


async def test_empty_audit_stream_returns_empty_status():
    redis = Redis([])

    result = await read_terminal_duplicate_cleanup_audit(redis, task_id="task-1")

    assert result.read_status == AUDIT_READ_EMPTY
    assert result.entry_count == 0
    assert result.entries == ()
    assert result.command_attempt_count == 0
    assert result.production_ready_claim is False
    assert result.command_safety["read_only"] is True
    assert result.command_safety["xadd_attempted"] is False
    assert redis.calls == [
        (
            "xrevrange",
            TERMINAL_DUPLICATE_CLEANUP_AUDIT_STREAM,
            "+",
            "-",
            500,
        )
    ]


async def test_matching_task_audit_events_are_normalized_and_unrelated_entries_ignored():
    redis = Redis(
        [
            _event("3-0", task_id="other-task"),
            _event("2-0", task_id="task-1", status="CLEANED", cleanup_executed="true", ack_executed="true", ack_count="1"),
            _event("1-0", task_id="task-1", event_phase=AUDIT_PHASE_INTENT, status="PENDING_EXECUTION"),
        ]
    )

    result = await read_terminal_duplicate_cleanup_audit(redis, task_id="task-1")

    assert result.read_status == AUDIT_READ_OK
    assert result.entry_count == 2
    assert len(result.entries) == 2
    assert [entry.audit_id for entry in result.entries] == ["1-0", "2-0"]
    assert [entry.event_phase for entry in result.entries] == [
        AUDIT_PHASE_INTENT,
        AUDIT_PHASE_OUTCOME,
    ]
    assert result.command_attempt_count == 1
    assert result.latest_status == "CLEANED"
    assert result.latest_cleanup_executed is True
    assert result.latest_ack_executed is True
    assert result.latest_ack_count == 1


async def test_intent_without_outcome_is_reported_without_cleanup_recommendation():
    redis = Redis(
        [
            _event(
                "1-0",
                task_id="task-1",
                command_attempt_id="tdc-intent-only",
                event_phase=AUDIT_PHASE_INTENT,
                status="PENDING_EXECUTION",
            )
        ]
    )

    result = await read_terminal_duplicate_cleanup_audit(redis, task_id="task-1")

    assert result.read_status == AUDIT_READ_OK
    assert result.has_intent_without_outcome is True
    assert "without a matching outcome" in result.operator_summary
    assert "Run cleanup" not in result.operator_summary
    assert "ACK recommended" not in result.operator_summary
    assert result.command_safety["cleanup_recommendation_emitted"] is False


async def test_operator_reason_full_text_is_detected_but_not_returned():
    secret = "do not leak this operator reason"
    redis = Redis(
        [
            _event(
                "1-0",
                task_id="task-1",
                extra={
                    "operator_reason": secret,
                    "operator_reason_present": "true",
                    "operator_reason_length": str(len(secret)),
                },
            )
        ]
    )

    result = await read_terminal_duplicate_cleanup_audit(redis, task_id="task-1")

    assert result.read_status == AUDIT_READ_SAFETY_VIOLATION
    assert result.has_operator_reason_text_exposure is True
    assert result.entries[0].operator_reason_present is True
    assert result.entries[0].operator_reason_length == len(secret)
    assert secret not in repr(result)
    assert "was not returned" in result.operator_summary


async def test_malformed_matching_row_degrades_without_crashing():
    redis = Redis(
        [
            _event(
                "1-0",
                task_id="task-1",
                extra={
                    "ack_count": "not-an-int",
                    "ack_executed": "not-a-bool",
                },
            )
        ]
    )

    result = await read_terminal_duplicate_cleanup_audit(redis, task_id="task-1")

    assert result.read_status == AUDIT_READ_DEGRADED
    assert result.entry_count == 1
    assert "ack_count" in result.read_error
    assert "ack_executed" in result.read_error


async def test_include_entries_false_preserves_summary_and_latest_fields():
    redis = Redis(
        [
            _event("2-0", task_id="task-1", status="CLEANED", cleanup_executed="true", ack_executed="true", ack_count="1"),
            _event("1-0", task_id="task-1", event_phase=AUDIT_PHASE_INTENT, status="PENDING_EXECUTION"),
        ]
    )

    result = await read_terminal_duplicate_cleanup_audit(
        redis,
        task_id="task-1",
        include_entries=False,
    )

    assert result.read_status == AUDIT_READ_OK
    assert result.entries == ()
    assert result.entry_count == 2
    assert result.latest_status == "CLEANED"
    assert result.latest_ack_count == 1
    assert result.operator_summary


async def test_limit_and_scan_limit_are_capped_and_possibly_truncated_is_reported():
    rows = [
        _event(f"{index}-0", task_id="task-1", command_attempt_id=f"tdc-{index}")
        for index in range(600, 0, -1)
    ]
    redis = Redis(rows)

    result = await read_terminal_duplicate_cleanup_audit(
        redis,
        task_id="task-1",
        limit=9999,
        scan_limit=999999,
    )

    assert result.limit_applied == MAX_AUDIT_READ_LIMIT
    assert result.scan_limit_applied == MAX_AUDIT_SCAN_LIMIT
    assert result.entry_count == 600
    assert len(result.entries) == MAX_AUDIT_READ_LIMIT
    assert result.raw_entries_scanned == 600
    assert result.possibly_truncated is False


async def test_possibly_truncated_when_scan_window_is_filled():
    rows = [
        _event(f"{index}-0", task_id="task-1", command_attempt_id=f"tdc-{index}")
        for index in range(500, 0, -1)
    ]
    redis = Redis(rows)

    result = await read_terminal_duplicate_cleanup_audit(
        redis,
        task_id="task-1",
        scan_limit=500,
    )

    assert result.raw_entries_scanned == 500
    assert result.scan_limit_applied == 500
    assert result.possibly_truncated is True


async def test_read_failure_fails_closed_without_mutation_claims():
    redis = Redis(fail=True)

    result = await read_terminal_duplicate_cleanup_audit(redis, task_id="task-1")

    assert result.read_status == "AUDIT_READ_FAILED"
    assert "synthetic audit read failure" in result.read_error
    assert result.entries == ()
    assert result.command_safety["read_only"] is True
    assert result.command_safety["xadd_attempted"] is False
    assert result.command_safety["xack_attempted"] is False
    assert result.production_ready_claim is False


def test_audit_read_model_static_contract_is_read_only_and_dedicated():
    import hfa_control.terminal_duplicate_cleanup_audit_read_model as module

    source = inspect.getsource(module)

    assert ".xrevrange(" in source
    assert ".xadd(" not in source
    assert ".xack(" not in source
    assert ".xclaim(" not in source
    assert ".xpending" not in source
    assert ".hset(" not in source
    assert ".set(" not in source
    assert ".delete(" not in source
    assert ".expire(" not in source
    assert "RedisKey.stream_shard" not in source
    assert "hfa:stream:runs" not in source
    assert "read_terminal_duplicate_operator_evidence" not in source
    assert "execute_terminal_duplicate_cleanup_command" not in source
    assert "operator_reason_text" in source
