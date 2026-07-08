from pathlib import Path

import pytest

from hfa_control.terminal_duplicate_operator_evidence import (
    ACK_POLICY_ACK_EXPLICIT_TASK_RUN_TERMINAL_EVIDENCE,
    ACK_POLICY_NO_ACK_WITHOUT_EXPLICIT_TASK_IDENTITY,
    ACK_POLICY_NO_ACK_WITHOUT_RUN_ID_MATCH,
    ACK_POLICY_NO_ACK_WITHOUT_TERMINAL_EVIDENCE,
    ACK_POLICY_NOT_TERMINAL,
    EVIDENCE_READ_DEGRADED,
    EXPLICIT_TERMINAL_DUPLICATE_CLEANUP_CANDIDATE,
    NO_ACK_RUN_ID_MISMATCH,
    NO_ACK_TASK_ID_MISMATCH,
    NO_ACK_TASK_META_RUN_ID_MISSING,
    NO_ACK_TASK_META_RUN_ID_MISMATCH,
    NO_ACK_TASK_NOT_TERMINAL,
    NO_ACK_WITHOUT_EXPLICIT_TASK_IDENTITY,
    NO_PENDING_STREAM_MESSAGE,
    PendingTerminalDuplicateMessageEvidence,
    evaluate_terminal_duplicate_operator_evidence,
    read_terminal_duplicate_operator_evidence,
)


def test_fallback_identity_requires_operator_attention_and_no_cleanup_candidate():
    evidence = evaluate_terminal_duplicate_operator_evidence(
        task_id="task-1",
        task_state="done",
        task_meta_run_id="run-1",
        pending_message=PendingTerminalDuplicateMessageEvidence(
            message_id="1-0",
            message_task_id="",
            message_run_id="run-1",
            raw_fields_found=True,
        ),
    )

    assert evidence.ack_allowed is False
    assert evidence.ack_policy == ACK_POLICY_NO_ACK_WITHOUT_EXPLICIT_TASK_IDENTITY
    assert evidence.cleanup_candidate is False
    assert evidence.operator_action_required is True
    assert evidence.reason == NO_ACK_WITHOUT_EXPLICIT_TASK_IDENTITY
    assert evidence.read_only is True
    assert evidence.mutation_allowed is False
    assert evidence.production_ready_claim is False


def test_explicit_identity_and_terminal_evidence_is_cleanup_candidate():
    evidence = evaluate_terminal_duplicate_operator_evidence(
        task_id="task-2",
        task_state="done",
        task_meta_run_id="run-2",
        pending_message=PendingTerminalDuplicateMessageEvidence(
            message_id="2-0",
            message_task_id="task-2",
            message_run_id="run-2",
            raw_fields_found=True,
        ),
    )

    assert evidence.ack_allowed is True
    assert evidence.ack_policy == ACK_POLICY_ACK_EXPLICIT_TASK_RUN_TERMINAL_EVIDENCE
    assert evidence.cleanup_candidate is True
    assert evidence.cleanup_done is False
    assert evidence.cleanup_executed is False
    assert evidence.operator_action_required is False
    assert evidence.reason == EXPLICIT_TERMINAL_DUPLICATE_CLEANUP_CANDIDATE
    assert evidence.message_identity_verified is True
    assert evidence.terminal_evidence_verified is True
    assert evidence.evidence_status == "cleanup_candidate"
    assert evidence.mutation_allowed is False


def test_task_id_mismatch_is_visible_and_blocks_ack():
    evidence = evaluate_terminal_duplicate_operator_evidence(
        task_id="task-3",
        task_state="done",
        task_meta_run_id="run-3",
        pending_message=PendingTerminalDuplicateMessageEvidence(
            message_id="3-0",
            message_task_id="other-task",
            message_run_id="run-3",
        ),
    )

    assert evidence.ack_allowed is False
    assert evidence.cleanup_candidate is False
    assert evidence.operator_action_required is True
    assert evidence.reason == NO_ACK_TASK_ID_MISMATCH
    assert evidence.message_task_id == "other-task"


def test_run_id_mismatch_is_visible_and_blocks_ack():
    evidence = evaluate_terminal_duplicate_operator_evidence(
        task_id="task-4",
        task_state="done",
        task_meta_run_id="run-4",
        pending_message=PendingTerminalDuplicateMessageEvidence(
            message_id="4-0",
            message_task_id="task-4",
            message_run_id="other-run",
        ),
    )

    assert evidence.ack_allowed is False
    assert evidence.ack_policy == ACK_POLICY_NO_ACK_WITHOUT_RUN_ID_MATCH
    assert evidence.cleanup_candidate is False
    assert evidence.operator_action_required is True
    assert evidence.reason == NO_ACK_TASK_META_RUN_ID_MISMATCH
    assert evidence.message_run_id == "other-run"


def test_missing_message_run_id_blocks_ack():
    evidence = evaluate_terminal_duplicate_operator_evidence(
        task_id="task-5",
        task_state="done",
        task_meta_run_id="run-5",
        pending_message=PendingTerminalDuplicateMessageEvidence(
            message_id="5-0",
            message_task_id="task-5",
            message_run_id="",
        ),
    )

    assert evidence.ack_allowed is False
    assert evidence.ack_policy == ACK_POLICY_NO_ACK_WITHOUT_RUN_ID_MATCH
    assert evidence.cleanup_candidate is False
    assert evidence.operator_action_required is True
    assert evidence.reason == NO_ACK_RUN_ID_MISMATCH


def test_task_meta_run_id_missing_blocks_ack():
    evidence = evaluate_terminal_duplicate_operator_evidence(
        task_id="task-6",
        task_state="done",
        task_meta_run_id="",
        pending_message=PendingTerminalDuplicateMessageEvidence(
            message_id="6-0",
            message_task_id="task-6",
            message_run_id="run-6",
        ),
    )

    assert evidence.ack_allowed is False
    assert evidence.ack_policy == ACK_POLICY_NO_ACK_WITHOUT_TERMINAL_EVIDENCE
    assert evidence.cleanup_candidate is False
    assert evidence.operator_action_required is True
    assert evidence.reason == NO_ACK_TASK_META_RUN_ID_MISSING


def test_non_terminal_task_blocks_ack():
    evidence = evaluate_terminal_duplicate_operator_evidence(
        task_id="task-7",
        task_state="running",
        task_meta_run_id="run-7",
        pending_message=PendingTerminalDuplicateMessageEvidence(
            message_id="7-0",
            message_task_id="task-7",
            message_run_id="run-7",
        ),
    )

    assert evidence.ack_allowed is False
    assert evidence.ack_policy == ACK_POLICY_NOT_TERMINAL
    assert evidence.cleanup_candidate is False
    assert evidence.operator_action_required is True
    assert evidence.reason == NO_ACK_TASK_NOT_TERMINAL
    assert evidence.terminal is False


def test_no_pending_stream_message_is_not_cleanup_candidate():
    evidence = evaluate_terminal_duplicate_operator_evidence(
        task_id="task-8",
        task_state="done",
        task_meta_run_id="run-8",
        pending_message=None,
    )

    assert evidence.stream_pending is False
    assert evidence.pending_message_id == ""
    assert evidence.ack_allowed is False
    assert evidence.cleanup_candidate is False
    assert evidence.operator_action_required is False
    assert evidence.reason == NO_PENDING_STREAM_MESSAGE
    assert evidence.evidence_status == "no_pending_message"


@pytest.mark.asyncio
async def test_read_model_collects_matching_pending_message_without_mutation():
    class Redis:
        def __init__(self):
            self.calls = []

        async def get(self, key):
            self.calls.append(("get", key))
            return "done"

        async def hgetall(self, key):
            self.calls.append(("hgetall", key))
            return {"run_id": "run-9"}

        async def xpending_range(self, stream, group, start, end, limit):
            self.calls.append(("xpending_range", stream, group, start, end, limit))
            return [{"message_id": "9-0", "consumer": "worker-1", "idle": 10, "deliveries": 2}]

        async def xrange(self, stream, start, end):
            self.calls.append(("xrange", stream, start, end))
            return [("9-0", {"task_id": "task-9", "run_id": "run-9"})]

    redis = Redis()

    evidence = await read_terminal_duplicate_operator_evidence(
        redis,
        task_id="task-9",
        stream_key="hfa:stream:shard:0",
        consumer_group="hfa-workers",
    )

    assert evidence.reason == EXPLICIT_TERMINAL_DUPLICATE_CLEANUP_CANDIDATE
    assert evidence.ack_allowed is True
    assert evidence.cleanup_candidate is True
    assert evidence.metadata["pending_message_consumer"] == "worker-1"
    assert evidence.metadata["pending_message_idle_ms"] == 10
    assert evidence.metadata["pending_message_deliveries"] == 2

    called = [call[0] for call in redis.calls]
    assert called == ["get", "hgetall", "xpending_range", "xrange"]


@pytest.mark.asyncio
async def test_read_model_fails_closed_as_degraded():
    class BrokenRedis:
        async def get(self, key):
            raise RuntimeError("redis unavailable")

    evidence = await read_terminal_duplicate_operator_evidence(
        BrokenRedis(),
        task_id="task-10",
        stream_key="hfa:stream:shard:0",
        consumer_group="hfa-workers",
    )

    assert evidence.ack_allowed is False
    assert evidence.cleanup_candidate is False
    assert evidence.operator_action_required is True
    assert evidence.reason == EVIDENCE_READ_DEGRADED
    assert evidence.evidence_status == "degraded"
    assert evidence.read_only is True
    assert evidence.mutation_allowed is False
    assert evidence.production_ready_claim is False
    assert "redis unavailable" in evidence.metadata["degraded_reason"]


def test_read_model_source_contains_no_runtime_mutation_calls():
    source = Path(
        "hfa-control/src/hfa_control/terminal_duplicate_operator_evidence.py"
    ).read_text(encoding="utf-8").lower()

    forbidden_tokens = [
        ".xack(",
        ".xclaim(",
        ".xadd(",
        ".hset(",
        ".set(",
        ".delete(",
        ".expire(",
        "requeue(",
        "repair(",
    ]

    for token in forbidden_tokens:
        assert token not in source
