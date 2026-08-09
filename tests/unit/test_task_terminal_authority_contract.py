from __future__ import annotations

import hashlib

import pytest

from hfa.authority import (
    AuthorityDecisionCode,
    OperationType,
    evaluate_authority_commit,
)
from hfa_control.task_terminal_authority import (
    TaskTerminalAuthorityInput,
    build_task_terminal_command,
    build_task_terminal_context,
    normalize_task_terminal_input,
    task_terminal_operation_id,
)


def _terminal(*, state: str = "done", output: str = '{"z":2,"a":1}'):
    canonical_output = '{"a":1,"z":2}' if state == "done" else ""
    return TaskTerminalAuthorityInput(
        task_id="task-84-7c",
        run_id="run-84-7c",
        tenant_id="tenant-84-7c",
        terminal_state=state,
        finished_at_ms=123456,
        reason_code="completed" if state == "done" else "boom",
        worker_instance_id="worker-84-7c",
        scheduler_epoch="epoch-84-7c",
        claim_epoch=1,
        output_data=output,
        output_sha256=(
            hashlib.sha256(canonical_output.encode("utf-8")).hexdigest()
            if state == "done"
            else ""
        ),
        claim_transition_id="claim-transition-84-7c",
        claim_record_hash="a" * 64,
        claim_command_hash="b" * 64,
        claim_revision=3,
        claim_operation_id="task-claim:v1:proof:attempt:1",
    )


def test_complete_normalizes_output_and_preserves_replay_payload():
    value = normalize_task_terminal_input(_terminal())

    assert value.output_data == '{"a":1,"z":2}'
    assert value.output_sha256 == hashlib.sha256(
        value.output_data.encode("utf-8")
    ).hexdigest()

    command = build_task_terminal_command(value)
    assert command.operation_type is OperationType.TASK_COMPLETE
    assert command.authoritative_metadata_changes["output_data"] == value.output_data
    assert command.authoritative_metadata_changes["output_sha256"] == value.output_sha256
    assert {item["kind"] for item in command.requested_projection_intents} == {
        "OUTPUT_PROJECTION",
        "DEPENDENCY_FANOUT_INTENT",
    }


def test_fail_has_failure_fanout_and_no_output_projection_payload():
    value = normalize_task_terminal_input(
        _terminal(state="failed", output='{"ignored":true}')
    )
    command = build_task_terminal_command(value)

    assert value.output_data == ""
    assert value.output_sha256 == ""
    assert command.operation_type is OperationType.TASK_FAIL
    assert tuple(command.requested_projection_intents) == (
        {"kind": "DEPENDENCY_FAILURE_FANOUT_INTENT"},
    )


@pytest.mark.parametrize("state", ["done", "failed"])
def test_terminal_command_is_accepted_from_exact_claim_revision(state):
    value = normalize_task_terminal_input(_terminal(state=state))
    command = build_task_terminal_command(value)
    evaluation = evaluate_authority_commit(
        context=build_task_terminal_context(
            command,
            scheduler_epoch=value.scheduler_epoch,
        ),
        command=command,
        current_revision=3,
        current_state="running",
        receipt_probe=None,
        committed_at_ms=value.finished_at_ms,
        correlation_id=None,
    )

    assert evaluation.decision.code is AuthorityDecisionCode.ACCEPTED
    assert evaluation.commit_plan is not None
    assert evaluation.commit_plan.record.from_revision == 3
    assert evaluation.commit_plan.record.to_revision == 4
    assert evaluation.commit_plan.record.causation_id == value.claim_transition_id


def test_one_terminal_operation_identity_per_claim_generation():
    done = normalize_task_terminal_input(_terminal(state="done"))
    failed = normalize_task_terminal_input(_terminal(state="failed"))

    assert task_terminal_operation_id(done) == task_terminal_operation_id(failed)
    assert (
        build_task_terminal_command(done).canonical_command_hash
        != build_task_terminal_command(failed).canonical_command_hash
    )


def test_changed_complete_output_keeps_operation_id_but_changes_command_hash():
    first = normalize_task_terminal_input(_terminal(output='{"z":2,"a":1}'))
    second_output = '{"a":9,"z":2}'
    second = normalize_task_terminal_input(
        TaskTerminalAuthorityInput(
            **{
                **first.__dict__,
                "output_data": second_output,
                "output_sha256": hashlib.sha256(
                    second_output.encode("utf-8")
                ).hexdigest(),
            }
        )
    )

    assert task_terminal_operation_id(first) == task_terminal_operation_id(second)
    assert (
        build_task_terminal_command(first).canonical_command_hash
        != build_task_terminal_command(second).canonical_command_hash
    )


def test_zero_scheduler_epoch_is_rejected():
    value = _terminal()
    with pytest.raises(ValueError, match="scheduler_epoch"):
        normalize_task_terminal_input(
            TaskTerminalAuthorityInput(
                **{**value.__dict__, "scheduler_epoch": "0"}
            )
        )
