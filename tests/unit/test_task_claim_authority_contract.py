from __future__ import annotations

import hashlib
from dataclasses import replace

import pytest

from hfa.authority import (
    AggregateType,
    AuthorityDecisionCode,
    CanonicalAggregateIdentity,
    OperationType,
    evaluate_authority_commit,
)
from hfa_control.task_claim_authority import (
    FEATURE_FLAG,
    TASK_CLAIM_DUPLICATE_STATUS,
    TASK_CLAIM_PROJECTED_STATUS,
    WRITER_ID,
    TaskClaimAuthorityInput,
    build_task_claim_command,
    build_task_claim_context,
    normalize_task_claim_input,
    parse_task_claim_binding_flag,
    task_claim_operation_id,
    task_claim_status_allows_execution,
)


def _claim(**changes) -> TaskClaimAuthorityInput:
    identity = CanonicalAggregateIdentity(
        aggregate_type=AggregateType.TASK,
        run_id="run-84-2",
        task_id="task-84-2",
    )

    claim = TaskClaimAuthorityInput(
        task_id="task-84-2",
        run_id="run-84-2",
        tenant_id="tenant-84-2",
        worker_instance_id="worker-a",
        dispatch_worker_id="worker-a",
        scheduler_epoch="epoch-7",
        claimed_at_ms=1_723_000_000_100,
        dispatch_attempt=1,
        dispatch_revision=2,
        previous_claim_epoch=0,
        dispatch_transition_id="dispatch-transition-1",
        dispatch_record_hash=hashlib.sha256(
            b"dispatch-record"
        ).hexdigest(),
        dispatch_command_hash=hashlib.sha256(
            b"dispatch-command"
        ).hexdigest(),
        dispatch_operation_id=(
            f"task-dispatch:v1:{identity.sha256}:attempt:1"
        ),
    )
    return replace(claim, **changes)


@pytest.mark.parametrize(
    "value",
    ["1", " true ", "YES", "on"],
)
def test_feature_flag_accepts_true_values(value):
    assert parse_task_claim_binding_flag(value)


@pytest.mark.parametrize(
    "value",
    [None, "", "0", " false ", "NO", "off"],
)
def test_feature_flag_accepts_false_values(value):
    assert not parse_task_claim_binding_flag(value)


def test_feature_flag_rejects_unknown_value():
    with pytest.raises(ValueError, match=FEATURE_FLAG):
        parse_task_claim_binding_flag("sometimes")


def test_claim_command_has_exact_authority_contract():
    command = build_task_claim_command(_claim())

    assert command.operation_type is OperationType.TASK_CLAIM
    assert command.expected_revision == 2
    assert command.intended_previous_state == "scheduled"
    assert command.intended_next_state == "running"
    assert (
        command.authoritative_metadata_changes[
            "previous_claim_epoch"
        ]
        == 0
    )
    assert (
        command.authoritative_metadata_changes["claim_epoch"]
        == 1
    )
    assert command.requested_projection_intents == (
        {
            "kind": "RUNNING_SET",
            "tenant_id": "tenant-84-2",
            "task_id": "task-84-2",
        },
    )
    assert command.causation_id == "dispatch-transition-1"


def test_same_attempt_has_stable_operation_identity():
    first = _claim()
    second = _claim(
        claimed_at_ms=first.claimed_at_ms + 500,
        worker_instance_id="worker-b",
        dispatch_worker_id="worker-b",
    )

    assert (
        task_claim_operation_id(first)
        == task_claim_operation_id(second)
    )
    assert (
        build_task_claim_command(first).canonical_command_hash
        != build_task_claim_command(second).canonical_command_hash
    )


def test_new_attempt_has_new_operation_identity():
    first = _claim()
    identity = CanonicalAggregateIdentity(
        aggregate_type=AggregateType.TASK,
        run_id=first.run_id,
        task_id=first.task_id,
    )
    second = replace(
        first,
        dispatch_attempt=2,
        dispatch_operation_id=(
            f"task-dispatch:v1:{identity.sha256}:attempt:2"
        ),
    )

    assert (
        task_claim_operation_id(first)
        != task_claim_operation_id(second)
    )


def test_dispatch_worker_mismatch_fails_closed():
    with pytest.raises(
        ValueError,
        match="canonical dispatch worker",
    ):
        normalize_task_claim_input(
            _claim(worker_instance_id="worker-b")
        )


def test_dispatch_operation_identity_mismatch_fails():
    with pytest.raises(
        ValueError,
        match="dispatch_operation_id",
    ):
        normalize_task_claim_input(
            _claim(dispatch_operation_id="wrong")
        )


def test_non_sha_dispatch_record_hash_rejected_before_command_build():
    with pytest.raises(
        ValueError,
        match="dispatch_record_hash",
    ):
        build_task_claim_command(
            _claim(dispatch_record_hash="not-a-sha256")
        )


def test_uppercase_dispatch_record_hash_rejected_before_command_build():
    with pytest.raises(
        ValueError,
        match="dispatch_record_hash",
    ):
        build_task_claim_command(
            _claim(dispatch_record_hash=("a" * 64).upper())
        )


def test_non_sha_dispatch_command_hash_rejected_before_command_build():
    with pytest.raises(
        ValueError,
        match="dispatch_command_hash",
    ):
        build_task_claim_command(
            _claim(dispatch_command_hash="not-a-sha256")
        )


def test_uppercase_dispatch_command_hash_rejected_before_command_build():
    with pytest.raises(
        ValueError,
        match="dispatch_command_hash",
    ):
        build_task_claim_command(
            _claim(dispatch_command_hash=("b" * 64).upper())
        )


def test_writer_identity_and_fence_are_fixed():
    command = build_task_claim_command(_claim())
    context = build_task_claim_context(
        command,
        scheduler_epoch="epoch-7",
    )

    assert context.authenticated_writer_id == WRITER_ID
    assert context.fence_required is True
    assert context.fence_valid is True
    assert context.allowed_operations == frozenset(
        {OperationType.TASK_CLAIM}
    )


def test_claim_evaluates_scheduled_to_running():
    claim = _claim()
    command = build_task_claim_command(claim)
    context = build_task_claim_context(
        command,
        scheduler_epoch=claim.scheduler_epoch,
    )

    evaluation = evaluate_authority_commit(
        context=context,
        command=command,
        current_revision=2,
        current_state="scheduled",
        receipt_probe=None,
        committed_at_ms=claim.claimed_at_ms,
        correlation_id=None,
    )

    assert (
        evaluation.decision.code
        is AuthorityDecisionCode.ACCEPTED
    )
    assert evaluation.commit_plan is not None
    assert evaluation.commit_plan.record.to_revision == 3


def test_only_first_projection_allows_execution():
    assert task_claim_status_allows_execution(
        TASK_CLAIM_PROJECTED_STATUS
    )
    assert not task_claim_status_allows_execution(
        TASK_CLAIM_DUPLICATE_STATUS
    )
    assert not task_claim_status_allows_execution(
        "canonical_projection_pending"
    )
