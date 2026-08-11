from __future__ import annotations

from dataclasses import replace

import pytest

from hfa.authority import (
    AggregateType,
    CanonicalAggregateIdentity,
    OperationType,
    OPERATION_CONTRACTS,
)
from hfa_control.task_requeue_authority import (
    FEATURE_FLAG,
    TaskRequeueAuthorityInput,
    build_task_requeue_command,
    parse_task_requeue_binding_flag,
    task_requeue_operation_id,
)
from hfa_control.task_recovery import TaskRecoveryManager


def _input(**overrides) -> TaskRequeueAuthorityInput:
    values = dict(
        task_id="task-84-9",
        run_id="run-84-9",
        tenant_id="tenant-a",
        reason_code="TASK_STALE_DETECTED",
        max_requeue_count=3,
        worker_instance_id="worker-a",
        scheduler_epoch="7",
        claim_epoch=1,
        dispatch_attempt=1,
        claim_transition_id="claim-transition-1",
        claim_record_hash="a" * 64,
        claim_command_hash="b" * 64,
        claim_revision=3,
    )
    explicit_claim_operation_id = overrides.pop("claim_operation_id", None)
    values.update(overrides)
    identity = CanonicalAggregateIdentity(
        aggregate_type=AggregateType.TASK,
        run_id=values["run_id"],
        task_id=values["task_id"],
    )
    values["claim_operation_id"] = (
        explicit_claim_operation_id
        if explicit_claim_operation_id is not None
        else (
            f"task-claim:v1:{identity.sha256}:"
            f"attempt:{values['dispatch_attempt']}"
        )
    )
    return TaskRequeueAuthorityInput(**values)


def test_feature_flag_name_is_canonical_task_requeue_binding():
    assert FEATURE_FLAG == "HFA_CANONICAL_TASK_REQUEUE_BINDING"


def test_feature_flag_defaults_false():
    assert parse_task_requeue_binding_flag(None) is False


def test_feature_flag_accepts_strict_true_and_false_values():
    assert parse_task_requeue_binding_flag("true") is True
    assert parse_task_requeue_binding_flag("0") is False


def test_feature_flag_rejects_invalid_value():
    with pytest.raises(ValueError):
        parse_task_requeue_binding_flag("sometimes")


def test_existing_operation_contract_is_running_to_ready_revisioned():
    contract = OPERATION_CONTRACTS[OperationType.TASK_REQUEUE]
    assert contract.aggregate_type is AggregateType.TASK
    assert contract.allowed_previous_states == frozenset({"running"})
    assert contract.allowed_next_states == frozenset({"ready"})
    assert contract.consumes_revision is True
    assert contract.operation_receipt_required is True
    assert contract.required_projection_intents == frozenset(
        {"READY_QUEUE", "REQUEUE_NOTIFICATION"}
    )


def test_command_binds_claim_retry_and_required_intents():
    value = _input()
    command = build_task_requeue_command(value)
    assert command.operation_type is OperationType.TASK_REQUEUE
    assert command.expected_revision == value.claim_revision
    assert command.intended_previous_state == "running"
    assert command.intended_next_state == "ready"
    assert command.causation_id == value.claim_transition_id
    assert command.authoritative_metadata_changes["retry_attempt"] == 1
    assert tuple(x["kind"] for x in command.requested_projection_intents) == (
        "READY_QUEUE",
        "REQUEUE_NOTIFICATION",
    )


def test_operation_identity_is_one_slot_per_claim_generation():
    first = _input()
    replay = replace(first)
    assert task_requeue_operation_id(first) == task_requeue_operation_id(replay)
    assert task_requeue_operation_id(first).endswith(":claim:1")


def test_projection_clock_is_not_part_of_canonical_command_semantics():
    value = _input()
    command = build_task_requeue_command(value)
    assert "requeued_at_ms" not in TaskRequeueAuthorityInput.__dataclass_fields__
    assert "ready_score" not in TaskRequeueAuthorityInput.__dataclass_fields__
    assert "requeued_at_ms" not in command.authoritative_metadata_changes
    assert "ready_score" not in command.authoritative_metadata_changes
    assert build_task_requeue_command(_input()).canonical_command_hash == command.canonical_command_hash


def test_divergent_reason_keeps_operation_slot_but_changes_command_hash():
    first = _input(reason_code="TASK_STALE_DETECTED")
    divergent = replace(first, reason_code="DIFFERENT_REASON")
    assert task_requeue_operation_id(first) == task_requeue_operation_id(divergent)
    assert (
        build_task_requeue_command(first).canonical_command_hash
        != build_task_requeue_command(divergent).canonical_command_hash
    )


def test_retry_attempt_is_existing_dispatch_attempt_not_second_counter():
    value = _input(claim_epoch=7, dispatch_attempt=3, max_requeue_count=3)
    assert value.retry_attempt == 3
    assert value.claim_epoch == 7
    assert value.claim_operation_id.endswith(":attempt:3")
    assert (
        build_task_requeue_command(value)
        .authoritative_metadata_changes["retry_attempt"]
        == 3
    )


def test_retry_attempt_beyond_policy_fails_closed():
    with pytest.raises(ValueError, match="exceeds max_requeue_count"):
        build_task_requeue_command(_input(claim_epoch=4, dispatch_attempt=4, max_requeue_count=3))


def test_recovery_manager_defaults_canonical_requeue_off(monkeypatch):
    monkeypatch.delenv(FEATURE_FLAG, raising=False)
    manager = TaskRecoveryManager(object())
    assert manager._canonical_task_requeue_binding is False
    assert manager._requeue_authority is None


def test_recovery_manager_rejects_non_boolean_explicit_flag():
    with pytest.raises(ValueError, match="exact bool"):
        TaskRecoveryManager(object(), canonical_task_requeue_binding="true")
