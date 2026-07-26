from __future__ import annotations

import math
from dataclasses import fields

import pytest

from hfa.authority import (
    OPERATION_CONTRACTS,
    AggregateType,
    AuthorityCommand,
    AuthorityCommitPlan,
    AuthorityContractError,
    AuthorityDecision,
    AuthorityDecisionCode,
    AuthorityEntryContext,
    CanonicalAggregateIdentity,
    CanonicalStoreDecision,
    CanonicalTransitionRecord,
    OperationReceipt,
    OperationType,
    ProjectionApplicationReceipt,
    ProjectionDecision,
    ReceiptProbe,
    canonical_json_bytes,
    canonical_json_sha256,
    classify_canonical_store_write,
    evaluate_authority_command,
    evaluate_authority_commit,
    evaluate_projection_application,
    validate_canonical_transition_record,
)

MAX_SAFE = 2**53 - 1


def identity(*, run_id: str = "run-1", task_id: str = "task-1") -> CanonicalAggregateIdentity:
    return CanonicalAggregateIdentity(AggregateType.TASK, run_id, task_id)


def run_identity(*, run_id: str = "run-1") -> CanonicalAggregateIdentity:
    return CanonicalAggregateIdentity(AggregateType.RUN, run_id)


def intent(kind: str) -> dict[str, str]:
    return {"kind": kind}


def command(
    *,
    aggregate_identity: CanonicalAggregateIdentity | None = None,
    operation_type: OperationType = OperationType.TASK_ADMIT,
    operation_id: str = "op-1",
    expected_revision: int = 0,
    previous_state: str | None = None,
    next_state: str | None = "ready",
    intents: tuple[dict[str, str], ...] | None = None,
    payload: object = None,
) -> AuthorityCommand:
    if aggregate_identity is None:
        aggregate_identity = identity()
    if intents is None:
        intents = (intent("READY_QUEUE_IF_READY"),) if next_state == "ready" else ()
    return AuthorityCommand(
        aggregate_identity=aggregate_identity,
        operation_type=operation_type,
        operation_id=operation_id,
        expected_revision=expected_revision,
        intended_previous_state=previous_state,
        intended_next_state=next_state,
        authoritative_payload={} if payload is None else payload,
        authoritative_metadata_changes={},
        requested_child_effects=[],
        requested_projection_intents=intents,
        causation_id=None,
    )


def context(cmd: AuthorityCommand, *, writer: str = "writer-1", allowed: bool = True, target: bool = True, fence_required: bool = False, fence_valid: bool = True) -> AuthorityEntryContext:
    return AuthorityEntryContext(
        authenticated_writer_id=writer,
        allowed_operations=frozenset({cmd.operation_type}) if allowed else frozenset(),
        target_aggregate_identity_sha256=cmd.aggregate_identity.sha256 if target else "0" * 64,
        fence_required=fence_required,
        fence_valid=fence_valid,
    )


def accepted(cmd: AuthorityCommand | None = None, *, state: str | None = None, revision: int | None = None, committed_at_ms: int = 10):
    cmd = cmd or command()
    result = evaluate_authority_commit(
        context=context(cmd),
        command=cmd,
        current_revision=cmd.expected_revision if revision is None else revision,
        current_state=cmd.intended_previous_state if state is None else state,
        receipt_probe=None,
        committed_at_ms=committed_at_ms,
        correlation_id="corr-1",
    )
    assert result.decision.code is AuthorityDecisionCode.ACCEPTED
    assert result.commit_plan is not None
    return result


def tamper(obj, **changes):
    for key, value in changes.items():
        object.__setattr__(obj, key, value)
    return obj


def rehash_record(record: CanonicalTransitionRecord) -> CanonicalTransitionRecord:
    object.__setattr__(record, "canonical_record_hash", canonical_json_sha256(record.immutable_payload()))
    return record


def test_operation_registry_exact_count_and_members():
    assert len(OPERATION_CONTRACTS) == 15
    assert set(OPERATION_CONTRACTS) == set(OperationType)


def test_identity_delimiter_collision_has_different_digest():
    left = identity(run_id="a:b", task_id="c")
    right = identity(run_id="a", task_id="b:c")
    assert left.value == right.value
    assert left.sha256 != right.sha256


def test_command_hash_binds_collision_safe_identity():
    left = command(aggregate_identity=identity(run_id="a:b", task_id="c"))
    right = command(aggregate_identity=identity(run_id="a", task_id="b:c"))
    assert left.aggregate_identity.value == right.aggregate_identity.value
    assert left.canonical_command_hash != right.canonical_command_hash


def test_identity_rejects_raw_aggregate_type():
    with pytest.raises(AuthorityContractError):
        CanonicalAggregateIdentity("task", "run", "task")  # type: ignore[arg-type]


def test_run_identity_forbids_task_id():
    with pytest.raises(AuthorityContractError):
        CanonicalAggregateIdentity(AggregateType.RUN, "run", "task")


@pytest.mark.parametrize("value", [True, False, -1, MAX_SAFE + 1, 1.0, "1"])
def test_command_revision_exact_safe_integer(value):
    with pytest.raises(AuthorityContractError):
        command(expected_revision=value)  # type: ignore[arg-type]


@pytest.mark.parametrize("name,value", [("fence_required", 1), ("fence_valid", 0), ("fence_valid", "true")])
def test_context_fence_fields_exact_bool(name, value):
    cmd = command()
    kwargs = {"fence_required": False, "fence_valid": True}
    kwargs[name] = value
    with pytest.raises(AuthorityContractError):
        AuthorityEntryContext(
            authenticated_writer_id="writer",
            allowed_operations=frozenset({cmd.operation_type}),
            target_aggregate_identity_sha256=cmd.aggregate_identity.sha256,
            **kwargs,
        )


def test_canonical_json_nfc_and_binary_profile():
    assert canonical_json_bytes({"x": "e\u0301"}) == canonical_json_bytes({"x": "é"})
    assert b"base64url" in canonical_json_bytes({"x": b"abc"})


@pytest.mark.parametrize("value", [math.nan, math.inf, -math.inf, MAX_SAFE + 1])
def test_canonical_json_rejects_non_jcs_values(value):
    with pytest.raises(AuthorityContractError):
        canonical_json_bytes({"x": value})


def test_outer_gate_capability_rejects_without_receipt_disclosure():
    cmd = command()
    result = evaluate_authority_commit(context=context(cmd, allowed=False), command=cmd, current_revision=0, current_state=None, receipt_probe=None, committed_at_ms=1)
    assert result.decision.code is AuthorityDecisionCode.AUTHORITY_ENTRY_REJECTED
    assert result.commit_plan is None


def test_outer_gate_target_rejects():
    cmd = command()
    result = evaluate_authority_commit(context=context(cmd, target=False), command=cmd, current_revision=0, current_state=None, receipt_probe=None, committed_at_ms=1)
    assert result.decision.code is AuthorityDecisionCode.AUTHORITY_ENTRY_REJECTED


def test_outer_gate_fence_rejects():
    cmd = command()
    result = evaluate_authority_commit(context=context(cmd, fence_required=True, fence_valid=False), command=cmd, current_revision=0, current_state=None, receipt_probe=None, committed_at_ms=1)
    assert result.decision.code is AuthorityDecisionCode.AUTHORITY_ENTRY_REJECTED


def test_acceptance_creates_one_record_one_receipt_one_revision():
    result = accepted()
    plan = result.commit_plan
    assert plan is not None
    assert result.decision.aggregate_mutation == 1
    assert result.decision.revision_increment == 1
    assert result.decision.canonical_record_count == 1
    assert plan.aggregate_revision == 1
    assert plan.record.to_revision == 1
    assert plan.receipt.aggregate_revision == 1
    assert plan.record.writer_id == "writer-1"
    validate_canonical_transition_record(plan.record)


def test_commit_plan_uses_typed_identity_not_ambiguous_string():
    plan = accepted().commit_plan
    assert plan is not None
    assert isinstance(plan.aggregate_identity, CanonicalAggregateIdentity)
    assert plan.canonical_aggregate_identity_sha256 == plan.aggregate_identity.sha256


def test_caller_cannot_construct_accepted_decision():
    with pytest.raises(AuthorityContractError):
        AuthorityDecision(
            _token=object(),
            code=AuthorityDecisionCode.ACCEPTED,
            aggregate_mutation=1,
            revision_increment=1,
            canonical_record_count=1,
            return_existing_transition_id=None,
            durable_conflict_record=False,
            reconciliation_candidate=False,
        )


def test_caller_cannot_construct_commit_plan():
    result = accepted()
    with pytest.raises(AuthorityContractError):
        AuthorityCommitPlan(
            _token=object(),
            aggregate_identity=result.commit_plan.aggregate_identity,
            aggregate_revision=1,
            record=result.commit_plan.record,
            receipt=result.commit_plan.receipt,
        )


def test_caller_cannot_construct_record_or_receipt():
    record_values = {field.name: None for field in fields(CanonicalTransitionRecord)}
    with pytest.raises(AuthorityContractError):
        CanonicalTransitionRecord(_token=object(), **record_values)
    receipt_values = {field.name: None for field in fields(OperationReceipt)}
    with pytest.raises(AuthorityContractError):
        OperationReceipt(_token=object(), **receipt_values)


def test_projection_receipt_requires_valid_factory_inputs():
    with pytest.raises(AuthorityContractError):
        ProjectionApplicationReceipt(_token=object(), applied_revision=1, applied_transition_id="x", applied_record_hash="0" * 64)
    with pytest.raises(AuthorityContractError):
        ProjectionApplicationReceipt.create(applied_revision=True, applied_transition_id="x", applied_record_hash="0" * 64)


def test_ready_state_requires_conditional_ready_intent():
    cmd = command(intents=())
    result = evaluate_authority_commit(context=context(cmd), command=cmd, current_revision=0, current_state=None, receipt_probe=None, committed_at_ms=1)
    assert result.decision.code is AuthorityDecisionCode.ILLEGAL_STATE_TRANSITION


def test_non_ready_state_forbids_conditional_ready_intent():
    cmd = command(next_state="pending", intents=(intent("READY_QUEUE_IF_READY"),))
    result = evaluate_authority_commit(context=context(cmd), command=cmd, current_revision=0, current_state=None, receipt_probe=None, committed_at_ms=1)
    assert result.decision.code is AuthorityDecisionCode.ILLEGAL_STATE_TRANSITION


def test_future_stale_and_create_conflicts():
    cmd = command(expected_revision=2, previous_state="ready", next_state="scheduled", operation_type=OperationType.TASK_DISPATCH, intents=(intent("CONTROL_NOTIFICATION"), intent("TASK_REQUEST_MESSAGE")))
    future = evaluate_authority_commit(context=context(cmd), command=cmd, current_revision=1, current_state="ready", receipt_probe=None, committed_at_ms=1)
    stale = evaluate_authority_commit(context=context(cmd), command=cmd, current_revision=3, current_state="ready", receipt_probe=None, committed_at_ms=1)
    create = command()
    exists = evaluate_authority_commit(context=context(create), command=create, current_revision=1, current_state="ready", receipt_probe=None, committed_at_ms=1)
    assert future.decision.code is AuthorityDecisionCode.FUTURE_REVISION_CONFLICT
    assert stale.decision.code is AuthorityDecisionCode.STALE_REVISION_CONFLICT
    assert exists.decision.code is AuthorityDecisionCode.AGGREGATE_ALREADY_EXISTS_CONFLICT


def test_current_revision_and_timestamp_reject_bool_and_unsafe():
    cmd = command()
    for revision in (True, -1, MAX_SAFE + 1):
        with pytest.raises(AuthorityContractError):
            evaluate_authority_commit(context=context(cmd), command=cmd, current_revision=revision, current_state=None, receipt_probe=None, committed_at_ms=1)
    with pytest.raises(AuthorityContractError):
        evaluate_authority_commit(context=context(cmd), command=cmd, current_revision=0, current_state=None, receipt_probe=None, committed_at_ms=True)


def test_duplicate_requires_actual_valid_record():
    result = accepted()
    plan = result.commit_plan
    probe = ReceiptProbe(plan.receipt, None)
    duplicate = evaluate_authority_commit(context=context(command()), command=command(), current_revision=1, current_state="ready", receipt_probe=probe, committed_at_ms=11)
    assert duplicate.decision.code is AuthorityDecisionCode.CANONICAL_RECORD_CORRUPTION_CONFLICT
    assert duplicate.decision.durable_conflict_record is True


def test_duplicate_same_receipt_and_record_is_already_applied():
    cmd = command()
    plan = accepted(cmd).commit_plan
    result = evaluate_authority_commit(context=context(cmd), command=cmd, current_revision=1, current_state="ready", receipt_probe=ReceiptProbe(plan.receipt, plan.record), committed_at_ms=11)
    assert result.decision.code is AuthorityDecisionCode.ALREADY_APPLIED
    assert result.decision.return_existing_transition_id == plan.record.transition_id


def test_same_operation_different_command_is_idempotency_conflict_after_valid_proof():
    original = command(payload={"x": 1})
    plan = accepted(original).commit_plan
    changed = command(payload={"x": 2})
    result = evaluate_authority_commit(context=context(changed), command=changed, current_revision=1, current_state="ready", receipt_probe=ReceiptProbe(plan.receipt, plan.record), committed_at_ms=11)
    assert result.decision.code is AuthorityDecisionCode.IDEMPOTENCY_CONFLICT


def test_fake_matching_hash_strings_do_not_prove_duplicate():
    cmd = command()
    plan = accepted(cmd).commit_plan
    tamper(plan.receipt, canonical_record_hash="a" * 64)
    result = evaluate_authority_commit(context=context(cmd), command=cmd, current_revision=1, current_state="ready", receipt_probe=ReceiptProbe(plan.receipt, plan.record), committed_at_ms=11)
    assert result.decision.code is AuthorityDecisionCode.CANONICAL_RECORD_CORRUPTION_CONFLICT


@pytest.mark.parametrize("field,value", [
    ("operation_id", "other"),
    ("operation_type", OperationType.TASK_COMPLETE.value),
    ("aggregate_revision", 2),
    ("transition_id", "ctr:v1:bad"),
    ("committed_at_ms", True),
])
def test_receipt_identity_or_type_mismatch_is_corruption(field, value):
    cmd = command()
    plan = accepted(cmd).commit_plan
    tamper(plan.receipt, **{field: value})
    result = evaluate_authority_commit(context=context(cmd), command=cmd, current_revision=1, current_state="ready", receipt_probe=ReceiptProbe(plan.receipt, plan.record), committed_at_ms=11)
    assert result.decision.code is AuthorityDecisionCode.CANONICAL_RECORD_CORRUPTION_CONFLICT


def test_store_empty_rejects_invalid_candidate_hash():
    record = accepted().commit_plan.record
    tamper(record, writer_id="tampered")
    assert record.verify_hash() is False
    assert classify_canonical_store_write(None, record) is CanonicalStoreDecision.CANONICAL_RECORD_CORRUPTION_CONFLICT


def test_store_accepts_valid_first_insert_and_exact_duplicate():
    record = accepted().commit_plan.record
    assert classify_canonical_store_write(None, record) is CanonicalStoreDecision.INSERT
    assert classify_canonical_store_write(record, record) is CanonicalStoreDecision.ALREADY_PRESENT


def test_store_rejects_invalid_existing_record():
    valid = accepted().commit_plan.record
    existing = accepted(command(operation_id="op-2")).commit_plan.record
    tamper(existing, writer_id="tampered")
    assert classify_canonical_store_write(existing, valid) is CanonicalStoreDecision.CANONICAL_RECORD_CORRUPTION_CONFLICT


@pytest.mark.parametrize("changes", [
    {"from_revision": 999},
    {"transition_id": "ctr:v1:arbitrary"},
    {"aggregate_identity_sha256": "0" * 64},
    {"operation_type": "UNKNOWN"},
    {"committed_at_ms": -1},
    {"canonical_command_hash": "not-a-hash"},
])
def test_self_hashed_semantically_invalid_record_is_rejected(changes):
    record = accepted().commit_plan.record
    tamper(record, **changes)
    rehash_record(record)
    with pytest.raises(AuthorityContractError):
        validate_canonical_transition_record(record)
    assert classify_canonical_store_write(None, record) is CanonicalStoreDecision.CANONICAL_RECORD_CORRUPTION_CONFLICT
    assert evaluate_projection_application(None, record) is ProjectionDecision.PROJECTION_CORRUPTION_CONFLICT


def test_self_hashed_wrong_projection_intents_is_rejected():
    record = accepted().commit_plan.record
    tamper(record, durable_projection_intents=())
    rehash_record(record)
    with pytest.raises(AuthorityContractError):
        validate_canonical_transition_record(record)


def second_record():
    first = accepted().commit_plan.record
    cmd = command(
        operation_type=OperationType.TASK_DISPATCH,
        operation_id="op-2",
        expected_revision=1,
        previous_state="ready",
        next_state="scheduled",
        intents=(intent("CONTROL_NOTIFICATION"), intent("TASK_REQUEST_MESSAGE")),
    )
    return first, accepted(cmd, state="ready", revision=1).commit_plan.record


def test_projection_first_revision_apply_and_gap_fail_closed():
    first, second = second_record()
    assert evaluate_projection_application(None, first) is ProjectionDecision.APPLY
    assert evaluate_projection_application(None, second) is ProjectionDecision.GAP_FAIL_CLOSED_AND_REPLAY_REQUIRED


def test_projection_duplicate_and_same_revision_corruption():
    record = accepted().commit_plan.record
    receipt = ProjectionApplicationReceipt.create(applied_revision=1, applied_transition_id=record.transition_id, applied_record_hash=record.canonical_record_hash)
    assert evaluate_projection_application(receipt, record) is ProjectionDecision.DUPLICATE_NOOP
    other = accepted(command(operation_id="other")).commit_plan.record
    assert evaluate_projection_application(receipt, other) is ProjectionDecision.PROJECTION_CORRUPTION_CONFLICT


def test_projection_next_older_and_gap():
    first, second = second_record()
    first_receipt = ProjectionApplicationReceipt.create(applied_revision=1, applied_transition_id=first.transition_id, applied_record_hash=first.canonical_record_hash)
    assert evaluate_projection_application(first_receipt, second) is ProjectionDecision.APPLY
    second_receipt = ProjectionApplicationReceipt.create(applied_revision=2, applied_transition_id=second.transition_id, applied_record_hash=second.canonical_record_hash)
    assert evaluate_projection_application(second_receipt, first) is ProjectionDecision.OLDER_REVISION_NOOP
    third_cmd = command(operation_type=OperationType.TASK_CLAIM, operation_id="op-3", expected_revision=2, previous_state="scheduled", next_state="running", intents=(intent("RUNNING_SET"),))
    third = accepted(third_cmd, state="scheduled", revision=2).commit_plan.record
    assert evaluate_projection_application(first_receipt, third) is ProjectionDecision.GAP_FAIL_CLOSED_AND_REPLAY_REQUIRED


def test_projection_rejects_bool_revision_receipt():
    record = accepted().commit_plan.record
    receipt = ProjectionApplicationReceipt.create(applied_revision=1, applied_transition_id=record.transition_id, applied_record_hash=record.canonical_record_hash)
    tamper(receipt, applied_revision=True)
    assert evaluate_projection_application(receipt, record) is ProjectionDecision.PROJECTION_CORRUPTION_CONFLICT


def test_read_only_compatibility_evaluator_cannot_accept_decision_as_input():
    cmd = command()
    decision = evaluate_authority_command(context=context(cmd), command=cmd, current_revision=0, current_state=None, receipt_probe=None, committed_at_ms=1)
    assert decision.code is AuthorityDecisionCode.ACCEPTED
    assert not hasattr(AuthorityCommitPlan, "create")


def test_record_writer_is_authorized_context_not_command_or_caller_override():
    cmd = command()
    result = evaluate_authority_commit(context=context(cmd, writer="authorized-writer"), command=cmd, current_revision=0, current_state=None, receipt_probe=None, committed_at_ms=1)
    assert result.commit_plan.record.writer_id == "authorized-writer"


def test_legacy_and_transport_operations_cannot_create_commit_plan():
    legacy = command(aggregate_identity=run_identity(), operation_type=OperationType.LEGACY_RUN_COMPLETE, previous_state="running", next_state="done", intents=())
    legacy_result = evaluate_authority_commit(context=context(legacy), command=legacy, current_revision=0, current_state="running", receipt_probe=None, committed_at_ms=1)
    assert legacy_result.decision.code is AuthorityDecisionCode.UNSUPPORTED_LEGACY_OPERATION
    transport = command(operation_type=OperationType.TERMINAL_DUPLICATE_CLEANUP, previous_state="terminal", next_state="terminal", intents=(intent("AUDIT_INTENT"), intent("AUDIT_OUTCOME")))
    transport_result = evaluate_authority_commit(context=context(transport), command=transport, current_revision=0, current_state="terminal", receipt_probe=None, committed_at_ms=1)
    assert transport_result.decision.code is AuthorityDecisionCode.OPERATION_NOT_AUTHORITY_MUTATION


def test_record_hash_and_transition_are_deterministic():
    cmd = command()
    first = accepted(cmd, committed_at_ms=10).commit_plan.record
    second = accepted(cmd, committed_at_ms=10).commit_plan.record
    assert first.transition_id == second.transition_id
    assert first.canonical_record_hash == second.canonical_record_hash


def test_metadata_and_effects_are_hash_bound():
    base = command(payload={"x": 1})
    changed_metadata = AuthorityCommand(**{**base.__dict__, "authoritative_metadata_changes": {"m": 1}})
    changed_child = AuthorityCommand(**{**base.__dict__, "requested_child_effects": [{"kind": "child"}]})
    changed_intent = AuthorityCommand(**{**base.__dict__, "requested_projection_intents": (intent("READY_QUEUE_IF_READY"), intent("EXTRA"))})
    assert len({base.canonical_command_hash, changed_metadata.canonical_command_hash, changed_child.canonical_command_hash, changed_intent.canonical_command_hash}) == 4
