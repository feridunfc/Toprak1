from __future__ import annotations

import math
from dataclasses import replace

import pytest

from hfa.authority import (
    OPERATION_CONTRACTS,
    AggregateType,
    AuthorityCommand,
    AuthorityCommitPlan,
    AuthorityContractError,
    AuthorityDecisionCode,
    AuthorityEntryContext,
    CanonicalAggregateIdentity,
    CanonicalStoreDecision,
    CanonicalTransitionRecord,
    OperationType,
    ProjectionApplicationReceipt,
    ProjectionDecision,
    ReceiptProbe,
    canonical_json_bytes,
    classify_canonical_store_write,
    evaluate_authority_command,
    evaluate_projection_application,
)


def identity(
    *,
    run_id: str = "run-1",
    task_id: str = "task-1",
) -> CanonicalAggregateIdentity:
    return CanonicalAggregateIdentity(
        AggregateType.TASK,
        run_id=run_id,
        task_id=task_id,
    )


def command(**overrides) -> AuthorityCommand:
    values = {
        "aggregate_identity": identity(),
        "operation_type": "TASK_ADMIT",
        "operation_id": "op-1",
        "expected_revision": 0,
        "intended_previous_state": None,
        "intended_next_state": "ready",
        "authoritative_payload": {"payload": 1},
        "authoritative_metadata_changes": {"tenant_id": "tenant-1"},
        "requested_child_effects": [],
        "requested_projection_intents": [
            {"kind": "READY_QUEUE_IF_READY", "task_id": "task-1"}
        ],
        "causation_id": None,
    }
    values.update(overrides)
    return AuthorityCommand(**values)


def dispatch_command(**overrides) -> AuthorityCommand:
    values = {
        "operation_id": "op-2",
        "operation_type": "TASK_DISPATCH",
        "expected_revision": 1,
        "intended_previous_state": "ready",
        "intended_next_state": "scheduled",
        "requested_projection_intents": [
            {"kind": "CONTROL_NOTIFICATION"},
            {"kind": "TASK_REQUEST_MESSAGE"},
        ],
    }
    values.update(overrides)
    return command(**values)


def context(cmd: AuthorityCommand, **overrides) -> AuthorityEntryContext:
    values = {
        "authenticated_writer_id": "scheduler-1",
        "authorized_operations": frozenset({cmd.operation_type}),
        "target_aggregate_identity": cmd.aggregate_identity,
    }
    values.update(overrides)
    return AuthorityEntryContext(**values)


def record(
    cmd: AuthorityCommand | None = None,
    **overrides,
) -> CanonicalTransitionRecord:
    return CanonicalTransitionRecord.create(
        cmd or command(),
        writer_id=overrides.pop("writer_id", "scheduler-1"),
        committed_at_ms=overrides.pop("committed_at_ms", 1_700_000_000_000),
        correlation_id=overrides.pop("correlation_id", "corr-1"),
        **overrides,
    )


def accepted_decision(cmd: AuthorityCommand):
    return evaluate_authority_command(
        context=context(cmd),
        command=cmd,
        current_revision=cmd.expected_revision,
        current_state=cmd.intended_previous_state,
    )


def test_task_identity_includes_run_id() -> None:
    assert identity(run_id="run-a").sha256 != identity(run_id="run-b").sha256


def test_length_prefix_prevents_delimiter_collision() -> None:
    left = identity(run_id="a:b", task_id="c")
    right = identity(run_id="a", task_id="b:c")
    assert left.value == right.value
    assert left.sha256 != right.sha256


def test_nfc_identity_normalization_is_stable() -> None:
    composed = identity(run_id="caf\u00e9")
    decomposed = identity(run_id="cafe\u0301")
    assert composed.run_id == decomposed.run_id
    assert composed.sha256 == decomposed.sha256


def test_run_identity_rejects_task_id() -> None:
    with pytest.raises(AuthorityContractError, match="must not include task_id"):
        CanonicalAggregateIdentity(
            AggregateType.RUN,
            run_id="run-1",
            task_id="task-1",
        )


def test_canonical_json_sorts_keys_and_normalizes_negative_zero() -> None:
    assert canonical_json_bytes({"z": -0.0, "a": 1}) == b'{"a":1,"z":0}'


def test_canonical_json_rejects_non_finite_numbers() -> None:
    with pytest.raises(AuthorityContractError, match="non-finite"):
        canonical_json_bytes({"bad": math.inf})


def test_canonical_json_rejects_duplicate_keys_after_nfc() -> None:
    with pytest.raises(AuthorityContractError, match="duplicate keys"):
        canonical_json_bytes({"caf\u00e9": 1, "cafe\u0301": 2})


def test_canonical_json_rejects_integer_outside_jcs_safe_domain() -> None:
    with pytest.raises(AuthorityContractError, match="safe integer domain"):
        canonical_json_bytes({"bad": 2**60})


def test_binary_values_use_explicit_base64url_type_tag() -> None:
    assert canonical_json_bytes({"blob": b"\xff\x00"}) == (
        b'{"blob":{"$base64url":"_wA","$type":"bytes"}}'
    )


def test_command_hash_is_mapping_order_independent() -> None:
    left = command(authoritative_payload={"b": 2, "a": 1})
    right = command(authoritative_payload={"a": 1, "b": 2})
    assert left.canonical_command_hash == right.canonical_command_hash


def test_explicit_null_and_empty_object_hash_differ() -> None:
    null_payload = command(authoritative_payload=None)
    empty_payload = command(authoritative_payload={})
    assert null_payload.canonical_command_hash != empty_payload.canonical_command_hash


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("authoritative_metadata_changes", {"tenant_id": "other"}),
        ("requested_child_effects", [{"child": "x"}]),
        ("requested_projection_intents", []),
    ],
)
def test_authoritative_effects_are_hash_bound(field: str, value) -> None:
    original = command()
    changed = command(**{field: value})
    assert original.canonical_command_hash != changed.canonical_command_hash


def test_command_effects_are_deeply_immutable() -> None:
    payload = {"nested": [1, 2]}
    cmd = command(authoritative_payload=payload)
    before = cmd.canonical_command_hash
    payload["nested"].append(3)
    assert cmd.canonical_command_hash == before
    assert tuple(cmd.authoritative_payload["nested"]) == (1, 2)


def test_operation_receipt_key_uses_identity_and_operation_hashes() -> None:
    cmd = command()
    assert cmd.operation_receipt_key == (
        f"oprcpt:v1:{cmd.aggregate_identity.sha256}:{cmd.operation_id_sha256}"
    )


def test_operation_registry_has_exact_fifteen_rows() -> None:
    assert len(OPERATION_CONTRACTS) == 15
    assert set(OPERATION_CONTRACTS) == set(OperationType)


def test_unknown_operation_is_rejected() -> None:
    with pytest.raises(AuthorityContractError, match="unsupported operation_type"):
        command(operation_type="TASK_MAGIC")


def test_operation_requires_correct_aggregate_owner() -> None:
    run_identity = CanonicalAggregateIdentity(AggregateType.RUN, run_id="run-1")
    with pytest.raises(AuthorityContractError, match="requires task aggregate"):
        command(aggregate_identity=run_identity)


def test_operation_specific_transition_is_enforced() -> None:
    with pytest.raises(AuthorityContractError, match="illegal previous state"):
        command(
            operation_type="TASK_COMPLETE",
            expected_revision=1,
            intended_previous_state="ready",
            intended_next_state="done",
            requested_projection_intents=[
                {"kind": "OUTPUT_PROJECTION"},
                {"kind": "DEPENDENCY_FANOUT_INTENT"},
            ],
        )


def test_required_projection_intents_are_enforced() -> None:
    with pytest.raises(AuthorityContractError, match="missing required"):
        command(
            operation_type="TASK_COMPLETE",
            expected_revision=1,
            intended_previous_state="running",
            intended_next_state="done",
            requested_projection_intents=[{"kind": "OUTPUT_PROJECTION"}],
        )


def test_undeclared_projection_intent_is_rejected() -> None:
    with pytest.raises(AuthorityContractError, match="undeclared projection intent"):
        command(requested_projection_intents=[{"kind": "NOT_ALLOWED"}])


def test_outer_gate_rejects_before_receipt_disclosure() -> None:
    cmd = command()
    committed = record(cmd)
    decision = evaluate_authority_command(
        context=context(cmd, authenticated_writer_id=None),
        command=cmd,
        current_revision=1,
        current_state="ready",
        receipt_probe=ReceiptProbe(
            committed.to_receipt(),
            committed.canonical_record_hash,
        ),
    )
    assert decision.code is AuthorityDecisionCode.AUTHORITY_ENTRY_REJECTED
    assert decision.receipt_disclosed is False


def test_outer_gate_requires_operation_capability() -> None:
    cmd = command()
    decision = evaluate_authority_command(
        context=context(cmd, authorized_operations=frozenset()),
        command=cmd,
        current_revision=0,
        current_state=None,
    )
    assert decision.code is AuthorityDecisionCode.AUTHORITY_ENTRY_REJECTED


def test_outer_gate_requires_target_identity_match() -> None:
    cmd = command()
    other = identity(run_id="other-run")
    decision = evaluate_authority_command(
        context=context(cmd, target_aggregate_identity=other),
        command=cmd,
        current_revision=0,
        current_state=None,
    )
    assert decision.code is AuthorityDecisionCode.AUTHORITY_ENTRY_REJECTED


def test_outer_gate_requires_fence_when_applicable() -> None:
    cmd = command()
    decision = evaluate_authority_command(
        context=context(cmd, fence_required=True, fence_valid=False),
        command=cmd,
        current_revision=0,
        current_state=None,
    )
    assert decision.code is AuthorityDecisionCode.AUTHORITY_ENTRY_REJECTED


def test_same_operation_same_command_is_already_applied() -> None:
    cmd = command()
    committed = record(cmd)
    decision = evaluate_authority_command(
        context=context(cmd),
        command=cmd,
        current_revision=1,
        current_state="ready",
        receipt_probe=ReceiptProbe(
            committed.to_receipt(),
            committed.canonical_record_hash,
        ),
    )
    assert decision.code is AuthorityDecisionCode.ALREADY_APPLIED
    assert decision.existing_transition_id == committed.transition_id
    assert decision.revision_increment == 0


def test_same_operation_different_command_is_idempotency_conflict() -> None:
    original = command()
    committed = record(original)
    changed = command(authoritative_payload={"payload": 2})
    decision = evaluate_authority_command(
        context=context(changed),
        command=changed,
        current_revision=1,
        current_state="ready",
        receipt_probe=ReceiptProbe(
            committed.to_receipt(),
            committed.canonical_record_hash,
        ),
    )
    assert decision.code is AuthorityDecisionCode.IDEMPOTENCY_CONFLICT
    assert decision.durable_conflict_record_count == 1
    assert decision.reconciliation_candidate is True


def test_receipt_store_hash_mismatch_is_corruption_conflict() -> None:
    cmd = command()
    committed = record(cmd)
    decision = evaluate_authority_command(
        context=context(cmd),
        command=cmd,
        current_revision=1,
        current_state="ready",
        receipt_probe=ReceiptProbe(committed.to_receipt(), "0" * 64),
    )
    assert decision.code is AuthorityDecisionCode.CANONICAL_RECORD_CORRUPTION_CONFLICT


def test_stale_create_is_aggregate_exists_conflict() -> None:
    cmd = command(expected_revision=0)
    decision = evaluate_authority_command(
        context=context(cmd),
        command=cmd,
        current_revision=1,
        current_state="ready",
    )
    assert decision.code is AuthorityDecisionCode.AGGREGATE_ALREADY_EXISTS_CONFLICT
    assert decision.durable_conflict_record_count == 1


def test_stale_non_create_revision_is_not_duplicate() -> None:
    cmd = dispatch_command(expected_revision=1)
    decision = evaluate_authority_command(
        context=context(cmd),
        command=cmd,
        current_revision=2,
        current_state="scheduled",
    )
    assert decision.code is AuthorityDecisionCode.STALE_REVISION_CONFLICT


def test_future_revision_fails_closed_and_requests_reconciliation() -> None:
    cmd = dispatch_command(expected_revision=3)
    decision = evaluate_authority_command(
        context=context(cmd),
        command=cmd,
        current_revision=1,
        current_state="ready",
    )
    assert decision.code is AuthorityDecisionCode.FUTURE_REVISION_CONFLICT
    assert decision.reconciliation_candidate is True


def test_state_mismatch_is_illegal_transition() -> None:
    cmd = command(
        operation_type="TASK_COMPLETE",
        expected_revision=1,
        intended_previous_state="running",
        intended_next_state="done",
        requested_projection_intents=[
            {"kind": "OUTPUT_PROJECTION"},
            {"kind": "DEPENDENCY_FANOUT_INTENT"},
        ],
    )
    decision = evaluate_authority_command(
        context=context(cmd),
        command=cmd,
        current_revision=1,
        current_state="ready",
    )
    assert decision.code is AuthorityDecisionCode.ILLEGAL_STATE_TRANSITION


def test_accepted_command_consumes_exactly_one_revision_and_record() -> None:
    decision = accepted_decision(command())
    assert decision.code is AuthorityDecisionCode.ACCEPTED
    assert decision.aggregate_mutation == 1
    assert decision.revision_increment == 1
    assert decision.canonical_record_count == 1


def test_heartbeat_is_not_an_authority_mutation() -> None:
    cmd = command(
        operation_type="TASK_HEARTBEAT",
        expected_revision=4,
        intended_previous_state="running",
        intended_next_state="running",
        requested_projection_intents=[{"kind": "LIVENESS_TTL"}],
    )
    decision = evaluate_authority_command(
        context=context(cmd),
        command=cmd,
        current_revision=4,
        current_state="running",
    )
    assert decision.code is AuthorityDecisionCode.OPERATION_NOT_AUTHORITY_MUTATION
    assert decision.revision_increment == 0


def test_legacy_run_completion_is_blocked() -> None:
    run_identity = CanonicalAggregateIdentity(AggregateType.RUN, run_id="run-1")
    cmd = command(
        aggregate_identity=run_identity,
        operation_type="LEGACY_RUN_COMPLETE",
        expected_revision=3,
        intended_previous_state="running",
        intended_next_state="done",
        requested_projection_intents=[],
    )
    decision = evaluate_authority_command(
        context=context(cmd),
        command=cmd,
        current_revision=3,
        current_state="running",
    )
    assert decision.code is AuthorityDecisionCode.UNSUPPORTED_LEGACY_OPERATION


def test_record_and_receipt_are_deterministic_and_hash_verified() -> None:
    cmd = command()
    committed = record(cmd)
    assert committed.from_revision == 0
    assert committed.to_revision == 1
    assert committed.transition_id.startswith(
        f"ctr:v1:{identity().sha256}:1:"
    )
    assert committed.verify_hash() is True
    receipt = committed.to_receipt()
    assert receipt.aggregate_revision == 1
    assert receipt.canonical_record_hash == committed.canonical_record_hash


def test_non_authority_operation_cannot_create_record() -> None:
    heartbeat = command(
        operation_type="TASK_HEARTBEAT",
        expected_revision=1,
        intended_previous_state="running",
        intended_next_state="running",
        requested_projection_intents=[{"kind": "LIVENESS_TTL"}],
    )
    with pytest.raises(AuthorityContractError, match="does not create"):
        record(heartbeat)


def test_commit_plan_binds_one_record_receipt_and_revision() -> None:
    cmd = command()
    decision = accepted_decision(cmd)
    plan = AuthorityCommitPlan.create(
        decision,
        cmd,
        writer_id="scheduler-1",
        committed_at_ms=1_700_000_000_000,
        correlation_id="corr-1",
    )
    assert plan.from_revision == 0
    assert plan.to_revision == 1
    assert plan.revision_increment == 1
    assert plan.canonical_record_count == 1
    assert plan.operation_receipt_count == 1
    assert plan.operation_receipt.transition_id == plan.canonical_record.transition_id


def test_rejected_decision_cannot_create_commit_plan() -> None:
    cmd = dispatch_command(expected_revision=2)
    rejected = evaluate_authority_command(
        context=context(cmd),
        command=cmd,
        current_revision=1,
        current_state="ready",
    )
    with pytest.raises(AuthorityContractError, match="requires an ACCEPTED"):
        AuthorityCommitPlan.create(
            rejected,
            cmd,
            writer_id="scheduler-1",
            committed_at_ms=1,
        )


def test_same_millisecond_transitions_are_ordered_by_revision() -> None:
    first = record(command(), committed_at_ms=1000)
    second = record(dispatch_command(), committed_at_ms=1000)
    assert first.committed_at_ms == second.committed_at_ms
    assert (first.to_revision, second.to_revision) == (1, 2)
    assert first.transition_id != second.transition_id


def test_store_same_transition_same_record_is_already_present() -> None:
    committed = record()
    result = classify_canonical_store_write(committed, committed)
    assert result is CanonicalStoreDecision.ALREADY_PRESENT


def test_store_same_transition_different_record_is_corruption() -> None:
    committed = record()
    different = record(writer_id="other-writer")
    result = classify_canonical_store_write(committed, different)
    assert result is CanonicalStoreDecision.CANONICAL_RECORD_CORRUPTION_CONFLICT


def test_projection_first_record_must_start_at_revision_one() -> None:
    second = record(dispatch_command())
    result = evaluate_projection_application(current=None, incoming=second)
    assert result is ProjectionDecision.GAP_FAIL_CLOSED_AND_REPLAY_REQUIRED


def test_projection_first_revision_applies() -> None:
    first = record()
    result = evaluate_projection_application(current=None, incoming=first)
    assert result is ProjectionDecision.APPLY


def test_projection_same_revision_same_record_is_duplicate_noop() -> None:
    committed = record()
    current = ProjectionApplicationReceipt(
        committed.to_revision,
        committed.transition_id,
        committed.canonical_record_hash,
    )
    result = evaluate_projection_application(current=current, incoming=committed)
    assert result is ProjectionDecision.DUPLICATE_NOOP


def test_projection_same_revision_different_record_is_corruption() -> None:
    committed = record()
    different = record(writer_id="other-writer")
    current = ProjectionApplicationReceipt(
        committed.to_revision,
        committed.transition_id,
        committed.canonical_record_hash,
    )
    result = evaluate_projection_application(current=current, incoming=different)
    assert result is ProjectionDecision.PROJECTION_CORRUPTION_CONFLICT


def test_projection_rejects_record_with_invalid_hash() -> None:
    committed = record()
    corrupted = replace(committed, writer_id="other-writer")
    result = evaluate_projection_application(current=None, incoming=corrupted)
    assert result is ProjectionDecision.PROJECTION_CORRUPTION_CONFLICT


def test_projection_older_revision_is_noop() -> None:
    first = record()
    second = record(dispatch_command())
    current = ProjectionApplicationReceipt(
        second.to_revision,
        second.transition_id,
        second.canonical_record_hash,
    )
    result = evaluate_projection_application(current=current, incoming=first)
    assert result is ProjectionDecision.OLDER_REVISION_NOOP


def test_projection_next_revision_applies_and_gap_fails_closed() -> None:
    first = record()
    current = ProjectionApplicationReceipt(
        first.to_revision,
        first.transition_id,
        first.canonical_record_hash,
    )
    second = record(dispatch_command())
    assert (
        evaluate_projection_application(current=current, incoming=second)
        is ProjectionDecision.APPLY
    )
    fourth_command = command(
        operation_id="op-4",
        operation_type="TASK_CANCEL",
        expected_revision=3,
        intended_previous_state="running",
        intended_next_state="skipped",
        requested_projection_intents=[{"kind": "TERMINAL_PROJECTION"}],
    )
    fourth = record(fourth_command)
    assert (
        evaluate_projection_application(current=current, incoming=fourth)
        is ProjectionDecision.GAP_FAIL_CLOSED_AND_REPLAY_REQUIRED
    )
