from __future__ import annotations

from dataclasses import replace

import pytest

from hfa.authority import AggregateType, OperationType
from hfa.governance.admission_resource_reservation import (
    AdmissionResourceReservationInput,
)
from hfa_control.run_create_authority import (
    FEATURE_FLAG,
    WRITER_ID,
    RunCreateAuthorityInput,
    RunCreateProjectionInput,
    RunCreateProjectionManager,
    build_run_create_command,
    build_run_create_context,
    normalize_run_create_input,
    parse_run_create_binding_flag,
    run_create_identity,
    run_create_operation_id,
)


def value(**overrides):
    base = RunCreateAuthorityInput(
        run_id="tenant-a:run-1",
        tenant_id="tenant-a",
        agent_type="research",
        priority=5,
        payload={"prompt": "hello", "nested": {"z": 2, "a": 1}},
        estimated_cost_cents=125,
        preferred_region="eu-west-1",
        preferred_placement="LEAST_LOADED",
        created_at_ms=1_700_000_000_123,
        control_stream="hfa:stream:control",
    )
    return replace(base, **overrides)


@pytest.mark.parametrize("raw", ["1", "true", "TRUE", "yes", "on", " On "])
def test_flag_true_values(raw):
    assert parse_run_create_binding_flag(raw) is True


@pytest.mark.parametrize("raw", [None, "", "0", "false", "FALSE", "no", "off", " off "])
def test_flag_false_values(raw):
    assert parse_run_create_binding_flag(raw) is False


def test_flag_rejects_unknown_value():
    with pytest.raises(ValueError, match=FEATURE_FLAG):
        parse_run_create_binding_flag("maybe")


def test_run_identity_and_operation_id_are_exact_and_stable():
    identity = run_create_identity("tenant-a:run-1")
    assert identity.aggregate_type is AggregateType.RUN
    assert identity.run_id == "tenant-a:run-1"
    assert identity.task_id is None
    assert run_create_operation_id(identity.run_id) == (
        f"run-create:v1:{identity.sha256}"
    )


def test_command_contract_is_canonical_run_create():
    command = build_run_create_command(value())
    assert command.operation_type is OperationType.RUN_CREATE
    assert command.expected_revision == 0
    assert command.intended_previous_state is None
    assert command.intended_next_state == "pending"
    assert command.operation_id == run_create_operation_id("tenant-a:run-1")
    assert tuple(intent["kind"] for intent in command.requested_projection_intents) == (
        "RUN_STATUS_PROJECTION",
    )
    context = build_run_create_context(command)
    assert context.authenticated_writer_id == WRITER_ID
    assert context.allowed_operations == frozenset({OperationType.RUN_CREATE})
    assert context.fence_required is False
    assert context.fence_valid is True


def test_normalization_sorts_payload_and_normalizes_unicode():
    normalized = normalize_run_create_input(
        value(payload={"z": "e\u0301", "a": [2, {"b": 1}]})
    )
    assert list(normalized.payload) == ["a", "z"]
    assert normalized.payload["z"] == "é"


def test_command_hash_is_deterministic_for_mapping_order():
    first = build_run_create_command(value(payload={"b": 2, "a": 1}))
    second = build_run_create_command(value(payload={"a": 1, "b": 2}))
    assert first.canonical_command_hash == second.canonical_command_hash


def test_changed_payload_changes_command_hash():
    first = build_run_create_command(value(payload={"prompt": "first"}))
    second = build_run_create_command(value(payload={"prompt": "second"}))
    assert first.canonical_command_hash != second.canonical_command_hash


def test_changed_cost_changes_command_hash():
    first = build_run_create_command(value(estimated_cost_cents=100))
    second = build_run_create_command(value(estimated_cost_cents=101))
    assert first.canonical_command_hash != second.canonical_command_hash


def test_changed_cost_changes_resource_reservation_proof():
    operation_id = run_create_operation_id("tenant-a:run-1")
    first = AdmissionResourceReservationInput(
        operation_id=operation_id,
        run_id="tenant-a:run-1",
        tenant_id="tenant-a",
        estimated_cost_cents=100,
    )
    second = AdmissionResourceReservationInput(
        operation_id=operation_id,
        run_id="tenant-a:run-1",
        tenant_id="tenant-a",
        estimated_cost_cents=101,
    )
    assert first.proof_sha256 != second.proof_sha256


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "override,match",
    [
        ({"operation_id": "run-create:v1:" + "0" * 64}, "operation_id"),
        ({"canonical_revision": 2}, "revision"),
        ({"legacy_state": "pending"}, "legacy projection state"),
    ],
)
async def test_projection_contract_rejects_noncanonical_identity_before_lua(
    override,
    match,
):
    class Loader:
        async def load(self):
            raise AssertionError("invalid projection must not load Lua")

    run_id = "tenant-a:run-1"
    projection = RunCreateProjectionInput(
        operation_id=run_create_operation_id(run_id),
        reservation_proof_sha256="a" * 64,
        canonical_transition_id="ctr:v1:test",
        canonical_record_hash="b" * 64,
        canonical_command_hash="c" * 64,
        canonical_revision=1,
        run_id=run_id,
        tenant_id="tenant-a",
        legacy_state="admitted",
        state_ttl_seconds=86400,
        control_stream="hfa:stream:control",
        event_fields={
            "event_type": "RunAdmitted",
            "run_id": run_id,
            "tenant_id": "tenant-a",
        },
    )
    projection = replace(projection, **override)
    manager = RunCreateProjectionManager(object(), loader=Loader())
    with pytest.raises(ValueError, match=match):
        await manager.project(projection)


@pytest.mark.parametrize(
    "field,bad",
    [
        ("priority", True),
        ("priority", 1.5),
        ("estimated_cost_cents", True),
        ("estimated_cost_cents", -1),
        ("created_at_ms", True),
        ("created_at_ms", 2**53),
    ],
)
def test_safe_integer_and_bool_rejection(field, bad):
    with pytest.raises(ValueError):
        normalize_run_create_input(value(**{field: bad}))


def test_identity_sensitive_text_rejects_surrounding_whitespace():
    with pytest.raises(ValueError, match="surrounding whitespace"):
        normalize_run_create_input(value(run_id=" tenant-a:run-1"))
