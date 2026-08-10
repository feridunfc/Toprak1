from __future__ import annotations

from types import SimpleNamespace

import pytest

from hfa.governance.admission_resource_reservation import (
    AdmissionResourceReservationInput,
    AdmissionResourceSettlementInput,
    AdmissionResourceSettlementResult,
    RESERVATION_STATE_SETTLED,
    RESERVATION_STATUS_ALREADY_SETTLED,
    RESERVATION_STATUS_SETTLED,
)
from hfa_control.run_create_authority import (
    RunCreateAuthorityInput,
    build_run_create_command,
    resource_reservation_from_run_create_record,
)
from hfa_control.run_terminate_authority import (
    RunTerminateAuthorityBinding,
    RESOURCE_SETTLEMENT_PENDING_STATUS,
)

HEX_A = "a" * 64
HEX_B = "b" * 64
HEX_C = "c" * 64


def _settlement(**overrides) -> AdmissionResourceSettlementInput:
    values = {
        "run_create_operation_id": f"run-create:v1:{HEX_A}",
        "run_id": "run-84-8-contract",
        "tenant_id": "tenant-84-8",
        "estimated_cost_cents": 125,
        "run_create_reservation_proof_sha256": HEX_B,
        "run_terminate_operation_id": f"run-terminate:v1:{HEX_C}",
        "terminal_proof_sha256": HEX_A,
        "canonical_transition_id": "transition-84-8",
        "canonical_record_hash": HEX_B,
        "canonical_command_hash": HEX_C,
        "canonical_revision": 2,
        "final_state": "done",
    }
    # Build the exact reservation proof when the caller does not override it.
    reservation = AdmissionResourceReservationInput(
        operation_id=values["run_create_operation_id"],
        run_id=values["run_id"],
        tenant_id=values["tenant_id"],
        estimated_cost_cents=values["estimated_cost_cents"],
    )
    values["run_create_reservation_proof_sha256"] = reservation.proof_sha256
    values.update(overrides)
    return AdmissionResourceSettlementInput(**values)


def test_settlement_proof_is_deterministic_and_binds_terminal_authority() -> None:
    first = _settlement()
    second = _settlement()
    changed = _settlement(canonical_record_hash=HEX_A)
    assert first.proof_sha256 == second.proof_sha256
    assert first.proof_sha256 != changed.proof_sha256
    assert len(first.proof_sha256) == 64


@pytest.mark.parametrize("final_state", ["running", "skipped", "cancelled"])
def test_settlement_rejects_non_run_terminal_states(final_state: str) -> None:
    with pytest.raises(ValueError, match="final_state"):
        _settlement(final_state=final_state)


def test_settlement_rejects_reservation_proof_not_bound_to_run_create() -> None:
    with pytest.raises(ValueError, match="reservation proof"):
        _settlement(run_create_reservation_proof_sha256=HEX_A)


def test_resource_reservation_helper_reconstructs_exact_run_create_record() -> None:
    command = build_run_create_command(
        RunCreateAuthorityInput(
            run_id="run-84-8-helper",
            tenant_id="tenant-84-8",
            agent_type="research",
            priority=5,
            payload={"prompt": "settle"},
            estimated_cost_cents=321,
            preferred_region="eu-west-1",
            preferred_placement="LEAST_LOADED",
            created_at_ms=100,
            control_stream="hfa:stream:control",
        )
    )
    record = SimpleNamespace(
        operation_type=command.operation_type.value,
        aggregate_identity=command.aggregate_identity,
        from_revision=0,
        to_revision=1,
        previous_state=None,
        next_state="pending",
        writer_id="hfa-control/run-create-writer:v1",
        authoritative_metadata_changes=command.authoritative_metadata_changes,
        operation_id=command.operation_id,
        canonical_command_hash=command.canonical_command_hash,
    )
    reservation = resource_reservation_from_run_create_record(record)
    assert reservation.operation_id == command.operation_id
    assert reservation.run_id == "run-84-8-helper"
    assert reservation.tenant_id == "tenant-84-8"
    assert reservation.estimated_cost_cents == 321


class _ResourceManager:
    def __init__(self) -> None:
        self.initialised = 0

    async def initialise(self) -> None:
        self.initialised += 1


class _Store:
    async def initialise(self) -> None:
        return None


class _Initialisable:
    async def initialise(self) -> None:
        return None


@pytest.mark.asyncio
async def test_run_terminate_initialises_optional_resource_manager() -> None:
    resource = _ResourceManager()
    binding = RunTerminateAuthorityBinding(
        object(),
        store=_Store(),
        proof_manager=_Initialisable(),
        projection_manager=_Initialisable(),
        resource_manager=resource,
    )
    await binding.initialise()
    assert resource.initialised == 1


@pytest.mark.asyncio
async def test_historical_run_terminate_initialisation_has_no_resource_dependency() -> None:
    binding = RunTerminateAuthorityBinding(
        object(),
        store=_Store(),
        proof_manager=_Initialisable(),
        projection_manager=_Initialisable(),
    )
    await binding.initialise()
    assert binding.resource_manager is None


def test_settlement_result_accepts_only_settled_statuses_for_success() -> None:
    exact = AdmissionResourceSettlementResult(
        status=RESERVATION_STATUS_SETTLED,
        run_create_operation_id=f"run-create:v1:{HEX_A}",
        settlement_proof_sha256=HEX_B,
        resource_mutated=True,
        state=RESERVATION_STATE_SETTLED,
    )
    replay = AdmissionResourceSettlementResult(
        status=RESERVATION_STATUS_ALREADY_SETTLED,
        run_create_operation_id=f"run-create:v1:{HEX_A}",
        settlement_proof_sha256=HEX_B,
        resource_mutated=False,
        state=RESERVATION_STATE_SETTLED,
    )
    assert exact.ok is True
    assert replay.ok is True
    assert RESOURCE_SETTLEMENT_PENDING_STATUS == "canonical_resource_settlement_pending"
