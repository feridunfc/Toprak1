"""Default-compatible coordinator proof transport, not a new authority boundary."""
from dataclasses import FrozenInstanceError, fields
from unittest.mock import AsyncMock

import pytest

from hfa_control.run_terminate_authority import RunTerminateAuthorityError, RunTerminateBindingResult
from hfa_control.run_termination import RunTerminationCoordinator, RunTerminationResult


PROOF_DEFAULTS = {
    "canonical_transition_id": "", "canonical_record_hash": "",
    "canonical_command_hash": "", "canonical_revision": 0,
    "terminal_proof_sha256": "", "canonical_head_keys": (),
    "canonical_head_witness": (),
}


def assert_proofless(result):
    assert {name: getattr(result, name) for name in PROOF_DEFAULTS} == PROOF_DEFAULTS


def test_old_constructor_order_and_immutability():
    old = (True, "finalized", "run", "done", 1, 1, 0, 0, False, True)
    result = RunTerminationResult(*old)
    assert tuple(getattr(result, field.name) for field in fields(result)[:10]) == old
    assert_proofless(result)
    with pytest.raises(FrozenInstanceError):
        result.canonical_revision = 3


@pytest.mark.parametrize("raw", [
    [1, "finalized", "done", 1, 1, 0, 0, 0, 1],
    [1, "already_finalized", "failed", 1, 0, 1, 0, 1, 1],
    [0, "not_ready", "", 2, 1, 0, 0, 0, 1],
    [0, "run_truth_corruption_conflict", "", 0, 0, 0, 0, 0, 0],
])
def test_legacy_nine_column_parser_preserves_outcomes_without_proof(raw):
    result = RunTerminationResult.from_lua(raw, run_id="legacy-run")
    assert (result.finalized, result.status, result.final_state, result.task_count,
            result.done_count, result.failed_count, result.skipped_count,
            result.already_finalized, result.ack_allowed) == tuple(
        bool(value) if index in {0, 7, 8} else value
        for index, value in enumerate(raw)
    )
    assert result.run_id == "legacy-run"
    assert_proofless(result)


@pytest.mark.asyncio
async def test_disabled_coordinator_does_not_initialize_or_acquire_proof():
    coordinator = RunTerminationCoordinator(object(), object(), enabled=False)
    coordinator.initialise = AsyncMock(side_effect=AssertionError("disabled I/O"))
    result = await coordinator.finalize_run_from_tasks(
        run_id="run", tenant_id="tenant", trigger_task_id="task", finalized_at_ms=1,
    )
    assert not result.finalized and not result.ack_allowed
    assert result.status == "run_termination_binding_disabled"
    assert_proofless(result)
    coordinator.initialise.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("finalized,ack_allowed", [(False, False), (False, True), (True, False)])
async def test_non_success_canonical_result_stays_proofless(finalized, ack_allowed):
    binding = AsyncMock()
    binding.terminate.return_value = RunTerminateBindingResult(
        finalized=finalized, status="not_ready", run_id="run", task_count=1,
        ack_allowed=ack_allowed, canonical_transition_id="untrusted-observation",
    )
    coordinator = RunTerminationCoordinator(object(), object(), enabled=True, authority_binding=binding)
    coordinator._loader = object()
    result = await coordinator.finalize_run_from_tasks(
        run_id="run", tenant_id="tenant", trigger_task_id="task", finalized_at_ms=1,
    )
    assert result.status == "not_ready" and result.task_count == 1
    assert result.finalized is finalized and result.ack_allowed is ack_allowed
    assert_proofless(result)


@pytest.mark.asyncio
async def test_initialization_failure_does_not_request_authority():
    binding = AsyncMock()
    coordinator = RunTerminationCoordinator(object(), object(), enabled=True, authority_binding=binding)
    failure = RuntimeError("initialization unavailable")
    coordinator.initialise = AsyncMock(side_effect=failure)
    with pytest.raises(RuntimeError) as caught:
        await coordinator.finalize_run_from_tasks(
            run_id="run", tenant_id="tenant", trigger_task_id="task", finalized_at_ms=1,
        )
    assert caught.value is failure
    binding.terminate.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("status,durable", [
    ("canonical_run_missing", False), ("canonical_run_terminate_authority_conflict", False),
    ("canonical_resource_settlement_pending", True), ("canonical_projection_pending", True),
])
async def test_failed_authority_is_never_converted_to_success(status, durable):
    failure = RunTerminateAuthorityError(status, canonical_commit_durable=durable)
    binding = AsyncMock()
    binding.terminate.side_effect = failure
    coordinator = RunTerminationCoordinator(object(), object(), enabled=True, authority_binding=binding)
    coordinator._loader = object()
    with pytest.raises(RunTerminateAuthorityError) as caught:
        await coordinator.finalize_run_from_tasks(
            run_id="run", tenant_id="tenant", trigger_task_id="task", finalized_at_ms=1,
        )
    assert caught.value is failure


@pytest.mark.asyncio
async def test_exact_task_no_op_never_requests_run_proof():
    from hfa_control.run_termination import CoordinatedTaskCompleteResult

    gateway, binding = AsyncMock(), AsyncMock()
    gateway.task_complete.return_value = CoordinatedTaskCompleteResult(
        completed=True, status="canonical_terminal_already_projected", exact_no_op=True,
    )
    coordinator = RunTerminationCoordinator(object(), gateway, enabled=True, authority_binding=binding)
    result = await coordinator.task_complete(run_id="run", task_id="task", tenant_id="tenant")
    assert result.completed and result.exact_no_op and not result.ack_allowed
    assert result.run_termination is None
    binding.terminate.assert_not_awaited()
