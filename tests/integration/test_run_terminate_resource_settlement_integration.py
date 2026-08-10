from __future__ import annotations

from dataclasses import dataclass

import pytest

from hfa.authority import (
    AuthorityDecisionCode,
    RedisAuthorityCommitStatus,
    RedisCanonicalAuthorityStore,
    evaluate_authority_commit,
)
from hfa.config.keys import RedisKey
from hfa.dag.schema import DagRedisKey
from hfa.governance.admission_resource_reservation import (
    AdmissionResourceReservationManager,
    AdmissionResourceSettlementInput,
    RESERVATION_STATE_FINALIZED,
    RESERVATION_STATE_SETTLED,
    RESERVATION_STATUS_ALREADY_SETTLED,
    RESERVATION_STATUS_RESOURCE_STATE_CONFLICT,
    RESERVATION_STATUS_SETTLED,
)
from hfa_control.run_create_authority import (
    RunCreateAuthorityBinding,
    RunCreateAuthorityInput,
    build_run_create_command,
    build_run_create_context,
    run_create_operation_id,
)
from hfa_control.run_terminate_authority import (
    NOT_READY_STATUS,
    RESOURCE_SETTLEMENT_PENDING_STATUS,
    RunTerminateAuthorityBinding,
    RunTerminateAuthorityError,
    RunTerminateProjectionPendingError,
    run_terminate_identity,
    run_terminate_operation_id,
)

pytestmark = [pytest.mark.asyncio, pytest.mark.integration]


@dataclass
class _Request:
    run_id: str
    tenant_id: str
    agent_type: str = "research"
    priority: int = 5
    payload: dict | None = None
    estimated_cost_cents: int = 125
    preferred_region: str = "eu-west-1"
    preferred_placement: str = "LEAST_LOADED"

    def __post_init__(self):
        if self.payload is None:
            self.payload = {"prompt": "84.8"}


async def _migration_ready(redis):
    await redis.hset(
        RedisKey.run_terminal_event_index(),
        mapping={
            "__contract__:schema_version": "1",
            "__contract__:producer_contract_version": "1",
            "__migration__:status": "ready",
            "__migration__:results_stream_key": RedisKey.stream_results(),
            "__migration__:source_history_complete": "1",
        },
    )
    await redis.persist(RedisKey.run_terminal_event_index())


async def _seed_resource_run(redis, *, run_id: str, tenant_id: str, tasks: dict[str, str], cost: int = 125):
    manager = AdmissionResourceReservationManager(redis)
    create = RunCreateAuthorityBinding(
        redis=redis,
        resource_manager=manager,
        control_stream=RedisKey.stream_control(),
    )
    await create.initialise()
    result = await create.admit(
        _Request(run_id=run_id, tenant_id=tenant_id, estimated_cost_cents=cost),
        tenant_inflight_limit=10,
        concurrent_run_limit=10,
        budget_limit_cents=10_000,
    )
    assert result.run_id == run_id
    operation_id = run_create_operation_id(result.run_id)
    reservation = manager.reservation_receipt_key(operation_id)
    receipt = await redis.hgetall(reservation)
    assert receipt["state"] == RESERVATION_STATE_FINALIZED
    assert await redis.ttl(reservation) == -1
    await redis.set(RedisKey.run_state(run_id), "running")
    await redis.hset(RedisKey.run_meta(run_id), mapping={"run_id": run_id, "tenant_id": tenant_id, "state": "running"})
    await redis.zadd(RedisKey.cp_running(), {run_id: 100})
    for task_id, state in tasks.items():
        await redis.sadd(DagRedisKey.run_tasks(run_id), task_id)
        await redis.set(DagRedisKey.task_state(task_id), state)
        await redis.hset(DagRedisKey.task_meta(task_id), mapping={"task_id": task_id, "run_id": run_id, "tenant_id": tenant_id})
    await _migration_ready(redis)
    binding = RunTerminateAuthorityBinding(redis, resource_manager=manager)
    await binding.initialise()
    return manager, create.store, binding, operation_id


async def _counters(manager, tenant_id):
    return await manager.get_resource_snapshot(tenant_id)


async def test_non_last_run_not_ready_does_not_settle(real_redis):
    run_id, tenant = "r84-8-not-ready", "t84-8"
    manager, store, binding, op = await _seed_resource_run(
        real_redis, run_id=run_id, tenant_id=tenant,
        tasks={"task-a": "done", "task-b": "running"},
    )
    before = await _counters(manager, tenant)
    result = await binding.terminate(
        run_id=run_id, tenant_id=tenant, trigger_task_id="task-a",
        finalized_at_ms=1000, worker_instance_id="worker-a", trigger_terminal_state="done",
    )
    assert result.status == NOT_READY_STATUS and result.ack_allowed is True
    receipt = await real_redis.hgetall(manager.reservation_receipt_key(op))
    assert receipt["state"] == RESERVATION_STATE_FINALIZED
    assert await _counters(manager, tenant) == before
    snap = await store.get_aggregate_snapshot(run_terminate_identity(run_id))
    assert snap.revision == 1 and snap.state == "pending"


async def test_last_success_settles_before_run_projection(real_redis):
    run_id, tenant = "r84-8-success", "t84-8-success"
    manager, store, binding, op = await _seed_resource_run(
        real_redis, run_id=run_id, tenant_id=tenant, tasks={"task-a": "done"},
    )
    result = await binding.terminate(
        run_id=run_id, tenant_id=tenant, trigger_task_id="task-a",
        finalized_at_ms=1100, worker_instance_id="worker-a", trigger_terminal_state="done",
    )
    assert result.ack_allowed is True and result.final_state == "done"
    receipt = await real_redis.hgetall(manager.reservation_receipt_key(op))
    assert receipt["state"] == RESERVATION_STATE_SETTLED
    assert await real_redis.ttl(manager.reservation_receipt_key(op)) == -1
    assert await _counters(manager, tenant) == {"concurrent_runs": 0, "budget_reserved_cents": 0, "tenant_inflight": 0}
    snap = await store.get_aggregate_snapshot(run_terminate_identity(run_id))
    assert snap.revision == 2 and snap.state == "done"
    assert await real_redis.get(RedisKey.run_state(run_id)) == "done"


async def test_last_failure_uses_same_settlement_contract(real_redis):
    run_id, tenant = "r84-8-failed", "t84-8-failed"
    manager, store, binding, op = await _seed_resource_run(
        real_redis, run_id=run_id, tenant_id=tenant, tasks={"task-a": "failed"},
    )
    result = await binding.terminate(
        run_id=run_id, tenant_id=tenant, trigger_task_id="task-a",
        finalized_at_ms=1200, worker_instance_id="worker-a", trigger_terminal_state="failed",
    )
    assert result.final_state == "failed" and result.canonical_revision == 2
    assert (await real_redis.hgetall(manager.reservation_receipt_key(op)))["state"] == RESERVATION_STATE_SETTLED
    assert await _counters(manager, tenant) == {"concurrent_runs": 0, "budget_reserved_cents": 0, "tenant_inflight": 0}
    snap = await store.get_aggregate_snapshot(run_terminate_identity(run_id))
    assert snap.revision == 2 and snap.state == "failed"


async def test_run_commit_durable_resource_drift_blocks_projection_without_mutation(real_redis):
    run_id, tenant = "r84-8-drift", "t84-8-drift"
    manager, store, binding, op = await _seed_resource_run(
        real_redis, run_id=run_id, tenant_id=tenant, tasks={"task-a": "done"},
    )
    await real_redis.delete(manager.concurrent_run_key(tenant))
    before = await real_redis.hgetall(manager.reservation_receipt_key(op))
    with pytest.raises(RunTerminateAuthorityError) as caught:
        await binding.terminate(
            run_id=run_id, tenant_id=tenant, trigger_task_id="task-a",
            finalized_at_ms=1300, worker_instance_id="worker-a", trigger_terminal_state="done",
        )
    assert caught.value.status == RESOURCE_SETTLEMENT_PENDING_STATUS
    snap = await store.get_aggregate_snapshot(run_terminate_identity(run_id))
    assert snap.revision == 2 and snap.state == "done"
    after = await real_redis.hgetall(manager.reservation_receipt_key(op))
    assert before == after and after["state"] == RESERVATION_STATE_FINALIZED
    assert await real_redis.get(RedisKey.run_state(run_id)) == "running"


async def test_redelivery_after_resource_drift_reuses_run_revision_and_settles_once(real_redis):
    run_id, tenant = "r84-8-drift-retry", "t84-8-drift-retry"
    manager, store, binding, op = await _seed_resource_run(
        real_redis, run_id=run_id, tenant_id=tenant, tasks={"task-a": "done"},
    )
    await real_redis.delete(manager.concurrent_run_key(tenant))
    with pytest.raises(RunTerminateAuthorityError):
        await binding.terminate(run_id=run_id, tenant_id=tenant, trigger_task_id="task-a", finalized_at_ms=1400, worker_instance_id="worker-a", trigger_terminal_state="done")
    before = await store.get_aggregate_snapshot(run_terminate_identity(run_id))
    await real_redis.set(manager.concurrent_run_key(tenant), "1")
    await real_redis.persist(manager.concurrent_run_key(tenant))
    result = await binding.terminate(run_id=run_id, tenant_id=tenant, trigger_task_id="task-a", finalized_at_ms=9999, worker_instance_id="worker-b", trigger_terminal_state="done")
    after = await store.get_aggregate_snapshot(run_terminate_identity(run_id))
    assert result.ack_allowed is True
    assert after.revision == before.revision == 2 and after.operation_id == before.operation_id
    assert (await real_redis.hgetall(manager.reservation_receipt_key(op)))["state"] == RESERVATION_STATE_SETTLED
    assert await _counters(manager, tenant) == {"concurrent_runs": 0, "budget_reserved_cents": 0, "tenant_inflight": 0}


async def test_settled_then_run_projection_failure_replays_without_second_decrement(real_redis, monkeypatch):
    run_id, tenant = "r84-8-proj-retry", "t84-8-proj-retry"
    manager, store, binding, op = await _seed_resource_run(
        real_redis, run_id=run_id, tenant_id=tenant, tasks={"task-a": "done"},
    )
    original = binding.projection_manager.project
    calls = 0
    async def fail_once(value):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RuntimeError("projection down")
        return await original(value)
    monkeypatch.setattr(binding.projection_manager, "project", fail_once)
    with pytest.raises(RunTerminateProjectionPendingError):
        await binding.terminate(run_id=run_id, tenant_id=tenant, trigger_task_id="task-a", finalized_at_ms=1500, worker_instance_id="worker-a", trigger_terminal_state="done")
    receipt = await real_redis.hgetall(manager.reservation_receipt_key(op))
    assert receipt["state"] == RESERVATION_STATE_SETTLED
    counters = await _counters(manager, tenant)
    snap = await store.get_aggregate_snapshot(run_terminate_identity(run_id))
    assert snap.revision == 2 and await real_redis.get(RedisKey.run_state(run_id)) == "running"
    result = await binding.terminate(run_id=run_id, tenant_id=tenant, trigger_task_id="task-a", finalized_at_ms=9999, worker_instance_id="worker-b", trigger_terminal_state="done")
    after = await store.get_aggregate_snapshot(run_terminate_identity(run_id))
    assert result.ack_allowed is True and after.revision == snap.revision == 2
    assert await _counters(manager, tenant) == counters


async def test_exact_settlement_retry_returns_already_settled_and_never_decrements_again(real_redis):
    run_id, tenant = "r84-8-manager-retry", "t84-8-manager-retry"
    manager, store, binding, op = await _seed_resource_run(
        real_redis, run_id=run_id, tenant_id=tenant, tasks={"task-a": "done"},
    )
    result = await binding.terminate(run_id=run_id, tenant_id=tenant, trigger_task_id="task-a", finalized_at_ms=1600, worker_instance_id="worker-a", trigger_terminal_state="done")
    receipt = await real_redis.hgetall(manager.reservation_receipt_key(op))
    settlement = AdmissionResourceSettlementInput(
        run_create_operation_id=op,
        run_id=run_id,
        tenant_id=tenant,
        estimated_cost_cents=125,
        run_create_reservation_proof_sha256=receipt["proof_sha256"],
        run_terminate_operation_id=run_terminate_operation_id(run_id, result.terminal_proof_sha256),
        terminal_proof_sha256=result.terminal_proof_sha256,
        canonical_transition_id=result.canonical_transition_id,
        canonical_record_hash=result.canonical_record_hash,
        canonical_command_hash=result.canonical_command_hash,
        canonical_revision=result.canonical_revision,
        final_state=result.final_state,
    )
    before = await _counters(manager, tenant)
    replay = await manager.settle_once(settlement, now_ms=9999)
    assert replay.status == RESERVATION_STATUS_ALREADY_SETTLED and replay.resource_mutated is False
    assert await _counters(manager, tenant) == before


async def test_changed_settlement_proof_conflicts_without_counter_mutation(real_redis):
    run_id, tenant = "r84-8-proof-conflict", "t84-8-proof-conflict"
    manager, store, binding, op = await _seed_resource_run(
        real_redis, run_id=run_id, tenant_id=tenant, tasks={"task-a": "done"},
    )
    result = await binding.terminate(run_id=run_id, tenant_id=tenant, trigger_task_id="task-a", finalized_at_ms=1700, worker_instance_id="worker-a", trigger_terminal_state="done")
    receipt = await real_redis.hgetall(manager.reservation_receipt_key(op))
    conflict = AdmissionResourceSettlementInput(
        run_create_operation_id=op, run_id=run_id, tenant_id=tenant, estimated_cost_cents=125,
        run_create_reservation_proof_sha256=receipt["proof_sha256"],
        run_terminate_operation_id=run_terminate_operation_id(run_id, result.terminal_proof_sha256),
        terminal_proof_sha256=result.terminal_proof_sha256,
        canonical_transition_id=result.canonical_transition_id,
        canonical_record_hash="f" * 64,
        canonical_command_hash=result.canonical_command_hash,
        canonical_revision=result.canonical_revision, final_state=result.final_state,
    )
    before = await _counters(manager, tenant)
    replay = await manager.settle_once(conflict, now_ms=9999)
    assert replay.status != RESERVATION_STATUS_SETTLED
    assert replay.resource_mutated is False
    assert await _counters(manager, tenant) == before


async def test_budget_ownership_insufficient_blocks_settlement_without_partial_decrement(real_redis):
    run_id, tenant = "r84-8-budget-drift", "t84-8-budget-drift"
    manager, store, binding, op = await _seed_resource_run(
        real_redis, run_id=run_id, tenant_id=tenant, tasks={"task-a": "done"}, cost=125,
    )
    await real_redis.set(manager.budget_reserved_key(tenant), "124")
    await real_redis.persist(manager.budget_reserved_key(tenant))
    before = await _counters(manager, tenant)
    with pytest.raises(RunTerminateAuthorityError) as caught:
        await binding.terminate(
            run_id=run_id, tenant_id=tenant, trigger_task_id="task-a",
            finalized_at_ms=1800, worker_instance_id="worker-a", trigger_terminal_state="done",
        )
    assert caught.value.status == RESOURCE_SETTLEMENT_PENDING_STATUS
    assert await _counters(manager, tenant) == before
    receipt = await real_redis.hgetall(manager.reservation_receipt_key(op))
    assert receipt["state"] == RESERVATION_STATE_FINALIZED
    snap = await store.get_aggregate_snapshot(run_terminate_identity(run_id))
    assert snap.revision == 2 and snap.state == "done"
    assert await real_redis.get(RedisKey.run_state(run_id)) == "running"


async def test_settlement_enabled_without_run_create_resource_receipt_fails_closed(real_redis):
    run_id, tenant = "r84-8-missing-resource", "t84-8-missing-resource"
    store = RedisCanonicalAuthorityStore(real_redis)
    await store.initialise()
    command = build_run_create_command(
        RunCreateAuthorityInput(
            run_id=run_id, tenant_id=tenant, agent_type="research", priority=5,
            payload={"prompt": "bare-run-create"}, estimated_cost_cents=125,
            preferred_region="eu-west-1", preferred_placement="LEAST_LOADED",
            created_at_ms=100, control_stream=RedisKey.stream_control(),
        )
    )
    evaluation = evaluate_authority_commit(
        context=build_run_create_context(command), command=command,
        current_revision=0, current_state=None, receipt_probe=None,
        committed_at_ms=100, correlation_id=None,
    )
    assert evaluation.decision.code is AuthorityDecisionCode.ACCEPTED
    assert evaluation.commit_plan is not None
    persisted = await store.commit(evaluation.commit_plan)
    assert persisted.status is RedisAuthorityCommitStatus.COMMITTED
    await real_redis.set(RedisKey.run_state(run_id), "running")
    await real_redis.hset(RedisKey.run_meta(run_id), mapping={"run_id": run_id, "tenant_id": tenant, "state": "running"})
    await real_redis.zadd(RedisKey.cp_running(), {run_id: 100})
    await real_redis.sadd(DagRedisKey.run_tasks(run_id), "task-a")
    await real_redis.set(DagRedisKey.task_state("task-a"), "done")
    await real_redis.hset(DagRedisKey.task_meta("task-a"), mapping={"task_id": "task-a", "run_id": run_id, "tenant_id": tenant})
    await _migration_ready(real_redis)
    manager = AdmissionResourceReservationManager(real_redis)
    binding = RunTerminateAuthorityBinding(real_redis, store=store, resource_manager=manager)
    await binding.initialise()
    with pytest.raises(RunTerminateAuthorityError) as caught:
        await binding.terminate(
            run_id=run_id, tenant_id=tenant, trigger_task_id="task-a",
            finalized_at_ms=1900, worker_instance_id="worker-a", trigger_terminal_state="done",
        )
    assert caught.value.status == RESOURCE_SETTLEMENT_PENDING_STATUS
    snap = await store.get_aggregate_snapshot(run_terminate_identity(run_id))
    assert snap.revision == 2 and snap.state == "done"
    assert await real_redis.get(RedisKey.run_state(run_id)) == "running"
    assert not await real_redis.exists(manager.reservation_receipt_key(command.operation_id))
