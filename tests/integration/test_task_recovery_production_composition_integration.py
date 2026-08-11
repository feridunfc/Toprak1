from __future__ import annotations

import asyncio
from dataclasses import dataclass

import pytest

from hfa.authority import (
    AggregateType,
    AuthorityCommand,
    AuthorityDecisionCode,
    AuthorityEntryContext,
    CanonicalAggregateIdentity,
    OperationType,
    RedisAuthorityCommitStatus,
    RedisCanonicalAuthorityStore,
    evaluate_authority_commit,
)
from hfa.config.keys import RedisKey
from hfa.dag.heartbeat import HeartbeatPolicy
from hfa.dag.schema import DagRedisKey
from hfa.governance.admission_resource_reservation import (
    AdmissionResourceReservationManager,
    RESERVATION_STATE_FINALIZED,
    RESERVATION_STATE_SETTLED,
)
from hfa_control.models import ControlPlaneConfig
from hfa_control.run_create_authority import (
    FEATURE_FLAG as RUN_CREATE_FEATURE_FLAG,
    RunCreateAuthorityBinding,
    run_create_identity,
    run_create_operation_id,
)
from hfa_control.service import ControlPlaneService
from hfa_control.task_claim_authority import TaskClaimAuthorityBinding
from hfa_control.task_recovery import TaskHeartbeatManager
from hfa_control.task_requeue_authority import (
    FEATURE_FLAG as TASK_REQUEUE_FEATURE_FLAG,
    TaskRequeueAuthorityBinding,
)
from hfa_control.task_terminal_authority import (
    TaskTerminalAuthorityBinding,
    TaskTerminalAuthorityError,
)

pytestmark = [pytest.mark.asyncio, pytest.mark.integration]

_DEPENDENCY_FLAGS = (
    "HFA_CANONICAL_TASK_ADMIT_BINDING",
    "HFA_CANONICAL_TASK_DISPATCH_BINDING",
    "HFA_CANONICAL_TASK_CLAIM_BINDING",
    "HFA_CANONICAL_TASK_TERMINAL_BINDING",
    RUN_CREATE_FEATURE_FLAG,
)


@dataclass
class _RunRequest:
    run_id: str
    tenant_id: str
    agent_type: str = "recovery"
    priority: int = 5
    payload: dict | None = None
    estimated_cost_cents: int = 125
    preferred_region: str = "eu-west-1"
    preferred_placement: str = "LEAST_LOADED"

    def __post_init__(self) -> None:
        if self.payload is None:
            self.payload = {"prompt": "84.9-r3"}


def _text(value) -> str:
    if isinstance(value, bytes):
        return value.decode("utf-8")
    return "" if value is None else str(value)


def _config(suffix: str) -> ControlPlaneConfig:
    return ControlPlaneConfig(
        instance_id=f"s84-9-cp-{suffix}",
        stale_run_timeout=0.0,
        recovery_sweep_interval=3600.0,
    )


def _enable_canonical_recovery(monkeypatch) -> None:
    monkeypatch.setenv(TASK_REQUEUE_FEATURE_FLAG, "true")
    for name in _DEPENDENCY_FLAGS:
        monkeypatch.setenv(name, "true")


def _clear_canonical_recovery(monkeypatch) -> None:
    monkeypatch.delenv(TASK_REQUEUE_FEATURE_FLAG, raising=False)
    for name in _DEPENDENCY_FLAGS:
        monkeypatch.delenv(name, raising=False)


def _context(command: AuthorityCommand) -> AuthorityEntryContext:
    return AuthorityEntryContext(
        authenticated_writer_id=f"test/{command.operation_type.value}",
        allowed_operations=frozenset({command.operation_type}),
        target_aggregate_identity_sha256=command.aggregate_identity.sha256,
        fence_required=False,
        fence_valid=True,
    )


async def _commit(
    store: RedisCanonicalAuthorityStore,
    command: AuthorityCommand,
    *,
    revision: int,
    state: str | None,
    at_ms: int,
):
    evaluation = evaluate_authority_commit(
        context=_context(command),
        command=command,
        current_revision=revision,
        current_state=state,
        receipt_probe=None,
        committed_at_ms=at_ms,
        correlation_id=None,
    )
    assert evaluation.decision.code is AuthorityDecisionCode.ACCEPTED
    assert evaluation.commit_plan is not None
    result = await store.commit(evaluation.commit_plan)
    assert result.status is RedisAuthorityCommitStatus.COMMITTED
    return evaluation.commit_plan.record


async def _migration_ready(redis_client) -> None:
    await redis_client.hset(
        RedisKey.run_terminal_event_index(),
        mapping={
            "__contract__:schema_version": "1",
            "__contract__:producer_contract_version": "1",
            "__migration__:status": "ready",
            "__migration__:results_stream_key": RedisKey.stream_results(),
            "__migration__:source_history_complete": "1",
        },
    )
    await redis_client.persist(RedisKey.run_terminal_event_index())


async def _seed_run_create(
    redis_client,
    *,
    run_id: str,
    tenant_id: str,
    cost: int = 125,
):
    resource_manager = AdmissionResourceReservationManager(redis_client)
    create = RunCreateAuthorityBinding(
        redis=redis_client,
        resource_manager=resource_manager,
        control_stream=RedisKey.stream_control(),
    )
    await create.initialise()
    result = await create.admit(
        _RunRequest(
            run_id=run_id,
            tenant_id=tenant_id,
            estimated_cost_cents=cost,
        ),
        tenant_inflight_limit=10,
        concurrent_run_limit=10,
        budget_limit_cents=10_000,
    )
    assert result.run_id == run_id
    operation_id = run_create_operation_id(result.run_id)
    resource_key = resource_manager.reservation_receipt_key(operation_id)
    receipt = await redis_client.hgetall(resource_key)
    assert _text(receipt["state"]) == RESERVATION_STATE_FINALIZED
    assert await redis_client.ttl(resource_key) == -1
    assert await resource_manager.get_resource_snapshot(tenant_id) == {
        "concurrent_runs": 1,
        "budget_reserved_cents": cost,
        "tenant_inflight": 1,
    }

    await redis_client.set(RedisKey.run_state(run_id), "running")
    await redis_client.hset(
        RedisKey.run_meta(run_id),
        mapping={
            "run_id": run_id,
            "tenant_id": tenant_id,
            "state": "running",
            "agent_type": "recovery",
            "worker_group": "group-a",
            "reschedule_count": "0",
        },
    )
    await redis_client.zadd(RedisKey.cp_running(), {run_id: 0.0})
    await _migration_ready(redis_client)
    return create.store, resource_manager, operation_id


async def _seed_canonical_running_task(
    redis_client,
    suffix: str,
    *,
    dispatch_attempt: int = 1,
    with_run_create: bool = True,
) -> dict:
    task_id = f"s84-9-prod-task-{suffix}"
    run_id = f"s84-9-prod-run-{suffix}"
    tenant_id = f"s84-9-prod-tenant-{suffix}"
    cost = 125

    if with_run_create:
        store, resource_manager, run_create_op = await _seed_run_create(
            redis_client,
            run_id=run_id,
            tenant_id=tenant_id,
            cost=cost,
        )
    else:
        store = RedisCanonicalAuthorityStore(redis_client)
        await store.initialise()
        resource_manager = AdmissionResourceReservationManager(redis_client)
        run_create_op = run_create_operation_id(run_id)
        await redis_client.set(RedisKey.run_state(run_id), "running")
        await redis_client.hset(
            RedisKey.run_meta(run_id),
            mapping={
                "run_id": run_id,
                "tenant_id": tenant_id,
                "state": "running",
                "agent_type": "recovery",
                "worker_group": "group-a",
                "reschedule_count": "0",
            },
        )
        await redis_client.zadd(RedisKey.cp_running(), {run_id: 0.0})
        await _migration_ready(redis_client)

    identity = CanonicalAggregateIdentity(AggregateType.TASK, run_id, task_id)
    admit = AuthorityCommand(
        aggregate_identity=identity,
        operation_type=OperationType.TASK_ADMIT,
        operation_id=f"task-admit:v1:{identity.sha256}",
        expected_revision=0,
        intended_previous_state=None,
        intended_next_state="ready",
        authoritative_payload={"task_id": task_id, "run_id": run_id},
        authoritative_metadata_changes={
            "task_id": task_id,
            "run_id": run_id,
            "tenant_id": tenant_id,
        },
        requested_child_effects={},
        requested_projection_intents=(
            {
                "kind": "READY_QUEUE_IF_READY",
                "task_id": task_id,
                "tenant_id": tenant_id,
            },
        ),
    )
    admit_record = await _commit(store, admit, revision=0, state=None, at_ms=100)
    revision = admit_record.to_revision
    state = "ready"
    previous_transition_id = admit_record.transition_id

    await redis_client.sadd(DagRedisKey.run_tasks(run_id), task_id)
    await redis_client.sadd(DagRedisKey.tenant_active_set(), tenant_id)

    for attempt in range(1, dispatch_attempt + 1):
        worker_id = f"s84-9-prod-worker-{suffix}-{attempt}"
        scheduler_epoch = str(6 + attempt)
        dispatch_metadata = {
            "task_id": task_id,
            "run_id": run_id,
            "tenant_id": tenant_id,
            "worker_id": worker_id,
            "scheduler_epoch": scheduler_epoch,
            "dispatch_attempt": attempt,
        }
        dispatch = AuthorityCommand(
            aggregate_identity=identity,
            operation_type=OperationType.TASK_DISPATCH,
            operation_id=(
                f"task-dispatch:v1:{identity.sha256}:attempt:{attempt}"
            ),
            expected_revision=revision,
            intended_previous_state=state,
            intended_next_state="scheduled",
            authoritative_payload=dispatch_metadata,
            authoritative_metadata_changes=dispatch_metadata,
            requested_child_effects={
                "worker_reservation": {
                    "worker_id": worker_id,
                    "task_id": task_id,
                    "scheduler_epoch": scheduler_epoch,
                }
            },
            requested_projection_intents=(
                {"kind": "CONTROL_NOTIFICATION", "stream": "control"},
                {"kind": "TASK_REQUEST_MESSAGE", "stream": "shard"},
            ),
            causation_id=previous_transition_id,
        )
        dispatch_record = await _commit(
            store,
            dispatch,
            revision=revision,
            state=state,
            at_ms=attempt * 1000 + 100,
        )
        revision = dispatch_record.to_revision

        claim_metadata = {
            "task_id": task_id,
            "run_id": run_id,
            "tenant_id": tenant_id,
            "worker_instance_id": worker_id,
            "scheduler_epoch": scheduler_epoch,
            "claimed_at_ms": attempt * 1000 + 200,
            "dispatch_attempt": attempt,
            "previous_claim_epoch": attempt - 1,
            "claim_epoch": attempt,
            "dispatch_transition_id": dispatch_record.transition_id,
            "dispatch_record_hash": dispatch_record.canonical_record_hash,
            "dispatch_command_hash": dispatch_record.canonical_command_hash,
            "dispatch_operation_id": dispatch_record.operation_id,
            "dispatch_revision": dispatch_record.to_revision,
        }
        claim = AuthorityCommand(
            aggregate_identity=identity,
            operation_type=OperationType.TASK_CLAIM,
            operation_id=(
                f"task-claim:v1:{identity.sha256}:attempt:{attempt}"
            ),
            expected_revision=revision,
            intended_previous_state="scheduled",
            intended_next_state="running",
            authoritative_payload=claim_metadata,
            authoritative_metadata_changes=claim_metadata,
            requested_child_effects={
                "worker_reservation": {
                    "action": "CONSUME",
                    "task_id": task_id,
                    "worker_id": worker_id,
                    "scheduler_epoch": scheduler_epoch,
                }
            },
            requested_projection_intents=(
                {
                    "kind": "RUNNING_SET",
                    "tenant_id": tenant_id,
                    "task_id": task_id,
                },
            ),
            causation_id=dispatch_record.transition_id,
        )
        claim_record = await _commit(
            store,
            claim,
            revision=revision,
            state="scheduled",
            at_ms=attempt * 1000 + 200,
        )
        revision = claim_record.to_revision
        state = "running"
        previous_transition_id = claim_record.transition_id

        await redis_client.set(DagRedisKey.task_state(task_id), "running")
        await redis_client.hset(
            DagRedisKey.task_meta(task_id),
            mapping={
                "task_id": task_id,
                "run_id": run_id,
                "tenant_id": tenant_id,
                "worker_instance_id": worker_id,
                "scheduler_epoch": scheduler_epoch,
                "claim_epoch": str(attempt),
                "requeue_count": str(attempt - 1),
                "heartbeat_at_ms": "1",
                "last_heartbeat_at_ms": "1",
                "canonical_transition_id": claim_record.transition_id,
                "canonical_record_hash": claim_record.canonical_record_hash,
                "canonical_command_hash": claim_record.canonical_command_hash,
                "canonical_revision": str(claim_record.to_revision),
                "canonical_operation_id": claim_record.operation_id,
                "claim_canonical_transition_id": claim_record.transition_id,
                "claim_canonical_record_hash": claim_record.canonical_record_hash,
                "claim_canonical_command_hash": claim_record.canonical_command_hash,
                "claim_canonical_revision": str(claim_record.to_revision),
                "claim_canonical_operation_id": claim_record.operation_id,
                "dispatch_attempt": str(attempt),
            },
        )
        await redis_client.zadd(
            DagRedisKey.task_running_zset(tenant_id),
            {task_id: 1.0},
        )

        if attempt < dispatch_attempt:
            requeue = TaskRequeueAuthorityBinding(redis_client, store=store)
            result = await requeue.requeue(
                task_id=task_id,
                run_id=run_id,
                tenant_id=tenant_id,
                observed_at_ms=attempt * 1000 + 300,
                max_requeue_count=3,
                reason_code="TASK_STALE_DETECTED",
                worker_instance_id=worker_id,
                scheduler_epoch=scheduler_epoch,
                claim_epoch=attempt,
            )
            assert result.canonical_revision == revision + 1
            revision = result.canonical_revision
            state = "ready"
            previous_transition_id = result.canonical_transition_id

    return {
        "store": store,
        "resource_manager": resource_manager,
        "run_create_operation_id": run_create_op,
        "cost": cost,
        "identity": identity,
        "task_id": task_id,
        "run_id": run_id,
        "tenant_id": tenant_id,
        "worker_id": worker_id,
        "scheduler_epoch": scheduler_epoch,
        "claim_epoch": dispatch_attempt,
        "dispatch_attempt": dispatch_attempt,
        "claim_revision": revision,
    }


async def _resource_state(redis_client, seeded: dict) -> str:
    key = seeded["resource_manager"].reservation_receipt_key(
        seeded["run_create_operation_id"]
    )
    return _text((await redis_client.hgetall(key))["state"])


async def _task_snapshot(seeded: dict):
    return await seeded["store"].get_aggregate_snapshot(seeded["identity"])


async def _run_snapshot(seeded: dict):
    return await seeded["store"].get_aggregate_snapshot(
        run_create_identity(seeded["run_id"])
    )


async def _event_types(redis_client, key: str) -> list[str]:
    return [
        _text(fields.get("event_type") or fields.get(b"event_type"))
        for _entry_id, fields in await redis_client.xrange(key)
    ]


async def test_flag_false_control_plane_preserves_historical_run_recovery(
    redis_client, monkeypatch
):
    _clear_canonical_recovery(monkeypatch)
    service = ControlPlaneService(redis_client, _config("legacy"))
    assert service.recovery._task_recovery is None

    run_id = "s84-9-legacy-run"
    task_id = "s84-9-legacy-task"
    tenant_id = "s84-9-legacy-tenant"
    await redis_client.set(RedisKey.run_state(run_id), "running")
    await redis_client.hset(
        RedisKey.run_meta(run_id),
        mapping={
            "run_id": run_id,
            "tenant_id": tenant_id,
            "agent_type": "test",
            "worker_group": "group-a",
            "reschedule_count": "0",
        },
    )
    await redis_client.sadd(DagRedisKey.run_tasks(run_id), task_id)
    await redis_client.set(DagRedisKey.task_state(task_id), "running")
    await redis_client.hset(
        DagRedisKey.task_meta(task_id),
        mapping={"task_id": task_id, "run_id": run_id, "tenant_id": tenant_id},
    )
    await redis_client.zadd(RedisKey.cp_running(), {run_id: 0.0})

    await service.recovery._sweep()
    assert _text(await redis_client.get(RedisKey.run_state(run_id))) == "rescheduled"


async def test_flag_true_control_plane_rejects_incomplete_canonical_retry_chain(
    redis_client, monkeypatch
):
    monkeypatch.setenv(TASK_REQUEUE_FEATURE_FLAG, "true")
    monkeypatch.setenv("HFA_CANONICAL_TASK_ADMIT_BINDING", "true")
    monkeypatch.setenv("HFA_CANONICAL_TASK_DISPATCH_BINDING", "true")
    monkeypatch.setenv("HFA_CANONICAL_TASK_CLAIM_BINDING", "true")
    monkeypatch.setenv("HFA_CANONICAL_TASK_TERMINAL_BINDING", "false")
    monkeypatch.setenv(RUN_CREATE_FEATURE_FLAG, "true")

    with pytest.raises(ValueError, match="TASK_TERMINAL"):
        ControlPlaneService(redis_client, _config("incomplete"))


async def test_flag_true_requires_canonical_run_create_dependency(
    redis_client, monkeypatch
):
    _enable_canonical_recovery(monkeypatch)
    monkeypatch.setenv(RUN_CREATE_FEATURE_FLAG, "false")
    with pytest.raises(ValueError, match="RUN_CREATE"):
        ControlPlaneService(redis_client, _config("missing-run-create"))


async def test_flag_true_requeues_then_existing_claim_path_and_suppresses_run_retry(
    redis_client, monkeypatch
):
    _enable_canonical_recovery(monkeypatch)
    seeded = await _seed_canonical_running_task(redis_client, "vertical")
    service = ControlPlaneService(redis_client, _config("vertical"))
    assert service.recovery._task_recovery is not None
    assert service.recovery._run_terminate_authority is not None

    await service.recovery._sweep()
    snapshot = await _task_snapshot(seeded)
    assert snapshot.revision == 4 and snapshot.state == "ready"
    assert _text(await redis_client.get(DagRedisKey.task_state(seeded["task_id"]))) == "ready"
    assert _text(
        await redis_client.hget(DagRedisKey.task_meta(seeded["task_id"]), "requeue_count")
    ) == "1"
    assert _text(await redis_client.get(RedisKey.run_state(seeded["run_id"]))) == "running"
    assert _text(
        await redis_client.hget(RedisKey.run_meta(seeded["run_id"]), "reschedule_count")
    ) == "0"

    # Next sweep reaches stale-RUN Case 1: canonical ready TASK suppresses the
    # legacy mutable RUN reschedule path without another TASK revision.
    await service.recovery._sweep()
    snapshot_again = await _task_snapshot(seeded)
    assert snapshot_again.revision == snapshot.revision == 4
    assert _text(await redis_client.get(RedisKey.run_state(seeded["run_id"]))) == "running"

    worker_id = "s84-9-prod-worker-vertical-b"
    scheduler_epoch = "9"
    dispatch_metadata = {
        "task_id": seeded["task_id"],
        "run_id": seeded["run_id"],
        "tenant_id": seeded["tenant_id"],
        "worker_id": worker_id,
        "scheduler_epoch": scheduler_epoch,
        "dispatch_attempt": 2,
    }
    dispatch = AuthorityCommand(
        aggregate_identity=seeded["identity"],
        operation_type=OperationType.TASK_DISPATCH,
        operation_id=f"task-dispatch:v1:{seeded['identity'].sha256}:attempt:2",
        expected_revision=4,
        intended_previous_state="ready",
        intended_next_state="scheduled",
        authoritative_payload=dispatch_metadata,
        authoritative_metadata_changes=dispatch_metadata,
        requested_child_effects={
            "worker_reservation": {
                "worker_id": worker_id,
                "task_id": seeded["task_id"],
                "scheduler_epoch": scheduler_epoch,
            }
        },
        requested_projection_intents=(
            {"kind": "CONTROL_NOTIFICATION", "stream": "control"},
            {"kind": "TASK_REQUEST_MESSAGE", "stream": "shard"},
        ),
        causation_id=snapshot.transition_id,
    )
    dispatch_record = await _commit(
        seeded["store"], dispatch, revision=4, state="ready", at_ms=6000
    )
    await redis_client.set(DagRedisKey.task_state(seeded["task_id"]), "scheduled")
    await redis_client.hset(
        DagRedisKey.task_meta(seeded["task_id"]),
        mapping={
            "scheduler_epoch": scheduler_epoch,
            "dispatch_worker_id": worker_id,
            "dispatch_attempt": "2",
            "canonical_transition_id": dispatch_record.transition_id,
            "canonical_record_hash": dispatch_record.canonical_record_hash,
            "canonical_command_hash": dispatch_record.canonical_command_hash,
            "canonical_revision": str(dispatch_record.to_revision),
            "canonical_operation_id": dispatch_record.operation_id,
        },
    )
    await redis_client.hset(
        DagRedisKey.worker_reservation(worker_id),
        mapping={
            "task_id": seeded["task_id"],
            "worker_id": worker_id,
            "scheduler_epoch": scheduler_epoch,
        },
    )
    await redis_client.hset(
        DagRedisKey.task_reservation_owner(seeded["task_id"]),
        mapping={
            "task_id": seeded["task_id"],
            "worker_id": worker_id,
            "scheduler_epoch": scheduler_epoch,
        },
    )

    claim = TaskClaimAuthorityBinding(redis_client)
    prepared = await claim.prepare_claim(
        task_id=seeded["task_id"],
        run_id=seeded["run_id"],
        tenant_id=seeded["tenant_id"],
        worker_instance_id=worker_id,
        scheduler_epoch=scheduler_epoch,
        claimed_at_ms=7000,
    )
    assert prepared.projection.canonical_revision == 6
    assert prepared.projection.claim_epoch == 2
    assert prepared.projection.dispatch_attempt == 2


async def test_production_requeue_durable_projection_failure_replays_same_revision(
    redis_client, monkeypatch
):
    _enable_canonical_recovery(monkeypatch)
    seeded = await _seed_canonical_running_task(redis_client, "requeue-replay")
    service = ControlPlaneService(redis_client, _config("requeue-replay"))
    authority = service.recovery._task_recovery._requeue_authority
    original_project = authority.projection_manager.project
    calls = 0

    async def fail_once(value):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RuntimeError("TASK_REQUEUE projection unavailable")
        return await original_project(value)

    monkeypatch.setattr(authority.projection_manager, "project", fail_once)
    await service.recovery._sweep()
    first = await _task_snapshot(seeded)
    assert first.state == "ready" and first.revision == 4
    assert _text(await redis_client.get(DagRedisKey.task_state(seeded["task_id"]))) == "running"
    assert await redis_client.xlen(
        DagRedisKey.completion_stream(seeded["tenant_id"])
    ) == 0

    await service.recovery._sweep()
    second = await _task_snapshot(seeded)
    assert second.revision == first.revision and second.operation_id == first.operation_id
    assert _text(await redis_client.get(DagRedisKey.task_state(seeded["task_id"]))) == "ready"
    assert await redis_client.xlen(
        DagRedisKey.completion_stream(seeded["tenant_id"])
    ) == 1
    assert _text(await redis_client.get(RedisKey.run_state(seeded["run_id"]))) == "running"


async def test_production_requeue_delivery_failure_is_recovered_from_stale_run(
    redis_client, monkeypatch
):
    _enable_canonical_recovery(monkeypatch)
    seeded = await _seed_canonical_running_task(redis_client, "delivery-replay")
    service = ControlPlaneService(redis_client, _config("delivery-replay"))
    authority = service.recovery._task_recovery._requeue_authority
    original_deliver = authority.projection_manager.deliver
    calls = 0

    async def fail_once(value):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RuntimeError("TASK_REQUEUE delivery unavailable")
        return await original_deliver(value)

    monkeypatch.setattr(authority.projection_manager, "deliver", fail_once)
    await service.recovery._sweep()
    first = await _task_snapshot(seeded)
    assert first.state == "ready" and first.revision == 4
    assert _text(await redis_client.get(DagRedisKey.task_state(seeded["task_id"]))) == "ready"
    assert await redis_client.zscore(
        DagRedisKey.task_running_zset(seeded["tenant_id"]), seeded["task_id"]
    ) is None
    assert await redis_client.xlen(
        DagRedisKey.completion_stream(seeded["tenant_id"])
    ) == 0

    # No stale TASK remains. Stale RUN Case 1 must replay the durable ready
    # TASK_REQUEUE and finish only the missing delivery effect.
    await service.recovery._sweep()
    second = await _task_snapshot(seeded)
    assert second.revision == first.revision and second.operation_id == first.operation_id
    assert await redis_client.xlen(
        DagRedisKey.completion_stream(seeded["tenant_id"])
    ) == 1
    assert _text(await redis_client.get(RedisKey.run_state(seeded["run_id"]))) == "running"


async def test_missing_per_run_run_create_resource_proof_fails_closed(
    redis_client, monkeypatch
):
    _enable_canonical_recovery(monkeypatch)
    seeded = await _seed_canonical_running_task(
        redis_client,
        "missing-proof",
        with_run_create=False,
    )
    service = ControlPlaneService(redis_client, _config("missing-proof"))
    before = await _task_snapshot(seeded)
    await service.recovery._sweep()
    after = await _task_snapshot(seeded)
    assert after.revision == before.revision and after.state == "running"
    assert _text(await redis_client.get(RedisKey.run_state(seeded["run_id"]))) == "running"
    assert _text(
        await redis_client.hget(RedisKey.run_meta(seeded["run_id"]), "reschedule_count")
    ) == "0"


async def test_corrupt_dag_task_authority_fails_closed_without_legacy_run_retry(
    redis_client, monkeypatch
):
    _enable_canonical_recovery(monkeypatch)
    seeded = await _seed_canonical_running_task(redis_client, "corrupt-index")
    service = ControlPlaneService(redis_client, _config("corrupt-index"))
    # Remove the genuine running candidate so stale-RUN Case 3 owns discovery,
    # then add an indexed TASK that has no canonical aggregate.
    await redis_client.zrem(
        DagRedisKey.task_running_zset(seeded["tenant_id"]), seeded["task_id"]
    )
    ghost = "s84-9-ghost-task"
    await redis_client.sadd(DagRedisKey.run_tasks(seeded["run_id"]), ghost)
    await redis_client.set(DagRedisKey.task_state(ghost), "running")
    await redis_client.hset(
        DagRedisKey.task_meta(ghost),
        mapping={
            "task_id": ghost,
            "run_id": seeded["run_id"],
            "tenant_id": seeded["tenant_id"],
        },
    )

    await service.recovery._sweep()
    assert _text(await redis_client.get(RedisKey.run_state(seeded["run_id"]))) == "running"
    assert _text(
        await redis_client.hget(RedisKey.run_meta(seeded["run_id"]), "reschedule_count")
    ) == "0"


async def test_K1_task_fail_commit_failure_leaves_task_run_and_resources_unchanged(
    redis_client, monkeypatch
):
    _enable_canonical_recovery(monkeypatch)
    seeded = await _seed_canonical_running_task(
        redis_client, "k1", dispatch_attempt=4
    )
    service = ControlPlaneService(redis_client, _config("k1"))
    terminal = service.recovery._task_recovery._terminal_authority
    original_commit = terminal.store.commit

    async def fail_commit(_plan):
        raise RuntimeError("K1 TASK_FAIL commit unavailable")

    monkeypatch.setattr(terminal.store, "commit", fail_commit)
    await service.recovery._sweep()
    monkeypatch.setattr(terminal.store, "commit", original_commit)

    task = await _task_snapshot(seeded)
    run = await _run_snapshot(seeded)
    assert task.state == "running" and task.revision == seeded["claim_revision"]
    assert run.state == "pending" and run.revision == 1
    assert await _resource_state(redis_client, seeded) == RESERVATION_STATE_FINALIZED
    assert await seeded["resource_manager"].get_resource_snapshot(seeded["tenant_id"]) == {
        "concurrent_runs": 1,
        "budget_reserved_cents": seeded["cost"],
        "tenant_inflight": 1,
    }
    assert _text(await redis_client.get(RedisKey.run_state(seeded["run_id"]))) == "running"


async def test_K2_task_fail_durable_projection_failure_replays_without_second_revision(
    redis_client, monkeypatch
):
    _enable_canonical_recovery(monkeypatch)
    seeded = await _seed_canonical_running_task(
        redis_client, "k2", dispatch_attempt=4
    )
    service = ControlPlaneService(redis_client, _config("k2"))
    terminal = service.recovery._task_recovery._terminal_authority
    original_project = terminal.projection_manager.project
    calls = 0

    async def fail_once(value):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RuntimeError("K2 TASK terminal projection unavailable")
        return await original_project(value)

    monkeypatch.setattr(terminal.projection_manager, "project", fail_once)
    await service.recovery._sweep()
    first_task = await _task_snapshot(seeded)
    first_run = await _run_snapshot(seeded)
    assert first_task.state == "failed"
    assert first_task.revision == seeded["claim_revision"] + 1
    assert first_run.state == "pending" and first_run.revision == 1
    assert _text(await redis_client.get(DagRedisKey.task_state(seeded["task_id"]))) == "running"
    assert await _resource_state(redis_client, seeded) == RESERVATION_STATE_FINALIZED

    await service.recovery._sweep()
    second_task = await _task_snapshot(seeded)
    assert second_task.revision == first_task.revision
    assert second_task.operation_id == first_task.operation_id
    assert _text(await redis_client.get(DagRedisKey.task_state(seeded["task_id"]))) == "failed"
    # Once replay proves TASK projection, canonical RUN closure may proceed.
    second_run = await _run_snapshot(seeded)
    assert second_run.state == "failed" and second_run.revision == 2


async def test_K3_task_terminal_complete_run_commit_failure_retries_from_stale_run(
    redis_client, monkeypatch
):
    _enable_canonical_recovery(monkeypatch)
    seeded = await _seed_canonical_running_task(
        redis_client, "k3", dispatch_attempt=4
    )
    service = ControlPlaneService(redis_client, _config("k3"))
    binding = service.recovery._run_terminate_authority
    original_commit = binding.store.commit
    calls = 0

    async def fail_once(plan):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RuntimeError("K3 RUN_TERMINATE commit unavailable")
        return await original_commit(plan)

    monkeypatch.setattr(binding.store, "commit", fail_once)
    await service.recovery._sweep()
    first_task = await _task_snapshot(seeded)
    first_run = await _run_snapshot(seeded)
    assert first_task.state == "failed"
    assert first_run.state == "pending" and first_run.revision == 1
    assert await _resource_state(redis_client, seeded) == RESERVATION_STATE_FINALIZED
    assert await redis_client.zscore(
        DagRedisKey.task_running_zset(seeded["tenant_id"]), seeded["task_id"]
    ) is None

    # TASK is no longer discoverable from running TASKs. The stale RUN path
    # must find the terminal canonical TASK evidence and retry RUN_TERMINATE.
    await service.recovery._sweep()
    second_task = await _task_snapshot(seeded)
    second_run = await _run_snapshot(seeded)
    assert second_task.revision == first_task.revision
    assert second_run.state == "failed" and second_run.revision == 2
    assert await _resource_state(redis_client, seeded) == RESERVATION_STATE_SETTLED


async def test_K4_run_terminate_durable_settlement_failure_reuses_operation_and_settles_once(
    redis_client, monkeypatch
):
    _enable_canonical_recovery(monkeypatch)
    seeded = await _seed_canonical_running_task(
        redis_client, "k4", dispatch_attempt=4
    )
    service = ControlPlaneService(redis_client, _config("k4"))
    manager = service.recovery._resource_manager
    original_settle = manager.settle_once
    calls = 0

    async def fail_once(settlement, *, now_ms=None):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RuntimeError("K4 settlement unavailable")
        return await original_settle(settlement, now_ms=now_ms)

    monkeypatch.setattr(manager, "settle_once", fail_once)
    await service.recovery._sweep()
    first_run = await _run_snapshot(seeded)
    assert first_run.state == "failed" and first_run.revision == 2
    first_operation = first_run.operation_id
    assert await _resource_state(redis_client, seeded) == RESERVATION_STATE_FINALIZED
    assert _text(await redis_client.get(RedisKey.run_state(seeded["run_id"]))) == "running"

    await service.recovery._sweep()
    second_run = await _run_snapshot(seeded)
    assert second_run.revision == first_run.revision == 2
    assert second_run.operation_id == first_operation
    assert await _resource_state(redis_client, seeded) == RESERVATION_STATE_SETTLED
    assert await manager.get_resource_snapshot(seeded["tenant_id"]) == {
        "concurrent_runs": 0,
        "budget_reserved_cents": 0,
        "tenant_inflight": 0,
    }


async def test_K5_settlement_success_run_projection_failure_replays_without_second_settlement(
    redis_client, monkeypatch
):
    _enable_canonical_recovery(monkeypatch)
    seeded = await _seed_canonical_running_task(
        redis_client, "k5", dispatch_attempt=4
    )
    service = ControlPlaneService(redis_client, _config("k5"))
    binding = service.recovery._run_terminate_authority
    manager = service.recovery._resource_manager
    original_project = binding.projection_manager.project
    calls = 0

    async def fail_once(value):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RuntimeError("K5 RUN projection unavailable")
        return await original_project(value)

    monkeypatch.setattr(binding.projection_manager, "project", fail_once)
    await service.recovery._sweep()
    first_run = await _run_snapshot(seeded)
    assert first_run.state == "failed" and first_run.revision == 2
    first_operation = first_run.operation_id
    assert await _resource_state(redis_client, seeded) == RESERVATION_STATE_SETTLED
    first_counters = await manager.get_resource_snapshot(seeded["tenant_id"])
    assert first_counters == {
        "concurrent_runs": 0,
        "budget_reserved_cents": 0,
        "tenant_inflight": 0,
    }
    assert _text(await redis_client.get(RedisKey.run_state(seeded["run_id"]))) == "running"

    await service.recovery._sweep()
    second_run = await _run_snapshot(seeded)
    assert second_run.revision == 2 and second_run.operation_id == first_operation
    assert await manager.get_resource_snapshot(seeded["tenant_id"]) == first_counters
    assert _text(await redis_client.get(RedisKey.run_state(seeded["run_id"]))) == "failed"


async def test_K6_full_exhaustion_closes_task_run_resources_once_without_legacy_retry(
    redis_client, monkeypatch
):
    _enable_canonical_recovery(monkeypatch)
    seeded = await _seed_canonical_running_task(
        redis_client, "k6", dispatch_attempt=4
    )
    service = ControlPlaneService(redis_client, _config("k6"))
    before_types = await _event_types(
        redis_client, DagRedisKey.completion_stream(seeded["tenant_id"])
    )
    assert before_types.count("TaskRequeued") == 3

    await service.recovery._sweep()
    task = await _task_snapshot(seeded)
    run = await _run_snapshot(seeded)
    manager = service.recovery._resource_manager
    receipt_key = manager.reservation_receipt_key(seeded["run_create_operation_id"])
    receipt = await redis_client.hgetall(receipt_key)

    assert task.state == "failed"
    assert task.revision == seeded["claim_revision"] + 1 == 13
    assert run.state == "failed" and run.revision == 2
    assert _text(receipt["state"]) == RESERVATION_STATE_SETTLED
    assert _text(receipt["run_terminate_operation_id"]) == run.operation_id
    assert await manager.get_resource_snapshot(seeded["tenant_id"]) == {
        "concurrent_runs": 0,
        "budget_reserved_cents": 0,
        "tenant_inflight": 0,
    }
    assert _text(await redis_client.get(RedisKey.run_state(seeded["run_id"]))) == "failed"
    assert _text(
        await redis_client.hget(RedisKey.run_meta(seeded["run_id"]), "reschedule_count")
    ) == "0"
    after_types = await _event_types(
        redis_client, DagRedisKey.completion_stream(seeded["tenant_id"])
    )
    assert after_types.count("TaskRequeued") == 3
    assert await redis_client.xlen(RedisKey.stream_results()) == 1

    operation = run.operation_id
    task_revision = task.revision
    await service.recovery._sweep()
    replay_task = await _task_snapshot(seeded)
    replay_run = await _run_snapshot(seeded)
    assert replay_task.revision == task_revision
    assert replay_run.revision == 2 and replay_run.operation_id == operation
    assert await redis_client.xlen(RedisKey.stream_results()) == 1
    assert await manager.get_resource_snapshot(seeded["tenant_id"]) == {
        "concurrent_runs": 0,
        "budget_reserved_cents": 0,
        "tenant_inflight": 0,
    }


async def test_heartbeat_after_stale_observation_is_too_late_and_old_completion_is_fenced(
    redis_client, monkeypatch
):
    _enable_canonical_recovery(monkeypatch)
    seeded = await _seed_canonical_running_task(redis_client, "heartbeat-race")
    service = ControlPlaneService(redis_client, _config("heartbeat-race"))
    manager = service.recovery._task_recovery
    authority = manager._requeue_authority
    original_commit = authority.store.commit
    commit_entered = asyncio.Event()
    allow_commit = asyncio.Event()

    async def gated_commit(plan):
        if plan.record.operation_type == OperationType.TASK_REQUEUE.value:
            commit_entered.set()
            await allow_commit.wait()
        return await original_commit(plan)

    monkeypatch.setattr(authority.store, "commit", gated_commit)
    recovery = asyncio.create_task(
        service.recovery._sweep_stale_tasks(now_ms=100_000)
    )
    await commit_entered.wait()

    heartbeat = TaskHeartbeatManager(
        redis_client,
        policy=HeartbeatPolicy(stale_after_ms=30_000),
    )
    hb = await heartbeat.record_heartbeat(
        task_id=seeded["task_id"],
        run_id=seeded["run_id"],
        tenant_id=seeded["tenant_id"],
        worker_id=seeded["worker_id"],
        claim_epoch=str(seeded["claim_epoch"]),
        now_ms=100_001,
    )
    assert hb.ok is True

    allow_commit.set()
    await recovery
    task = await _task_snapshot(seeded)
    assert task.state == "ready" and task.revision == 4
    assert _text(await redis_client.get(DagRedisKey.task_state(seeded["task_id"]))) == "ready"

    stale_terminal = TaskTerminalAuthorityBinding(redis_client)
    with pytest.raises(TaskTerminalAuthorityError):
        await stale_terminal.fail(
            task_id=seeded["task_id"],
            run_id=seeded["run_id"],
            tenant_id=seeded["tenant_id"],
            finished_at_ms=100_002,
            worker_instance_id=seeded["worker_id"],
            scheduler_epoch=seeded["scheduler_epoch"],
            claim_epoch=seeded["claim_epoch"],
            reason_code="STALE_WORKER_COMPLETION",
        )
    final = await _task_snapshot(seeded)
    assert final.revision == task.revision and final.state == "ready"


async def test_control_plane_recovery_start_fails_closed_before_loop_on_dependency_initialise_error(
    redis_client, monkeypatch
):
    _enable_canonical_recovery(monkeypatch)
    service = ControlPlaneService(redis_client, _config("startup-fail"))
    manager = service.recovery._task_recovery
    assert manager is not None

    async def fail_initialise():
        raise RuntimeError("forced canonical retry dependency failure")

    monkeypatch.setattr(manager, "initialise", fail_initialise)
    with pytest.raises(RuntimeError, match="forced canonical retry dependency failure"):
        await service.recovery.start()
    assert service.recovery._task is None
