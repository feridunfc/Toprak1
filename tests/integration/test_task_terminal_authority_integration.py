from __future__ import annotations

import asyncio
from dataclasses import replace

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
from hfa.dag.schema import DagRedisKey
from hfa_control.dag_lua import DagLua
from hfa_control.task_terminal_authority import (
    TASK_TERMINAL_DUPLICATE_STATUS,
    TASK_TERMINAL_EVIDENCE_CONFLICT_STATUS,
    TASK_TERMINAL_PROJECTED_STATUS,
    TASK_TERMINAL_PROJECTION_PENDING_STATUS,
    TaskTerminalAuthorityBinding,
    TaskTerminalAuthorityError,
    TaskTerminalCanonicalProjectionInput,
    TaskTerminalProjectionManager,
    task_terminal_operation_id,
    TaskTerminalAuthorityInput,
)

pytestmark = [pytest.mark.asyncio, pytest.mark.integration]


def _text(value) -> str:
    if isinstance(value, bytes):
        return value.decode("utf-8")
    return "" if value is None else str(value)


def _context(command: AuthorityCommand) -> AuthorityEntryContext:
    return AuthorityEntryContext(
        authenticated_writer_id=f"test/{command.operation_type.value}",
        allowed_operations=frozenset({command.operation_type}),
        target_aggregate_identity_sha256=command.aggregate_identity.sha256,
        fence_required=command.operation_type is not OperationType.TASK_ADMIT,
        fence_valid=True,
    )


async def _commit(
    store: RedisCanonicalAuthorityStore,
    command: AuthorityCommand,
    *,
    revision: int,
    state: str | None,
    committed_at_ms: int,
):
    evaluation = evaluate_authority_commit(
        context=_context(command),
        command=command,
        current_revision=revision,
        current_state=state,
        receipt_probe=None,
        committed_at_ms=committed_at_ms,
        correlation_id=None,
    )
    assert evaluation.decision.code is AuthorityDecisionCode.ACCEPTED
    assert evaluation.commit_plan is not None
    result = await store.commit(evaluation.commit_plan)
    assert result.status is RedisAuthorityCommitStatus.COMMITTED
    return evaluation.commit_plan.record


async def _seed_claimed_task(
    redis_client,
    *,
    task_id: str,
    run_id: str,
    tenant_id: str,
    worker_id: str,
    scheduler_epoch: str,
):
    store = RedisCanonicalAuthorityStore(redis_client)
    await store.initialise()
    identity = CanonicalAggregateIdentity(
        aggregate_type=AggregateType.TASK,
        run_id=run_id,
        task_id=task_id,
    )

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
    await _commit(store, admit, revision=0, state=None, committed_at_ms=100)

    dispatch_metadata = {
        "task_id": task_id,
        "run_id": run_id,
        "tenant_id": tenant_id,
        "worker_id": worker_id,
        "scheduler_epoch": scheduler_epoch,
        "dispatch_attempt": 1,
    }
    dispatch = AuthorityCommand(
        aggregate_identity=identity,
        operation_type=OperationType.TASK_DISPATCH,
        operation_id=f"task-dispatch:v1:{identity.sha256}:attempt:1",
        expected_revision=1,
        intended_previous_state="ready",
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
    )
    dispatch_record = await _commit(
        store,
        dispatch,
        revision=1,
        state="ready",
        committed_at_ms=200,
    )

    claim_metadata = {
        "task_id": task_id,
        "run_id": run_id,
        "tenant_id": tenant_id,
        "worker_instance_id": worker_id,
        "scheduler_epoch": scheduler_epoch,
        "claimed_at_ms": 300,
        "dispatch_attempt": 1,
        "previous_claim_epoch": 0,
        "claim_epoch": 1,
        "dispatch_transition_id": dispatch_record.transition_id,
        "dispatch_record_hash": dispatch_record.canonical_record_hash,
        "dispatch_command_hash": dispatch_record.canonical_command_hash,
        "dispatch_operation_id": dispatch_record.operation_id,
        "dispatch_revision": dispatch_record.to_revision,
    }
    claim = AuthorityCommand(
        aggregate_identity=identity,
        operation_type=OperationType.TASK_CLAIM,
        operation_id=f"task-claim:v1:{identity.sha256}:attempt:1",
        expected_revision=2,
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
        revision=2,
        state="scheduled",
        committed_at_ms=300,
    )

    await redis_client.set(DagRedisKey.task_state(task_id), "running")
    await redis_client.hset(
        DagRedisKey.task_meta(task_id),
        mapping={
            "task_id": task_id,
            "run_id": run_id,
            "tenant_id": tenant_id,
            "worker_instance_id": worker_id,
            "scheduler_epoch": scheduler_epoch,
            "claim_epoch": "1",
            "claim_canonical_transition_id": claim_record.transition_id,
            "claim_canonical_record_hash": claim_record.canonical_record_hash,
            "claim_canonical_command_hash": claim_record.canonical_command_hash,
            "claim_canonical_revision": str(claim_record.to_revision),
            "claim_canonical_operation_id": claim_record.operation_id,
            "canonical_transition_id": claim_record.transition_id,
            "canonical_record_hash": claim_record.canonical_record_hash,
            "canonical_command_hash": claim_record.canonical_command_hash,
            "canonical_revision": str(claim_record.to_revision),
            "canonical_operation_id": claim_record.operation_id,
        },
    )
    await redis_client.set(RedisKey.run_state(run_id), "running")
    await redis_client.zadd(
        DagRedisKey.task_running_zset(tenant_id),
        {task_id: 300.0},
    )
    return store, identity, claim_record


async def _head(store, identity):
    snapshot = await store.get_aggregate_snapshot(identity)
    assert snapshot is not None
    probe = await store.load_receipt_probe(identity, snapshot.operation_id)
    assert probe is not None and probe.canonical_store_record is not None
    return snapshot, probe.canonical_store_record, probe.receipt


def _forged_projection_without_terminal_commit(
    *,
    task_id: str,
    run_id: str,
    tenant_id: str,
    worker_id: str,
    scheduler_epoch: str,
    claim_record,
) -> TaskTerminalCanonicalProjectionInput:
    terminal = TaskTerminalAuthorityInput(
        task_id=task_id,
        run_id=run_id,
        tenant_id=tenant_id,
        terminal_state="done",
        finished_at_ms=350,
        reason_code="completed",
        worker_instance_id=worker_id,
        scheduler_epoch=scheduler_epoch,
        claim_epoch=1,
        output_data='{"forged":true}',
        output_sha256=(
            ""  # replaced below with a normalized hash-compatible projection
        ),
        claim_transition_id=claim_record.transition_id,
        claim_record_hash=claim_record.canonical_record_hash,
        claim_command_hash=claim_record.canonical_command_hash,
        claim_revision=claim_record.to_revision,
        claim_operation_id=claim_record.operation_id,
    )
    # Use the command builder's normalization path to obtain the deterministic
    # operation identity while keeping every terminal authority proof field fake.
    import hashlib
    output = '{"forged":true}'
    output_sha = hashlib.sha256(output.encode("utf-8")).hexdigest()
    terminal = replace(terminal, output_sha256=output_sha)
    return TaskTerminalCanonicalProjectionInput(
        **terminal.__dict__,
        canonical_transition_id="forged-terminal-transition",
        canonical_record_hash="a" * 64,
        canonical_command_hash="b" * 64,
        canonical_revision=claim_record.to_revision + 1,
        canonical_operation_id=task_terminal_operation_id(terminal),
        canonical_operation_type=OperationType.TASK_COMPLETE.value,
    )




@pytest.mark.integration
async def test_projector_rejects_forged_terminal_proof_without_canonical_record(
    redis_client,
) -> None:
    task_id = "c1-r2-forged-no-terminal"
    run_id = "c1-r2-run-forged-no-terminal"
    tenant_id = "c1-r2-tenant"
    worker_id = "c1-r2-worker"
    scheduler_epoch = "c1-r2-epoch"
    child = "c1-r2-child"
    store, identity, claim_record = await _seed_claimed_task(
        redis_client,
        task_id=task_id,
        run_id=run_id,
        tenant_id=tenant_id,
        worker_id=worker_id,
        scheduler_epoch=scheduler_epoch,
    )
    await redis_client.sadd(DagRedisKey.task_children(task_id), child)
    await redis_client.set(DagRedisKey.task_state(child), "pending")
    await redis_client.set(DagRedisKey.task_remaining_deps(child), 1)
    forged = _forged_projection_without_terminal_commit(
        task_id=task_id,
        run_id=run_id,
        tenant_id=tenant_id,
        worker_id=worker_id,
        scheduler_epoch=scheduler_epoch,
        claim_record=claim_record,
    )

    projector = TaskTerminalProjectionManager(redis_client, store=store)
    with pytest.raises(TaskTerminalAuthorityError) as error:
        await projector.project(forged)
    assert error.value.status == TASK_TERMINAL_EVIDENCE_CONFLICT_STATUS

    snapshot = await store.get_aggregate_snapshot(identity)
    assert snapshot is not None
    assert snapshot.revision == claim_record.to_revision
    assert snapshot.state == "running"
    assert _text(await redis_client.get(DagRedisKey.task_state(task_id))) == "running"
    assert await redis_client.get(DagRedisKey.task_output(task_id)) is None
    assert _text(await redis_client.get(DagRedisKey.task_state(child))) == "pending"
    assert _text(await redis_client.get(DagRedisKey.task_remaining_deps(child))) == "1"


@pytest.mark.parametrize(
    ("field", "replacement"),
    [
        ("canonical_transition_id", "tampered-transition"),
        ("canonical_record_hash", "c" * 64),
        ("canonical_command_hash", "d" * 64),
        ("canonical_revision", 5),
        ("canonical_operation_id", "task-terminal:v1:" + "e" * 64 + ":claim:1"),
        ("canonical_operation_type", OperationType.TASK_FAIL.value),
    ],
)
@pytest.mark.integration
async def test_projector_rejects_each_tampered_terminal_authority_dimension(
    redis_client,
    field,
    replacement,
) -> None:
    task_id = f"c1-r2-tamper-{field}"
    run_id = f"c1-r2-run-tamper-{field}"
    tenant_id = "c1-r2-tenant"
    worker_id = "c1-r2-worker"
    scheduler_epoch = "c1-r2-epoch"
    child = f"c1-r2-child-{field}"
    store, identity, _claim = await _seed_claimed_task(
        redis_client,
        task_id=task_id,
        run_id=run_id,
        tenant_id=tenant_id,
        worker_id=worker_id,
        scheduler_epoch=scheduler_epoch,
    )
    await redis_client.sadd(DagRedisKey.task_children(task_id), child)
    await redis_client.set(DagRedisKey.task_state(child), "pending")
    await redis_client.set(DagRedisKey.task_remaining_deps(child), 1)

    binding = TaskTerminalAuthorityBinding(redis_client, store=store)
    prepared = await binding.prepare_terminal(
        task_id=task_id,
        run_id=run_id,
        tenant_id=tenant_id,
        terminal_state="done",
        finished_at_ms=360,
        reason_code="completed",
        worker_instance_id=worker_id,
        output_data='{"valid":true}',
        scheduler_epoch=scheduler_epoch,
        claim_epoch=1,
    )
    snapshot = await store.get_aggregate_snapshot(identity)
    assert snapshot is not None and snapshot.revision == 4 and snapshot.state == "done"
    assert _text(await redis_client.get(DagRedisKey.task_state(task_id))) == "running"

    tampered = replace(prepared.projection, **{field: replacement})
    projector = TaskTerminalProjectionManager(redis_client, store=store)
    with pytest.raises((TaskTerminalAuthorityError, ValueError)):
        await projector.project(tampered)

    after = await store.get_aggregate_snapshot(identity)
    assert after is not None and after.revision == 4 and after.state == "done"
    assert _text(await redis_client.get(DagRedisKey.task_state(task_id))) == "running"
    assert await redis_client.get(DagRedisKey.task_output(task_id)) is None
    assert _text(await redis_client.get(DagRedisKey.task_state(child))) == "pending"
    assert _text(await redis_client.get(DagRedisKey.task_remaining_deps(child))) == "1"


@pytest.mark.integration
async def test_projector_accepts_exact_durable_terminal_authority_proof(
    redis_client,
) -> None:
    task_id = "c1-r2-exact-proof"
    run_id = "c1-r2-run-exact-proof"
    tenant_id = "c1-r2-tenant"
    worker_id = "c1-r2-worker"
    scheduler_epoch = "c1-r2-epoch"
    child = "c1-r2-exact-child"
    store, identity, _claim = await _seed_claimed_task(
        redis_client,
        task_id=task_id,
        run_id=run_id,
        tenant_id=tenant_id,
        worker_id=worker_id,
        scheduler_epoch=scheduler_epoch,
    )
    await redis_client.sadd(DagRedisKey.task_children(task_id), child)
    await redis_client.set(DagRedisKey.task_state(child), "pending")
    await redis_client.set(DagRedisKey.task_remaining_deps(child), 1)

    binding = TaskTerminalAuthorityBinding(redis_client, store=store)
    prepared = await binding.prepare_terminal(
        task_id=task_id,
        run_id=run_id,
        tenant_id=tenant_id,
        terminal_state="done",
        finished_at_ms=370,
        reason_code="completed",
        worker_instance_id=worker_id,
        output_data='{"valid":true}',
        scheduler_epoch=scheduler_epoch,
        claim_epoch=1,
    )

    projector = TaskTerminalProjectionManager(redis_client, store=store)
    first = await projector.project(prepared.projection)
    second = await projector.project(prepared.projection)
    assert first.status == TASK_TERMINAL_PROJECTED_STATUS
    assert first.projected is True
    assert second.status == TASK_TERMINAL_DUPLICATE_STATUS
    assert second.already_projected is True

    snapshot = await store.get_aggregate_snapshot(identity)
    assert snapshot is not None and snapshot.revision == 4 and snapshot.state == "done"
    assert _text(await redis_client.get(DagRedisKey.task_state(task_id))) == "done"
    assert _text(await redis_client.get(DagRedisKey.task_output(task_id))) == '{"valid":true}'
    assert _text(await redis_client.get(DagRedisKey.task_state(child))) == "ready"
    assert await redis_client.zscore(DagRedisKey.tenant_ready_queue(tenant_id), child) is not None

@pytest.mark.integration
async def test_canonical_complete_commits_receipt_then_projects_success_effects(
    redis_client,
) -> None:
    task_id = "c1-complete"
    run_id = "c1-run-complete"
    tenant_id = "c1-tenant"
    worker_id = "c1-worker"
    scheduler_epoch = "c1-epoch"
    child = "c1-child"
    store, identity, _claim = await _seed_claimed_task(
        redis_client,
        task_id=task_id,
        run_id=run_id,
        tenant_id=tenant_id,
        worker_id=worker_id,
        scheduler_epoch=scheduler_epoch,
    )
    await redis_client.sadd(DagRedisKey.task_children(task_id), child)
    await redis_client.set(DagRedisKey.task_state(child), "pending")
    await redis_client.set(DagRedisKey.task_remaining_deps(child), 1)

    binding = TaskTerminalAuthorityBinding(redis_client)
    result = await binding.complete(
        task_id=task_id,
        run_id=run_id,
        tenant_id=tenant_id,
        finished_at_ms=400,
        worker_instance_id=worker_id,
        scheduler_epoch=scheduler_epoch,
        claim_epoch=1,
        output_data='{"z":2,"a":1}',
    )

    assert result.completed is True
    assert result.status == TASK_TERMINAL_PROJECTED_STATUS
    assert result.canonical_revision == 4
    snapshot, record, receipt = await _head(store, identity)
    assert snapshot.revision == 4 and snapshot.state == "done"
    assert record.operation_type == OperationType.TASK_COMPLETE.value
    assert receipt.operation_id == record.operation_id
    assert record.from_revision == 3 and record.to_revision == 4
    assert record.authoritative_metadata_changes["output_data"] == '{"a":1,"z":2}'
    assert _text(await redis_client.get(DagRedisKey.task_state(task_id))) == "done"
    assert _text(await redis_client.get(DagRedisKey.task_output(task_id))) == '{"a":1,"z":2}'
    assert _text(await redis_client.get(DagRedisKey.task_state(child))) == "ready"
    assert await redis_client.zscore(DagRedisKey.tenant_ready_queue(tenant_id), child) is not None
    assert await redis_client.zscore(DagRedisKey.task_running_zset(tenant_id), task_id) is None


@pytest.mark.integration
async def test_canonical_fail_projects_dependency_failure_fanout(redis_client) -> None:
    task_id = "c1-fail"
    run_id = "c1-run-fail"
    tenant_id = "c1-tenant"
    worker_id = "c1-worker"
    scheduler_epoch = "c1-epoch"
    pending_child = "c1-pending-child"
    ready_child = "c1-ready-child"
    terminal_child = "c1-terminal-child"
    store, identity, _claim = await _seed_claimed_task(
        redis_client,
        task_id=task_id,
        run_id=run_id,
        tenant_id=tenant_id,
        worker_id=worker_id,
        scheduler_epoch=scheduler_epoch,
    )
    await redis_client.sadd(
        DagRedisKey.task_children(task_id),
        pending_child,
        ready_child,
        terminal_child,
    )
    await redis_client.set(DagRedisKey.task_state(pending_child), "pending")
    await redis_client.set(DagRedisKey.task_state(ready_child), "ready")
    await redis_client.set(DagRedisKey.task_state(terminal_child), "done")
    await redis_client.zadd(
        DagRedisKey.tenant_ready_queue(tenant_id),
        {ready_child: 1.0},
    )

    binding = TaskTerminalAuthorityBinding(redis_client)
    result = await binding.fail(
        task_id=task_id,
        run_id=run_id,
        tenant_id=tenant_id,
        finished_at_ms=401,
        worker_instance_id=worker_id,
        scheduler_epoch=scheduler_epoch,
        claim_epoch=1,
        reason_code="executor_failed",
    )

    assert result.completed is True
    assert result.blocked_count == 2
    snapshot, record, _receipt = await _head(store, identity)
    assert snapshot.revision == 4 and snapshot.state == "failed"
    assert record.operation_type == OperationType.TASK_FAIL.value
    assert tuple(record.durable_projection_intents) == (
        {"kind": "DEPENDENCY_FAILURE_FANOUT_INTENT"},
    )
    assert _text(await redis_client.get(DagRedisKey.task_state(task_id))) == "failed"
    assert _text(await redis_client.get(DagRedisKey.task_state(pending_child))) == "blocked_by_failure"
    assert _text(await redis_client.get(DagRedisKey.task_state(ready_child))) == "blocked_by_failure"
    assert _text(await redis_client.get(DagRedisKey.task_state(terminal_child))) == "done"
    assert await redis_client.zscore(DagRedisKey.tenant_ready_queue(tenant_id), ready_child) is None


@pytest.mark.integration
async def test_exact_complete_replay_reuses_receipt_and_does_not_repeat_fanout(
    redis_client,
) -> None:
    task_id = "c1-replay"
    run_id = "c1-run-replay"
    tenant_id = "c1-tenant"
    worker_id = "c1-worker"
    scheduler_epoch = "c1-epoch"
    child = "c1-replay-child"
    store, identity, _claim = await _seed_claimed_task(
        redis_client,
        task_id=task_id,
        run_id=run_id,
        tenant_id=tenant_id,
        worker_id=worker_id,
        scheduler_epoch=scheduler_epoch,
    )
    await redis_client.sadd(DagRedisKey.task_children(task_id), child)
    await redis_client.set(DagRedisKey.task_state(child), "pending")
    await redis_client.set(DagRedisKey.task_remaining_deps(child), 1)
    binding = TaskTerminalAuthorityBinding(redis_client)

    first = await binding.complete(
        task_id=task_id, run_id=run_id, tenant_id=tenant_id,
        finished_at_ms=410, worker_instance_id=worker_id,
        scheduler_epoch=scheduler_epoch, claim_epoch=1,
        output_data='{"v":1}',
    )
    first_snapshot, first_record, first_receipt = await _head(store, identity)
    second = await binding.complete(
        task_id=task_id, run_id=run_id, tenant_id=tenant_id,
        finished_at_ms=999999, worker_instance_id=worker_id,
        scheduler_epoch=scheduler_epoch, claim_epoch=1,
        output_data='{"v":1}',
    )
    second_snapshot, second_record, second_receipt = await _head(store, identity)

    assert first.canonical_revision == second.canonical_revision == 4
    assert second.status == TASK_TERMINAL_DUPLICATE_STATUS
    assert second.exact_retry is True
    assert first_snapshot.revision == second_snapshot.revision == 4
    assert first_record.transition_id == second_record.transition_id
    assert first_receipt.transition_id == second_receipt.transition_id
    assert _text(await redis_client.get(DagRedisKey.task_remaining_deps(child))) == "0"
    assert await redis_client.zcard(DagRedisKey.tenant_ready_queue(tenant_id)) == 1


@pytest.mark.integration
async def test_divergent_complete_is_durable_idempotency_conflict(redis_client) -> None:
    task_id = "c1-divergent"
    run_id = "c1-run-divergent"
    tenant_id = "c1-tenant"
    worker_id = "c1-worker"
    scheduler_epoch = "c1-epoch"
    store, identity, _claim = await _seed_claimed_task(
        redis_client,
        task_id=task_id, run_id=run_id, tenant_id=tenant_id,
        worker_id=worker_id, scheduler_epoch=scheduler_epoch,
    )
    binding = TaskTerminalAuthorityBinding(redis_client)
    await binding.complete(
        task_id=task_id, run_id=run_id, tenant_id=tenant_id,
        finished_at_ms=420, worker_instance_id=worker_id,
        scheduler_epoch=scheduler_epoch, claim_epoch=1,
        output_data='{"v":1}',
    )

    with pytest.raises(TaskTerminalAuthorityError) as error:
        await binding.complete(
            task_id=task_id, run_id=run_id, tenant_id=tenant_id,
            finished_at_ms=421, worker_instance_id=worker_id,
            scheduler_epoch=scheduler_epoch, claim_epoch=1,
            output_data='{"v":2}',
        )
    assert error.value.status == RedisAuthorityCommitStatus.IDEMPOTENCY_CONFLICT.value
    snapshot = await store.get_aggregate_snapshot(identity)
    assert snapshot is not None and snapshot.revision == 4 and snapshot.state == "done"
    assert await redis_client.xlen(store.keyspace(identity.sha256).conflicts) >= 1


@pytest.mark.integration
async def test_complete_vs_fail_same_claim_has_one_canonical_terminal_truth(redis_client) -> None:
    task_id = "c1-race"
    run_id = "c1-run-race"
    tenant_id = "c1-tenant"
    worker_id = "c1-worker"
    scheduler_epoch = "c1-epoch"
    store, identity, _claim = await _seed_claimed_task(
        redis_client,
        task_id=task_id, run_id=run_id, tenant_id=tenant_id,
        worker_id=worker_id, scheduler_epoch=scheduler_epoch,
    )
    complete_binding = TaskTerminalAuthorityBinding(redis_client)
    fail_binding = TaskTerminalAuthorityBinding(redis_client)

    results = await asyncio.gather(
        complete_binding.complete(
            task_id=task_id, run_id=run_id, tenant_id=tenant_id,
            finished_at_ms=430, worker_instance_id=worker_id,
            scheduler_epoch=scheduler_epoch, claim_epoch=1,
            output_data='{"winner":"complete"}',
        ),
        fail_binding.fail(
            task_id=task_id, run_id=run_id, tenant_id=tenant_id,
            finished_at_ms=430, worker_instance_id=worker_id,
            scheduler_epoch=scheduler_epoch, claim_epoch=1,
            reason_code="winner-fail",
        ),
        return_exceptions=True,
    )
    successes = [item for item in results if not isinstance(item, Exception)]
    failures = [item for item in results if isinstance(item, Exception)]
    assert len(successes) == 1
    assert len(failures) == 1
    snapshot, record, _receipt = await _head(store, identity)
    assert snapshot.revision == 4
    assert snapshot.state in {"done", "failed"}
    assert record.operation_type in {
        OperationType.TASK_COMPLETE.value,
        OperationType.TASK_FAIL.value,
    }
    assert _text(await redis_client.get(DagRedisKey.task_state(task_id))) == snapshot.state


@pytest.mark.integration
async def test_stale_worker_cannot_authorize_terminal_transition(redis_client) -> None:
    task_id = "c1-stale-owner"
    run_id = "c1-run-stale-owner"
    tenant_id = "c1-tenant"
    worker_id = "c1-worker"
    scheduler_epoch = "c1-epoch"
    store, identity, _claim = await _seed_claimed_task(
        redis_client,
        task_id=task_id, run_id=run_id, tenant_id=tenant_id,
        worker_id=worker_id, scheduler_epoch=scheduler_epoch,
    )
    binding = TaskTerminalAuthorityBinding(redis_client)
    with pytest.raises(TaskTerminalAuthorityError):
        await binding.complete(
            task_id=task_id, run_id=run_id, tenant_id=tenant_id,
            finished_at_ms=440, worker_instance_id="stale-worker",
            scheduler_epoch=scheduler_epoch, claim_epoch=1,
            output_data='{"v":1}',
        )
    snapshot = await store.get_aggregate_snapshot(identity)
    assert snapshot is not None and snapshot.revision == 3 and snapshot.state == "running"
    assert _text(await redis_client.get(DagRedisKey.task_state(task_id))) == "running"


class _FailProjectionOnce:
    def __init__(self) -> None:
        self.calls = 0

    async def initialise(self) -> None:
        return None

    async def project(self, value):
        self.calls += 1
        raise RuntimeError("simulated projection interruption")


@pytest.mark.integration
async def test_authority_commit_can_replay_projection_without_second_transition(
    redis_client,
) -> None:
    task_id = "c1-projection-replay"
    run_id = "c1-run-projection-replay"
    tenant_id = "c1-tenant"
    worker_id = "c1-worker"
    scheduler_epoch = "c1-epoch"
    store, identity, _claim = await _seed_claimed_task(
        redis_client,
        task_id=task_id, run_id=run_id, tenant_id=tenant_id,
        worker_id=worker_id, scheduler_epoch=scheduler_epoch,
    )
    first = TaskTerminalAuthorityBinding(
        redis_client,
        store=store,
        projection_manager=_FailProjectionOnce(),
    )
    with pytest.raises(TaskTerminalAuthorityError) as error:
        await first.complete(
            task_id=task_id, run_id=run_id, tenant_id=tenant_id,
            finished_at_ms=450, worker_instance_id=worker_id,
            scheduler_epoch=scheduler_epoch, claim_epoch=1,
            output_data='{"durable":true}',
        )
    assert error.value.status == TASK_TERMINAL_PROJECTION_PENDING_STATUS
    assert error.value.canonical_commit_durable is True
    snapshot = await store.get_aggregate_snapshot(identity)
    assert snapshot is not None and snapshot.revision == 4 and snapshot.state == "done"
    assert _text(await redis_client.get(DagRedisKey.task_state(task_id))) == "running"

    retry = TaskTerminalAuthorityBinding(redis_client, store=store)
    replayed = await retry.replay_terminal_projection(
        task_id=task_id,
        run_id=run_id,
        tenant_id=tenant_id,
        worker_instance_id=worker_id,
        scheduler_epoch=scheduler_epoch,
        claim_epoch=1,
    )
    assert replayed.completed is True
    assert replayed.canonical_revision == 4
    assert _text(await redis_client.get(DagRedisKey.task_state(task_id))) == "done"
    after = await store.get_aggregate_snapshot(identity)
    assert after is not None and after.revision == 4


@pytest.mark.integration
async def test_canonical_projection_preflight_prevents_partial_parent_write(redis_client) -> None:
    task_id = "c1-preflight"
    run_id = "c1-run-preflight"
    tenant_id = "c1-tenant"
    worker_id = "c1-worker"
    scheduler_epoch = "c1-epoch"
    child = "c1-preflight-child"
    store, identity, _claim = await _seed_claimed_task(
        redis_client,
        task_id=task_id, run_id=run_id, tenant_id=tenant_id,
        worker_id=worker_id, scheduler_epoch=scheduler_epoch,
    )
    await redis_client.sadd(DagRedisKey.task_children(task_id), child)
    await redis_client.set(DagRedisKey.task_state(child), "pending")
    await redis_client.hset(DagRedisKey.task_remaining_deps(child), mapping={"bad": "type"})
    binding = TaskTerminalAuthorityBinding(redis_client)
    with pytest.raises(TaskTerminalAuthorityError) as error:
        await binding.complete(
            task_id=task_id, run_id=run_id, tenant_id=tenant_id,
            finished_at_ms=460, worker_instance_id=worker_id,
            scheduler_epoch=scheduler_epoch, claim_epoch=1,
            output_data='{"v":1}',
        )
    assert error.value.canonical_commit_durable is True
    snapshot = await store.get_aggregate_snapshot(identity)
    assert snapshot is not None and snapshot.revision == 4 and snapshot.state == "done"
    assert _text(await redis_client.get(DagRedisKey.task_state(task_id))) == "running"
    assert _text(await redis_client.get(DagRedisKey.task_state(child))) == "pending"
    assert await redis_client.get(DagRedisKey.task_output(task_id)) is None


@pytest.mark.integration
async def test_legacy_failed_completion_keeps_failure_sweeper_era_behavior(redis_client) -> None:
    task_id = "c1-legacy-fail"
    child = "c1-legacy-child"
    run_id = "c1-legacy-run"
    tenant_id = "c1-legacy-tenant"
    worker_id = "c1-legacy-worker"
    epoch = "c1-legacy-epoch"
    await redis_client.set(DagRedisKey.task_state(task_id), "running")
    await redis_client.hset(
        DagRedisKey.task_meta(task_id),
        mapping={
            "task_id": task_id,
            "run_id": run_id,
            "tenant_id": tenant_id,
            "worker_instance_id": worker_id,
            "scheduler_epoch": epoch,
            "claim_epoch": "1",
        },
    )
    await redis_client.set(RedisKey.run_state(run_id), "running")
    await redis_client.sadd(DagRedisKey.task_children(task_id), child)
    await redis_client.set(DagRedisKey.task_state(child), "pending")
    dag = DagLua(redis_client)
    await dag.initialise()
    result = await dag.task_complete(
        task_id=task_id,
        run_id=run_id,
        tenant_id=tenant_id,
        terminal_state="failed",
        finished_at_ms=500,
        reason_code="legacy-failure",
        worker_instance_id=worker_id,
        output_data="{}",
        expected_scheduler_epoch=epoch,
        expected_claim_epoch="1",
    )
    assert result.completed is True
    assert result.status == "committed"
    assert _text(await redis_client.get(DagRedisKey.task_state(child))) == "pending"

@pytest.mark.integration
async def test_exact_fail_replay_does_not_repeat_failure_fanout(redis_client) -> None:
    task_id = "c1-fail-replay"
    run_id = "c1-run-fail-replay"
    tenant_id = "c1-tenant"
    worker_id = "c1-worker"
    scheduler_epoch = "c1-epoch"
    child = "c1-fail-replay-child"
    store, identity, _claim = await _seed_claimed_task(
        redis_client,
        task_id=task_id, run_id=run_id, tenant_id=tenant_id,
        worker_id=worker_id, scheduler_epoch=scheduler_epoch,
    )
    await redis_client.sadd(DagRedisKey.task_children(task_id), child)
    await redis_client.set(DagRedisKey.task_state(child), "ready")
    await redis_client.zadd(DagRedisKey.tenant_ready_queue(tenant_id), {child: 1.0})
    binding = TaskTerminalAuthorityBinding(redis_client)

    first = await binding.fail(
        task_id=task_id, run_id=run_id, tenant_id=tenant_id,
        finished_at_ms=510, worker_instance_id=worker_id,
        scheduler_epoch=scheduler_epoch, claim_epoch=1,
        reason_code="boom",
    )
    second = await binding.fail(
        task_id=task_id, run_id=run_id, tenant_id=tenant_id,
        finished_at_ms=999999, worker_instance_id=worker_id,
        scheduler_epoch=scheduler_epoch, claim_epoch=1,
        reason_code="boom",
    )
    snapshot = await store.get_aggregate_snapshot(identity)
    assert first.canonical_revision == second.canonical_revision == 4
    assert second.status == TASK_TERMINAL_DUPLICATE_STATUS
    assert second.exact_retry is True
    assert snapshot is not None and snapshot.revision == 4 and snapshot.state == "failed"
    assert _text(await redis_client.get(DagRedisKey.task_state(child))) == "blocked_by_failure"
    assert await redis_client.zscore(DagRedisKey.tenant_ready_queue(tenant_id), child) is None


@pytest.mark.integration
async def test_divergent_fail_reason_is_durable_idempotency_conflict(redis_client) -> None:
    task_id = "c1-fail-divergent"
    run_id = "c1-run-fail-divergent"
    tenant_id = "c1-tenant"
    worker_id = "c1-worker"
    scheduler_epoch = "c1-epoch"
    store, identity, _claim = await _seed_claimed_task(
        redis_client,
        task_id=task_id, run_id=run_id, tenant_id=tenant_id,
        worker_id=worker_id, scheduler_epoch=scheduler_epoch,
    )
    binding = TaskTerminalAuthorityBinding(redis_client)
    await binding.fail(
        task_id=task_id, run_id=run_id, tenant_id=tenant_id,
        finished_at_ms=520, worker_instance_id=worker_id,
        scheduler_epoch=scheduler_epoch, claim_epoch=1,
        reason_code="reason-a",
    )
    with pytest.raises(TaskTerminalAuthorityError) as error:
        await binding.fail(
            task_id=task_id, run_id=run_id, tenant_id=tenant_id,
            finished_at_ms=521, worker_instance_id=worker_id,
            scheduler_epoch=scheduler_epoch, claim_epoch=1,
            reason_code="reason-b",
        )
    assert error.value.status == RedisAuthorityCommitStatus.IDEMPOTENCY_CONFLICT.value
    snapshot = await store.get_aggregate_snapshot(identity)
    assert snapshot is not None and snapshot.revision == 4 and snapshot.state == "failed"
    assert await redis_client.xlen(store.keyspace(identity.sha256).conflicts) >= 1


@pytest.mark.integration
async def test_fail_authority_commit_can_replay_projection(redis_client) -> None:
    task_id = "c1-fail-projection-replay"
    run_id = "c1-run-fail-projection-replay"
    tenant_id = "c1-tenant"
    worker_id = "c1-worker"
    scheduler_epoch = "c1-epoch"
    child = "c1-fail-projection-child"
    store, identity, _claim = await _seed_claimed_task(
        redis_client,
        task_id=task_id, run_id=run_id, tenant_id=tenant_id,
        worker_id=worker_id, scheduler_epoch=scheduler_epoch,
    )
    await redis_client.sadd(DagRedisKey.task_children(task_id), child)
    await redis_client.set(DagRedisKey.task_state(child), "pending")
    first = TaskTerminalAuthorityBinding(
        redis_client,
        store=store,
        projection_manager=_FailProjectionOnce(),
    )
    with pytest.raises(TaskTerminalAuthorityError) as error:
        await first.fail(
            task_id=task_id, run_id=run_id, tenant_id=tenant_id,
            finished_at_ms=530, worker_instance_id=worker_id,
            scheduler_epoch=scheduler_epoch, claim_epoch=1,
            reason_code="boom",
        )
    assert error.value.status == TASK_TERMINAL_PROJECTION_PENDING_STATUS
    snapshot = await store.get_aggregate_snapshot(identity)
    assert snapshot is not None and snapshot.revision == 4 and snapshot.state == "failed"
    assert _text(await redis_client.get(DagRedisKey.task_state(task_id))) == "running"

    retry = TaskTerminalAuthorityBinding(redis_client, store=store)
    replayed = await retry.replay_terminal_projection(
        task_id=task_id, run_id=run_id, tenant_id=tenant_id,
        worker_instance_id=worker_id, scheduler_epoch=scheduler_epoch,
        claim_epoch=1,
    )
    assert replayed.completed is True
    assert _text(await redis_client.get(DagRedisKey.task_state(task_id))) == "failed"
    assert _text(await redis_client.get(DagRedisKey.task_state(child))) == "blocked_by_failure"
    after = await store.get_aggregate_snapshot(identity)
    assert after is not None and after.revision == 4


@pytest.mark.parametrize(
    "overrides",
    [
        {"worker_instance_id": "stale-worker"},
        {"scheduler_epoch": "stale-epoch"},
        {"claim_epoch": 2},
    ],
)
@pytest.mark.integration
async def test_stale_claim_fence_dimensions_cannot_authorize_terminal(
    redis_client,
    overrides,
) -> None:
    task_id = "c1-stale-dimensions"
    run_id = "c1-run-stale-dimensions"
    tenant_id = "c1-tenant"
    worker_id = "c1-worker"
    scheduler_epoch = "c1-epoch"
    store, identity, _claim = await _seed_claimed_task(
        redis_client,
        task_id=task_id, run_id=run_id, tenant_id=tenant_id,
        worker_id=worker_id, scheduler_epoch=scheduler_epoch,
    )
    args = {
        "task_id": task_id,
        "run_id": run_id,
        "tenant_id": tenant_id,
        "finished_at_ms": 540,
        "worker_instance_id": worker_id,
        "scheduler_epoch": scheduler_epoch,
        "claim_epoch": 1,
        "output_data": '{"v":1}',
    }
    args.update(overrides)
    binding = TaskTerminalAuthorityBinding(redis_client)
    with pytest.raises(TaskTerminalAuthorityError):
        await binding.complete(**args)
    snapshot = await store.get_aggregate_snapshot(identity)
    assert snapshot is not None and snapshot.revision == 3 and snapshot.state == "running"
    assert _text(await redis_client.get(DagRedisKey.task_state(task_id))) == "running"
