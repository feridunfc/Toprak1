from __future__ import annotations

import hashlib
from dataclasses import replace
from types import SimpleNamespace
from typing import Any, Awaitable, Callable

import pytest

from hfa.authority import (
    AuthorityDecisionCode,
    AuthorityEntryContext,
    OperationType,
    RedisAuthorityCommitStatus,
    RedisCanonicalAuthorityStore,
    canonical_json_bytes,
    evaluate_authority_commit,
)
from hfa.config.keys import RedisKey
from hfa.governance.admission_resource_reservation import AdmissionResourceReservationManager
from hfa.dag.schema import DagRedisKey, DagTaskDispatchInput, DagTaskSeed
from hfa_control.dag_lua import DagLua
from hfa_control.reconciliation import (
    CurrentHeadCanonicalReconciliationReader,
    CurrentHeadReconciler,
    CurrentHeadReconciliationRedisReader,
    ReconciliationReason,
    ReconciliationSeverity,
    ReconciliationStatus,
)
from hfa_control.run_create_authority import (
    RunCreateAuthorityBinding,
    RunCreateAuthorityInput,
    RunCreateProjectionInput,
    RunCreateProjectionManager,
    _stable_event_fields,
    build_run_create_command,
)
from hfa_control.run_terminate_authority import (
    RunTerminateAuthorityBinding,
    TerminalAggregateProof,
    TerminalTaskEvidence,
    build_run_terminate_command,
)
from hfa_control.task_admit_authority import build_task_admit_command
from hfa_control.task_claim_authority import (
    TaskClaimAuthorityBinding,
    TaskClaimAuthorityInput,
    TaskClaimCanonicalProjectionInput,
    build_task_claim_command,
)
from hfa_control.task_dispatch_authority import build_task_dispatch_command
from hfa_control.task_terminal_authority import (
    TaskTerminalAuthorityBinding,
    TaskTerminalAuthorityInput,
    build_task_terminal_command,
)
from hfa_control.worker_reservation import WorkerReservationManager

pytestmark = [pytest.mark.asyncio, pytest.mark.integration]


def _text(value: Any) -> str:
    if isinstance(value, bytes):
        return value.decode("utf-8")
    return "" if value is None else str(value)


def _context(command):
    return AuthorityEntryContext(
        authenticated_writer_id=f"test/{command.operation_type.value}",
        allowed_operations=frozenset({command.operation_type}),
        target_aggregate_identity_sha256=command.aggregate_identity.sha256,
        fence_required=False,
        fence_valid=True,
    )


async def _commit(store, command, *, revision: int, state: str | None, at_ms: int):
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


def _projection_receipt_key(kind: str, operation_id: str) -> str:
    digest = hashlib.sha256(operation_id.encode("utf-8")).hexdigest()
    return f"{RedisKey.PREFIX}:{kind}:projection:v1:{digest}"


def _run_terminate_event_id(run_id: str, final_state: str, task_count: int) -> str:
    material = f"RUN_TERMINATE\x1f{run_id}\x1f{final_state}\x1f{task_count}"
    return hashlib.sha1(material.encode("utf-8")).hexdigest()


async def _store(redis_client):
    store = RedisCanonicalAuthorityStore(redis_client)
    await store.initialise()
    return store


async def _seed_run_create(redis_client, suffix: str):
    store = await _store(redis_client)
    value = RunCreateAuthorityInput(
        run_id=f"s85b-run-create-{suffix}",
        tenant_id=f"s85b-tenant-{suffix}",
        agent_type="agent",
        priority=3,
        payload={"work": suffix},
        estimated_cost_cents=7,
        preferred_region="",
        preferred_placement="LEAST_LOADED",
        created_at_ms=1000,
        control_stream="hfa:stream:control",
    )
    command = build_run_create_command(value)
    record = await _commit(store, command, revision=0, state=None, at_ms=value.created_at_ms)
    await redis_client.set(RedisKey.run_state(value.run_id), "admitted")
    fields = _stable_event_fields(value, record.operation_id)
    event_hash = hashlib.sha256(canonical_json_bytes(fields)).hexdigest()
    receipt_key = _projection_receipt_key("run-create", record.operation_id)
    await redis_client.hset(
        receipt_key,
        mapping={
            "operation_id": record.operation_id,
            "reservation_proof_sha256": "e" * 64,
            "canonical_transition_id": record.transition_id,
            "canonical_record_hash": record.canonical_record_hash,
            "canonical_command_hash": record.canonical_command_hash,
            "canonical_revision": str(record.to_revision),
            "run_id": value.run_id,
            "tenant_id": value.tenant_id,
            "legacy_state": "admitted",
            "event_payload_hash": event_hash,
            "stream_entry_id": "1-0",
        },
    )
    return {
        "operation": OperationType.RUN_CREATE,
        "store": store,
        "record": record,
        "run_id": value.run_id,
        "task_id": None,
        "tenant_id": value.tenant_id,
        "receipt_key": receipt_key,
    }


async def _seed_task_admit(redis_client, suffix: str, *, ready: bool = True):
    store = await _store(redis_client)
    seed = DagTaskSeed(
        task_id=f"s85b-task-{suffix}",
        run_id=f"s85b-run-{suffix}",
        tenant_id=f"s85b-tenant-{suffix}",
        agent_type="agent",
        priority=4,
        admitted_at=2000,
        dependency_count=0 if ready else 2,
        region="tr",
        policy="LEAST_LOADED",
        payload_json='{"x":1}',
        trace_parent="trace-parent",
        trace_state="trace-state",
    )
    command = build_task_admit_command(seed)
    record = await _commit(store, command, revision=0, state=None, at_ms=2000)
    await redis_client.set(DagRedisKey.task_state(seed.task_id), record.next_state)
    await redis_client.hset(
        DagRedisKey.task_meta(seed.task_id),
        mapping={
            "task_id": seed.task_id,
            "run_id": seed.run_id,
            "tenant_id": seed.tenant_id,
            "agent_type": seed.agent_type,
            "priority": str(seed.priority),
            "admitted_at": str(int(seed.admitted_at)),
            "payload_json": seed.payload_json,
            "trace_parent": seed.trace_parent,
            "trace_state": seed.trace_state,
            "region": seed.region,
            "policy": seed.policy,
        },
    )
    await redis_client.set(DagRedisKey.task_remaining_deps(seed.task_id), str(seed.dependency_count))
    await redis_client.sadd(DagRedisKey.run_tasks(seed.run_id), seed.task_id)
    if ready:
        await redis_client.zadd(DagRedisKey.task_ready_queue(seed.tenant_id), {seed.task_id: float(seed.admitted_at)})
        await redis_client.set(DagRedisKey.task_ready_emitted(seed.task_id), "1")
    return {
        "operation": OperationType.TASK_ADMIT,
        "store": store,
        "record": record,
        "seed": seed,
        "run_id": seed.run_id,
        "task_id": seed.task_id,
        "tenant_id": seed.tenant_id,
    }


async def _seed_task_dispatch(redis_client, suffix: str):
    seeded = await _seed_task_admit(redis_client, suffix, ready=True)
    seed = seeded["seed"]
    dispatch = DagTaskDispatchInput(
        task_id=seed.task_id,
        run_id=seed.run_id,
        tenant_id=seed.tenant_id,
        worker_id=f"s85b-worker-{suffix}",
        worker_group="workers",
        agent_type=seed.agent_type,
        shard=1,
        priority=seed.priority,
        admitted_at=seed.admitted_at,
        scheduled_at=3000,
        scheduled_zset=DagRedisKey.task_scheduled_zset(seed.tenant_id),
        running_zset=DagRedisKey.task_running_zset(seed.tenant_id),
        control_stream="hfa:stream:control",
        shard_stream="hfa:stream:runs:1",
        region=seed.region,
        policy=seed.policy,
        payload_json=seed.payload_json,
        trace_parent=seed.trace_parent,
        trace_state=seed.trace_state,
        scheduler_epoch="7",
        attempt=1,
    )
    command = build_task_dispatch_command(dispatch, expected_revision=seeded["record"].to_revision)
    record = await _commit(
        seeded["store"],
        command,
        revision=seeded["record"].to_revision,
        state="ready",
        at_ms=3000,
    )
    await redis_client.zrem(DagRedisKey.task_ready_queue(seed.tenant_id), seed.task_id)
    await redis_client.set(DagRedisKey.task_state(seed.task_id), "scheduled")
    await redis_client.hset(
        DagRedisKey.task_meta(seed.task_id),
        mapping={
            "task_id": seed.task_id,
            "run_id": seed.run_id,
            "tenant_id": seed.tenant_id,
            "scheduler_epoch": dispatch.scheduler_epoch,
            "dispatch_attempt": str(dispatch.attempt),
            "dispatch_worker_id": dispatch.worker_id,
            "canonical_transition_id": record.transition_id,
            "canonical_record_hash": record.canonical_record_hash,
            "canonical_command_hash": record.canonical_command_hash,
            "canonical_revision": str(record.to_revision),
            "canonical_operation_id": record.operation_id,
        },
    )
    await redis_client.zadd(dispatch.scheduled_zset, {seed.task_id: float(dispatch.scheduled_at)})
    seeded.update(
        operation=OperationType.TASK_DISPATCH,
        record=record,
        dispatch=dispatch,
        worker_id=dispatch.worker_id,
        scheduler_epoch=dispatch.scheduler_epoch,
    )
    return seeded


async def _seed_task_claim(redis_client, suffix: str):
    seeded = await _seed_task_dispatch(redis_client, suffix)
    dispatch_record = seeded["record"]
    dispatch = seeded["dispatch"]
    claim = TaskClaimAuthorityInput(
        task_id=seeded["task_id"],
        run_id=seeded["run_id"],
        tenant_id=seeded["tenant_id"],
        worker_instance_id=seeded["worker_id"],
        dispatch_worker_id=seeded["worker_id"],
        scheduler_epoch=seeded["scheduler_epoch"],
        claimed_at_ms=4000,
        dispatch_attempt=dispatch.attempt,
        dispatch_revision=dispatch_record.to_revision,
        previous_claim_epoch=0,
        dispatch_transition_id=dispatch_record.transition_id,
        dispatch_record_hash=dispatch_record.canonical_record_hash,
        dispatch_command_hash=dispatch_record.canonical_command_hash,
        dispatch_operation_id=dispatch_record.operation_id,
    )
    command = build_task_claim_command(claim)
    record = await _commit(
        seeded["store"],
        command,
        revision=dispatch_record.to_revision,
        state="scheduled",
        at_ms=4000,
    )
    meta_key = DagRedisKey.task_meta(seeded["task_id"])
    await redis_client.set(DagRedisKey.task_state(seeded["task_id"]), "running")
    await redis_client.hset(
        meta_key,
        mapping={
            "task_id": seeded["task_id"],
            "run_id": seeded["run_id"],
            "tenant_id": seeded["tenant_id"],
            "worker_instance_id": seeded["worker_id"],
            "scheduler_epoch": seeded["scheduler_epoch"],
            "claimed_at_ms": "4000",
            "last_heartbeat_at_ms": "4000",
            "heartbeat_at_ms": "4000",
            "heartbeat_owner": seeded["worker_id"],
            "claim_epoch": "1",
            "dispatch_attempt": "1",
            "dispatch_worker_id": seeded["worker_id"],
            "dispatch_canonical_transition_id": dispatch_record.transition_id,
            "dispatch_canonical_record_hash": dispatch_record.canonical_record_hash,
            "dispatch_canonical_command_hash": dispatch_record.canonical_command_hash,
            "dispatch_canonical_revision": str(dispatch_record.to_revision),
            "dispatch_canonical_operation_id": dispatch_record.operation_id,
            "claim_canonical_transition_id": record.transition_id,
            "claim_canonical_record_hash": record.canonical_record_hash,
            "claim_canonical_command_hash": record.canonical_command_hash,
            "claim_canonical_revision": str(record.to_revision),
            "claim_canonical_operation_id": record.operation_id,
            "canonical_transition_id": record.transition_id,
            "canonical_record_hash": record.canonical_record_hash,
            "canonical_command_hash": record.canonical_command_hash,
            "canonical_revision": str(record.to_revision),
            "canonical_operation_id": record.operation_id,
        },
    )
    await redis_client.zrem(dispatch.scheduled_zset, seeded["task_id"])
    await redis_client.zadd(DagRedisKey.task_running_zset(seeded["tenant_id"]), {seeded["task_id"]: 4000.0})
    await redis_client.delete(DagRedisKey.worker_reservation(seeded["worker_id"]))
    await redis_client.delete(DagRedisKey.task_reservation_owner(seeded["task_id"]))
    seeded.update(
        operation=OperationType.TASK_CLAIM,
        record=record,
        claim=claim,
        claim_record=record,
    )
    return seeded


async def _seed_task_terminal(redis_client, suffix: str, *, failed: bool = False):
    seeded = await _seed_task_claim(redis_client, suffix)
    claim_record = seeded["claim_record"]
    terminal_state = "failed" if failed else "done"
    output_data = "" if failed else canonical_json_bytes({"ok": True}).decode("utf-8")
    output_sha = "" if failed else hashlib.sha256(output_data.encode("utf-8")).hexdigest()
    value = TaskTerminalAuthorityInput(
        task_id=seeded["task_id"],
        run_id=seeded["run_id"],
        tenant_id=seeded["tenant_id"],
        terminal_state=terminal_state,
        finished_at_ms=5000,
        reason_code="EXECUTION_FAILED" if failed else "EXECUTION_COMPLETE",
        worker_instance_id=seeded["worker_id"],
        scheduler_epoch=seeded["scheduler_epoch"],
        claim_epoch=1,
        output_data=output_data,
        output_sha256=output_sha,
        claim_transition_id=claim_record.transition_id,
        claim_record_hash=claim_record.canonical_record_hash,
        claim_command_hash=claim_record.canonical_command_hash,
        claim_revision=claim_record.to_revision,
        claim_operation_id=claim_record.operation_id,
    )
    command = build_task_terminal_command(value)
    record = await _commit(
        seeded["store"],
        command,
        revision=claim_record.to_revision,
        state="running",
        at_ms=5000,
    )
    await redis_client.set(DagRedisKey.task_state(seeded["task_id"]), terminal_state)
    await redis_client.hset(
        DagRedisKey.task_meta(seeded["task_id"]),
        mapping={
            "completed_at_ms": "5000",
            "terminal_state": terminal_state,
            "completion_reason": value.reason_code,
            "worker_instance_id": seeded["worker_id"],
            "scheduler_epoch": seeded["scheduler_epoch"],
            "claim_epoch": "1",
            "terminal_canonical_transition_id": record.transition_id,
            "terminal_canonical_record_hash": record.canonical_record_hash,
            "terminal_canonical_command_hash": record.canonical_command_hash,
            "terminal_canonical_revision": str(record.to_revision),
            "terminal_canonical_operation_id": record.operation_id,
            "terminal_canonical_operation_type": command.operation_type.value,
            "terminal_output_sha256": output_sha,
            "canonical_transition_id": record.transition_id,
            "canonical_record_hash": record.canonical_record_hash,
            "canonical_command_hash": record.canonical_command_hash,
            "canonical_revision": str(record.to_revision),
            "canonical_operation_id": record.operation_id,
            # claim_canonical_* and identity fields are retained from claim seed.
        },
    )
    await redis_client.zrem(DagRedisKey.task_running_zset(seeded["tenant_id"]), seeded["task_id"])
    if not failed:
        await redis_client.set(DagRedisKey.task_output(seeded["task_id"]), output_data)
    seeded.update(
        operation=OperationType.TASK_FAIL if failed else OperationType.TASK_COMPLETE,
        record=record,
        terminal=value,
        output_data=output_data,
    )
    return seeded


async def _seed_run_terminate(redis_client, suffix: str, *, failed: bool = False):
    seeded = await _seed_run_create(redis_client, suffix)
    final_state = "failed" if failed else "done"
    task_state = "failed" if failed else "done"
    payload_object = {
        "schema_version": 1,
        "run_id": seeded["run_id"],
        "tenant_id": seeded["tenant_id"],
        "tasks": [{"task_id": f"terminal-{suffix}", "state": task_state}],
        "task_count": 1,
        "done_count": 0 if failed else 1,
        "failed_count": 1 if failed else 0,
        "skipped_count": 0,
        "final_state": final_state,
    }
    proof_payload_json = canonical_json_bytes(payload_object).decode("utf-8")
    proof_sha = hashlib.sha256(proof_payload_json.encode("utf-8")).hexdigest()
    proof = TerminalAggregateProof(
        schema_version=1,
        run_id=seeded["run_id"],
        tenant_id=seeded["tenant_id"],
        tasks=(TerminalTaskEvidence(task_id=f"terminal-{suffix}", state=task_state),),
        task_count=1,
        done_count=0 if failed else 1,
        failed_count=1 if failed else 0,
        skipped_count=0,
        final_state=final_state,
        proof_sha256=proof_sha,
        proof_payload_json=proof_payload_json,
        finalized_at_ms=6000,
        worker_instance_id=f"worker-{suffix}",
        trigger_task_id=f"terminal-{suffix}",
        trigger_terminal_state=task_state,
        canonical_expected_revision=1,
        canonical_previous_state="pending",
    )
    command = build_run_terminate_command(proof)
    record = await _commit(seeded["store"], command, revision=1, state="pending", at_ms=6000)
    projection_payload = {
        "task_count": 1,
        "done_count": 0 if failed else 1,
        "failed_count": 1 if failed else 0,
        "skipped_count": 0,
        "trigger_task_id": proof.trigger_task_id,
        "trigger_terminal_state": proof.trigger_terminal_state,
    }
    payload_json = canonical_json_bytes(projection_payload).decode("utf-8")
    payload_sha = hashlib.sha256(payload_json.encode("utf-8")).hexdigest()
    event_id = _run_terminate_event_id(seeded["run_id"], final_state, 1)
    await redis_client.set(RedisKey.run_state(seeded["run_id"]), final_state)
    await redis_client.hset(
        RedisKey.run_meta(seeded["run_id"]),
        mapping={
            "run_id": seeded["run_id"],
            "tenant_id": seeded["tenant_id"],
            "state": final_state,
            "finalized_at_ms": "6000",
            "finalization_operation": "RUN_TERMINATE",
            "finalization_source": "terminal_task_aggregate",
            "terminal_proof_sha256": proof_sha,
            "canonical_transition_id": record.transition_id,
            "canonical_record_hash": record.canonical_record_hash,
            "canonical_revision": str(record.to_revision),
            "result_event_id": event_id,
        },
    )
    await redis_client.hset(
        RedisKey.run_result(seeded["run_id"]),
        mapping={
            "run_id": seeded["run_id"],
            "tenant_id": seeded["tenant_id"],
            "status": final_state,
            "payload": payload_json,
            "finalized_at_ms": "6000",
            "finalization_operation": "RUN_TERMINATE",
            "finalization_source": "terminal_task_aggregate",
            "terminal_proof_sha256": proof_sha,
            "canonical_transition_id": record.transition_id,
            "canonical_record_hash": record.canonical_record_hash,
            "canonical_revision": str(record.to_revision),
            "result_event_id": event_id,
        },
    )
    receipt_key = _projection_receipt_key("run-terminate", record.operation_id)
    await redis_client.hset(
        receipt_key,
        mapping={
            "operation_id": record.operation_id,
            "terminal_proof_sha256": proof_sha,
            "canonical_transition_id": record.transition_id,
            "canonical_record_hash": record.canonical_record_hash,
            "canonical_command_hash": record.canonical_command_hash,
            "canonical_revision": str(record.to_revision),
            "run_id": seeded["run_id"],
            "tenant_id": seeded["tenant_id"],
            "final_state": final_state,
            "finalized_at_ms": "6000",
            "result_payload_sha256": payload_sha,
            "event_id": event_id,
            "stream_entry_id": "2-0",
        },
    )
    await redis_client.zrem(RedisKey.cp_running(), seeded["run_id"])
    seeded.update(
        operation=OperationType.RUN_TERMINATE,
        record=record,
        proof=proof,
        receipt_key=receipt_key,
    )
    return seeded


async def _head_record(store, identity):
    snapshot = await store.get_aggregate_snapshot(identity)
    assert snapshot is not None
    probe = await store.load_receipt_probe(identity, snapshot.operation_id)
    assert probe is not None and probe.canonical_store_record is not None
    return probe.canonical_store_record


async def _golden_run_create(
    redis_client, suffix: str, *, run_id: str | None = None, tenant_id: str | None = None
):
    store = await _store(redis_client)
    run_id = run_id or f"s85b-golden-run-create-{suffix}"
    tenant_id = tenant_id or f"s85b-golden-tenant-{suffix}"
    resource = AdmissionResourceReservationManager(redis_client, clock_ms=lambda: 1000)
    projection = RunCreateProjectionManager(redis_client)
    binding = RunCreateAuthorityBinding(
        redis=redis_client,
        resource_manager=resource,
        control_stream=RedisKey.stream_control(),
        store=store,
        projection_manager=projection,
    )
    request = SimpleNamespace(
        run_id=run_id,
        tenant_id=tenant_id,
        agent_type="agent",
        priority=3,
        payload={"work": suffix},
        estimated_cost_cents=7,
        preferred_region="",
        preferred_placement="LEAST_LOADED",
    )
    await binding.admit(request, tenant_inflight_limit=10)
    value = RunCreateAuthorityInput(
        run_id=run_id,
        tenant_id=tenant_id,
        agent_type="agent",
        priority=3,
        payload={"work": suffix},
        estimated_cost_cents=7,
        preferred_region="",
        preferred_placement="LEAST_LOADED",
        created_at_ms=1000,
        control_stream=RedisKey.stream_control(),
    )
    command = build_run_create_command(value)
    record = await _head_record(store, command.aggregate_identity)
    assert await redis_client.get(RedisKey.run_state(run_id)) == "admitted"
    return {
        "operation": OperationType.RUN_CREATE,
        "store": store,
        "record": record,
        "run_id": run_id,
        "task_id": None,
        "tenant_id": tenant_id,
        "receipt_key": projection.receipt_key(record.operation_id),
    }


def _golden_task_seed(suffix: str, *, dependency_count: int = 0, run_id: str | None = None,
                      tenant_id: str | None = None, child_task_ids=()):
    return DagTaskSeed(
        task_id=f"s85b-golden-task-{suffix}",
        run_id=run_id or f"s85b-golden-run-{suffix}",
        tenant_id=tenant_id or f"s85b-golden-tenant-{suffix}",
        agent_type="agent",
        priority=4,
        admitted_at=2000,
        dependency_count=dependency_count,
        region="tr",
        policy="LEAST_LOADED",
        payload_json='{"x":1}',
        trace_parent="trace-parent",
        trace_state="trace-state",
        child_task_ids=tuple(child_task_ids),
    )


async def _golden_task_admit(redis_client, suffix: str, *, dependency_count: int = 0):
    dag = DagLua(redis_client, canonical_task_admit_binding=True)
    seed = _golden_task_seed(suffix, dependency_count=dependency_count)
    result = await dag.task_admit(seed)
    assert result.admitted is True
    binding = dag._task_admit_authority_binding
    assert binding is not None and binding.store is not None
    command = build_task_admit_command(seed)
    record = await _head_record(binding.store, command.aggregate_identity)
    return {
        "operation": OperationType.TASK_ADMIT,
        "store": binding.store,
        "record": record,
        "seed": seed,
        "dag": dag,
        "run_id": seed.run_id,
        "task_id": seed.task_id,
        "tenant_id": seed.tenant_id,
    }


def _golden_dispatch_input(seed: DagTaskSeed, suffix: str):
    return DagTaskDispatchInput(
        task_id=seed.task_id,
        run_id=seed.run_id,
        tenant_id=seed.tenant_id,
        worker_id=f"s85b-golden-worker-{suffix}",
        worker_group="workers",
        agent_type=seed.agent_type,
        shard=1,
        priority=seed.priority,
        admitted_at=seed.admitted_at,
        scheduled_at=3000,
        scheduled_zset=DagRedisKey.task_scheduled_zset(seed.tenant_id),
        running_zset=DagRedisKey.task_running_zset(seed.tenant_id),
        control_stream=RedisKey.stream_control(),
        shard_stream=RedisKey.stream_shard(1),
        region=seed.region,
        policy=seed.policy,
        payload_json=seed.payload_json,
        trace_parent=seed.trace_parent,
        trace_state=seed.trace_state,
        scheduler_epoch="7",
        attempt=1,
    )


async def _golden_task_dispatch(
    redis_client,
    suffix: str,
    *,
    run_id: str | None = None,
    tenant_id: str | None = None,
    ensure_run: bool = True,
):
    dag = DagLua(
        redis_client,
        canonical_task_admit_binding=True,
        canonical_task_dispatch_binding=True,
    )
    seed = _golden_task_seed(suffix, run_id=run_id, tenant_id=tenant_id)
    if ensure_run:
        await _golden_run_create(
            redis_client,
            f"{suffix}-support-run",
            run_id=seed.run_id,
            tenant_id=seed.tenant_id,
        )
    admitted = await dag.task_admit(seed)
    assert admitted.admitted and admitted.ready
    dispatch = _golden_dispatch_input(seed, suffix)
    projected = await dag.task_dispatch_commit(dispatch)
    assert projected.committed is True
    binding = dag._task_dispatch_authority_binding
    assert binding is not None and binding.store is not None
    command = build_task_dispatch_command(dispatch, expected_revision=1)
    record = await _head_record(binding.store, command.aggregate_identity)
    return {
        "operation": OperationType.TASK_DISPATCH,
        "store": binding.store,
        "record": record,
        "seed": seed,
        "dag": dag,
        "dispatch": dispatch,
        "run_id": seed.run_id,
        "task_id": seed.task_id,
        "tenant_id": seed.tenant_id,
        "worker_id": dispatch.worker_id,
        "scheduler_epoch": dispatch.scheduler_epoch,
    }


async def _golden_claim_from_dispatch(redis_client, seeded, *, claimed_at_ms: int = 4000):
    reservation = WorkerReservationManager(redis_client, reservation_ttl_seconds=30)
    reserved = await reservation.reserve(
        worker_id=seeded["worker_id"],
        task_id=seeded["task_id"],
        scheduler_epoch=seeded["scheduler_epoch"],
        reserved_at_ms=claimed_at_ms - 100,
    )
    assert reserved.ok is True
    binding = TaskClaimAuthorityBinding(redis_client, store=seeded["store"])
    prepared = await binding.prepare_claim(
        task_id=seeded["task_id"],
        run_id=seeded["run_id"],
        tenant_id=seeded["tenant_id"],
        worker_instance_id=seeded["worker_id"],
        scheduler_epoch=seeded["scheduler_epoch"],
        claimed_at_ms=claimed_at_ms,
    )
    result = await seeded["dag"].task_claim_canonical_projection(prepared.projection)
    assert result.ok is True
    record = await _head_record(
        seeded["store"],
        build_task_admit_command(seeded["seed"]).aggregate_identity,
    )
    seeded.update(
        operation=OperationType.TASK_CLAIM,
        record=record,
        claim_record=record,
    )
    return seeded


async def _golden_task_claim(
    redis_client, suffix: str, *, run_id: str | None = None, tenant_id: str | None = None,
    ensure_run: bool = True,
):
    return await _golden_claim_from_dispatch(
        redis_client,
        await _golden_task_dispatch(
            redis_client, suffix, run_id=run_id, tenant_id=tenant_id, ensure_run=ensure_run
        ),
    )


async def _golden_task_terminal(
    redis_client, suffix: str, *, failed: bool, run_id: str | None = None,
    tenant_id: str | None = None, ensure_run: bool = True,
):
    seeded = await _golden_task_claim(
        redis_client, suffix, run_id=run_id, tenant_id=tenant_id, ensure_run=ensure_run
    )
    binding = TaskTerminalAuthorityBinding(redis_client, store=seeded["store"])
    if failed:
        result = await binding.fail(
            task_id=seeded["task_id"],
            run_id=seeded["run_id"],
            tenant_id=seeded["tenant_id"],
            finished_at_ms=5000,
            worker_instance_id=seeded["worker_id"],
            scheduler_epoch=seeded["scheduler_epoch"],
            claim_epoch=1,
            reason_code="EXECUTION_FAILED",
        )
    else:
        result = await binding.complete(
            task_id=seeded["task_id"],
            run_id=seeded["run_id"],
            tenant_id=seeded["tenant_id"],
            finished_at_ms=5000,
            worker_instance_id=seeded["worker_id"],
            scheduler_epoch=seeded["scheduler_epoch"],
            claim_epoch=1,
            output_data='{"ok":true}',
        )
    assert result.completed is True
    record = await _head_record(seeded["store"], build_task_admit_command(seeded["seed"]).aggregate_identity)
    seeded.update(
        operation=OperationType.TASK_FAIL if failed else OperationType.TASK_COMPLETE,
        record=record,
        output_data="" if failed else '{"ok":true}',
    )
    return seeded


async def _golden_run_terminate(redis_client, suffix: str, *, failed: bool):
    seeded = await _golden_run_create(redis_client, suffix)
    terminal = await _golden_task_terminal(
        redis_client,
        f"{suffix}-terminal-task",
        failed=failed,
        run_id=seeded["run_id"],
        tenant_id=seeded["tenant_id"],
        ensure_run=False,
    )
    task_id = terminal["task_id"]
    task_state = "failed" if failed else "done"
    migration = RedisKey.run_terminal_event_index()
    await redis_client.hset(
        migration,
        mapping={
            "__contract__:schema_version": "1",
            "__contract__:producer_contract_version": "1",
            "__migration__:status": "ready",
            "__migration__:results_stream_key": RedisKey.stream_results(),
            "__migration__:source_history_complete": "1",
        },
    )
    await redis_client.persist(migration)
    binding = RunTerminateAuthorityBinding(redis_client, store=seeded["store"])
    result = await binding.terminate(
        run_id=seeded["run_id"],
        tenant_id=seeded["tenant_id"],
        trigger_task_id=task_id,
        finalized_at_ms=6000,
        worker_instance_id=f"worker-{suffix}",
        trigger_terminal_state=task_state,
    )
    assert result.final_state == ("failed" if failed else "done")
    record = await _head_record(
        seeded["store"],
        build_run_create_command(
            RunCreateAuthorityInput(
                run_id=seeded["run_id"],
                tenant_id=seeded["tenant_id"],
                agent_type="agent",
                priority=3,
                payload={"work": suffix},
                estimated_cost_cents=7,
                preferred_region="",
                preferred_placement="LEAST_LOADED",
                created_at_ms=1000,
                control_stream=RedisKey.stream_control(),
            )
        ).aggregate_identity,
    )
    seeded.update(
        operation=OperationType.RUN_TERMINATE,
        record=record,
        task_id=None,
    )
    return seeded


async def _golden_dependency_fanout_child(redis_client, suffix: str, *, dependency_count: int, failed: bool):
    tenant_id = f"s85b-fanout-tenant-{suffix}"
    run_id = f"s85b-fanout-run-{suffix}"
    dag = DagLua(
        redis_client,
        canonical_task_admit_binding=True,
        canonical_task_dispatch_binding=True,
    )
    await _golden_run_create(
        redis_client, f"{suffix}-support-run", run_id=run_id, tenant_id=tenant_id
    )
    child = _golden_task_seed(
        f"{suffix}-child",
        dependency_count=dependency_count,
        run_id=run_id,
        tenant_id=tenant_id,
    )
    parent = _golden_task_seed(
        f"{suffix}-parent",
        dependency_count=0,
        run_id=run_id,
        tenant_id=tenant_id,
        child_task_ids=(child.task_id,),
    )
    child_result = await dag.task_admit(child)
    assert child_result.admitted and not child_result.ready
    child_binding = dag._task_admit_authority_binding
    assert child_binding is not None and child_binding.store is not None
    child_record = await _head_record(
        child_binding.store,
        build_task_admit_command(child).aggregate_identity,
    )
    parent_result = await dag.task_admit(parent)
    assert parent_result.admitted and parent_result.ready
    dispatch = _golden_dispatch_input(parent, f"{suffix}-parent")
    dispatched = await dag.task_dispatch_commit(dispatch)
    assert dispatched.committed is True
    dispatch_binding = dag._task_dispatch_authority_binding
    assert dispatch_binding is not None and dispatch_binding.store is not None
    dispatch_record = await _head_record(
        dispatch_binding.store,
        build_task_dispatch_command(dispatch, expected_revision=1).aggregate_identity,
    )
    parent_seeded = {
        "operation": OperationType.TASK_DISPATCH,
        "store": dispatch_binding.store,
        "record": dispatch_record,
        "seed": parent,
        "dag": dag,
        "dispatch": dispatch,
        "run_id": run_id,
        "task_id": parent.task_id,
        "tenant_id": tenant_id,
        "worker_id": dispatch.worker_id,
        "scheduler_epoch": dispatch.scheduler_epoch,
    }
    parent_seeded = await _golden_claim_from_dispatch(redis_client, parent_seeded)
    terminal = TaskTerminalAuthorityBinding(redis_client, store=parent_seeded["store"])
    if failed:
        terminal_result = await terminal.fail(
            task_id=parent.task_id,
            run_id=run_id,
            tenant_id=tenant_id,
            finished_at_ms=5000,
            worker_instance_id=parent_seeded["worker_id"],
            scheduler_epoch=parent_seeded["scheduler_epoch"],
            claim_epoch=1,
            reason_code="PARENT_FAILED",
        )
    else:
        terminal_result = await terminal.complete(
            task_id=parent.task_id,
            run_id=run_id,
            tenant_id=tenant_id,
            finished_at_ms=5000,
            worker_instance_id=parent_seeded["worker_id"],
            scheduler_epoch=parent_seeded["scheduler_epoch"],
            claim_epoch=1,
            output_data='{"parent":true}',
        )
    assert terminal_result.completed is True
    # The child canonical head intentionally remains TASK_ADMIT. Only the
    # accepted parent terminal projector has mutated dependency-owned runtime.
    child_head = await _head_record(
        child_binding.store,
        build_task_admit_command(child).aggregate_identity,
    )
    assert child_head.operation_type == OperationType.TASK_ADMIT.value
    return {
        "operation": OperationType.TASK_ADMIT,
        "store": child_binding.store,
        "record": child_record,
        "seed": child,
        "run_id": run_id,
        "task_id": child.task_id,
        "tenant_id": tenant_id,
    }


async def _seed_for_operation(redis_client, operation: OperationType, suffix: str):
    if operation is OperationType.RUN_CREATE:
        return await _seed_run_create(redis_client, suffix)
    if operation is OperationType.TASK_ADMIT:
        return await _seed_task_admit(redis_client, suffix, ready=True)
    if operation is OperationType.TASK_DISPATCH:
        return await _seed_task_dispatch(redis_client, suffix)
    if operation is OperationType.TASK_CLAIM:
        return await _seed_task_claim(redis_client, suffix)
    if operation is OperationType.TASK_COMPLETE:
        return await _seed_task_terminal(redis_client, suffix, failed=False)
    if operation is OperationType.TASK_FAIL:
        return await _seed_task_terminal(redis_client, suffix, failed=True)
    if operation is OperationType.RUN_TERMINATE:
        return await _seed_run_terminate(redis_client, suffix, failed=False)
    raise AssertionError(operation)


def _reconciler(redis_client, *, canonical_reader=None, runtime_reader=None):
    return CurrentHeadReconciler(
        canonical_reader=canonical_reader or CurrentHeadCanonicalReconciliationReader(redis_client),
        runtime_reader=runtime_reader or CurrentHeadReconciliationRedisReader(redis_client),
        clock_ms=lambda: 9000,
    )


async def _reconcile(redis_client, seeded, *, canonical_reader=None, runtime_reader=None):
    return await _reconciler(
        redis_client,
        canonical_reader=canonical_reader,
        runtime_reader=runtime_reader,
    ).reconcile(
        operation_type=seeded["operation"],
        run_id=seeded["run_id"],
        task_id=seeded["task_id"],
    )


def _reasons(findings):
    return {item.reason_code for item in findings}


def _snapshot_bytes(value: Any) -> bytes:
    if isinstance(value, bytes):
        return value
    if isinstance(value, str):
        return value.encode("utf-8")
    raise AssertionError(f"semantic Redis snapshot requires bytes/text, got {type(value).__name__}")


async def _redis_snapshot(redis_client, *, require_persistent: bool = False):
    discovered = {}
    async for key in redis_client.scan_iter(match="*"):
        discovered[_snapshot_bytes(key)] = key

    result = []
    for raw_key in sorted(discovered):
        key = discovered[raw_key]
        pttl = await redis_client.pttl(key)
        if require_persistent and pttl != -1:
            raise AssertionError(
                f"ZERO_MUTATION_TTL_MODEL_REQUIRED key={raw_key!r} pttl={pttl}"
            )

        redis_type = _text(await redis_client.type(key))
        if redis_type == "string":
            value = await redis_client.get(key)
            if value is None:
                raise AssertionError(f"semantic snapshot key disappeared: {raw_key!r}")
            semantic_value = _snapshot_bytes(value)
        elif redis_type == "hash":
            values = await redis_client.hgetall(key)
            semantic_value = tuple(
                sorted(
                    (_snapshot_bytes(field), _snapshot_bytes(value))
                    for field, value in values.items()
                )
            )
        elif redis_type == "set":
            values = await redis_client.smembers(key)
            semantic_value = tuple(sorted(_snapshot_bytes(value) for value in values))
        elif redis_type == "zset":
            values = await redis_client.zrange(key, 0, -1, withscores=True)
            semantic_value = tuple(
                (_snapshot_bytes(member), float(score)) for member, score in values
            )
        elif redis_type == "list":
            values = await redis_client.lrange(key, 0, -1)
            semantic_value = tuple(_snapshot_bytes(value) for value in values)
        elif redis_type == "stream":
            entries = await redis_client.xrange(key, min="-", max="+")
            semantic_value = tuple(
                (
                    _snapshot_bytes(entry_id),
                    tuple(
                        sorted(
                            (_snapshot_bytes(field), _snapshot_bytes(value))
                            for field, value in fields.items()
                        )
                    ),
                )
                for entry_id, fields in entries
            )
        elif redis_type == "none":
            raise AssertionError(f"semantic snapshot key disappeared: {raw_key!r}")
        else:
            raise AssertionError(
                f"unsupported Redis type in semantic zero-mutation oracle: {redis_type}"
            )

        result.append((raw_key, redis_type, semantic_value))
    return tuple(result)


async def test_golden_run_create_projection_is_consistent(redis_client):
    seeded = await _golden_run_create(redis_client, "run-create")
    findings = await _reconcile(redis_client, seeded)
    assert {f.status for f in findings} == {ReconciliationStatus.CONSISTENT}


async def test_golden_task_admit_ready_projection_is_consistent(redis_client):
    seeded = await _golden_task_admit(redis_client, "admit-ready", dependency_count=0)
    findings = await _reconcile(redis_client, seeded)
    assert {f.status for f in findings} == {ReconciliationStatus.CONSISTENT}


async def test_golden_task_admit_untouched_pending_is_consistent(redis_client):
    seeded = await _golden_task_admit(redis_client, "admit-pending", dependency_count=2)
    before = await _redis_snapshot(redis_client)
    findings = await _reconcile(redis_client, seeded)
    after = await _redis_snapshot(redis_client)
    assert before == after
    assert {f.status for f in findings} == {ReconciliationStatus.CONSISTENT}


async def test_golden_task_dispatch_projection_is_consistent(redis_client):
    seeded = await _golden_task_dispatch(redis_client, "dispatch")
    findings = await _reconcile(redis_client, seeded)
    assert {f.status for f in findings} == {ReconciliationStatus.CONSISTENT}


async def test_golden_task_claim_projection_is_consistent(redis_client):
    seeded = await _golden_task_claim(redis_client, "claim")
    findings = await _reconcile(redis_client, seeded)
    assert {f.status for f in findings} == {ReconciliationStatus.CONSISTENT}


async def test_golden_task_complete_projection_is_consistent(redis_client):
    seeded = await _golden_task_terminal(redis_client, "complete", failed=False)
    findings = await _reconcile(redis_client, seeded)
    assert {f.status for f in findings} == {ReconciliationStatus.CONSISTENT}


async def test_golden_task_fail_projection_is_consistent(redis_client):
    seeded = await _golden_task_terminal(redis_client, "fail", failed=True)
    findings = await _reconcile(redis_client, seeded)
    assert {f.status for f in findings} == {ReconciliationStatus.CONSISTENT}


@pytest.mark.parametrize("failed", [False, True])
async def test_golden_run_terminate_projection_is_consistent(redis_client, failed):
    seeded = await _golden_run_terminate(redis_client, f"terminate-{failed}", failed=failed)
    findings = await _reconcile(redis_client, seeded)
    assert {f.status for f in findings} == {ReconciliationStatus.CONSISTENT}


async def test_task_admit_partial_dependency_progress_requires_cross_task_evidence(redis_client):
    seeded = await _golden_dependency_fanout_child(
        redis_client, "partial", dependency_count=2, failed=False
    )
    assert await redis_client.get(DagRedisKey.task_state(seeded["task_id"])) == "pending"
    assert await redis_client.get(DagRedisKey.task_remaining_deps(seeded["task_id"])) == "1"
    before = await _redis_snapshot(redis_client)
    findings = await _reconcile(redis_client, seeded)
    after = await _redis_snapshot(redis_client)
    assert before == after
    assert len(findings) == 1
    assert findings[0].status is ReconciliationStatus.BLOCKED_EVIDENCE
    assert findings[0].severity is ReconciliationSeverity.WARNING
    assert findings[0].reason_code is ReconciliationReason.DEPENDENCY_FANOUT_EVIDENCE_REQUIRED


async def test_task_admit_dependency_unlock_requires_cross_task_evidence(redis_client):
    seeded = await _golden_dependency_fanout_child(
        redis_client, "unlock", dependency_count=1, failed=False
    )
    assert await redis_client.get(DagRedisKey.task_state(seeded["task_id"])) == "ready"
    assert await redis_client.get(DagRedisKey.task_remaining_deps(seeded["task_id"])) == "0"
    assert await redis_client.zscore(
        DagRedisKey.task_ready_queue(seeded["tenant_id"]), seeded["task_id"]
    ) is not None
    before = await _redis_snapshot(redis_client)
    findings = await _reconcile(redis_client, seeded)
    after = await _redis_snapshot(redis_client)
    assert before == after
    assert len(findings) == 1
    assert findings[0].status is ReconciliationStatus.BLOCKED_EVIDENCE
    assert findings[0].severity is ReconciliationSeverity.WARNING
    assert findings[0].reason_code is ReconciliationReason.DEPENDENCY_FANOUT_EVIDENCE_REQUIRED


async def test_task_admit_dependency_failure_requires_cross_task_evidence(redis_client):
    seeded = await _golden_dependency_fanout_child(
        redis_client, "failure", dependency_count=2, failed=True
    )
    assert (
        await redis_client.get(DagRedisKey.task_state(seeded["task_id"]))
        == "blocked_by_failure"
    )
    before = await _redis_snapshot(redis_client)
    findings = await _reconcile(redis_client, seeded)
    after = await _redis_snapshot(redis_client)
    assert before == after
    assert len(findings) == 1
    assert findings[0].status is ReconciliationStatus.BLOCKED_EVIDENCE
    assert findings[0].severity is ReconciliationSeverity.WARNING
    assert findings[0].reason_code is ReconciliationReason.DEPENDENCY_FANOUT_EVIDENCE_REQUIRED



async def test_fanout_identity_drift_not_masked(redis_client):
    seeded = await _golden_dependency_fanout_child(
        redis_client, "r1-2-identity", dependency_count=2, failed=False
    )
    assert await redis_client.get(DagRedisKey.task_remaining_deps(seeded["task_id"])) == "1"
    await redis_client.hset(
        DagRedisKey.task_meta(seeded["task_id"]),
        "tenant_id",
        "corrupt-tenant",
    )
    findings = await _reconcile(redis_client, seeded)
    assert {f.status for f in findings} == {ReconciliationStatus.DRIFT}
    assert ReconciliationReason.TASK_META_IDENTITY_MISMATCH in _reasons(findings)
    assert ReconciliationReason.DEPENDENCY_FANOUT_EVIDENCE_REQUIRED not in _reasons(findings)


async def test_fanout_run_membership_drift_not_masked(redis_client):
    seeded = await _golden_dependency_fanout_child(
        redis_client, "r1-2-membership", dependency_count=1, failed=False
    )
    assert await redis_client.get(DagRedisKey.task_state(seeded["task_id"])) == "ready"
    await redis_client.srem(DagRedisKey.run_tasks(seeded["run_id"]), seeded["task_id"])
    findings = await _reconcile(redis_client, seeded)
    assert {f.status for f in findings} == {ReconciliationStatus.DRIFT}
    assert ReconciliationReason.PROJECTION_MEMBERSHIP_MISMATCH in _reasons(findings)
    assert ReconciliationReason.DEPENDENCY_FANOUT_EVIDENCE_REQUIRED not in _reasons(findings)


async def test_fanout_impossible_failure_shape_not_blocked(redis_client):
    seeded = await _golden_dependency_fanout_child(
        redis_client, "r1-2-impossible-failure", dependency_count=2, failed=True
    )
    assert await redis_client.get(DagRedisKey.task_state(seeded["task_id"])) == "blocked_by_failure"
    assert await redis_client.get(DagRedisKey.task_remaining_deps(seeded["task_id"])) == "2"
    await redis_client.set(DagRedisKey.task_ready_emitted(seeded["task_id"]), "1")
    findings = await _reconcile(redis_client, seeded)
    assert {f.status for f in findings} == {ReconciliationStatus.DRIFT}
    assert ReconciliationReason.DEPENDENCY_FANOUT_EVIDENCE_REQUIRED not in _reasons(findings)


async def test_pending_ready_emitted_corruption_not_consistent(redis_client):
    seeded = await _golden_task_admit(
        redis_client, "r1-2-pending-marker", dependency_count=2
    )
    assert await redis_client.get(DagRedisKey.task_state(seeded["task_id"])) == "pending"
    assert await redis_client.get(DagRedisKey.task_remaining_deps(seeded["task_id"])) == "2"
    await redis_client.set(DagRedisKey.task_ready_emitted(seeded["task_id"]), "1")
    findings = await _reconcile(redis_client, seeded)
    assert {f.status for f in findings} == {ReconciliationStatus.DRIFT}
    assert ReconciliationReason.PROJECTION_VALUE_MISMATCH in _reasons(findings)
    assert ReconciliationReason.DEPENDENCY_FANOUT_EVIDENCE_REQUIRED not in _reasons(findings)


async def test_run_create_consistent_pending_to_admitted_mapping(redis_client):
    seeded = await _seed_run_create(redis_client, "rc-consistent")
    findings = await _reconcile(redis_client, seeded)
    assert len(findings) == 1
    assert findings[0].status is ReconciliationStatus.CONSISTENT


async def test_run_create_state_drift(redis_client):
    seeded = await _seed_run_create(redis_client, "rc-state")
    await redis_client.set(RedisKey.run_state(seeded["run_id"]), "pending")
    findings = await _reconcile(redis_client, seeded)
    assert ReconciliationReason.RUN_STATE_MISMATCH in _reasons(findings)


async def test_run_create_projection_receipt_proof_drift(redis_client):
    seeded = await _seed_run_create(redis_client, "rc-proof")
    await redis_client.hset(seeded["receipt_key"], "canonical_transition_id", "wrong")
    findings = await _reconcile(redis_client, seeded)
    assert ReconciliationReason.CANONICAL_PROOF_MISMATCH in _reasons(findings)


async def test_task_admit_ready_consistent_without_invented_canonical_meta(redis_client):
    seeded = await _seed_task_admit(redis_client, "admit-ready", ready=True)
    meta = await redis_client.hgetall(DagRedisKey.task_meta(seeded["task_id"]))
    assert all(
        field.encode() not in meta and field not in meta
        for field in (
            "canonical_transition_id",
            "canonical_record_hash",
            "canonical_command_hash",
            "canonical_revision",
            "canonical_operation_id",
        )
    )
    findings = await _reconcile(redis_client, seeded)
    assert {f.status for f in findings} == {ReconciliationStatus.CONSISTENT}


async def test_task_admit_pending_consistent(redis_client):
    seeded = await _seed_task_admit(redis_client, "admit-pending", ready=False)
    findings = await _reconcile(redis_client, seeded)
    assert {f.status for f in findings} == {ReconciliationStatus.CONSISTENT}


async def test_task_admit_ready_membership_drift(redis_client):
    seeded = await _seed_task_admit(redis_client, "admit-member", ready=True)
    await redis_client.zrem(DagRedisKey.task_ready_queue(seeded["tenant_id"]), seeded["task_id"])
    findings = await _reconcile(redis_client, seeded)
    assert ReconciliationReason.READY_QUEUE_MEMBERSHIP_MISSING in _reasons(findings)


async def test_task_admit_ready_score_drift(redis_client):
    seeded = await _seed_task_admit(redis_client, "admit-score", ready=True)
    await redis_client.zadd(DagRedisKey.task_ready_queue(seeded["tenant_id"]), {seeded["task_id"]: 9999.0})
    findings = await _reconcile(redis_client, seeded)
    assert ReconciliationReason.READY_QUEUE_SCORE_MISMATCH in _reasons(findings)


async def test_task_admit_pending_illegal_ready_membership(redis_client):
    seeded = await _seed_task_admit(redis_client, "admit-illegal-ready", ready=False)
    await redis_client.zadd(DagRedisKey.task_ready_queue(seeded["tenant_id"]), {seeded["task_id"]: 2000.0})
    findings = await _reconcile(redis_client, seeded)
    assert ReconciliationReason.PROJECTION_MEMBERSHIP_MISMATCH in _reasons(findings)


async def test_task_dispatch_consistent(redis_client):
    seeded = await _seed_task_dispatch(redis_client, "dispatch-ok")
    findings = await _reconcile(redis_client, seeded)
    assert {f.status for f in findings} == {ReconciliationStatus.CONSISTENT}


async def test_task_dispatch_ready_scheduled_running_membership_drift(redis_client):
    seeded = await _seed_task_dispatch(redis_client, "dispatch-members")
    await redis_client.zadd(DagRedisKey.task_ready_queue(seeded["tenant_id"]), {seeded["task_id"]: 2000.0})
    await redis_client.zrem(seeded["dispatch"].scheduled_zset, seeded["task_id"])
    await redis_client.zadd(seeded["dispatch"].running_zset, {seeded["task_id"]: 3000.0})
    findings = await _reconcile(redis_client, seeded)
    assert ReconciliationReason.PROJECTION_MEMBERSHIP_MISMATCH in _reasons(findings)
    assert ReconciliationReason.RUNNING_INDEX_MEMBERSHIP_PRESENT in _reasons(findings)


async def test_task_dispatch_scheduled_score_drift(redis_client):
    seeded = await _seed_task_dispatch(redis_client, "dispatch-score")
    await redis_client.zadd(seeded["dispatch"].scheduled_zset, {seeded["task_id"]: 3333.0})
    findings = await _reconcile(redis_client, seeded)
    assert ReconciliationReason.PROJECTION_SCORE_MISMATCH in _reasons(findings)


async def test_task_dispatch_canonical_proof_drift(redis_client):
    seeded = await _seed_task_dispatch(redis_client, "dispatch-proof")
    await redis_client.hset(DagRedisKey.task_meta(seeded["task_id"]), "canonical_record_hash", "0" * 64)
    findings = await _reconcile(redis_client, seeded)
    assert ReconciliationReason.CANONICAL_PROOF_MISMATCH in _reasons(findings)


async def test_task_claim_consistent(redis_client):
    seeded = await _seed_task_claim(redis_client, "claim-ok")
    findings = await _reconcile(redis_client, seeded)
    assert {f.status for f in findings} == {ReconciliationStatus.CONSISTENT}


async def test_task_claim_claim_proof_drift(redis_client):
    seeded = await _seed_task_claim(redis_client, "claim-proof")
    await redis_client.hset(DagRedisKey.task_meta(seeded["task_id"]), "claim_canonical_transition_id", "wrong")
    findings = await _reconcile(redis_client, seeded)
    assert ReconciliationReason.CANONICAL_PROOF_MISMATCH in _reasons(findings)


async def test_task_claim_dispatch_predecessor_projection_drift(redis_client):
    seeded = await _seed_task_claim(redis_client, "claim-predecessor")
    await redis_client.hset(DagRedisKey.task_meta(seeded["task_id"]), "dispatch_canonical_record_hash", "0" * 64)
    findings = await _reconcile(redis_client, seeded)
    assert ReconciliationReason.CLAIM_PREDECESSOR_PROOF_MISMATCH in _reasons(findings)


async def test_task_claim_running_membership_drift(redis_client):
    seeded = await _seed_task_claim(redis_client, "claim-running")
    await redis_client.zrem(DagRedisKey.task_running_zset(seeded["tenant_id"]), seeded["task_id"])
    findings = await _reconcile(redis_client, seeded)
    assert ReconciliationReason.PROJECTION_MEMBERSHIP_MISMATCH in _reasons(findings)


async def test_task_claim_ownership_postimage_drift(redis_client):
    seeded = await _seed_task_claim(redis_client, "claim-owner")
    await redis_client.hset(
        DagRedisKey.worker_reservation(seeded["worker_id"]),
        mapping={"worker_id": seeded["worker_id"], "task_id": seeded["task_id"]},
    )
    await redis_client.hset(
        DagRedisKey.task_reservation_owner(seeded["task_id"]),
        mapping={"worker_id": seeded["worker_id"]},
    )
    findings = await _reconcile(redis_client, seeded)
    assert ReconciliationReason.OWNERSHIP_PROJECTION_MISMATCH in _reasons(findings)


class _HeartbeatBetweenReads:
    def __init__(self, inner, redis_client, seeded):
        self.inner = inner
        self.redis = redis_client
        self.seeded = seeded
        self.calls = 0

    async def read_current_projection(self, *, canonical):
        self.calls += 1
        if self.calls == 2:
            await self.redis.hset(
                DagRedisKey.task_meta(self.seeded["task_id"]),
                mapping={
                    "last_heartbeat_at_ms": "4500",
                    "heartbeat_at_ms": "4500",
                    "heartbeat_owner": self.seeded["worker_id"],
                },
            )
            await self.redis.zadd(
                DagRedisKey.task_running_zset(self.seeded["tenant_id"]),
                {self.seeded["task_id"]: 4500.0},
            )
        return await self.inner.read_current_projection(canonical=canonical)


async def test_task_claim_legitimate_heartbeat_between_observations_does_not_false_fail(redis_client):
    seeded = await _seed_task_claim(redis_client, "claim-heartbeat")
    reader = _HeartbeatBetweenReads(CurrentHeadReconciliationRedisReader(redis_client), redis_client, seeded)
    findings = await _reconcile(redis_client, seeded, runtime_reader=reader)
    assert {f.status for f in findings} == {ReconciliationStatus.CONSISTENT}
    assert ReconciliationReason.PROJECTION_OBSERVATION_CHANGED not in _reasons(findings)


async def test_task_complete_consistent_with_retained_owner_fence(redis_client):
    seeded = await _seed_task_terminal(redis_client, "complete-ok", failed=False)
    meta = await redis_client.hmget(
        DagRedisKey.task_meta(seeded["task_id"]), "worker_instance_id", "scheduler_epoch"
    )
    assert [_text(v) for v in meta] == [seeded["worker_id"], seeded["scheduler_epoch"]]
    findings = await _reconcile(redis_client, seeded)
    assert {f.status for f in findings} == {ReconciliationStatus.CONSISTENT}


async def test_task_complete_output_mismatch(redis_client):
    seeded = await _seed_task_terminal(redis_client, "complete-output", failed=False)
    await redis_client.set(DagRedisKey.task_output(seeded["task_id"]), '{"wrong":true}')
    findings = await _reconcile(redis_client, seeded)
    assert ReconciliationReason.TASK_OUTPUT_MISMATCH in _reasons(findings)


async def test_task_complete_terminal_proof_mismatch(redis_client):
    seeded = await _seed_task_terminal(redis_client, "complete-proof", failed=False)
    await redis_client.hset(DagRedisKey.task_meta(seeded["task_id"]), "terminal_canonical_operation_id", "wrong")
    findings = await _reconcile(redis_client, seeded)
    assert ReconciliationReason.CANONICAL_PROOF_MISMATCH in _reasons(findings)


async def test_task_complete_stale_running_membership(redis_client):
    seeded = await _seed_task_terminal(redis_client, "complete-running", failed=False)
    await redis_client.zadd(DagRedisKey.task_running_zset(seeded["tenant_id"]), {seeded["task_id"]: 5000.0})
    findings = await _reconcile(redis_client, seeded)
    assert ReconciliationReason.RUNNING_INDEX_MEMBERSHIP_PRESENT in _reasons(findings)


async def test_task_fail_consistent_and_does_not_invent_output_absence_invariant(redis_client):
    seeded = await _seed_task_terminal(redis_client, "fail-ok", failed=True)
    await redis_client.set(DagRedisKey.task_output(seeded["task_id"]), "unowned-preexisting-value")
    findings = await _reconcile(redis_client, seeded)
    assert {f.status for f in findings} == {ReconciliationStatus.CONSISTENT}


async def test_task_fail_failure_metadata_mismatch(redis_client):
    seeded = await _seed_task_terminal(redis_client, "fail-meta", failed=True)
    await redis_client.hset(DagRedisKey.task_meta(seeded["task_id"]), "completion_reason", "wrong")
    findings = await _reconcile(redis_client, seeded)
    assert ReconciliationReason.PROJECTION_VALUE_MISMATCH in _reasons(findings)


async def test_task_fail_terminal_proof_mismatch(redis_client):
    seeded = await _seed_task_terminal(redis_client, "fail-proof", failed=True)
    await redis_client.hset(DagRedisKey.task_meta(seeded["task_id"]), "terminal_canonical_transition_id", "wrong")
    findings = await _reconcile(redis_client, seeded)
    assert ReconciliationReason.CANONICAL_PROOF_MISMATCH in _reasons(findings)


async def test_task_fail_stale_running_membership(redis_client):
    seeded = await _seed_task_terminal(redis_client, "fail-running", failed=True)
    await redis_client.zadd(DagRedisKey.task_running_zset(seeded["tenant_id"]), {seeded["task_id"]: 5000.0})
    findings = await _reconcile(redis_client, seeded)
    assert ReconciliationReason.RUNNING_INDEX_MEMBERSHIP_PRESENT in _reasons(findings)


@pytest.mark.parametrize("failed", [False, True], ids=["done", "failed"])
async def test_run_terminate_done_and_failed_consistent(redis_client, failed):
    seeded = await _seed_run_terminate(redis_client, f"rt-{'fail' if failed else 'done'}", failed=failed)
    findings = await _reconcile(redis_client, seeded)
    assert {f.status for f in findings} == {ReconciliationStatus.CONSISTENT}


async def test_run_terminate_run_meta_drift(redis_client):
    seeded = await _seed_run_terminate(redis_client, "rt-meta", failed=False)
    await redis_client.hset(RedisKey.run_meta(seeded["run_id"]), "finalization_source", "wrong")
    findings = await _reconcile(redis_client, seeded)
    assert ReconciliationReason.RUN_META_MISMATCH in _reasons(findings)


async def test_run_terminate_run_result_drift(redis_client):
    seeded = await _seed_run_terminate(redis_client, "rt-result", failed=False)
    await redis_client.hset(RedisKey.run_result(seeded["run_id"]), "payload", "{}")
    findings = await _reconcile(redis_client, seeded)
    assert ReconciliationReason.RUN_RESULT_MISMATCH in _reasons(findings)


async def test_run_terminate_stale_cp_running_membership(redis_client):
    seeded = await _seed_run_terminate(redis_client, "rt-running", failed=False)
    await redis_client.zadd(RedisKey.cp_running(), {seeded["run_id"]: 1.0})
    findings = await _reconcile(redis_client, seeded)
    assert ReconciliationReason.PROJECTION_MEMBERSHIP_MISMATCH in _reasons(findings)


@pytest.mark.parametrize(
    "operation",
    [
        OperationType.RUN_CREATE,
        OperationType.TASK_ADMIT,
        OperationType.TASK_DISPATCH,
        OperationType.TASK_CLAIM,
        OperationType.TASK_COMPLETE,
        OperationType.TASK_FAIL,
        OperationType.RUN_TERMINATE,
    ],
)
async def test_shared_wrong_state_redis_type_is_critical_schema_drift(redis_client, operation):
    seeded = await _seed_for_operation(redis_client, operation, f"wrong-type-{operation.value.lower()}")
    state_key = (
        RedisKey.run_state(seeded["run_id"])
        if seeded["task_id"] is None
        else DagRedisKey.task_state(seeded["task_id"])
    )
    await redis_client.delete(state_key)
    await redis_client.hset(state_key, mapping={"wrong": "type"})
    findings = await _reconcile(redis_client, seeded)
    schema = [f for f in findings if f.reason_code is ReconciliationReason.PROJECTION_SCHEMA_MISMATCH]
    assert schema
    assert all(f.status is ReconciliationStatus.DRIFT for f in schema)
    assert all(f.severity is ReconciliationSeverity.CRITICAL for f in schema)


async def test_shared_canonical_corruption_blocks_projection_comparison(redis_client):
    seeded = await _seed_task_admit(redis_client, "canonical-corrupt", ready=True)
    keyspace = seeded["store"].keyspace(seeded["record"].aggregate_identity_sha256)
    await redis_client.hset(
        keyspace.operation_records,
        keyspace.operation_field(seeded["record"].operation_id),
        "corrupt-envelope",
    )
    findings = await _reconcile(redis_client, seeded)
    assert {f.status for f in findings} == {ReconciliationStatus.BLOCKED_EVIDENCE}
    assert {f.reason_code for f in findings} == {ReconciliationReason.CANONICAL_RECORD_CORRUPTION}


class _CanonicalMutationOnSecondRead:
    def __init__(self, inner, mutate: Callable[[], Awaitable[None]]):
        self.inner = inner
        self.mutate = mutate
        self.calls = 0

    async def read_current_head(self, **kwargs):
        self.calls += 1
        if self.calls == 2:
            await self.mutate()
        return await self.inner.read_current_head(**kwargs)


async def test_shared_canonical_a_b_race_is_blocked(redis_client):
    seeded = await _seed_task_admit(redis_client, "canonical-race", ready=True)

    async def mutate():
        seed = seeded["seed"]
        dispatch = DagTaskDispatchInput(
            task_id=seed.task_id,
            run_id=seed.run_id,
            tenant_id=seed.tenant_id,
            worker_id="race-worker",
            scheduled_at=3001,
            admitted_at=seed.admitted_at,
            scheduled_zset=DagRedisKey.task_scheduled_zset(seed.tenant_id),
            running_zset=DagRedisKey.task_running_zset(seed.tenant_id),
            control_stream="control",
            shard_stream="shard",
            scheduler_epoch="8",
            attempt=1,
        )
        command = build_task_dispatch_command(dispatch, expected_revision=1)
        await _commit(seeded["store"], command, revision=1, state="ready", at_ms=3001)

    reader = _CanonicalMutationOnSecondRead(CurrentHeadCanonicalReconciliationReader(redis_client), mutate)
    findings = await _reconcile(redis_client, seeded, canonical_reader=reader)
    assert {f.status for f in findings} == {ReconciliationStatus.BLOCKED_EVIDENCE}
    assert {f.reason_code for f in findings} == {ReconciliationReason.CANONICAL_CHANGED_DURING_OBSERVATION}


class _RuntimeMutationOnSecondRead:
    def __init__(self, inner, mutate: Callable[[], Awaitable[None]]):
        self.inner = inner
        self.mutate = mutate
        self.calls = 0

    async def read_current_projection(self, *, canonical):
        self.calls += 1
        if self.calls == 2:
            await self.mutate()
        return await self.inner.read_current_projection(canonical=canonical)


async def test_shared_runtime_immutable_a_b_race_is_blocked(redis_client):
    seeded = await _seed_task_admit(redis_client, "runtime-race", ready=True)

    async def mutate():
        await redis_client.set(DagRedisKey.task_state(seeded["task_id"]), "pending")

    reader = _RuntimeMutationOnSecondRead(CurrentHeadReconciliationRedisReader(redis_client), mutate)
    findings = await _reconcile(redis_client, seeded, runtime_reader=reader)
    assert {f.status for f in findings} == {ReconciliationStatus.BLOCKED_EVIDENCE}
    assert {f.reason_code for f in findings} == {ReconciliationReason.PROJECTION_OBSERVATION_CHANGED}


@pytest.mark.parametrize(
    "operation",
    [
        OperationType.RUN_CREATE,
        OperationType.TASK_ADMIT,
        OperationType.TASK_DISPATCH,
        OperationType.TASK_CLAIM,
        OperationType.TASK_COMPLETE,
        OperationType.TASK_FAIL,
        OperationType.RUN_TERMINATE,
    ],
)
async def test_zero_mutation_for_every_85_0b_target_operation(redis_client, operation):
    seeded = await _seed_for_operation(redis_client, operation, f"zero-{operation.value.lower()}")
    before = await _redis_snapshot(redis_client, require_persistent=True)
    findings = await _reconcile(redis_client, seeded)
    after = await _redis_snapshot(redis_client, require_persistent=True)
    assert before == after
    assert findings
    assert all(f.mutation_attempted is False for f in findings)

    # TEST-ONLY mutation-sensitivity control. This runs only after the
    # reconciliation zero-mutation assertion and is not reconciler behavior.
    probe_key = f"hfa:test:s85b:zero-mutation-probe:{operation.value}"
    assert await redis_client.exists(probe_key) == 0
    await redis_client.set(probe_key, "test-only-logical-mutation")
    try:
        mutated = await _redis_snapshot(redis_client, require_persistent=True)
        assert mutated != after
    finally:
        await redis_client.delete(probe_key)
