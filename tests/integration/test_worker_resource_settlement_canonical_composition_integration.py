from __future__ import annotations

import asyncio
from dataclasses import dataclass

import pytest

import hfa_worker.consumer as consumer_module
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
from hfa.events.codec import serialize_event
from hfa.events.schema import RunRequestedEvent
from hfa.governance.admission_resource_reservation import (
    AdmissionResourceReservationInput,
    AdmissionResourceReservationManager,
    AdmissionResourceSettlementInput,
    AdmissionResourceSettlementResult,
    RESERVATION_STATE_FINALIZED,
    RESERVATION_STATE_SETTLED,
    RESERVATION_STATUS_ALREADY_SETTLED,
    RESERVATION_STATUS_CONFLICT,
    RESERVATION_STATUS_RESOURCE_STATE_CONFLICT,
)
from hfa_control.run_create_authority import (
    RunCreateAuthorityBinding,
    run_create_operation_id,
)
from hfa_control.run_terminate_authority import run_terminate_identity
from hfa_control.task_recovery import TaskRecoveryManager
from hfa_control.task_terminal_authority import (
    TASK_TERMINAL_PROJECTION_PENDING_STATUS,
    TaskTerminalAuthorityError,
)
from hfa_worker.consumer import CONSUMER_GROUP
from hfa_worker.main import WorkerService
from hfa_worker.task_executor import TaskExecutionResult, TaskExecutor

pytestmark = [pytest.mark.asyncio, pytest.mark.integration]


@pytest.fixture(autouse=True)
def _forbid_requeue(monkeypatch):
    async def forbidden(*_args, **_kwargs):
        raise AssertionError("TASK_REQUEUE must be unreachable in Sprint 84.8")
    monkeypatch.setattr(TaskRecoveryManager, "requeue_stale_task", forbidden)

    async def forbidden_legacy_tenant_decrement(*_args, **_kwargs):
        raise AssertionError("legacy tenant inflight decrement must be unreachable")

    monkeypatch.setattr(
        consumer_module,
        "decrement_tenant_inflight_if_needed",
        forbidden_legacy_tenant_decrement,
    )


@dataclass
class _Request:
    run_id: str
    tenant_id: str
    agent_type: str = "research"
    priority: int = 5
    payload: dict | None = None
    estimated_cost_cents: int = 100
    preferred_region: str = "eu-west-1"
    preferred_placement: str = "LEAST_LOADED"
    def __post_init__(self):
        if self.payload is None:
            self.payload = {"prompt": "84.8-worker"}


@dataclass
class _ExecutionResult:
    status: str = "done"
    payload: dict | None = None
    error: str = ""


class _ForbiddenLegacyExecutor:
    product_executor_capability = "executor:deterministic"
    async def execute(self, _event):
        raise AssertionError("legacy BaseExecutor.execute must be unreachable")


class _CountingTaskExecutor(TaskExecutor):
    def __init__(self, calls: list[str], result: TaskExecutionResult | None = None, *, forbid=False):
        self.calls = calls
        self.result = result or TaskExecutionResult(ok=True, output={"ok": True})
        self.forbid = forbid
    async def execute(self, ctx):
        if self.forbid:
            raise AssertionError("duplicate executor on terminal replay")
        self.calls.append(ctx.worker_instance_id)
        return self.result


def _text(value):
    return value.decode() if isinstance(value, bytes) else ("" if value is None else str(value))


def _ctx(command):
    return AuthorityEntryContext(
        authenticated_writer_id=f"test/{command.operation_type.value}",
        allowed_operations=frozenset({command.operation_type}),
        target_aggregate_identity_sha256=command.aggregate_identity.sha256,
        fence_required=command.operation_type is not OperationType.TASK_ADMIT,
        fence_valid=True,
    )


async def _commit(store, command, revision, state, at):
    evaluation = evaluate_authority_commit(
        context=_ctx(command), command=command, current_revision=revision,
        current_state=state, receipt_probe=None, committed_at_ms=at, correlation_id=None,
    )
    assert evaluation.decision.code is AuthorityDecisionCode.ACCEPTED
    assert evaluation.commit_plan is not None
    result = await store.commit(evaluation.commit_plan)
    assert result.status is RedisAuthorityCommitStatus.COMMITTED
    return evaluation.commit_plan.record


async def _migration_ready(redis):
    await redis.hset(RedisKey.run_terminal_event_index(), mapping={
        "__contract__:schema_version": "1",
        "__contract__:producer_contract_version": "1",
        "__migration__:status": "ready",
        "__migration__:results_stream_key": RedisKey.stream_results(),
        "__migration__:source_history_complete": "1",
    })
    await redis.persist(RedisKey.run_terminal_event_index())


async def _seed_resource_run(redis, run_id, tenant_id, *, cost=100):
    manager = AdmissionResourceReservationManager(redis)
    binding = RunCreateAuthorityBinding(redis=redis, resource_manager=manager, control_stream=RedisKey.stream_control())
    await binding.initialise()
    created = await binding.admit(
        _Request(run_id=run_id, tenant_id=tenant_id, estimated_cost_cents=cost),
        tenant_inflight_limit=10, concurrent_run_limit=10, budget_limit_cents=10_000,
    )
    operation_id = run_create_operation_id(created.run_id)
    receipt_key = manager.reservation_receipt_key(operation_id)
    assert (await redis.hgetall(receipt_key))["state"] == RESERVATION_STATE_FINALIZED
    await redis.set(RedisKey.run_state(run_id), "running")
    await redis.hset(RedisKey.run_meta(run_id), mapping={"run_id": run_id, "tenant_id": tenant_id, "state": "running"})
    await redis.zadd(RedisKey.cp_running(), {run_id: 50})
    return manager, binding.store, operation_id


async def _seed_dispatch(redis, task_id, run_id, tenant_id, worker_id, epoch):
    store = RedisCanonicalAuthorityStore(redis); await store.initialise()
    identity = CanonicalAggregateIdentity(aggregate_type=AggregateType.TASK, run_id=run_id, task_id=task_id)
    admit = AuthorityCommand(
        aggregate_identity=identity, operation_type=OperationType.TASK_ADMIT,
        operation_id=f"task-admit:v1:{identity.sha256}", expected_revision=0,
        intended_previous_state=None, intended_next_state="ready",
        authoritative_payload={"task_id": task_id, "run_id": run_id},
        authoritative_metadata_changes={"task_id": task_id, "run_id": run_id, "tenant_id": tenant_id},
        requested_child_effects={},
        requested_projection_intents=({"kind": "READY_QUEUE_IF_READY", "tenant_id": tenant_id, "task_id": task_id},),
    )
    await _commit(store, admit, 0, None, 100)
    metadata = {"task_id": task_id, "run_id": run_id, "tenant_id": tenant_id, "worker_id": worker_id,
                "scheduler_epoch": epoch, "dispatch_attempt": 1, "scheduled_at_ms": 200}
    dispatch = AuthorityCommand(
        aggregate_identity=identity, operation_type=OperationType.TASK_DISPATCH,
        operation_id=f"task-dispatch:v1:{identity.sha256}:attempt:1", expected_revision=1,
        intended_previous_state="ready", intended_next_state="scheduled",
        authoritative_payload=metadata, authoritative_metadata_changes=metadata,
        requested_child_effects={"worker_reservation": {"worker_id": worker_id, "task_id": task_id, "scheduler_epoch": epoch}},
        requested_projection_intents=({"kind": "CONTROL_NOTIFICATION", "stream": "control"}, {"kind": "TASK_REQUEST_MESSAGE", "stream": "shard"}),
    )
    record = await _commit(store, dispatch, 1, "ready", 200)
    await redis.set(DagRedisKey.task_state(task_id), "scheduled")
    await redis.hset(DagRedisKey.task_meta(task_id), mapping={
        "task_id": task_id, "run_id": run_id, "tenant_id": tenant_id, "scheduler_epoch": epoch,
        "dispatch_attempt": "1", "dispatch_worker_id": worker_id, "claim_epoch": "0",
        "canonical_transition_id": record.transition_id, "canonical_record_hash": record.canonical_record_hash,
        "canonical_command_hash": record.canonical_command_hash, "canonical_revision": str(record.to_revision),
        "canonical_operation_id": record.operation_id,
    })
    await redis.zadd(DagRedisKey.task_scheduled_zset(tenant_id), {task_id: 200.0})
    reservation = {"worker_id": worker_id, "task_id": task_id, "scheduler_epoch": epoch,
                   "reserved_at_ms": "190", "scheduler_id": "scheduler-84-8"}
    await redis.hset(DagRedisKey.worker_reservation(worker_id), mapping=reservation)
    await redis.hset(DagRedisKey.task_reservation_owner(task_id), mapping=reservation)
    await redis.expire(DagRedisKey.worker_reservation(worker_id), 60)
    await redis.expire(DagRedisKey.task_reservation_owner(task_id), 60)
    await redis.sadd(DagRedisKey.run_tasks(run_id), task_id)
    return store, identity


async def _worker(redis, worker_id, task_executor, *, reclaim_idle_ms=60_000):
    await _migration_ready(redis)
    service = WorkerService(redis, {
        "production": True, "worker_id": worker_id, "worker_group": "group-84-8", "region": "integration",
        "shards": [0], "capacity": 1, "executor": _ForbiddenLegacyExecutor(), "task_executor": task_executor,
        "canonical_task_admit_binding": True, "canonical_task_dispatch_binding": True,
        "canonical_task_claim_binding": True, "canonical_task_terminal_binding": True,
        "run_termination_binding_enabled": True, "canonical_resource_settlement_binding": True,
        "reclaim_idle_ms": reclaim_idle_ms,
    })
    await service.start()
    async def no_task(*_a, **_k): raise AssertionError("legacy DagLua.task_complete")
    async def no_run(*_a, **_k): raise AssertionError("legacy run_terminate_from_tasks.lua")
    service._dag_lua.task_complete = no_task
    service._run_termination_coordinator._loader.run = no_run
    return service


async def _enqueue(redis, task_id, run_id, tenant_id, epoch):
    stream = RedisKey.stream_shard(0)
    event = RunRequestedEvent(event_type="TaskRequested", task_id=task_id, run_id=run_id, tenant_id=tenant_id,
                              agent_type="84.8", payload={"prompt": "settle"}, scheduler_epoch=epoch)
    return stream, _text(await redis.xadd(stream, serialize_event(event)))


async def _pending(redis, stream):
    return await redis.xpending_range(stream, CONSUMER_GROUP, min="-", max="+", count=20)


async def _wait(check, timeout=5.0):
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        value = await check()
        if value:
            return value
        await asyncio.sleep(0.02)
    raise AssertionError("timed out")


async def _wait_pending(redis, stream, count):
    async def check():
        rows = await _pending(redis, stream)
        return True if len(rows) == count else None
    return await _wait(check)


async def _wait_value(redis, key, expected):
    async def check():
        return _text(await redis.get(key)) == expected
    return await _wait(check)


async def _wait_event(event: asyncio.Event, timeout=5.0):
    await asyncio.wait_for(event.wait(), timeout=timeout)


async def _close(service):
    await service.close(drain_timeout=0.1)


async def _snapshot(manager, tenant):
    return await manager.get_resource_snapshot(tenant)


async def test_a_non_last_task_acks_without_settlement(redis_client):
    t, sib, run, tenant, wid, epoch = "s8-a-task", "s8-a-sib", "s8-a-run", "s8-a-tenant", "s8-a-worker", "s8-a-epoch"
    manager, rstore, op = await _seed_resource_run(redis_client, run, tenant)
    await _seed_dispatch(redis_client, t, run, tenant, wid, epoch)
    await redis_client.sadd(DagRedisKey.run_tasks(run), sib); await redis_client.set(DagRedisKey.task_state(sib), "running")
    await redis_client.hset(DagRedisKey.task_meta(sib), mapping={"task_id": sib, "run_id": run, "tenant_id": tenant})
    before = await _snapshot(manager, tenant); calls=[]; service=await _worker(redis_client,wid,_CountingTaskExecutor(calls))
    stream,_=await _enqueue(redis_client,t,run,tenant,epoch)
    await _wait_value(redis_client, DagRedisKey.task_state(t), "done")
    await _wait_pending(redis_client,stream,0)
    assert (await redis_client.hgetall(manager.reservation_receipt_key(op)))["state"] == RESERVATION_STATE_FINALIZED
    assert await _snapshot(manager,tenant) == before and len(calls)==1
    rs=await rstore.get_aggregate_snapshot(run_terminate_identity(run)); assert rs.revision==1
    await _close(service)


async def test_b_last_success_settles_once_then_acks(redis_client):
    t,run,tenant,wid,epoch="s8-b-task","s8-b-run","s8-b-tenant","s8-b-worker","s8-b-epoch"
    manager,rstore,op=await _seed_resource_run(redis_client,run,tenant); await _seed_dispatch(redis_client,t,run,tenant,wid,epoch)
    calls=[]; service=await _worker(redis_client,wid,_CountingTaskExecutor(calls)); stream,_=await _enqueue(redis_client,t,run,tenant,epoch)
    await _wait_value(redis_client, RedisKey.run_state(run), "done")
    await _wait_pending(redis_client,stream,0)
    assert (await redis_client.hgetall(manager.reservation_receipt_key(op)))["state"]==RESERVATION_STATE_SETTLED
    assert await _snapshot(manager,tenant)=={"concurrent_runs":0,"budget_reserved_cents":0,"tenant_inflight":0}
    rs=await rstore.get_aggregate_snapshot(run_terminate_identity(run)); assert rs.revision==2 and rs.state=="done" and len(calls)==1
    await _close(service)


async def test_c_last_failure_settles_once_then_acks(redis_client):
    t,run,tenant,wid,epoch="s8-c-task","s8-c-run","s8-c-tenant","s8-c-worker","s8-c-epoch"
    manager,rstore,op=await _seed_resource_run(redis_client,run,tenant); await _seed_dispatch(redis_client,t,run,tenant,wid,epoch)
    calls=[]; service=await _worker(redis_client,wid,_CountingTaskExecutor(calls,TaskExecutionResult(ok=False,output={},error="boom"))); stream,_=await _enqueue(redis_client,t,run,tenant,epoch)
    await _wait_value(redis_client, RedisKey.run_state(run), "failed")
    await _wait_pending(redis_client,stream,0)
    assert (await redis_client.hgetall(manager.reservation_receipt_key(op)))["state"]==RESERVATION_STATE_SETTLED
    rs=await rstore.get_aggregate_snapshot(run_terminate_identity(run)); assert rs.revision==2 and rs.state=="failed"
    await _close(service)


async def test_d_run_commit_settlement_failure_keeps_finalized_and_pending(redis_client, monkeypatch):
    t,run,tenant,wid,epoch="s8-d-task","s8-d-run","s8-d-tenant","s8-d-worker","s8-d-epoch"
    manager,rstore,op=await _seed_resource_run(redis_client,run,tenant); await _seed_dispatch(redis_client,t,run,tenant,wid,epoch)
    calls=[]; service=await _worker(redis_client,wid,_CountingTaskExecutor(calls)); settlement_failed=asyncio.Event()
    async def fail(value, **kwargs):
        settlement_failed.set()
        return AdmissionResourceSettlementResult(RESERVATION_STATUS_RESOURCE_STATE_CONFLICT,value.run_create_operation_id,value.proof_sha256,False,RESERVATION_STATE_FINALIZED)
    monkeypatch.setattr(service._resource_settlement_manager,"settle_once",fail)
    stream,_=await _enqueue(redis_client,t,run,tenant,epoch)
    await _wait_event(settlement_failed)
    rs=await rstore.get_aggregate_snapshot(run_terminate_identity(run)); assert rs.revision==2
    assert (await redis_client.hgetall(manager.reservation_receipt_key(op)))["state"]==RESERVATION_STATE_FINALIZED
    assert await _snapshot(manager,tenant)=={"concurrent_runs":1,"budget_reserved_cents":100,"tenant_inflight":1}
    assert _text(await redis_client.get(RedisKey.run_state(run)))=="running"
    assert len(await _pending(redis_client,stream))==1
    await _close(service)


async def test_e_restart_reclaim_after_settlement_pending_reuses_run_revision(redis_client, monkeypatch):
    t,run,tenant,worker_a,worker_b,epoch=(
        "s8-e-task","s8-e-run","s8-e-tenant","s8-e-worker-a","s8-e-worker-b","s8-e-epoch"
    )
    manager,rstore,op=await _seed_resource_run(redis_client,run,tenant)
    await _seed_dispatch(redis_client,t,run,tenant,worker_a,epoch)
    calls=[]
    first=await _worker(redis_client,worker_a,_CountingTaskExecutor(calls))
    rm=first._resource_settlement_manager
    original_settle=rm.settle_once
    first_call=True
    settlement_failed=asyncio.Event()

    async def fail_once(value, **kwargs):
        nonlocal first_call
        if first_call:
            first_call=False
            settlement_failed.set()
            return AdmissionResourceSettlementResult(
                RESERVATION_STATUS_RESOURCE_STATE_CONFLICT,
                value.run_create_operation_id,
                value.proof_sha256,
                False,
                RESERVATION_STATE_FINALIZED,
            )
        return await original_settle(value,**kwargs)

    monkeypatch.setattr(rm,"settle_once",fail_once)
    stream,_=await _enqueue(redis_client,t,run,tenant,epoch)
    await _wait_event(settlement_failed)
    before=await rstore.get_aggregate_snapshot(run_terminate_identity(run))
    assert before.revision==2
    receipt_key=manager.reservation_receipt_key(op)
    assert (await redis_client.hgetall(receipt_key))["state"]==RESERVATION_STATE_FINALIZED
    assert await _snapshot(manager,tenant)=={
        "concurrent_runs":1,"budget_reserved_cents":100,"tenant_inflight":1
    }
    await _close(first)

    pending_before=await _pending(redis_client,stream)
    assert len(pending_before)==1
    assert _text(pending_before[0].get("consumer"))==worker_a

    original_xclaim=redis_client.xclaim
    xclaim_calls=[]

    async def recording_xclaim(*args, **kwargs):
        xclaim_calls.append((args,kwargs))
        return await original_xclaim(*args,**kwargs)

    monkeypatch.setattr(redis_client,"xclaim",recording_xclaim)
    second=await _worker(
        redis_client,worker_b,_CountingTaskExecutor(calls,forbid=True),reclaim_idle_ms=0
    )
    await _wait_value(redis_client,RedisKey.run_state(run),"done")
    await _wait_pending(redis_client,stream,0)

    assert xclaim_calls
    def claimed_consumer(call):
        args,kwargs=call
        if "consumername" in kwargs:
            return _text(kwargs["consumername"])
        if "consumer" in kwargs:
            return _text(kwargs["consumer"])
        return _text(args[2]) if len(args)>2 else ""
    assert any(claimed_consumer(call)==worker_b for call in xclaim_calls)

    after=await rstore.get_aggregate_snapshot(run_terminate_identity(run))
    assert after.revision==before.revision==2
    assert after.operation_id==before.operation_id
    assert len(calls)==1
    assert (await redis_client.hgetall(receipt_key))["state"]==RESERVATION_STATE_SETTLED
    settled_counters={"concurrent_runs":0,"budget_reserved_cents":0,"tenant_inflight":0}
    assert await _snapshot(manager,tenant)==settled_counters
    run_result=await redis_client.hgetall(RedisKey.run_result(run))
    assert _text(run_result.get("status"))=="done"

    reservation=AdmissionResourceReservationInput(
        operation_id=op,run_id=run,tenant_id=tenant,estimated_cost_cents=100
    )
    receipt=await manager.get_receipt(reservation)
    assert receipt is not None and receipt.state==RESERVATION_STATE_SETTLED
    replay=await manager.settle_once(
        AdmissionResourceSettlementInput(
            run_create_operation_id=reservation.operation_id,
            run_id=reservation.run_id,
            tenant_id=reservation.tenant_id,
            estimated_cost_cents=reservation.estimated_cost_cents,
            run_create_reservation_proof_sha256=reservation.proof_sha256,
            run_terminate_operation_id=receipt.run_terminate_operation_id,
            terminal_proof_sha256=receipt.terminal_proof_sha256,
            canonical_transition_id=receipt.canonical_transition_id,
            canonical_record_hash=receipt.canonical_record_hash,
            canonical_command_hash=receipt.canonical_command_hash,
            canonical_revision=receipt.canonical_revision,
            final_state=receipt.final_state,
        ),
        now_ms=(receipt.settled_at_ms or 0)+1,
    )
    assert replay.status==RESERVATION_STATUS_ALREADY_SETTLED
    assert replay.resource_mutated is False
    assert replay.state==RESERVATION_STATE_SETTLED
    assert await _snapshot(manager,tenant)==settled_counters
    assert await redis_client.ttl(receipt_key)==-1
    await _close(second)


async def test_f_settlement_success_run_projection_failure_no_ack(redis_client, monkeypatch):
    t,run,tenant,wid,epoch="s8-f-task","s8-f-run","s8-f-tenant","s8-f-worker","s8-f-epoch"
    manager,rstore,op=await _seed_resource_run(redis_client,run,tenant); await _seed_dispatch(redis_client,t,run,tenant,wid,epoch)
    calls=[]; service=await _worker(redis_client,wid,_CountingTaskExecutor(calls)); projection_failed=asyncio.Event()
    async def fail(_value):
        projection_failed.set()
        raise RuntimeError("run projection down")
    monkeypatch.setattr(service._run_terminate_authority_binding.projection_manager,"project",fail)
    stream,_=await _enqueue(redis_client,t,run,tenant,epoch)
    await _wait_event(projection_failed)
    rs=await rstore.get_aggregate_snapshot(run_terminate_identity(run)); assert rs.revision==2
    assert (await redis_client.hgetall(manager.reservation_receipt_key(op)))["state"]==RESERVATION_STATE_SETTLED
    assert await _snapshot(manager,tenant)=={"concurrent_runs":0,"budget_reserved_cents":0,"tenant_inflight":0}
    assert _text(await redis_client.get(RedisKey.run_state(run)))=="running"
    assert len(await _pending(redis_client,stream))==1
    await _close(service)


async def test_g_redelivery_after_projection_failure_never_decrements_twice(redis_client, monkeypatch):
    t,run,tenant,wid,epoch="s8-g-task","s8-g-run","s8-g-tenant","s8-g-worker","s8-g-epoch"
    manager,rstore,op=await _seed_resource_run(redis_client,run,tenant); await _seed_dispatch(redis_client,t,run,tenant,wid,epoch)
    calls=[]; first=await _worker(redis_client,wid,_CountingTaskExecutor(calls)); pm=first._run_terminate_authority_binding.projection_manager; original=pm.project; once=True; projection_failed=asyncio.Event()
    async def fail_once(value):
        nonlocal once
        if once:
            once=False
            projection_failed.set()
            raise RuntimeError("projection down")
        return await original(value)
    monkeypatch.setattr(pm,"project",fail_once)
    stream,_=await _enqueue(redis_client,t,run,tenant,epoch)
    await _wait_event(projection_failed)
    before=await _snapshot(manager,tenant); rs=await rstore.get_aggregate_snapshot(run_terminate_identity(run)); assert rs.revision==2
    assert (await redis_client.hgetall(manager.reservation_receipt_key(op)))["state"]==RESERVATION_STATE_SETTLED
    assert len(await _pending(redis_client,stream))==1
    await _close(first)
    second=await _worker(redis_client,wid,_CountingTaskExecutor(calls,forbid=True),reclaim_idle_ms=0); await _wait_pending(redis_client,stream,0)
    after=await rstore.get_aggregate_snapshot(run_terminate_identity(run)); assert after.revision==rs.revision==2 and after.operation_id==rs.operation_id
    assert await _snapshot(manager,tenant)==before and len(calls)==1
    assert _text(await redis_client.get(RedisKey.run_state(run)))=="done"
    await _close(second)


async def test_h_ack_loss_replay_is_delivery_only(redis_client, monkeypatch):
    t,run,tenant,wid,epoch="s8-h-task","s8-h-run","s8-h-tenant","s8-h-worker","s8-h-epoch"
    manager,rstore,op=await _seed_resource_run(redis_client,run,tenant); await _seed_dispatch(redis_client,t,run,tenant,wid,epoch)
    calls=[]; first=await _worker(redis_client,wid,_CountingTaskExecutor(calls)); original_ack=consumer_module.ack_message; once=True; ack_lost=asyncio.Event()
    async def lose_once(*args,**kwargs):
        nonlocal once
        if once:
            once=False
            ack_lost.set()
            raise RuntimeError("ack lost")
        return await original_ack(*args,**kwargs)
    monkeypatch.setattr(consumer_module,"ack_message",lose_once)
    stream,_=await _enqueue(redis_client,t,run,tenant,epoch)
    await _wait_event(ack_lost)
    rb=await rstore.get_aggregate_snapshot(run_terminate_identity(run)); counters=await _snapshot(manager,tenant); assert rb.revision==2
    assert (await redis_client.hgetall(manager.reservation_receipt_key(op)))["state"]==RESERVATION_STATE_SETTLED
    assert counters=={"concurrent_runs":0,"budget_reserved_cents":0,"tenant_inflight":0}
    assert _text(await redis_client.get(RedisKey.run_state(run)))=="done"
    assert len(await _pending(redis_client,stream))==1
    await _close(first)
    second=await _worker(redis_client,wid,_CountingTaskExecutor(calls,forbid=True),reclaim_idle_ms=0); await _wait_pending(redis_client,stream,0)
    ra=await rstore.get_aggregate_snapshot(run_terminate_identity(run)); assert ra.revision==rb.revision and ra.operation_id==rb.operation_id
    assert await _snapshot(manager,tenant)==counters and len(calls)==1
    await _close(second)


async def test_i_resource_proof_conflict_fails_closed_without_projection_or_ack(redis_client, monkeypatch):
    t,run,tenant,wid,epoch="s8-i-task","s8-i-run","s8-i-tenant","s8-i-worker","s8-i-epoch"
    manager,rstore,op=await _seed_resource_run(redis_client,run,tenant); await _seed_dispatch(redis_client,t,run,tenant,wid,epoch)
    calls=[]; service=await _worker(redis_client,wid,_CountingTaskExecutor(calls)); before=await _snapshot(manager,tenant); settlement_conflict=asyncio.Event()
    async def conflict(value, **kwargs):
        settlement_conflict.set()
        return AdmissionResourceSettlementResult(RESERVATION_STATUS_CONFLICT,value.run_create_operation_id,value.proof_sha256,False,RESERVATION_STATE_FINALIZED)
    monkeypatch.setattr(service._resource_settlement_manager,"settle_once",conflict)
    stream,_=await _enqueue(redis_client,t,run,tenant,epoch)
    await _wait_event(settlement_conflict)
    rs=await rstore.get_aggregate_snapshot(run_terminate_identity(run)); assert rs.revision==2
    assert await _snapshot(manager,tenant)==before
    assert (await redis_client.hgetall(manager.reservation_receipt_key(op)))["state"]==RESERVATION_STATE_FINALIZED
    assert _text(await redis_client.get(RedisKey.run_state(run)))=="running"
    assert not await redis_client.exists(RedisKey.run_result(run))
    assert len(await _pending(redis_client,stream))==1
    await _close(service)


async def test_j_task_terminal_projection_failure_never_reaches_run_or_settlement(redis_client, monkeypatch):
    t,run,tenant,wid,epoch="s8-j-task","s8-j-run","s8-j-tenant","s8-j-worker","s8-j-epoch"
    manager,rstore,op=await _seed_resource_run(redis_client,run,tenant); task_store,task_identity=await _seed_dispatch(redis_client,t,run,tenant,wid,epoch)
    calls=[]; service=await _worker(redis_client,wid,_CountingTaskExecutor(calls)); before=await _snapshot(manager,tenant); settle_calls=[]; task_projection_failed=asyncio.Event()
    async def fail_projection(_projection):
        task_projection_failed.set()
        raise TaskTerminalAuthorityError(status=TASK_TERMINAL_PROJECTION_PENDING_STATUS,detail="forced",canonical_commit_durable=True)
    async def forbidden_settle(*a,**k):
        settle_calls.append(1); raise AssertionError("settlement before TASK projection")
    monkeypatch.setattr(service._task_terminal_authority_binding.projection_manager,"project",fail_projection)
    monkeypatch.setattr(service._resource_settlement_manager,"settle_once",forbidden_settle)
    stream,_=await _enqueue(redis_client,t,run,tenant,epoch)
    await _wait_event(task_projection_failed)
    task_snapshot=await task_store.get_aggregate_snapshot(task_identity)
    assert task_snapshot is not None and task_snapshot.revision==4 and task_snapshot.state=="done"
    rs=await rstore.get_aggregate_snapshot(run_terminate_identity(run)); assert rs.revision==1 and rs.state=="pending"
    assert settle_calls==[]
    assert await _snapshot(manager,tenant)==before
    assert (await redis_client.hgetall(manager.reservation_receipt_key(op)))["state"]==RESERVATION_STATE_FINALIZED
    assert _text(await redis_client.get(RedisKey.run_state(run)))=="running"
    assert len(await _pending(redis_client,stream))==1
    await _close(service)
