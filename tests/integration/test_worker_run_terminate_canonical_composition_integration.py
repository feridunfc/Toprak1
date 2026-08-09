from __future__ import annotations

from dataclasses import dataclass
import pytest

from hfa.authority import (AggregateType, AuthorityCommand, AuthorityDecisionCode,
    AuthorityEntryContext, CanonicalAggregateIdentity, OperationType,
    RedisAuthorityCommitStatus, RedisCanonicalAuthorityStore, evaluate_authority_commit)
from hfa.config.keys import RedisKey
from hfa.dag.schema import DagRedisKey
from hfa.events.codec import serialize_event
from hfa.events.schema import RunRequestedEvent
from hfa_control.run_create_authority import RunCreateAuthorityInput, build_run_create_command, build_run_create_context
from hfa_control.run_terminate_authority import run_terminate_identity
from hfa_control.task_terminal_authority import TASK_TERMINAL_PROJECTION_PENDING_STATUS, TaskTerminalAuthorityError
from hfa_worker.consumer import CONSUMER_GROUP
from hfa_worker.main import WorkerService
from hfa_worker.redis_utils import ensure_consumer_group
from hfa_worker.task_executor import TaskExecutionResult, TaskExecutor

pytestmark = [pytest.mark.asyncio, pytest.mark.integration]

@dataclass
class _ExecutionResult:
    status: str = "done"
    payload: dict | None = None
    error: str = ""

class _Executor:
    async def execute(self, event): return _ExecutionResult(payload={"run_id": event.run_id})

class _TaskExecutor(TaskExecutor):
    def __init__(self, result): self.result, self.calls, self.forbid = result, [], False
    async def execute(self, ctx):
        if self.forbid: raise AssertionError("TASK executor must not run during RUN-only redelivery recovery")
        self.calls.append(ctx); return self.result

def _text(v): return v.decode() if isinstance(v, bytes) else ("" if v is None else str(v))

def _ctx(command):
    return AuthorityEntryContext(authenticated_writer_id=f"test/{command.operation_type.value}",
        allowed_operations=frozenset({command.operation_type}),
        target_aggregate_identity_sha256=command.aggregate_identity.sha256,
        fence_required=command.operation_type is not OperationType.TASK_ADMIT, fence_valid=True)

async def _commit(store, command, revision, state, at):
    e=evaluate_authority_commit(context=_ctx(command), command=command, current_revision=revision,
        current_state=state, receipt_probe=None, committed_at_ms=at, correlation_id=None)
    assert e.decision.code is AuthorityDecisionCode.ACCEPTED and e.commit_plan is not None
    r=await store.commit(e.commit_plan); assert r.status is RedisAuthorityCommitStatus.COMMITTED
    return e.commit_plan.record

async def _seed_run(r, run_id, tenant_id):
    store=RedisCanonicalAuthorityStore(r); await store.initialise()
    cmd=build_run_create_command(RunCreateAuthorityInput(run_id=run_id, tenant_id=tenant_id,
        agent_type="research", priority=5, payload={"prompt":"84.7d"}, estimated_cost_cents=100,
        preferred_region="eu-west-1", preferred_placement="LEAST_LOADED", created_at_ms=50,
        control_stream=RedisKey.stream_control()))
    e=evaluate_authority_commit(context=build_run_create_context(cmd), command=cmd, current_revision=0,
        current_state=None, receipt_probe=None, committed_at_ms=50, correlation_id=None)
    assert e.commit_plan is not None
    result=await store.commit(e.commit_plan); assert result.status is RedisAuthorityCommitStatus.COMMITTED
    await r.set(RedisKey.run_state(run_id), "running")
    await r.hset(RedisKey.run_meta(run_id), mapping={"run_id":run_id,"tenant_id":tenant_id,"state":"running"})
    await r.zadd(RedisKey.cp_running(), {run_id:50})
    return store

async def _migration_ready(r):
    await r.hset(RedisKey.run_terminal_event_index(), mapping={
        "__contract__:schema_version":"1", "__contract__:producer_contract_version":"1",
        "__migration__:status":"ready", "__migration__:results_stream_key":RedisKey.stream_results(),
        "__migration__:source_history_complete":"1"})
    await r.persist(RedisKey.run_terminal_event_index())

async def _seed_dispatch(r, task_id, run_id, tenant_id, worker_id, epoch):
    store=RedisCanonicalAuthorityStore(r); await store.initialise()
    ident=CanonicalAggregateIdentity(aggregate_type=AggregateType.TASK, run_id=run_id, task_id=task_id)
    admit=AuthorityCommand(aggregate_identity=ident, operation_type=OperationType.TASK_ADMIT,
        operation_id=f"task-admit:v1:{ident.sha256}", expected_revision=0, intended_previous_state=None,
        intended_next_state="ready", authoritative_payload={"task_id":task_id,"run_id":run_id},
        authoritative_metadata_changes={"task_id":task_id,"run_id":run_id,"tenant_id":tenant_id},
        requested_child_effects={}, requested_projection_intents=({"kind":"READY_QUEUE_IF_READY","tenant_id":tenant_id,"task_id":task_id},))
    await _commit(store, admit, 0, None, 100)
    meta={"task_id":task_id,"run_id":run_id,"tenant_id":tenant_id,"worker_id":worker_id,
        "scheduler_epoch":epoch,"dispatch_attempt":1,"scheduled_at_ms":200}
    dispatch=AuthorityCommand(aggregate_identity=ident, operation_type=OperationType.TASK_DISPATCH,
        operation_id=f"task-dispatch:v1:{ident.sha256}:attempt:1", expected_revision=1,
        intended_previous_state="ready", intended_next_state="scheduled", authoritative_payload=meta,
        authoritative_metadata_changes=meta, requested_child_effects={"worker_reservation":{"worker_id":worker_id,"task_id":task_id,"scheduler_epoch":epoch}},
        requested_projection_intents=({"kind":"CONTROL_NOTIFICATION","stream":"control"},{"kind":"TASK_REQUEST_MESSAGE","stream":"shard"}))
    record=await _commit(store, dispatch, 1, "ready", 200)
    await r.set(DagRedisKey.task_state(task_id), "scheduled")
    await r.hset(DagRedisKey.task_meta(task_id), mapping={"task_id":task_id,"run_id":run_id,"tenant_id":tenant_id,
        "scheduler_epoch":epoch,"dispatch_attempt":"1","dispatch_worker_id":worker_id,"claim_epoch":"0",
        "canonical_transition_id":record.transition_id,"canonical_record_hash":record.canonical_record_hash,
        "canonical_command_hash":record.canonical_command_hash,"canonical_revision":str(record.to_revision),
        "canonical_operation_id":record.operation_id})
    await r.zadd(DagRedisKey.task_scheduled_zset(tenant_id), {task_id:200.0})
    reservation={"worker_id":worker_id,"task_id":task_id,"scheduler_epoch":epoch,"reserved_at_ms":"190","scheduler_id":"scheduler-84-7d"}
    await r.hset(DagRedisKey.worker_reservation(worker_id), mapping=reservation)
    await r.hset(DagRedisKey.task_reservation_owner(task_id), mapping=reservation)
    await r.expire(DagRedisKey.worker_reservation(worker_id),60); await r.expire(DagRedisKey.task_reservation_owner(task_id),60)
    await r.sadd(DagRedisKey.run_tasks(run_id), task_id)
    return store, ident

async def _sibling(r, task_id, run_id, tenant_id, state):
    await r.sadd(DagRedisKey.run_tasks(run_id),task_id); await r.set(DagRedisKey.task_state(task_id),state)
    await r.hset(DagRedisKey.task_meta(task_id), mapping={"task_id":task_id,"run_id":run_id,"tenant_id":tenant_id})

async def _worker(r, worker_id, executor):
    w=WorkerService(r,{"production":True,"worker_id":worker_id,"worker_group":"group-84-7d","region":"integration","shards":[0],"capacity":1,
        "executor":_Executor(),"task_executor":executor,"canonical_task_admit_binding":True,"canonical_task_dispatch_binding":True,
        "canonical_task_claim_binding":True,"canonical_task_terminal_binding":True,"run_termination_binding_enabled":True})
    assert w._run_termination_coordinator._task_completion_gateway is w._task_terminal_completion_gateway
    assert w._run_termination_coordinator._authority_binding is w._run_terminate_authority_binding
    await _migration_ready(r); await w._dag_lua.initialise(); await w._task_terminal_authority_binding.initialise(); await w._run_termination_coordinator.initialise()
    async def no_task(*a,**k): raise AssertionError("DagLua.task_complete must be unreachable in canonical Profile D")
    async def no_run(*a,**k): raise AssertionError("legacy run_terminate_from_tasks.lua must be unreachable in canonical Profile D")
    w._dag_lua.task_complete=no_task; w._run_termination_coordinator._loader.run=no_run
    return w

async def _enqueue(r, stream, event_type, task_id, run_id, tenant_id, worker_id, epoch):
    await ensure_consumer_group(r,stream,CONSUMER_GROUP,start_id="0",mkstream=True)
    event=RunRequestedEvent(event_type=event_type,task_id=task_id,run_id=run_id,tenant_id=tenant_id,
        agent_type="run-terminate-composition-proof",payload={"prompt":"84.7d"},scheduler_epoch=epoch)
    await r.xadd(stream,serialize_event(event))
    msgs=await r.xreadgroup(groupname=CONSUMER_GROUP,consumername=worker_id,streams={stream:">"},count=1,block=100)
    assert msgs; return msgs[0][1][0]

async def _pending(r,stream): return await r.xpending_range(stream,CONSUMER_GROUP,min="-",max="+",count=10)
async def _events(r,run_id):
    if await r.type(RedisKey.stream_results()) != "stream": return []
    return [f for _,f in await r.xrange(RedisKey.stream_results(),min="-",max="+") if _text(f.get("run_id"))==run_id]
async def _run_terminal(store,run_id,state):
    snap=await store.get_aggregate_snapshot(run_terminate_identity(run_id)); assert snap.revision==2 and snap.state==state
    probe=await store.load_receipt_probe(run_terminate_identity(run_id),snap.operation_id)
    assert probe.canonical_store_record.operation_type==OperationType.RUN_TERMINATE.value
    return snap

async def test_non_last_task_terminalizes_but_run_not_ready_and_message_acks(redis_client):
    t,s,run,tenant,wid,e="d-nonlast-task","d-sibling","d-nonlast-run","d-tenant","d-nonlast-worker","d-epoch"; stream=RedisKey.stream_shard(0)
    rs=await _seed_run(redis_client,run,tenant); ts,ti=await _seed_dispatch(redis_client,t,run,tenant,wid,e); await _sibling(redis_client,s,run,tenant,"running")
    w=await _worker(redis_client,wid,_TaskExecutor(TaskExecutionResult(ok=True,output={"ok":True}))); mid,data=await _enqueue(redis_client,stream,"TaskRequested",t,run,tenant,wid,e)
    await w._consumer._process_message(_text(mid),data,stream,0)
    snap=await ts.get_aggregate_snapshot(ti); assert snap.revision==4 and snap.state=="done"
    run_snap=await rs.get_aggregate_snapshot(run_terminate_identity(run)); assert run_snap.revision==1 and run_snap.state=="pending"
    assert _text(await redis_client.get(RedisKey.run_state(run)))=="running" and not await redis_client.exists(RedisKey.run_result(run)) and await _pending(redis_client,stream)==[]

async def test_last_task_success_canonical_run_terminate_done_then_acks(redis_client):
    t,run,tenant,wid,e="d-success-task","d-success-run","d-success-tenant","d-success-worker","d-success-epoch"; stream=RedisKey.stream_shard(0)
    rs=await _seed_run(redis_client,run,tenant); await _seed_dispatch(redis_client,t,run,tenant,wid,e); w=await _worker(redis_client,wid,_TaskExecutor(TaskExecutionResult(ok=True,output={"answer":42})))
    mid,data=await _enqueue(redis_client,stream,"TaskRequested",t,run,tenant,wid,e); await w._consumer._process_message(_text(mid),data,stream,0)
    await _run_terminal(rs,run,"done"); result=await redis_client.hgetall(RedisKey.run_result(run)); events=await _events(redis_client,run)
    assert _text(await redis_client.get(RedisKey.run_state(run)))=="done" and _text(result.get("status"))=="done" and len(events)==1 and _text(events[0].get("event_type"))=="RunCompleted" and await _pending(redis_client,stream)==[]

async def test_last_task_failure_canonical_run_terminate_failed_then_acks(redis_client):
    t,run,tenant,wid,e="d-fail-task","d-fail-run","d-fail-tenant","d-fail-worker","d-fail-epoch"; stream=RedisKey.stream_shard(0)
    rs=await _seed_run(redis_client,run,tenant); await _seed_dispatch(redis_client,t,run,tenant,wid,e); w=await _worker(redis_client,wid,_TaskExecutor(TaskExecutionResult(ok=False,output={},error="boom")))
    mid,data=await _enqueue(redis_client,stream,"RunRequested",t,run,tenant,wid,e); await w._consumer._process_message(_text(mid),data,stream,0)
    await _run_terminal(rs,run,"failed"); events=await _events(redis_client,run)
    assert _text(await redis_client.get(RedisKey.run_state(run)))=="failed" and len(events)==1 and _text(events[0].get("event_type"))=="RunFailed" and await _pending(redis_client,stream)==[]

async def test_run_authority_commit_projection_failure_leaves_message_unacked(redis_client,monkeypatch):
    t,run,tenant,wid,e="d-run-proj-task","d-run-proj-run","d-run-proj-tenant","d-run-proj-worker","d-run-proj-epoch"; stream=RedisKey.stream_shard(0)
    rs=await _seed_run(redis_client,run,tenant); ts,ti=await _seed_dispatch(redis_client,t,run,tenant,wid,e); w=await _worker(redis_client,wid,_TaskExecutor(TaskExecutionResult(ok=True,output={"durable":True})))
    async def fail(_): raise RuntimeError("forced RUN projection failure")
    monkeypatch.setattr(w._run_terminate_authority_binding.projection_manager,"project",fail)
    mid,data=await _enqueue(redis_client,stream,"TaskRequested",t,run,tenant,wid,e); await w._consumer._process_message(_text(mid),data,stream,0)
    tsnap=await ts.get_aggregate_snapshot(ti); assert tsnap.revision==4 and _text(await redis_client.get(DagRedisKey.task_state(t)))=="done"
    await _run_terminal(rs,run,"done"); assert _text(await redis_client.get(RedisKey.run_state(run)))=="running" and not await redis_client.exists(RedisKey.run_result(run)) and len(await _pending(redis_client,stream))==1

async def test_redelivery_repairs_only_run_projection_without_task_reexecution(redis_client,monkeypatch):
    t,run,tenant,wid,e="d-replay-task","d-replay-run","d-replay-tenant","d-replay-worker","d-replay-epoch"; stream=RedisKey.stream_shard(0)
    rs=await _seed_run(redis_client,run,tenant); ts,ti=await _seed_dispatch(redis_client,t,run,tenant,wid,e); ex=_TaskExecutor(TaskExecutionResult(ok=True,output={"once":True})); w=await _worker(redis_client,wid,ex)
    pm=w._run_terminate_authority_binding.projection_manager; original=pm.project; first=True
    async def fail_once(value):
        nonlocal first
        if first: first=False; raise RuntimeError("first RUN projection fails")
        return await original(value)
    monkeypatch.setattr(pm,"project",fail_once)
    mid,data=await _enqueue(redis_client,stream,"TaskRequested",t,run,tenant,wid,e); await w._consumer._process_message(_text(mid),data,stream,0)
    tb=await ts.get_aggregate_snapshot(ti); rb=await rs.get_aggregate_snapshot(run_terminate_identity(run)); assert tb.revision==4 and rb.revision==2 and len(ex.calls)==1
    ex.forbid=True; claimed=await redis_client.xclaim(stream,CONSUMER_GROUP,wid,min_idle_time=0,message_ids=[_text(mid)]); assert len(claimed)==1
    rid,rdata=claimed[0]; await w._consumer._process_message(_text(rid),rdata,stream,0)
    ta=await ts.get_aggregate_snapshot(ti); ra=await rs.get_aggregate_snapshot(run_terminate_identity(run))
    assert ta.revision==tb.revision==4 and ta.operation_id==tb.operation_id and ra.revision==rb.revision==2 and ra.operation_id==rb.operation_id and len(ex.calls)==1
    assert _text(await redis_client.get(RedisKey.run_state(run)))=="done" and len(await _events(redis_client,run))==1 and await _pending(redis_client,stream)==[]

async def test_task_projection_failure_never_attempts_run_terminate_and_does_not_ack(redis_client,monkeypatch):
    t,run,tenant,wid,e="d-task-proj-task","d-task-proj-run","d-task-proj-tenant","d-task-proj-worker","d-task-proj-epoch"; stream=RedisKey.stream_shard(0)
    rs=await _seed_run(redis_client,run,tenant); ts,ti=await _seed_dispatch(redis_client,t,run,tenant,wid,e); w=await _worker(redis_client,wid,_TaskExecutor(TaskExecutionResult(ok=True,output={"durable":True})))
    async def fail_task(_): raise TaskTerminalAuthorityError(status=TASK_TERMINAL_PROJECTION_PENDING_STATUS,detail="forced TASK projection failure",canonical_commit_durable=True)
    async def no_run(**kwargs): raise AssertionError("RUN_TERMINATE must not be attempted before TASK projection succeeds")
    monkeypatch.setattr(w._task_terminal_authority_binding.projection_manager,"project",fail_task); monkeypatch.setattr(w._run_terminate_authority_binding,"terminate",no_run)
    mid,data=await _enqueue(redis_client,stream,"TaskRequested",t,run,tenant,wid,e); await w._consumer._process_message(_text(mid),data,stream,0)
    tsnap=await ts.get_aggregate_snapshot(ti); rsnap=await rs.get_aggregate_snapshot(run_terminate_identity(run))
    assert tsnap.revision==4 and tsnap.state=="done" and _text(await redis_client.get(DagRedisKey.task_state(t)))=="running"
    assert rsnap.revision==1 and rsnap.state=="pending" and _text(await redis_client.get(RedisKey.run_state(run)))=="running" and len(await _pending(redis_client,stream))==1
