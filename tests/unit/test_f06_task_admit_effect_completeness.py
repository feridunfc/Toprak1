"""F06 exact TASK_ADMIT effects, semantic late-failure and bounded retry oracles."""
import asyncio
import json
from dataclasses import replace
from uuid import uuid4

import pytest

from hfa.authority import RedisCanonicalAuthorityStore
from hfa.dag.schema import DagRedisKey, DagTaskSeed
from hfa_control.dag_lua import DagLua
from hfa_control.task_admit_authority import (
    TaskAdmitAuthorityError, TaskAdmitProjectionConflictError,
    TaskAdmitProjectionPendingError, build_task_admit_command,
)
from hfa_control.task_terminal_authority import TaskTerminalAuthorityBinding
from hfa_control.run_submission import SingleTaskRunSubmission
from tests.unit.test_f02a1_root_binding import alpha, root_redis, create, seed, request
from tests.unit.test_f04_4a_task_admit_projection_proof import setup
from tests.unit.test_f04_4c_task_claim_projection_proof import setup as claim_setup, claim
from tests.unit.test_f04_task_projection_proof import semantic_snapshot, write_counts
from tests.unit.test_f04_4c_task_claim_projection_proof import snapshot
from tests.unit.test_f04_run_create_projection_proof import inject_fault
from tests.integration.test_task_dispatch_authority_binding_integration import dispatch


async def capture(alpha, redis, shape='root'):
    if shape == 'root':
        _, store, dag, item, task = await setup(alpha)
    else:
        item = DagTaskSeed(task_id='f06-task-'+str(uuid4()), run_id='f06-run-'+str(uuid4()),
            tenant_id='f06', agent_type='default', priority=2, admitted_at=1000,
            dependency_count=2, child_task_ids=('child-one','child-two'), payload_json='{}')
        dag = DagLua(redis, canonical_task_admit_binding=True, require_task_owner_binding=True)
        await dag.initialise()
        store = RedisCanonicalAuthorityStore(redis)
        await store.initialise()
        task = build_task_admit_command(item).aggregate_identity
    call = {}
    original = dag._admit_loader.run
    async def crash(**kw):
        call.update(kw)
        raise RuntimeError('controlled crash after durable admission before first effect')
    dag._admit_loader.run = crash
    try:
        with pytest.raises(TaskAdmitProjectionPendingError):
            await dag.task_admit(item)
    finally:
        dag._admit_loader.run = original
    assert (await store.get_aggregate_snapshot(task)).revision == 1
    return store, dag, item, task, call


async def reject_retry(redis, dag, item, status='task_admit_effect_conflict'):
    before, counts = await snapshot(redis), await write_counts(redis)
    with pytest.raises(TaskAdmitProjectionConflictError) as error:
        await dag.task_admit(item)
    assert error.value.status == status
    assert error.value.canonical_commit_durable and not error.value.retry_safe
    assert error.value.recovery_owner == 'operations/task-admission'
    assert not error.value.legacy_projection_completed
    assert await snapshot(redis) == before
    assert await write_counts(redis) == counts


@pytest.mark.asyncio
@pytest.mark.parametrize('shape', ['root','waiting'])
async def test_first_exact_duplicate_lost_ack_and_restored_empty_retry(alpha, root_redis, shape):
    store, dag, item, task, call = await capture(alpha, root_redis, shape)
    authority = await snapshot(root_redis)
    result = await dag.task_admit(item)
    assert result.admitted and result.status == ('seeded_root' if shape=='root' else 'seeded_waiting')
    assert await root_redis.get(call['keys'][0]) == ('ready' if shape=='root' else 'pending')
    assert await root_redis.hlen(call['keys'][1]) == 11
    assert await root_redis.get(call['keys'][2]) == str(item.dependency_count)
    assert await root_redis.smembers(call['keys'][3]) == set(item.child_task_ids)
    assert await root_redis.sismember(call['keys'][6], item.task_id)
    assert await root_redis.zscore(call['keys'][5], item.task_id) == (1000 if shape=='root' else None)
    assert await root_redis.get(call['keys'][4]) == ('1' if shape=='root' else None)
    before, counts = await snapshot(root_redis), await write_counts(root_redis)
    restarted = DagLua(root_redis, canonical_task_admit_binding=True, require_task_owner_binding=True)
    assert (await restarted.task_admit(item)).status == 'already_exists'
    assert await snapshot(root_redis) == before and await write_counts(root_redis) == counts
    # Known empty restore of effects, same durable decision; no new revision.
    await root_redis.delete(*call['keys'][:5])
    await root_redis.srem(call['keys'][6], item.task_id)
    await root_redis.zrem(call['keys'][5], item.task_id)
    assert (await restarted.task_admit(item)).status == result.status
    assert (await store.get_aggregate_snapshot(task)).revision == 1
    for key,value in authority.items():
        if key not in call['keys'][:8]:
            assert (await snapshot(root_redis))[key] == value


@pytest.mark.asyncio
@pytest.mark.parametrize('shape', ['root','waiting'])
@pytest.mark.parametrize('index', range(8))
async def test_each_destination_wrong_type_before_first_write(alpha, root_redis, shape, index):
    _, dag, item, _, call = await capture(alpha, root_redis, shape)
    key = call['keys'][index]
    await root_redis.delete(key)
    if index in (0,2,4):
        await root_redis.hset(key, 'wrong','type')
    else:
        await root_redis.set(key, 'wrong-type')
    await reject_retry(root_redis, dag, item, 'task_admit_target_type_conflict')


@pytest.mark.asyncio
@pytest.mark.parametrize('field,raw', [(5,'nan'),(5,'inf'),(9,'-1'),(9,'1.5'),(10,'0'),(10,'1.5'),(11,'nope'),(12,'2147483648')])
async def test_invalid_late_command_arguments_do_not_write(alpha, root_redis, field, raw):
    _, dag, _, _, call = await capture(alpha, root_redis)
    call['args'][field] = raw
    before, counts = await snapshot(root_redis), await write_counts(root_redis)
    result = await dag._admit_loader.run(**call)
    assert result[0] in {'task_admit_input_conflict','canonical_task_admit_proof_conflict'}
    assert await snapshot(root_redis) == before and await write_counts(root_redis) == counts


@pytest.mark.asyncio
@pytest.mark.parametrize('shape', ['root','waiting'])
@pytest.mark.parametrize('partial', ['run-only','meta-only','deps-only','marker-only','state-only','queue-only','children-only',
    'missing-meta','missing-state','missing-deps','missing-member','bad-meta','bad-children','persisted-meta','missing-ready'])
async def test_partial_footprint_is_visible_manual_conflict(alpha, root_redis, shape, partial):
    _, dag, item, _, call = await capture(alpha, root_redis, shape)
    keys = call['keys']
    if partial in {'run-only','meta-only','deps-only','marker-only','state-only','queue-only','children-only'}:
        if partial=='queue-only': await root_redis.zadd(keys[5],{item.task_id:1000})
        elif partial=='children-only': await root_redis.sadd(keys[3],'child-one')
        elif partial=='run-only': await root_redis.sadd(keys[6],item.task_id)
        elif partial=='meta-only': await root_redis.hset(keys[1],mapping={'task_id':item.task_id,'run_id':item.run_id})
        else: await root_redis.set(keys[{'deps-only':2,'marker-only':4,'state-only':0}[partial]], 'ready' if partial=='state-only' else '1')
    else:
        assert (await dag.task_admit(item)).admitted
        if partial.startswith('missing-'):
            if partial=='missing-member': await root_redis.srem(keys[6],item.task_id)
            elif partial=='missing-ready':
                if shape=='root': await root_redis.zrem(keys[5],item.task_id)
                else: await root_redis.set(keys[4],'1')
            else: await root_redis.delete(keys[{'missing-meta':1,'missing-state':0,'missing-deps':2}[partial]])
        elif partial=='bad-meta': await root_redis.hset(keys[1],'run_id','foreign')
        elif partial=='bad-children': await root_redis.sadd(keys[3],'unexpected')
        else: await root_redis.persist(keys[1])
    await reject_retry(root_redis, dag, item)


@pytest.mark.asyncio
@pytest.mark.parametrize('shape', ['root','waiting'])
async def test_concurrent_duplicate_materializes_once(alpha, root_redis, shape):
    store, _, item, task, _ = await capture(alpha, root_redis, shape)
    workers = [DagLua(root_redis,canonical_task_admit_binding=True,require_task_owner_binding=True) for _ in range(2)]
    counts = await write_counts(root_redis)
    results = await asyncio.gather(*(d.task_admit(item) for d in workers))
    assert sorted(r.status for r in results) == ['already_exists','seeded_root' if shape=='root' else 'seeded_waiting']
    assert (await store.get_aggregate_snapshot(task)).revision==1
    if counts is not None:
        after = await write_counts(root_redis)
        assert after['hset']-counts['hset']==1
        assert after['zadd']-counts['zadd']==(1 if shape=='root' else 0)


@pytest.mark.asyncio
@pytest.mark.parametrize('state', ['scheduled','running','done','failed'])
async def test_actual_lifecycle_progress_does_not_recreate_admission(alpha, root_redis, state):
    if state=='scheduled':
        _, store, dag, item, task = await setup(alpha)
        assert (await dag.task_admit(item)).admitted
        assert (await dag.task_dispatch_commit(dispatch(item))).committed
    else:
        _, store, dag, item, task, manager = await claim_setup(alpha, root_redis)
        assert (await claim(manager,item)).ok
        if state in {'done','failed'}:
            binding = TaskTerminalAuthorityBinding(root_redis)
            args=dict(task_id=item.task_id,run_id=item.run_id,tenant_id=item.tenant_id,
                finished_at_ms=4000,worker_instance_id='worker-1',scheduler_epoch='epoch-7',claim_epoch=1)
            result = await binding.complete(**args,output_data='{}') if state=='done' else await binding.fail(**args,reason_code='failed')
            assert result.completed
    assert await root_redis.get(DagRedisKey.task_state(item.task_id))==state
    before,counts=await snapshot(root_redis),await write_counts(root_redis)
    restarted=DagLua(root_redis,canonical_task_admit_binding=True,require_root_task_binding=True,require_task_owner_binding=True)
    assert (await restarted.task_admit(item)).status=='already_exists'
    assert await snapshot(root_redis)==before and await write_counts(root_redis)==counts
    assert await root_redis.zscore(DagRedisKey.tenant_ready_queue(item.tenant_id),item.task_id) is None


@pytest.mark.asyncio
@pytest.mark.parametrize('part', ['record','receipt','index','head'])
async def test_progressed_duplicate_requires_exact_history_and_current_head(alpha, root_redis, monkeypatch, part):
    _,store,dag,item,task=await setup(alpha)
    assert (await dag.task_admit(item)).admitted
    admission=(await store.get_aggregate_snapshot(task)).operation_id
    assert (await dag.task_dispatch_commit(dispatch(item))).committed
    original, observed = dag._admit_loader.run, {}
    async def interleave(**call):
        old=json.loads(call['args'][16]); later=json.loads(call['args'][17]);assert later[4]=='2'
        keys=call['keys']
        if part=='head': await root_redis.hset(keys[8],'revision','99')
        else: await root_redis.hdel(keys[{'record':11,'receipt':10,'index':9}[part]],old[3] if part=='index' else old[2])
        observed['before'],observed['counts']=await snapshot(root_redis),await write_counts(root_redis)
        return await original(**call)
    monkeypatch.setattr(dag._admit_loader,'run',interleave)
    with pytest.raises(TaskAdmitAuthorityError): await dag.task_admit(item)
    assert await snapshot(root_redis)==observed['before'] and await write_counts(root_redis)==observed['counts']


@pytest.mark.asyncio
@pytest.mark.parametrize('shape,n', [('root',n) for n in range(1,11)]+[('waiting',n) for n in range(1,10)])
async def test_error_after_each_effect_never_makes_partial_retry_success(alpha, root_redis, shape, n):
    _,dag,item,_,call=await capture(alpha,root_redis,shape)
    source=dag._admit_loader._source
    marker=source.index('-- Register this task in its run.')
    prefix="""local f06_n = 0
local function f06_fault_call(...)
 local args={...};local value=redis.call(unpack(args))
 f06_n=f06_n+1
 if f06_n==%d then error('controlled F06 late error') end
 return value
end
""" % n
    injected=prefix+source[:marker]+source[marker:].replace('redis.call(', 'f06_fault_call(')
    with pytest.raises(Exception,match='controlled F06 late error'):
        await root_redis.eval(injected,call['num_keys'],*call['keys'],*call['args'])
    # A complete final postimage is lost response/ACK; all preceding partials
    # are explicit manual conflicts. No compensation or rollback is assumed.
    if n==(10 if shape=='root' else 9):
        before,counts=await snapshot(root_redis),await write_counts(root_redis)
        assert (await dag.task_admit(item)).status=='already_exists'
        assert await snapshot(root_redis)==before and await write_counts(root_redis)==counts
    else:
        await reject_retry(root_redis,dag,item)


@pytest.mark.asyncio
async def test_submission_conflict_is_terminal_operator_visible_not_invisible_pending(alpha,root_redis,monkeypatch):
    await alpha._scheduler.composition.dag_lua.initialise()
    from hfa.lua.loader import LuaScriptLoader
    original=LuaScriptLoader.run
    async def corrupt(loader, **call):
        if loader._path.name == 'task_admit.lua':
            await root_redis.set(call['keys'][7],'wrong-type')
        return await original(loader, **call)
    monkeypatch.setattr(LuaScriptLoader,'run',corrupt)
    result=await alpha._run_submission.submit(SingleTaskRunSubmission(tenant_id='f02a1',payload={'test':'f06'},idempotency_key='f06-conflict'))
    assert result.status.value=='SUBMISSION_INCOMPLETE'
    assert result.failure_type=='TaskAdmitProjectionConflictError'
    assert result.task_admit_status=='task_admit_target_type_conflict'
    assert not result.automatic_retry and not result.automatic_repair and not result.automatic_rollback
    second=await alpha._run_submission.submit(SingleTaskRunSubmission(tenant_id='f02a1',payload={'test':'f06'},idempotency_key='f06-conflict'))
    assert second.idempotent_replay
    assert second.failure_type==result.failure_type and second.task_admit_status==result.task_admit_status


@pytest.mark.asyncio
@pytest.mark.parametrize('runtime', ['active','expired'])
async def test_cross_run_task_id_explicit_f07_disposition(alpha,root_redis,runtime):
    req,store,dag,item,task=await setup(alpha)
    assert (await dag.task_admit(item)).admitted
    other=request();other.root_task_id=item.task_id
    await create(alpha,other)
    if runtime=='active':
        before=await snapshot(root_redis)
        with pytest.raises(TaskAdmitAuthorityError): await dag.task_admit(seed(other))
        assert await snapshot(root_redis)==before
        assert await store.get_aggregate_snapshot(build_task_admit_command(seed(other)).aggregate_identity) is None
    else:
        # F07 closes the formerly deferred gap: runtime expiry/deletion never authorizes reuse.
        await root_redis.delete(*(f'hfa:dag:task:{item.task_id}:{s}' for s in ('state','meta','remaining_deps','children','ready_emitted')))
        await root_redis.srem(DagRedisKey.run_tasks(item.run_id),item.task_id)
        await root_redis.zrem(DagRedisKey.tenant_ready_queue(item.tenant_id),item.task_id)
        before=await snapshot(root_redis)
        with pytest.raises(TaskAdmitAuthorityError) as error: await dag.task_admit(seed(other))
        assert 'task_owner_conflict' in str(error.value)
        assert await snapshot(root_redis)==before
        assert await store.get_aggregate_snapshot(build_task_admit_command(seed(other)).aggregate_identity) is None
        assert (await store.get_aggregate_snapshot(task)).revision==1


@pytest.mark.asyncio
async def test_valid_current_head_progress_after_python_witness_cannot_slip_through(alpha,root_redis,monkeypatch):
    from hfa_control.worker_reservation import WorkerReservationManager
    from tests.unit.test_f04_4c_task_claim_projection_proof import manager_for
    _,store,dag,item,task=await setup(alpha)
    assert (await dag.task_admit(item)).admitted
    assert (await dag.task_dispatch_commit(dispatch(item))).committed
    assert (await WorkerReservationManager(root_redis).reserve(worker_id='worker-1',task_id=item.task_id,
        scheduler_epoch='epoch-7',reserved_at_ms=1900)).ok
    manager=manager_for(root_redis,dag,store)
    original,observed=dag._admit_loader.run,{}
    async def interleave(**call):
        assert json.loads(call['args'][17])[4]=='2'
        assert (await claim(manager,item)).ok
        observed['before'],observed['counts']=await snapshot(root_redis),await write_counts(root_redis)
        return await original(**call)
    monkeypatch.setattr(dag._admit_loader,'run',interleave)
    with pytest.raises(TaskAdmitAuthorityError):await dag.task_admit(item)
    assert await snapshot(root_redis)==observed['before'] and await write_counts(root_redis)==observed['counts']
    monkeypatch.setattr(dag._admit_loader,'run',original)
    before,counts=await snapshot(root_redis),await write_counts(root_redis)
    assert (await dag.task_admit(item)).status=='already_exists'
    assert await snapshot(root_redis)==before and await write_counts(root_redis)==counts
