"""F04.4E: immutable requeue delivery evidence and effect share one Lua invocation."""
import asyncio
import json
import hashlib

import pytest

from hfa.config.keys import RedisKey
from hfa.dag.schema import DagRedisKey
from hfa_control.task_requeue_authority import TaskRequeueAuthorityBinding, TaskRequeueAuthorityError
from hfa_control.task_dispatch_authority import TaskDispatchProjectionPendingError
from tests.unit.test_f02a1_root_binding import alpha, root_redis
from tests.unit.test_f04_4d_task_requeue_projection_proof import setup as requeue_setup
from tests.unit.test_f04_4c_task_claim_projection_proof import snapshot
from tests.unit.test_f04_task_projection_proof import write_counts
from tests.unit.test_f04_run_create_projection_proof import FAULTS, inject_fault
from tests.integration.test_task_dispatch_authority_binding_integration import dispatch

CONFLICT='canonical_requeue_proof_invalid'


async def setup(alpha, redis):
    store, dag, item, task, binding, args = await requeue_setup(alpha, redis)
    prepared = await binding.prepare_requeue(**args)
    assert (await binding.projection_manager.project(prepared.projection)).projected
    return store, dag, item, task, binding, args, prepared.projection


async def capture(alpha, redis):
    store, dag, item, task, binding, args, projection = await setup(alpha, redis)
    loader, captured = binding.projection_manager._loader, {}
    original = loader.run
    async def crash(**kwargs):
        captured.update(kwargs)
        raise RuntimeError('controlled stop before delivery Lua')
    loader.run = crash
    try:
        with pytest.raises(RuntimeError, match='controlled stop'):
            await binding.projection_manager.deliver(projection)
    finally:
        loader.run = original
    assert captured['args'][9]=='canonical_deliver'
    return store, dag, item, task, binding, args, projection, captured


async def assert_rejection(redis, loader, captured):
    before, counts = await snapshot(redis), await write_counts(redis)
    assert (await loader.run(**captured))[0]==CONFLICT
    assert await snapshot(redis)==before
    assert await write_counts(redis)==counts


async def later_dispatch(store, dag, item, task):
    original=dag._dispatch_loader.run
    async def crash(**kwargs):
        raise RuntimeError('controlled stop after durable later dispatch')
    dag._dispatch_loader.run=crash
    try:
        with pytest.raises(TaskDispatchProjectionPendingError) as pending:
            await dag.task_dispatch_commit(dispatch(item, attempt=2, scheduled_at=5000))
        assert pending.value.canonical_commit_durable
        assert pending.value.detail=='controlled stop after durable later dispatch'
    finally:
        dag._dispatch_loader.run=original
    head=await store.get_aggregate_snapshot(task)
    assert head.revision==5 and head.state=='scheduled'


@pytest.mark.asyncio
async def test_first_delivery_and_exact_duplicate(alpha, root_redis):
    store, dag, item, task, binding, args, projection = await setup(alpha, root_redis)
    manager=binding.projection_manager
    stream=DagRedisKey.completion_stream(item.tenant_id)
    meta=DagRedisKey.task_meta(item.task_id)
    # An existing deadline is observed, never renewed by delivery/duplicate.
    await root_redis.pexpire(meta,600000)
    before=await snapshot(root_redis)
    assert await manager.deliver(projection)==(True,False)
    after=await snapshot(root_redis)
    assert {k:v for k,v in before.items() if k not in (stream,meta)}=={k:v for k,v in after.items() if k not in (stream,meta)}
    assert before[meta][2]==after[meta][2]
    old_meta=before[meta][1]
    assert after[meta][1]=={**old_meta,'requeue_notification_operation_id':projection.canonical_operation_id}
    events=await root_redis.xrange(stream)
    assert len(events)==1
    event=events[0][1]
    assert event=={'event_type':'TaskRequeued','task_id':item.task_id,'run_id':item.run_id,'tenant_id':item.tenant_id,
        'reason_code':projection.reason_code,'requeue_count':'1','canonical_transition_id':projection.canonical_transition_id,
        'canonical_revision':'4','canonical_operation_id':projection.canonical_operation_id,'at_ms':'4000'}
    before,counts=await snapshot(root_redis),await write_counts(root_redis)
    assert await manager.deliver(projection)==(False,True)
    assert await snapshot(root_redis)==before and await write_counts(root_redis)==counts


@pytest.mark.asyncio
@pytest.mark.parametrize('fault',FAULTS)
async def test_own_proof_fault_after_python_validation(alpha, root_redis, fault):
    store, dag, item, task, binding, args, projection = await setup(alpha, root_redis)
    loader=binding.projection_manager._loader
    original,observed=loader.run,{}
    async def interleave(**kwargs):
        own=json.loads(kwargs['args'][23])
        assert own==json.loads(kwargs['args'][26])
        assert kwargs['keys'][9:]==store.keyspace(task.sha256).commit_keys()[:6]
        await inject_fault(root_redis,store,task,own,fault)
        observed['before'],observed['counts']=await snapshot(root_redis),await write_counts(root_redis)
        return await original(**kwargs)
    loader.run=interleave
    with pytest.raises(TaskRequeueAuthorityError,match=CONFLICT): await binding.projection_manager.deliver(projection)
    assert await snapshot(root_redis)==observed['before']
    assert await write_counts(root_redis)==observed['counts']


@pytest.mark.asyncio
@pytest.mark.parametrize('footprint',['absent','partial','forged','exact'])
@pytest.mark.parametrize('part',['receipt','record','all'])
async def test_runtime_notification_cannot_replace_proof(alpha, root_redis, footprint, part):
    store, dag, item, task, binding, args, projection, captured = await capture(alpha, root_redis)
    loader=binding.projection_manager._loader
    meta=DagRedisKey.task_meta(item.task_id)
    if footprint=='exact': assert (await loader.run(**captured))[0]=='TASK_REQUEUE_DELIVERED'
    elif footprint=='absent': await root_redis.delete(meta,DagRedisKey.task_state(item.task_id))
    elif footprint=='partial': await root_redis.hdel(meta,'requeue_canonical_operation_id')
    else: await root_redis.hset(meta,'requeue_notification_operation_id',projection.canonical_operation_id)
    keys=store.keyspace(task.sha256);own=json.loads(captured['args'][23])
    if part=='all': await root_redis.delete(*keys.commit_keys()[:6])
    else: await root_redis.hdel(keys.receipts if part=='receipt' else keys.operation_records,own[2])
    await assert_rejection(root_redis,loader,captured)


@pytest.mark.asyncio
@pytest.mark.parametrize('arg',[0,1,2,4,5,6,7,10,11,12,13,14,15,16,17,18,19,20,21,22,24,25])
async def test_exact_delivery_record_binds_inputs(alpha, root_redis, arg):
    store, dag, item, task, binding, args, projection, captured = await capture(alpha, root_redis)
    captured['args'][arg]='99' if arg in (4,5,6,13,19,21,22) else 'foreign'
    await assert_rejection(root_redis,binding.projection_manager._loader,captured)


@pytest.mark.asyncio
@pytest.mark.parametrize('key',list(range(15)))
async def test_delivery_destination_and_proof_key_binding(alpha, root_redis, key):
    store, dag, item, task, binding, args, projection, captured = await capture(alpha, root_redis)
    captured['keys'][key]+=':foreign'
    await assert_rejection(root_redis,binding.projection_manager._loader,captured)


@pytest.mark.asyncio
@pytest.mark.parametrize('shape',['no_own','no_head','no_keys','malformed_own','malformed_head','typed_head'])
async def test_missing_or_malformed_proof_transport(alpha, root_redis, shape):
    store, dag, item, task, binding, args, projection, captured = await capture(alpha, root_redis)
    if shape=='no_own':captured['args']=captured['args'][:23]
    elif shape=='no_head':captured['args']=captured['args'][:26]
    elif shape=='no_keys':captured['keys']=captured['keys'][:9];captured['num_keys']=9
    elif shape=='malformed_own':captured['args'][23]='{'
    elif shape=='malformed_head':captured['args'][26]='{'
    else:captured['args'][26]=json.dumps([None]*14)
    await assert_rejection(root_redis,binding.projection_manager._loader,captured)


@pytest.mark.asyncio
@pytest.mark.parametrize('part',['own','head','both'])
async def test_foreign_canonical_witness(alpha, root_redis, part):
    store, dag, item, task, binding, args, projection, captured = await capture(alpha, root_redis)
    _,_,foreign_item,_,_,_,_,foreign=await capture(alpha,root_redis)
    assert foreign_item.task_id!=item.task_id
    if part in ('own','both'):captured['args'][23]=foreign['args'][23]
    if part in ('head','both'):captured['args'][26]=foreign['args'][26]
    if part=='both':captured['keys'][9:]=foreign['keys'][9:]
    await assert_rejection(root_redis,binding.projection_manager._loader,captured)


@pytest.mark.asyncio
@pytest.mark.parametrize('part',['receipt','record'])
async def test_second_connection_proof_loss(alpha, root_redis, part):
    store, dag, item, task, binding, args, projection = await setup(alpha, root_redis)
    loader=binding.projection_manager._loader
    original=loader.run;prepared,release=asyncio.Event(),asyncio.Event()
    async def barrier(**kwargs):
        prepared.set();await asyncio.wait_for(release.wait(),5)
        return await original(**kwargs)
    loader.run=barrier
    pending=asyncio.create_task(binding.projection_manager.deliver(projection))
    try:
        await asyncio.wait_for(prepared.wait(),5)
        head=await store.get_aggregate_snapshot(task);keys=store.keyspace(task.sha256)
        async with root_redis.client() as other:
            assert await other.hdel(keys.receipts if part=='receipt' else keys.operation_records,head.operation_digest)==1
        before,counts=await snapshot(root_redis),await write_counts(root_redis)
        release.set()
        with pytest.raises(TaskRequeueAuthorityError,match=CONFLICT):await pending
        assert await snapshot(root_redis)==before and await write_counts(root_redis)==counts
    finally:
        release.set()
        if not pending.done():pending.cancel();await asyncio.gather(pending,return_exceptions=True)


@pytest.mark.asyncio
@pytest.mark.parametrize('delivered',[False,True])
async def test_stale_head_rejected_then_fresh_historical_delivery_allowed(alpha, root_redis, delivered):
    store, dag, item, task, binding, args, projection, captured = await capture(alpha, root_redis)
    if delivered:assert await binding.projection_manager.deliver(projection)==(True,False)
    await later_dispatch(store,dag,item,task)
    await assert_rejection(root_redis,binding.projection_manager._loader,captured)
    restarted=TaskRequeueAuthorityBinding(root_redis)
    result=await restarted.requeue(**{**args,'observed_at_ms':9999})
    assert result.exact_retry and result.projection_already_applied and not result.projection_applied
    assert result.delivery_already_applied==delivered
    assert result.delivery_applied!=delivered
    assert await root_redis.xlen(DagRedisKey.completion_stream(item.tenant_id))==1
    event=(await root_redis.xrange(DagRedisKey.completion_stream(item.tenant_id)))[0][1]
    assert event['at_ms']=='4000' and event['canonical_revision']=='4'
    head=await store.get_aggregate_snapshot(task)
    assert head.revision==5 and head.state=='scheduled'
    before,counts=await snapshot(root_redis),await write_counts(root_redis)
    assert (await restarted.requeue(**args)).delivery_already_applied
    assert await snapshot(root_redis)==before and await write_counts(root_redis)==counts


@pytest.mark.asyncio
@pytest.mark.parametrize('part',['receipt','record','index'])
@pytest.mark.parametrize('damage',['missing','checksum','valid_envelope_drift'])
async def test_historical_own_proof_still_required_under_valid_later_head(alpha, root_redis, part, damage):
    store, dag, item, task, binding, args, projection = await setup(alpha, root_redis)
    await later_dispatch(store,dag,item,task)
    loader=binding.projection_manager._loader;original=loader.run;seen={}
    async def interleave(**kwargs):
        own=json.loads(kwargs['args'][23]);head=json.loads(kwargs['args'][26])
        assert own[4]=='4' and head[4]=='5'
        keys=store.keyspace(task.sha256)
        key={'receipt':keys.receipts,'record':keys.operation_records,'index':keys.transition_indexes}[part]
        field=own[3] if part=='index' else own[2]
        if damage=='missing':await root_redis.hdel(key,field)
        else:
            env=json.loads(await root_redis.hget(key,field))
            if damage=='checksum':env['storage_sha1']='0'*40
            else:
                payload=json.loads(env['payload']);payload['canonical_record_hash']='f'*64
                env['payload']=json.dumps(payload,sort_keys=True,separators=(',',':'))
                env['storage_sha1']=hashlib.sha1(env['payload'].encode()).hexdigest()
            await root_redis.hset(key,field,json.dumps(env))
        seen['before'],seen['counts']=await snapshot(root_redis),await write_counts(root_redis)
        return await original(**kwargs)
    loader.run=interleave
    with pytest.raises(TaskRequeueAuthorityError,match=CONFLICT):await binding.projection_manager.deliver(projection)
    assert await snapshot(root_redis)==seen['before'] and await write_counts(root_redis)==seen['counts']


@pytest.mark.asyncio
@pytest.mark.parametrize('runtime_fault',['run_missing','task_missing','task_type','meta_missing'])
async def test_missing_proof_blocks_conflict_writers(alpha, root_redis, runtime_fault):
    store, dag, item, task, binding, args, projection, captured = await capture(alpha, root_redis)
    own=json.loads(captured['args'][23]);await root_redis.hdel(store.keyspace(task.sha256).receipts,own[2])
    if runtime_fault=='run_missing':await root_redis.delete(RedisKey.run_state(item.run_id))
    elif runtime_fault=='task_missing':await root_redis.delete(DagRedisKey.task_state(item.task_id))
    elif runtime_fault=='meta_missing':await root_redis.delete(DagRedisKey.task_meta(item.task_id))
    else:
        await root_redis.delete(DagRedisKey.task_state(item.task_id))
        await root_redis.hset(DagRedisKey.task_state(item.task_id),mapping={'wrong':'type'})
    await assert_rejection(root_redis,binding.projection_manager._loader,captured)
    assert not await root_redis.exists(RedisKey.runtime_truth_conflict_index(),RedisKey.runtime_truth_conflict_stream())


@pytest.mark.asyncio
@pytest.mark.parametrize('field,arg,meta_field',[
    (3,10,'requeue_canonical_transition_id'),
    (5,12,'requeue_canonical_command_hash'),
    (6,11,'requeue_canonical_record_hash'),
])
async def test_historical_fingerprint_cannot_diverge_from_exact_record(alpha, root_redis, field, arg, meta_field):
    store, dag, item, task, binding, args, projection = await setup(alpha, root_redis)
    await later_dispatch(store,dag,item,task)
    loader=binding.projection_manager._loader;original=loader.run;captured={}
    async def crash(**kwargs):
        captured.update(kwargs)
        raise RuntimeError('capture later-head delivery')
    loader.run=crash
    try:
        with pytest.raises(RuntimeError):await binding.projection_manager.deliver(projection)
    finally:loader.run=original
    own=json.loads(captured['args'][23]);own[field]='f'*64
    captured['args'][23]=json.dumps(own);captured['args'][arg]='f'*64
    await root_redis.hset(DagRedisKey.task_meta(item.task_id),meta_field,'f'*64)
    await assert_rejection(root_redis,loader,captured)


@pytest.mark.asyncio
async def test_different_operation_same_task_cannot_authorize_delivery(alpha, root_redis):
    store, dag, item, task, binding, args, projection, captured = await capture(alpha, root_redis)
    claim_probe=await store.load_receipt_probe(task,projection.claim_operation_id)
    _,claim_witness=store.authority_head_witness(claim_probe.canonical_store_record,claim_probe.receipt)
    captured['args'][23]=json.dumps(claim_witness)
    await assert_rejection(root_redis,binding.projection_manager._loader,captured)


@pytest.mark.asyncio
@pytest.mark.parametrize('ack_lost',[False,True])
async def test_restart_after_projected_requeue_never_duplicates_delivery(alpha, root_redis, ack_lost):
    store, dag, item, task, binding, args, projection, captured = await capture(alpha, root_redis)
    if ack_lost:assert (await binding.projection_manager._loader.run(**captured))[0]=='TASK_REQUEUE_DELIVERED'
    restarted=TaskRequeueAuthorityBinding(root_redis)
    result=await restarted.requeue(**{**args,'observed_at_ms':9999})
    assert result.exact_retry and result.projection_already_applied
    assert result.delivery_already_applied==ack_lost
    assert result.delivery_applied!=ack_lost
    assert await root_redis.xlen(DagRedisKey.completion_stream(item.tenant_id))==1
    assert (await store.get_aggregate_snapshot(task)).revision==4
    before,counts=await snapshot(root_redis),await write_counts(root_redis)
    assert (await restarted.requeue(**args)).delivery_already_applied
    assert await snapshot(root_redis)==before and await write_counts(root_redis)==counts
