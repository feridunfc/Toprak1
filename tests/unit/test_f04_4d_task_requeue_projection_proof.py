"""F04.4D: production requeue lifecycle; own-proof before every projection effect."""
import asyncio
import json

import pytest

from hfa.config.keys import RedisKey
from hfa.dag.schema import DagRedisKey
from hfa_control.task_requeue_authority import TaskRequeueAuthorityBinding, TaskRequeueAuthorityError
from tests.unit.test_f02a1_root_binding import alpha, root_redis
from tests.unit.test_f04_4c_task_claim_projection_proof import setup as claim_setup, claim, snapshot
from tests.unit.test_f04_task_projection_proof import write_counts
from tests.unit.test_f04_run_create_projection_proof import FAULTS, inject_fault
from tests.integration.test_task_dispatch_authority_binding_integration import dispatch

CONFLICT = 'canonical_requeue_proof_invalid'


async def setup(alpha, redis):
    req, store, dag, item, task, claim_manager = await claim_setup(alpha, redis)
    assert (await claim(claim_manager, item)).ok
    binding = TaskRequeueAuthorityBinding(redis, store=store)
    await binding.initialise()
    args = dict(task_id=item.task_id, run_id=item.run_id, tenant_id=item.tenant_id,
                observed_at_ms=4000, max_requeue_count=3, reason_code='TASK_STALE_DETECTED',
                worker_instance_id='worker-1', scheduler_epoch='epoch-7', claim_epoch=1)
    return store, dag, item, task, binding, args


async def capture_unprojected(alpha, redis):
    store, dag, item, task, binding, args = await setup(alpha, redis)
    loader = binding.projection_manager._loader
    captured, original = {}, loader.run
    async def crash(**kwargs):
        captured.update(kwargs)
        raise RuntimeError('controlled stop after durable requeue before projection')
    loader.run = crash
    try:
        with pytest.raises(TaskRequeueAuthorityError, match='canonical_requeue_projection_pending'):
            await binding.requeue(**args)
    finally:
        loader.run = original
    head = await store.get_aggregate_snapshot(task)
    assert head.revision == 4 and head.state == 'ready'
    return store, dag, item, task, binding, args, captured


async def assert_rejection(redis, loader, captured):
    before, counts = await snapshot(redis), await write_counts(redis)
    assert (await loader.run(**captured))[0] == CONFLICT
    assert await snapshot(redis) == before
    assert await write_counts(redis) == counts


@pytest.mark.asyncio
async def test_normal_projection_duplicate_and_delivery_remains_separate(alpha, root_redis):
    store, dag, item, task, binding, args = await setup(alpha, root_redis)
    prepared = await binding.prepare_requeue(**args)
    manager = binding.projection_manager
    stream = DagRedisKey.completion_stream(item.tenant_id)
    stream_before = await root_redis.xlen(stream)
    result = await manager.project(prepared.projection)
    assert result.projected and result.requeue_count == 1
    assert await root_redis.get(DagRedisKey.task_state(item.task_id)) == 'ready'
    assert await root_redis.zscore(DagRedisKey.task_running_zset(item.tenant_id), item.task_id) is None
    assert await root_redis.zscore(DagRedisKey.tenant_ready_queue(item.tenant_id), item.task_id) == 4000
    assert await root_redis.xlen(stream) == stream_before
    assert not await root_redis.hget(DagRedisKey.task_meta(item.task_id), 'requeue_notification_operation_id')
    before, counts = await snapshot(root_redis), await write_counts(root_redis)
    assert (await manager.project(prepared.projection)).already_projected
    assert await snapshot(root_redis) == before
    assert await write_counts(root_redis) == counts
    assert await manager.deliver(prepared.projection) == (True, False)
    assert await root_redis.xlen(stream) == stream_before + 1
    before, counts = await snapshot(root_redis), await write_counts(root_redis)
    assert await manager.deliver(prepared.projection) == (False, True)
    assert await snapshot(root_redis) == before
    assert await write_counts(root_redis) == counts


@pytest.mark.asyncio
@pytest.mark.parametrize('fault', FAULTS)
async def test_proof_fault_after_python_validation(alpha, root_redis, fault):
    store, dag, item, task, binding, args = await setup(alpha, root_redis)
    loader, observed = binding.projection_manager._loader, {}
    original = loader.run
    async def interleave(**kwargs):
        assert kwargs['args'][9] == 'canonical_project'
        own = json.loads(kwargs['args'][23])
        assert own[4] == '4' and own[10] == 'ready'
        assert kwargs['keys'][9:] == store.keyspace(task.sha256).commit_keys()[:6]
        await inject_fault(root_redis, store, task, own, fault)
        observed['before'], observed['counts'] = await snapshot(root_redis), await write_counts(root_redis)
        return await original(**kwargs)
    loader.run = interleave
    with pytest.raises(TaskRequeueAuthorityError, match=CONFLICT):
        await binding.requeue(**args)
    assert await snapshot(root_redis) == observed['before']
    assert await write_counts(root_redis) == observed['counts']
    assert await root_redis.get(DagRedisKey.task_state(item.task_id)) == 'running'


@pytest.mark.asyncio
async def test_restart_materializes_once(alpha, root_redis):
    store, dag, item, task, binding, args, captured = await capture_unprojected(alpha, root_redis)
    restarted = TaskRequeueAuthorityBinding(root_redis)
    result = await restarted.requeue(**{**args, 'observed_at_ms': 9999})
    assert result.projection_applied and result.delivery_applied
    assert await root_redis.hget(DagRedisKey.task_meta(item.task_id), 'last_requeue_at_ms') == '4000'
    before, counts = await snapshot(root_redis), await write_counts(root_redis)
    result = await restarted.requeue(**args)
    assert result.projection_already_applied and result.delivery_already_applied
    assert await snapshot(root_redis) == before
    assert await write_counts(root_redis) == counts


@pytest.mark.asyncio
@pytest.mark.parametrize('footprint', ['absent', 'partial', 'forged', 'exact'])
@pytest.mark.parametrize('part', ['receipt', 'record', 'all'])
async def test_runtime_cannot_substitute_for_missing_proof(alpha, root_redis, footprint, part):
    store, dag, item, task, binding, args, captured = await capture_unprojected(alpha, root_redis)
    loader = binding.projection_manager._loader
    if footprint == 'exact':
        assert (await loader.run(**captured))[0] == 'TASK_REQUEUED'
    elif footprint == 'absent':
        await root_redis.delete(DagRedisKey.task_state(item.task_id), DagRedisKey.task_meta(item.task_id))
    else:
        await root_redis.set(DagRedisKey.task_state(item.task_id), 'ready', px=600000)
        if footprint == 'forged':
            a = captured['args']
            await root_redis.hset(DagRedisKey.task_meta(item.task_id), mapping={
                'canonical_transition_id': a[10], 'canonical_record_hash': a[11],
                'canonical_command_hash': a[12], 'canonical_revision': a[13],
                'canonical_operation_id': a[14], 'requeue_count': a[22]})
            await root_redis.zadd(DagRedisKey.tenant_ready_queue(item.tenant_id), {item.task_id: 4000})
    own, keys = json.loads(captured['args'][23]), store.keyspace(task.sha256)
    if part == 'all': await root_redis.delete(*keys.commit_keys()[:6])
    else: await root_redis.hdel(keys.receipts if part == 'receipt' else keys.operation_records, own[2])
    await assert_rejection(root_redis, loader, captured)


@pytest.mark.asyncio
@pytest.mark.parametrize('arg', [0, 1, 2, 4, 5, 6, 7, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 24, 25])
async def test_exact_record_binds_projection_input(alpha, root_redis, arg):
    store, dag, item, task, binding, args, captured = await capture_unprojected(alpha, root_redis)
    captured['args'][arg] = '99' if arg in (4, 5, 6, 13, 19, 21, 22) else 'foreign'
    await assert_rejection(root_redis, binding.projection_manager._loader, captured)


@pytest.mark.asyncio
@pytest.mark.parametrize('key', list(range(15)))
async def test_foreign_destination_or_proof_key(alpha, root_redis, key):
    store, dag, item, task, binding, args, captured = await capture_unprojected(alpha, root_redis)
    captured['keys'][key] += ':foreign'
    await assert_rejection(root_redis, binding.projection_manager._loader, captured)


@pytest.mark.asyncio
@pytest.mark.parametrize('part', ['receipt', 'record'])
async def test_separate_connection_barrier(alpha, root_redis, part):
    store, dag, item, task, binding, args = await setup(alpha, root_redis)
    prepared, release = asyncio.Event(), asyncio.Event()
    loader, original = binding.projection_manager._loader, binding.projection_manager._loader.run
    async def barrier(**kwargs):
        prepared.set()
        await asyncio.wait_for(release.wait(), 5)
        return await original(**kwargs)
    loader.run = barrier
    pending = asyncio.create_task(binding.requeue(**args))
    try:
        await asyncio.wait_for(prepared.wait(), 5)
        head, keys = await store.get_aggregate_snapshot(task), store.keyspace(task.sha256)
        async with root_redis.client() as other:
            assert await other.hdel(keys.receipts if part == 'receipt' else keys.operation_records, head.operation_digest) == 1
        before, counts = await snapshot(root_redis), await write_counts(root_redis)
        release.set()
        with pytest.raises(TaskRequeueAuthorityError, match=CONFLICT): await pending
        assert await snapshot(root_redis) == before
        assert await write_counts(root_redis) == counts
    finally:
        release.set()
        if not pending.done():
            pending.cancel()
            await asyncio.gather(pending, return_exceptions=True)


@pytest.mark.asyncio
@pytest.mark.parametrize('footprint', ['later', 'pre_requeue', 'exact_requeue'])
async def test_actual_later_dispatch_rejects_stale_requeue(alpha, root_redis, footprint):
    store, dag, item, task, binding, args, captured = await capture_unprojected(alpha, root_redis)
    loader = binding.projection_manager._loader
    pre = await root_redis.hgetall(DagRedisKey.task_meta(item.task_id))
    assert (await loader.run(**captured))[0] == 'TASK_REQUEUED'
    exact = await root_redis.hgetall(DagRedisKey.task_meta(item.task_id))
    # The race needs a real later canonical decision, not a second dispatch
    # projection (a closed, separate owner). Stop at its durable boundary.
    from hfa_control.task_dispatch_authority import TaskDispatchProjectionPendingError
    original = dag._dispatch_loader.run
    async def stop_after_commit(**kwargs):
        raise RuntimeError('controlled stop after durable second dispatch')
    dag._dispatch_loader.run = stop_after_commit
    try:
        with pytest.raises(TaskDispatchProjectionPendingError) as pending:
            await dag.task_dispatch_commit(dispatch(item, attempt=2, scheduled_at=5000))
        assert pending.value.canonical_commit_durable
        assert pending.value.detail == 'controlled stop after durable second dispatch'
    finally:
        dag._dispatch_loader.run = original
    head = await store.get_aggregate_snapshot(task)
    assert head.revision == 5 and head.state == 'scheduled'
    if footprint != 'later':
        await root_redis.set(DagRedisKey.task_state(item.task_id), 'running' if footprint == 'pre_requeue' else 'ready')
        await root_redis.delete(DagRedisKey.task_meta(item.task_id))
        await root_redis.hset(DagRedisKey.task_meta(item.task_id), mapping=pre if footprint == 'pre_requeue' else exact)
    await assert_rejection(root_redis, loader, captured)


@pytest.mark.asyncio
@pytest.mark.parametrize('runtime_fault', ['run_missing', 'task_missing', 'task_type', 'run_terminal', 'ready_type', 'claim_drift'])
async def test_missing_proof_blocks_error_evidence_writers(alpha, root_redis, runtime_fault):
    store, dag, item, task, binding, args, captured = await capture_unprojected(alpha, root_redis)
    own = json.loads(captured['args'][23])
    await root_redis.hdel(store.keyspace(task.sha256).receipts, own[2])
    if runtime_fault == 'run_missing': await root_redis.delete(RedisKey.run_state(item.run_id))
    elif runtime_fault == 'task_missing': await root_redis.delete(DagRedisKey.task_state(item.task_id))
    elif runtime_fault == 'task_type':
        await root_redis.delete(DagRedisKey.task_state(item.task_id))
        await root_redis.hset(DagRedisKey.task_state(item.task_id), mapping={'bad': 'type'})
    elif runtime_fault == 'run_terminal': await root_redis.set(RedisKey.run_state(item.run_id), 'done')
    elif runtime_fault == 'ready_type': await root_redis.set(DagRedisKey.tenant_ready_queue(item.tenant_id), 'bad')
    else: await root_redis.hset(DagRedisKey.task_meta(item.task_id), 'claim_epoch', '99')
    await assert_rejection(root_redis, binding.projection_manager._loader, captured)
    assert not await root_redis.exists(RedisKey.runtime_truth_conflict_index(), RedisKey.runtime_truth_conflict_stream())


@pytest.mark.asyncio
async def test_complete_foreign_canonical_witness_rejected(alpha, root_redis):
    store, dag, item, task, binding, args, captured = await capture_unprojected(alpha, root_redis)
    foreign_store, _, foreign_item, foreign_task, _, _, foreign = await capture_unprojected(alpha, root_redis)
    assert foreign_task != task and foreign_item.task_id != item.task_id
    captured['keys'][9:] = foreign['keys'][9:]
    captured['args'][23] = foreign['args'][23]
    await assert_rejection(root_redis, binding.projection_manager._loader, captured)


@pytest.mark.asyncio
@pytest.mark.parametrize('shape', ['no_witness', 'no_keys', 'malformed', 'wrong_type'])
async def test_missing_or_malformed_witness_transport(alpha, root_redis, shape):
    store, dag, item, task, binding, args, captured = await capture_unprojected(alpha, root_redis)
    if shape == 'no_witness': captured['args'] = captured['args'][:23]
    elif shape == 'no_keys':
        captured['keys'] = captured['keys'][:9]
        captured['num_keys'] = 9
    elif shape == 'malformed': captured['args'][23] = '{'
    else: captured['args'][23] = json.dumps([None] * 14)
    await assert_rejection(root_redis, binding.projection_manager._loader, captured)
