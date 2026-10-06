"""F04.4B: production alpha dispatch own-proof and effects in one Lua call."""
import asyncio
import json
from unittest.mock import Mock

import pytest

from hfa.config.keys import RedisKey
from hfa.dag.schema import DagRedisKey
from hfa_control.dag_lua import DagLua
from hfa_control.task_dispatch_authority import TaskDispatchProjectionPendingError
from tests.unit.test_f02a1_root_binding import alpha, root_redis
from tests.unit.test_f04_4a_task_admit_projection_proof import setup as admitted_setup
from tests.unit.test_f04_task_projection_proof import semantic_snapshot, write_counts
from tests.unit.test_f04_run_create_projection_proof import FAULTS, inject_fault
from tests.integration.test_task_dispatch_authority_binding_integration import dispatch


CONFLICT = 'canonical_task_dispatch_proof_conflict'


async def setup(alpha):
    req, store, dag, item, task = await admitted_setup(alpha)
    assert (await dag.task_admit(item)).status == 'seeded_root'
    return req, store, dag, item, task


async def capture_unprojected(alpha):
    req, store, dag, item, task = await setup(alpha)
    captured = {}
    original = dag._dispatch_loader.run
    async def crash(**kwargs):
        captured.update(kwargs)
        raise RuntimeError('controlled crash after durable TASK_DISPATCH')
    dag._dispatch_loader.run = crash
    try:
        with pytest.raises(TaskDispatchProjectionPendingError):
            await dag.task_dispatch_commit(dispatch(item))
    finally:
        dag._dispatch_loader.run = original
    head = await store.get_aggregate_snapshot(task)
    assert head.revision == 2 and head.state == 'scheduled'
    return req, store, dag, item, task, captured


async def assert_read_only_rejection(redis, loader, captured):
    before, writes = await semantic_snapshot(redis), await write_counts(redis)
    assert (await loader.run(**captured))[0] == CONFLICT
    assert await semantic_snapshot(redis) == before
    assert await write_counts(redis) == writes


@pytest.mark.asyncio
@pytest.mark.parametrize('scheduled_at', [0, 2000])
async def test_normal_first_projection_and_exact_duplicate_zero_effect(alpha, root_redis, scheduled_at):
    req, store, dag, item, task = await setup(alpha)
    value = dispatch(item, scheduled_at=scheduled_at)
    control_before = await root_redis.xlen(value.control_stream)
    assert (await dag.task_dispatch_commit(value)).status == 'committed'
    head = await store.get_aggregate_snapshot(task)
    assert head.revision == 2 and head.state == 'scheduled'
    assert await root_redis.get(DagRedisKey.task_state(item.task_id)) == 'scheduled'
    assert await root_redis.zscore(value.scheduled_zset, item.task_id) == scheduled_at
    assert await root_redis.zscore(DagRedisKey.tenant_ready_queue(item.tenant_id), item.task_id) is None
    assert await root_redis.xlen(value.control_stream) == control_before + 1
    assert await root_redis.xlen(value.shard_stream) == 1
    before, writes = await semantic_snapshot(root_redis), await write_counts(root_redis)
    # Retries use the durable time, not a new scheduler clock.
    duplicate = await dag.task_dispatch_commit(dispatch(item, scheduled_at=9000))
    assert duplicate.committed and duplicate.status == 'already_projected'
    assert await semantic_snapshot(root_redis) == before
    assert await write_counts(root_redis) == writes


@pytest.mark.asyncio
@pytest.mark.parametrize('fault', FAULTS)
async def test_proof_loss_or_corruption_rejected_at_actual_lua(alpha, root_redis, fault):
    req, store, dag, item, task = await setup(alpha)
    original, observed = dag._dispatch_loader.run, {}
    async def interleave(**kwargs):
        own = json.loads(kwargs['args'][27])
        assert kwargs['args'][26] == '1'
        assert kwargs['keys'][10:] == store.keyspace(task.sha256).commit_keys()[:6]
        assert own[4] == '2' and own[10] == 'scheduled'
        await inject_fault(root_redis, store, task, own, fault)
        observed['before'] = await semantic_snapshot(root_redis)
        observed['writes'] = await write_counts(root_redis)
        return await original(**kwargs)
    dag._dispatch_loader.run = interleave
    with pytest.raises(TaskDispatchProjectionPendingError) as rejected:
        await dag.task_dispatch_commit(dispatch(item))
    assert rejected.value.status == CONFLICT
    assert await semantic_snapshot(root_redis) == observed['before']
    assert await write_counts(root_redis) == observed['writes']
    assert await root_redis.get(DagRedisKey.task_state(item.task_id)) == 'ready'
    assert await root_redis.zscore(dispatch(item).scheduled_zset, item.task_id) is None
    assert await root_redis.xlen(dispatch(item).shard_stream) == 0


@pytest.mark.asyncio
async def test_restart_materializes_once(alpha, root_redis):
    req, store, dag, item, task, captured = await capture_unprojected(alpha)
    assert await root_redis.get(DagRedisKey.task_state(item.task_id)) == 'ready'
    restarted = DagLua(root_redis, canonical_task_admit_binding=True, canonical_task_dispatch_binding=True)
    assert (await restarted.task_dispatch_commit(dispatch(item))).status == 'committed'
    before, writes = await semantic_snapshot(root_redis), await write_counts(root_redis)
    assert (await restarted.task_dispatch_commit(dispatch(item))).status == 'already_projected'
    assert (await store.get_aggregate_snapshot(task)).revision == 2
    assert await semantic_snapshot(root_redis) == before
    assert await write_counts(root_redis) == writes


@pytest.mark.asyncio
@pytest.mark.parametrize('footprint', ['ready', 'partial', 'forged', 'exact'])
@pytest.mark.parametrize('part', ['receipt', 'record', 'all'])
async def test_runtime_cannot_hide_missing_proof_even_on_duplicate(alpha, root_redis, footprint, part):
    req, store, dag, item, task, captured = await capture_unprojected(alpha)
    if footprint == 'exact':
        assert (await dag._dispatch_loader.run(**captured))[0] == 'committed'
    elif footprint != 'ready':
        await root_redis.set(DagRedisKey.task_state(item.task_id), 'scheduled', px=600000)
        if footprint == 'forged':
            await root_redis.hset(DagRedisKey.task_meta(item.task_id), mapping={
                'canonical_transition_id': captured['args'][21],
                'canonical_record_hash': captured['args'][22],
                'canonical_command_hash': captured['args'][23],
                'canonical_revision': captured['args'][24],
                'canonical_operation_id': captured['args'][25],
                'dispatch_attempt': captured['args'][19], 'dispatch_worker_id': captured['args'][20],
            })
            await root_redis.sadd(DagRedisKey.run_tasks(item.run_id), item.task_id)
            await root_redis.zadd(dispatch(item).scheduled_zset, {item.task_id: 2000})
    own = json.loads(captured['args'][27])
    keys = store.keyspace(task.sha256)
    if part == 'all':
        await root_redis.delete(*keys.commit_keys()[:6])
    else:
        await root_redis.hdel(keys.receipts if part == 'receipt' else keys.operation_records, own[2])
    await assert_read_only_rejection(root_redis, dag._dispatch_loader, captured)


# Every record-owned projector argument and caller-selected destination is bound.
ARGUMENT_FIELDS = {
    'task': 0, 'run': 1, 'tenant': 2, 'agent': 3, 'group': 4, 'shard': 5,
    'priority': 6, 'admitted_at': 7, 'scheduled_at': 8, 'trace_parent': 13,
    'trace_state': 14, 'policy': 15, 'region': 16, 'payload': 17, 'epoch': 18,
    'attempt': 19, 'worker': 20, 'transition': 21, 'record_hash': 22,
    'command_hash': 23, 'revision': 24, 'operation_id': 25,
}
DESTINATIONS = {'scheduled': 2, 'control': 3, 'shard_stream': 4, 'ready': 5, 'running': 6}


@pytest.mark.asyncio
@pytest.mark.parametrize('changed', list(ARGUMENT_FIELDS) + list(DESTINATIONS) + ['operation'])
async def test_exact_record_binds_projection_inputs(alpha, root_redis, changed):
    req, store, dag, item, task, captured = await capture_unprojected(alpha)
    if changed == 'operation':
        # A real prior TASK_ADMIT receipt is not a TASK_DISPATCH witness.
        probe = await store.load_receipt_probe(task, 'task-admit:v1:' + task.sha256)
        keys, own = store.authority_head_witness(probe.canonical_store_record, probe.receipt)
        captured['keys'][10:] = keys
        captured['args'][27] = json.dumps(own)
    elif changed in DESTINATIONS:
        captured['keys'][DESTINATIONS[changed]] = 'different-destination'
    else:
        captured['args'][ARGUMENT_FIELDS[changed]] = '999' if changed in {
            'shard', 'priority', 'admitted_at', 'scheduled_at', 'attempt', 'revision'
        } else 'different-input'
    await assert_read_only_rejection(root_redis, dag._dispatch_loader, captured)


@pytest.mark.asyncio
@pytest.mark.parametrize('missing', ['witness', 'keys', 'mode'])
async def test_no_witness_cannot_fall_back_to_runtime(alpha, root_redis, missing):
    req, store, dag, item, task, captured = await capture_unprojected(alpha)
    if missing == 'witness':
        captured['args'][27] = '[]'
    elif missing == 'keys':
        captured['keys'] = captured['keys'][:10]
        captured['num_keys'] = 10
    else:
        captured['args'][26] = '0'
    await assert_read_only_rejection(root_redis, dag._dispatch_loader, captured)


@pytest.mark.asyncio
@pytest.mark.parametrize('part', ['receipt', 'record'])
async def test_concurrent_proof_loss_between_preparation_and_eval(alpha, root_redis, part):
    req, store, dag, item, task = await setup(alpha)
    prepared, release = asyncio.Event(), asyncio.Event()
    original = dag._dispatch_loader.run
    async def wait_for_other_client(**kwargs):
        prepared.set()
        await asyncio.wait_for(release.wait(), 5)
        return await original(**kwargs)
    dag._dispatch_loader.run = wait_for_other_client
    pending = asyncio.create_task(dag.task_dispatch_commit(dispatch(item)))
    try:
        await asyncio.wait_for(prepared.wait(), 5)
        head = await store.get_aggregate_snapshot(task)
        keys = store.keyspace(task.sha256)
        async with root_redis.client() as other:
            assert await other.hdel(keys.receipts if part == 'receipt' else keys.operation_records,
                                    head.operation_digest) == 1
        before, writes = await semantic_snapshot(root_redis), await write_counts(root_redis)
        release.set()
        with pytest.raises(TaskDispatchProjectionPendingError) as rejected:
            await pending
        assert rejected.value.status == CONFLICT
        assert await semantic_snapshot(root_redis) == before
        assert await write_counts(root_redis) == writes
    finally:
        release.set()
        if not pending.done():
            pending.cancel()
            await asyncio.gather(pending, return_exceptions=True)


@pytest.mark.asyncio
@pytest.mark.parametrize('footprint', ['claim', 'ready', 'dispatch_exact'])
async def test_stale_dispatch_witness_after_actual_claim(alpha, root_redis, monkeypatch, footprint):
    from hfa_control.worker_reservation import WorkerReservationManager
    from hfa_control.task_claim import TaskClaimManager
    req, store, dag, item, task, captured = await capture_unprojected(alpha)
    assert (await dag._dispatch_loader.run(**captured))[0] == 'committed'
    exact_meta = await root_redis.hgetall(DagRedisKey.task_meta(item.task_id))
    await WorkerReservationManager(root_redis).reserve(
        worker_id='worker-1', task_id=item.task_id, scheduler_epoch='epoch-7', reserved_at_ms=1900,
    )
    monkeypatch.setattr('hfa_control.task_claim.emit_event_background', Mock())
    manager = TaskClaimManager(dag, None, canonical_task_claim_binding=True,
        canonical_task_admit_binding=True, canonical_task_dispatch_binding=True,
        canonical_authority_store=store)
    result = await manager.claim_start(task_id=item.task_id, run_id=item.run_id,
        tenant_id=item.tenant_id, worker_instance_id='worker-1', claimed_at_ms=3000,
        scheduler_epoch='epoch-7')
    assert result.ok
    head = await store.get_aggregate_snapshot(task)
    assert head.revision == 3 and head.state == 'running'
    if footprint != 'claim':
        await root_redis.set(DagRedisKey.task_state(item.task_id),
                             'ready' if footprint == 'ready' else 'scheduled', px=600000)
        await root_redis.hset(DagRedisKey.task_meta(item.task_id), mapping=exact_meta)
    await assert_read_only_rejection(root_redis, dag._dispatch_loader, captured)


@pytest.mark.asyncio
async def test_missing_proof_cannot_emit_runtime_truth_conflict(alpha, root_redis):
    req, store, dag, item, task, captured = await capture_unprojected(alpha)
    own = json.loads(captured['args'][27])
    await root_redis.hdel(store.keyspace(task.sha256).receipts, own[2])
    await root_redis.delete(RedisKey.run_state(item.run_id))
    await assert_read_only_rejection(root_redis, dag._dispatch_loader, captured)
    assert not await root_redis.exists(RedisKey.runtime_truth_conflict_index())
    assert not await root_redis.exists(RedisKey.runtime_truth_conflict_stream())
