"""F04.4C: exact canonical claim proof and every claim effect in one Lua call."""
import asyncio
import json

import pytest

from hfa.config.keys import RedisKey
from hfa.dag.schema import DagRedisKey
from hfa_control.dag_lua import DagLua
from hfa_control.event_store import EventStore
from hfa_control.task_claim import TaskClaimManager
from hfa_control.worker_reservation import WorkerReservationManager
from tests.unit.test_f02a1_root_binding import alpha, root_redis
from tests.unit.test_f04_4b_task_dispatch_projection_proof import setup as admitted_setup
from tests.unit.test_f04_task_projection_proof import semantic_snapshot as base_snapshot, write_counts
from tests.unit.test_f04_run_create_projection_proof import FAULTS, inject_fault
from tests.integration.test_task_dispatch_authority_binding_integration import dispatch

CONFLICT = 'canonical_projection_conflict'


async def snapshot(redis):
    # Extend the accepted semantic oracle for the existing EventStore LIST.
    keys = await redis.keys('*')
    lists = [k for k in keys if await redis.type(k) == 'list']
    result = await base_snapshot(redis, [k for k in keys if k not in lists])
    for k in lists:
        result[k] = ('list', await redis.lrange(k, 0, -1), await redis.pexpiretime(k))
    return result


def manager_for(redis, dag, store):
    return TaskClaimManager(dag, EventStore(redis), canonical_task_claim_binding=True,
        canonical_task_admit_binding=True, canonical_task_dispatch_binding=True,
        canonical_authority_store=store)


async def claim(manager, item, **changes):
    args = dict(task_id=item.task_id, run_id=item.run_id, tenant_id=item.tenant_id,
                worker_instance_id='worker-1', claimed_at_ms=3000, scheduler_epoch='epoch-7')
    args.update(changes)
    return await manager.claim_start(**args)


async def setup(alpha, redis):
    req, store, dag, item, task = await admitted_setup(alpha)
    assert (await dag.task_dispatch_commit(dispatch(item))).committed
    reservation = await WorkerReservationManager(redis).reserve(
        worker_id='worker-1', task_id=item.task_id, scheduler_epoch='epoch-7', reserved_at_ms=1900)
    assert reservation.ok
    manager = manager_for(redis, dag, store)
    return req, store, dag, item, task, manager


async def capture_unprojected(alpha, redis):
    req, store, dag, item, task, manager = await setup(alpha, redis)
    captured, original = {}, dag._claim_loader.run
    async def crash(**kwargs):
        captured.update(kwargs)
        raise RuntimeError('controlled crash after durable TASK_CLAIM')
    dag._claim_loader.run = crash
    try:
        assert (await claim(manager, item)).status == 'canonical_projection_pending'
    finally:
        dag._claim_loader.run = original
    head = await store.get_aggregate_snapshot(task)
    assert head.revision == 3 and head.state == 'running'
    return req, store, dag, item, task, manager, captured


async def assert_rejection(redis, loader, captured):
    before, writes = await snapshot(redis), await write_counts(redis)
    assert (await loader.run(**captured))[0] == CONFLICT
    assert await snapshot(redis) == before
    assert await write_counts(redis) == writes


@pytest.mark.asyncio
async def test_normal_claim_and_exact_duplicate_zero_effect(alpha, root_redis):
    req, store, dag, item, task, manager = await setup(alpha, root_redis)
    assert (await claim(manager, item)).ok
    head = await store.get_aggregate_snapshot(task)
    assert head.revision == 3 and head.state == 'running'
    assert await root_redis.get(DagRedisKey.task_state(item.task_id)) == 'running'
    assert await root_redis.zscore(dispatch(item).scheduled_zset, item.task_id) is None
    assert await root_redis.zscore(dispatch(item).running_zset, item.task_id) == 3000
    assert not await root_redis.exists(DagRedisKey.worker_reservation('worker-1'))
    assert not await root_redis.exists(DagRedisKey.task_reservation_owner(item.task_id))
    events = await EventStore(root_redis).get_run_history(item.run_id)
    assert len(events) == 1 and events[0]['event_type'] == 'TASK_CLAIMED'
    assert events[0]['worker_id'] == 'worker-1'
    assert events[0]['details'] == {'tenant_id': item.tenant_id, 'claim_epoch': '1'}
    before, writes = await snapshot(root_redis), await write_counts(root_redis)
    duplicate = await claim(manager, item, claimed_at_ms=9999)
    assert not duplicate.ok and duplicate.status == 'canonical_claim_already_projected'
    assert await snapshot(root_redis) == before
    assert await write_counts(root_redis) == writes


@pytest.mark.asyncio
@pytest.mark.parametrize('fault', FAULTS)
async def test_proof_loss_or_corruption_rejected_at_actual_lua(alpha, root_redis, fault):
    req, store, dag, item, task, manager = await setup(alpha, root_redis)
    original, observed = dag._claim_loader.run, {}
    async def interleave(**kwargs):
        own = json.loads(kwargs['args'][24])
        assert kwargs['args'][9] == '1'
        assert kwargs['keys'][9:15] == store.keyspace(task.sha256).commit_keys()[:6]
        assert own[4] == '3' and own[10] == 'running'
        await inject_fault(root_redis, store, task, own, fault)
        observed['before'] = await snapshot(root_redis)
        observed['writes'] = await write_counts(root_redis)
        return await original(**kwargs)
    dag._claim_loader.run = interleave
    result = await claim(manager, item)
    assert not result.ok and result.status == CONFLICT
    assert await snapshot(root_redis) == observed['before']
    assert await write_counts(root_redis) == observed['writes']
    assert await root_redis.get(DagRedisKey.task_state(item.task_id)) == 'scheduled'
    assert await root_redis.exists(DagRedisKey.worker_reservation('worker-1'))
    assert await root_redis.llen(EventStore.event_key(item.run_id)) == 0


@pytest.mark.asyncio
async def test_restart_projects_once(alpha, root_redis):
    req, store, dag, item, task, manager, captured = await capture_unprojected(alpha, root_redis)
    restarted = manager_for(root_redis, DagLua(root_redis), store)
    assert (await claim(restarted, item, claimed_at_ms=8888)).ok
    assert await root_redis.hget(DagRedisKey.task_meta(item.task_id), 'claimed_at_ms') == '3000'
    before, writes = await snapshot(root_redis), await write_counts(root_redis)
    assert (await claim(restarted, item)).status == 'canonical_claim_already_projected'
    assert await snapshot(root_redis) == before
    assert await write_counts(root_redis) == writes
    assert await root_redis.llen(EventStore.event_key(item.run_id)) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize('footprint', ['absent', 'partial', 'forged', 'exact'])
@pytest.mark.parametrize('part', ['receipt', 'record', 'all'])
async def test_runtime_footprint_never_substitutes_for_proof(alpha, root_redis, footprint, part):
    req, store, dag, item, task, manager, captured = await capture_unprojected(alpha, root_redis)
    if footprint == 'exact':
        assert (await dag._claim_loader.run(**captured))[0] == 'task_claimed'
    elif footprint == 'absent':
        await root_redis.delete(DagRedisKey.task_state(item.task_id), DagRedisKey.task_meta(item.task_id))
    else:
        await root_redis.set(DagRedisKey.task_state(item.task_id), 'running', px=600000)
        if footprint == 'forged':
            a = captured['args']
            await root_redis.hset(DagRedisKey.task_meta(item.task_id), mapping={
                'worker_instance_id': a[1], 'claim_epoch': a[17],
                'claim_canonical_transition_id': a[11], 'claim_canonical_record_hash': a[12],
                'claim_canonical_command_hash': a[13], 'claim_canonical_revision': a[14],
                'claim_canonical_operation_id': a[15], 'dispatch_canonical_transition_id': a[18],
                'dispatch_canonical_record_hash': a[19], 'dispatch_canonical_command_hash': a[20],
                'dispatch_canonical_revision': a[21], 'dispatch_canonical_operation_id': a[22],
            })
            await root_redis.zadd(dispatch(item).running_zset, {item.task_id: 3000})
    own, keys = json.loads(captured['args'][24]), store.keyspace(task.sha256)
    if part == 'all': await root_redis.delete(*keys.commit_keys()[:6])
    else: await root_redis.hdel(keys.receipts if part == 'receipt' else keys.operation_records, own[2])
    await assert_rejection(root_redis, dag._claim_loader, captured)


ARGUMENT_FIELDS = {'task':0, 'worker':1, 'claimed_at':2, 'heartbeat_score':5, 'scheduler_epoch':6,
    'legacy_direct':7, 'run':8, 'tenant':10, 'transition':11, 'record_hash':12, 'command_hash':13, 'revision':14,
    'operation_id':15, 'previous_epoch':16, 'claim_epoch':17, 'dispatch_transition':18,
    'dispatch_record_hash':19, 'dispatch_command_hash':20, 'dispatch_revision':21,
    'dispatch_operation':22, 'dispatch_attempt':23}
DESTINATIONS = {'task_state':0, 'task_meta':1, 'scheduled':2, 'running':3,
                'worker_reservation':4, 'owner_reservation':5, 'parent_run':6,
                'conflict_index':7, 'conflict_stream':8, 'event':15}


@pytest.mark.asyncio
@pytest.mark.parametrize('changed', list(ARGUMENT_FIELDS) + list(DESTINATIONS) + ['operation'])
async def test_exact_claim_record_binds_inputs_and_effects(alpha, root_redis, changed):
    req, store, dag, item, task, manager, captured = await capture_unprojected(alpha, root_redis)
    if changed == 'operation':
        probe = await store.load_receipt_probe(task, 'task-dispatch:v1:' + task.sha256 + ':attempt:1')
        keys, own = store.authority_head_witness(probe.canonical_store_record, probe.receipt)
        captured['keys'][9:15] = keys
        captured['args'][24] = json.dumps(own)
    elif changed in DESTINATIONS:
        captured['keys'][DESTINATIONS[changed]] = 'foreign-target'
    elif changed == 'legacy_direct':
        captured['args'][7] = '1'
    else:
        captured['args'][ARGUMENT_FIELDS[changed]] = '999' if changed in {
            'claimed_at', 'heartbeat_score', 'revision', 'previous_epoch', 'claim_epoch',
            'dispatch_revision', 'dispatch_attempt'} else 'different-input'
    await assert_rejection(root_redis, dag._claim_loader, captured)


@pytest.mark.asyncio
@pytest.mark.parametrize('missing', ['witness', 'keys', 'mode'])
async def test_no_proof_transport_cannot_fall_back(alpha, root_redis, missing):
    req, store, dag, item, task, manager, captured = await capture_unprojected(alpha, root_redis)
    if missing == 'witness': captured['args'][24] = '[]'
    elif missing == 'keys':
        captured['keys'] = captured['keys'][:9]
        captured['num_keys'] = 9
    else: captured['args'][9] = '0'
    await assert_rejection(root_redis, dag._claim_loader, captured)


@pytest.mark.asyncio
@pytest.mark.parametrize('part', ['receipt', 'record'])
async def test_separate_client_proof_loss_after_preparation(alpha, root_redis, part):
    req, store, dag, item, task, manager = await setup(alpha, root_redis)
    prepared, release = asyncio.Event(), asyncio.Event()
    original = dag._claim_loader.run
    async def barrier(**kwargs):
        prepared.set()
        await asyncio.wait_for(release.wait(), 5)
        return await original(**kwargs)
    dag._claim_loader.run = barrier
    pending = asyncio.create_task(claim(manager, item))
    try:
        await asyncio.wait_for(prepared.wait(), 5)
        head, keys = await store.get_aggregate_snapshot(task), store.keyspace(task.sha256)
        async with root_redis.client() as other:
            assert await other.hdel(keys.receipts if part == 'receipt' else keys.operation_records,
                                    head.operation_digest) == 1
        before, writes = await snapshot(root_redis), await write_counts(root_redis)
        release.set()
        assert (await pending).status == CONFLICT
        assert await snapshot(root_redis) == before
        assert await write_counts(root_redis) == writes
    finally:
        release.set()
        if not pending.done():
            pending.cancel()
            await asyncio.gather(pending, return_exceptions=True)


@pytest.mark.asyncio
@pytest.mark.parametrize('terminal', ['done', 'failed'])
@pytest.mark.parametrize('footprint', ['terminal', 'preclaim', 'exact_claim'])
async def test_stale_claim_after_real_terminal_transition(alpha, root_redis, terminal, footprint):
    from hfa_control.task_terminal_authority import TaskTerminalAuthorityBinding
    req, store, dag, item, task, manager, captured = await capture_unprojected(alpha, root_redis)
    preclaim = await root_redis.hgetall(DagRedisKey.task_meta(item.task_id))
    assert (await dag._claim_loader.run(**captured))[0] == 'task_claimed'
    exact = await root_redis.hgetall(DagRedisKey.task_meta(item.task_id))
    binding = TaskTerminalAuthorityBinding(root_redis)
    args = dict(task_id=item.task_id, run_id=item.run_id, tenant_id=item.tenant_id,
                finished_at_ms=4000, worker_instance_id='worker-1', scheduler_epoch='epoch-7', claim_epoch='1')
    if terminal == 'done': result = await binding.complete(**args, output_data='{}')
    else: result = await binding.fail(**args, reason_code='controlled_failure')
    head = await store.get_aggregate_snapshot(task)
    assert head.revision == 4 and head.state == terminal
    assert await root_redis.get(DagRedisKey.task_state(item.task_id)) == terminal
    if footprint != 'terminal':
        await root_redis.set(DagRedisKey.task_state(item.task_id),
                             'scheduled' if footprint == 'preclaim' else 'running', px=600000)
        await root_redis.delete(DagRedisKey.task_meta(item.task_id))
        await root_redis.hset(DagRedisKey.task_meta(item.task_id),
                             mapping=preclaim if footprint == 'preclaim' else exact)
    await assert_rejection(root_redis, dag._claim_loader, captured)


@pytest.mark.asyncio
async def test_missing_own_proof_cannot_emit_truth_conflict(alpha, root_redis):
    req, store, dag, item, task, manager, captured = await capture_unprojected(alpha, root_redis)
    own = json.loads(captured['args'][24])
    await root_redis.hdel(store.keyspace(task.sha256).receipts, own[2])
    await root_redis.delete(RedisKey.run_state(item.run_id))
    await assert_rejection(root_redis, dag._claim_loader, captured)
    assert not await root_redis.exists(RedisKey.runtime_truth_conflict_index())
    assert not await root_redis.exists(RedisKey.runtime_truth_conflict_stream())
