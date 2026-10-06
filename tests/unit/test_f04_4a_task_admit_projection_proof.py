"""F04.4A: real alpha admission, exact own-proof checks at the Lua effect seam."""
import asyncio
import json

import pytest

from hfa.dag.schema import DagRedisKey
from hfa_control.dag_lua import DagLua
from hfa_control.task_admit_authority import (
    TaskAdmitAuthorityError, build_task_admit_command,
)
from tests.unit.test_f02a1_root_binding import alpha, root_redis, create, seed
from tests.unit.test_f04_task_projection_proof import semantic_snapshot, write_counts
from tests.unit.test_f04_run_create_projection_proof import FAULTS, inject_fault
from tests.integration.test_task_dispatch_authority_binding_integration import dispatch


async def setup(alpha):
    req, store, _ = await create(alpha)
    dag = alpha._scheduler.composition.dag_lua
    await dag.initialise()
    item = seed(req)
    task = build_task_admit_command(item).aggregate_identity
    return req, store, dag, item, task


async def capture_unprojected(alpha):
    req, store, dag, item, task = await setup(alpha)
    captured = {}
    original = dag._admit_loader.run
    async def crash(**kwargs):
        captured.update(kwargs)
        raise RuntimeError('controlled stop after canonical TASK_ADMIT commit')
    dag._admit_loader.run = crash
    try:
        with pytest.raises(TaskAdmitAuthorityError):
            await dag.task_admit(item)
    finally:
        dag._admit_loader.run = original
    assert (await store.get_aggregate_snapshot(task)).revision == 1
    return req, store, dag, item, task, captured


@pytest.mark.asyncio
@pytest.mark.parametrize('fault', FAULTS)
async def test_proof_loss_or_corruption_rejected_at_actual_lua(alpha, root_redis, fault):
    req, store, dag, item, task = await setup(alpha)
    original = dag._admit_loader.run
    observed = {}
    async def interleave(**kwargs):
        own = json.loads(kwargs['args'][16])
        assert kwargs['args'][15] == '1'
        assert kwargs['keys'][8:] == store.keyspace(task.sha256).commit_keys()[:6]
        assert own[4] == '1' and own[10] == 'ready'
        await inject_fault(root_redis, store, task, own, fault)
        observed['before'] = await semantic_snapshot(root_redis)
        observed['writes'] = await write_counts(root_redis)
        return await original(**kwargs)
    dag._admit_loader.run = interleave
    with pytest.raises(TaskAdmitAuthorityError):
        await dag.task_admit(item)
    assert await semantic_snapshot(root_redis) == observed['before']
    assert await write_counts(root_redis) == observed['writes']
    assert not await root_redis.exists(DagRedisKey.task_state(item.task_id))
    assert not await root_redis.sismember(DagRedisKey.run_tasks(item.run_id), item.task_id)
    assert await root_redis.zscore(DagRedisKey.tenant_ready_queue(item.tenant_id), item.task_id) is None


@pytest.mark.asyncio
async def test_normal_first_projection_and_exact_duplicate_zero_effect(alpha, root_redis):
    req, store, dag, item, task = await setup(alpha)
    assert (await dag.task_admit(item)).status == 'seeded_root'
    head = await store.get_aggregate_snapshot(task)
    assert head.revision == 1 and head.state == 'ready'
    assert await root_redis.sismember(DagRedisKey.run_tasks(item.run_id), item.task_id)
    assert await root_redis.zscore(DagRedisKey.tenant_ready_queue(item.tenant_id), item.task_id) == item.admitted_at
    before, writes = await semantic_snapshot(root_redis), await write_counts(root_redis)
    duplicate = await dag.task_admit(item)
    assert duplicate.admitted and duplicate.status == 'already_exists'
    assert await semantic_snapshot(root_redis) == before
    assert await write_counts(root_redis) == writes


@pytest.mark.asyncio
async def test_restart_materializes_once(alpha, root_redis):
    req, store, dag, item, task, captured = await capture_unprojected(alpha)
    assert not await root_redis.exists(DagRedisKey.task_state(item.task_id))
    restarted = DagLua(root_redis, canonical_task_admit_binding=True, require_root_task_binding=True)
    assert (await restarted.task_admit(item)).status == 'seeded_root'
    before, writes = await semantic_snapshot(root_redis), await write_counts(root_redis)
    assert (await restarted.task_admit(item)).status == 'already_exists'
    assert (await store.get_aggregate_snapshot(task)).revision == 1
    assert await semantic_snapshot(root_redis) == before
    assert await write_counts(root_redis) == writes


@pytest.mark.asyncio
@pytest.mark.parametrize('footprint', ['absent', 'partial', 'forged', 'exact'])
@pytest.mark.parametrize('part', ['receipt', 'record', 'all'])
async def test_runtime_cannot_hide_missing_proof_even_on_duplicate(alpha, root_redis, footprint, part):
    req, store, dag, item, task, captured = await capture_unprojected(alpha)
    if footprint == 'exact':
        assert (await dag._admit_loader.run(**captured))[0] == 'seeded_root'
    elif footprint != 'absent':
        await root_redis.set(DagRedisKey.task_state(item.task_id), 'ready', px=600000)
        if footprint == 'forged':
            await root_redis.hset(DagRedisKey.task_meta(item.task_id), mapping={
                'task_id':item.task_id, 'run_id':item.run_id, 'tenant_id':item.tenant_id,
            })
            await root_redis.sadd(DagRedisKey.run_tasks(item.run_id), item.task_id)
    own = json.loads(captured['args'][16])
    keys = store.keyspace(task.sha256)
    if part == 'all':
        await root_redis.delete(*keys.commit_keys()[:6])
    else:
        await root_redis.hdel(keys.receipts if part == 'receipt' else keys.operation_records, own[2])
    before, writes = await semantic_snapshot(root_redis), await write_counts(root_redis)
    raw = await dag._admit_loader.run(**captured)
    assert raw[0] == 'canonical_task_admit_proof_conflict'
    assert await semantic_snapshot(root_redis) == before
    assert await write_counts(root_redis) == writes


@pytest.mark.asyncio
@pytest.mark.parametrize('changed', ['run', 'task', 'tenant', 'payload', 'child', 'operation'])
async def test_proof_must_bind_exact_target_and_operation(alpha, root_redis, changed):
    req, store, dag, item, task, captured = await capture_unprojected(alpha)
    # The witness is genuinely accepted canonical proof, not a hand-made hash.
    if changed == 'operation':
        assert (await dag._admit_loader.run(**captured))[0] == 'seeded_root'
        assert (await dag.task_dispatch_commit(dispatch(item))).committed
        head = await store.get_aggregate_snapshot(task)
        probe = await store.load_receipt_probe(task, head.operation_id)
        keys, own = store.authority_head_witness(probe.canonical_store_record, probe.receipt)
        captured['keys'][8:] = keys
        captured['args'][16] = json.dumps(own)
    elif changed == 'child':
        captured['args'].append('unaccepted-child')
    else:
        offset = {'task':0, 'run':1, 'tenant':2, 'payload':6}[changed]
        captured['args'][offset] = 'different-target'
        if changed == 'task':
            captured['keys'][0] = DagRedisKey.task_state('different-target')
            captured['keys'][1] = DagRedisKey.task_meta('different-target')
        elif changed == 'run':
            captured['keys'][6] = DagRedisKey.run_tasks('different-target')
    before, writes = await semantic_snapshot(root_redis), await write_counts(root_redis)
    assert (await dag._admit_loader.run(**captured))[0] == 'canonical_task_admit_proof_conflict'
    assert await semantic_snapshot(root_redis) == before
    assert await write_counts(root_redis) == writes


@pytest.mark.asyncio
async def test_stale_admit_witness_after_actual_head_movement(alpha, root_redis):
    req, store, dag, item, task, captured = await capture_unprojected(alpha)
    assert (await dag._admit_loader.run(**captured))[0] == 'seeded_root'
    assert (await dag.task_dispatch_commit(dispatch(item))).committed
    head = await store.get_aggregate_snapshot(task)
    assert head.revision == 2 and head.state == 'scheduled'
    before, writes = await semantic_snapshot(root_redis), await write_counts(root_redis)
    # Still the original revision-1 witness; runtime EXISTS must not hide drift.
    assert (await dag._admit_loader.run(**captured))[0] == 'canonical_task_admit_proof_conflict'
    assert await semantic_snapshot(root_redis) == before
    assert await write_counts(root_redis) == writes


@pytest.mark.asyncio
@pytest.mark.parametrize('missing', ['witness', 'keys'])
async def test_no_witness_cannot_fall_back_to_runtime(alpha, root_redis, missing):
    req, store, dag, item, task, captured = await capture_unprojected(alpha)
    if missing == 'witness':
        captured['args'][16] = '[]'
    else:
        captured['keys'] = captured['keys'][:8]
        captured['num_keys'] = 8
    before, writes = await semantic_snapshot(root_redis), await write_counts(root_redis)
    assert (await dag._admit_loader.run(**captured))[0] == 'canonical_task_admit_proof_conflict'
    assert await semantic_snapshot(root_redis) == before
    assert await write_counts(root_redis) == writes


@pytest.mark.asyncio
@pytest.mark.parametrize('part', ['receipt', 'record'])
async def test_concurrent_proof_loss_between_preparation_and_eval(alpha, root_redis, part):
    req, store, dag, item, task = await setup(alpha)
    prepared, release = asyncio.Event(), asyncio.Event()
    original = dag._admit_loader.run
    async def wait_for_other_client(**kwargs):
        prepared.set()
        await asyncio.wait_for(release.wait(), 5)
        return await original(**kwargs)
    dag._admit_loader.run = wait_for_other_client
    admission = asyncio.create_task(dag.task_admit(item))
    try:
        await asyncio.wait_for(prepared.wait(), 5)
        head = await store.get_aggregate_snapshot(task)
        keys = store.keyspace(task.sha256)
        # Separate connection; deletion completes after witness preparation.
        async with root_redis.client() as other:
            assert await other.hdel(keys.receipts if part == 'receipt' else keys.operation_records,
                                    head.operation_digest) == 1
        before, writes = await semantic_snapshot(root_redis), await write_counts(root_redis)
        release.set()
        with pytest.raises(TaskAdmitAuthorityError):
            await admission
        assert await semantic_snapshot(root_redis) == before
        assert await write_counts(root_redis) == writes
    finally:
        release.set()
        if not admission.done():
            admission.cancel()
            await asyncio.gather(admission, return_exceptions=True)


@pytest.mark.asyncio
@pytest.mark.parametrize('canonical', [False, True])
async def test_historical_waiting_seed_children_preserved(root_redis, canonical):
    from tests.integration.test_task_admit_authority_binding_integration import seed as historical_seed
    dag = DagLua(root_redis, canonical_task_admit_binding=canonical)
    item = historical_seed(dependency_count=2, child_task_ids=('a','b'))
    result = await dag.task_admit(item)
    assert result.admitted and result.status == 'seeded_waiting'
    assert await root_redis.smembers(DagRedisKey.task_children(item.task_id)) == {'a','b'}
    before, writes = await semantic_snapshot(root_redis), await write_counts(root_redis)
    assert (await dag.task_admit(item)).status == 'already_exists'
    assert await semantic_snapshot(root_redis) == before
    assert await write_counts(root_redis) == writes
