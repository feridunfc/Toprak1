"""F04.3: own RUN evidence races, never a second TASK authorization."""
from dataclasses import replace
import json

import pytest

from hfa.authority import RedisCanonicalAuthorityStore
from hfa.config.keys import RedisKey
from hfa_control.run_terminate_authority import (
    RunTerminateAuthorityBinding, RunTerminateAuthorityError, RunTerminateProjectionManager,
)
from tests.unit.test_f02a1_root_binding import alpha, root_redis
from tests.unit.test_f02b_canonical_run_terminate import domain, finish, forged_runtime
from tests.unit.test_f04_run_create_projection_proof import FAULTS, inject_fault
from tests.unit.test_f04_task_projection_proof import semantic_snapshot, write_counts


@pytest.mark.asyncio
@pytest.mark.parametrize('state', ['done', 'failed'])
@pytest.mark.parametrize('fault', FAULTS + ['head_rewound'])
async def test_own_run_proof_race_blocks_all_projection_writes(alpha, root_redis, state, fault):
    req, store, run, task, binding = await domain(alpha, root_redis, state)
    keys = store.keyspace(run.sha256).commit_keys()[:6]
    prior_head = await root_redis.hgetall(keys[0])
    manager = binding.projection_manager
    original = manager._loader.run
    observed = {}

    async def interleave(**kwargs):
        own = json.loads(kwargs['args'][23])
        assert kwargs['keys'][7:] == keys
        assert own[4] == '2' and own[10] == state
        if fault == 'head_rewound':
            # Controlled head movement after witness preparation, not a new
            # lifecycle operation after terminality. Keep terminal record durable.
            await root_redis.hset(keys[0], mapping=prior_head)
        else:
            await inject_fault(root_redis, store, run, own, fault)
        observed['before'] = await semantic_snapshot(root_redis)
        observed['writes'] = await write_counts(root_redis)
        return await original(**kwargs)

    manager._loader.run = interleave
    with pytest.raises(RunTerminateAuthorityError, match='canonical_run_terminate_proof_conflict'):
        await finish(binding, req)
    assert await semantic_snapshot(root_redis) == observed['before']
    assert await write_counts(root_redis) == observed['writes']
    assert await root_redis.get(RedisKey.run_state(req.run_id)) == 'admitted'
    assert not await root_redis.exists(RedisKey.run_result(req.run_id))
    assert await root_redis.xlen(RedisKey.stream_results()) == 0


async def capture_durable_unprojected(alpha, redis, state):
    req, store, run, task, binding = await domain(alpha, redis, state)
    original = binding.projection_manager.project
    captured = {}

    async def crash(value):
        captured['input'] = value
        raise RuntimeError('controlled crash after RUN durability and settlement')

    binding.projection_manager.project = crash
    try:
        with pytest.raises(RunTerminateAuthorityError, match='controlled crash'):
            await finish(binding, req)
    finally:
        binding.projection_manager.project = original
    assert (await store.get_aggregate_snapshot(run)).state == state
    return req, store, run, task, binding, captured['input']


@pytest.mark.asyncio
@pytest.mark.parametrize('state', ['done', 'failed'])
async def test_restart_projects_once_without_reauthorizing_task(alpha, root_redis, monkeypatch, state):
    req, store, run, task, binding, witness = await capture_durable_unprojected(alpha, root_redis, state)
    await forged_runtime(root_redis, req, 'running')
    await root_redis.delete(*store.keyspace(task.sha256).commit_keys())

    async def forbidden(*args, **kwargs):
        pytest.fail('F04.3 must not reauthorize TASK after durable RUN_TERMINATE')

    monkeypatch.setattr(RedisCanonicalAuthorityStore, 'run_terminate_task_guard', forbidden)
    authority = await semantic_snapshot(root_redis, witness.canonical_head_keys)
    resources = await binding.resource_manager.get_resource_snapshot(req.tenant_id)
    restarted = RunTerminateAuthorityBinding(root_redis, resource_manager=binding.resource_manager)
    await restarted.initialise()
    first = await finish(restarted, req)
    assert first.finalized and not first.already_finalized and first.final_state == state
    assert await root_redis.get(RedisKey.run_state(req.run_id)) == state
    assert await root_redis.xlen(RedisKey.stream_results()) == 1
    assert await semantic_snapshot(root_redis, witness.canonical_head_keys) == authority
    assert await binding.resource_manager.get_resource_snapshot(req.tenant_id) == resources
    before, counts = await semantic_snapshot(root_redis), await write_counts(root_redis)
    again = RunTerminateAuthorityBinding(root_redis, resource_manager=binding.resource_manager)
    await again.initialise()
    duplicate = await finish(again, req)
    assert duplicate.finalized and duplicate.already_finalized
    assert await semantic_snapshot(root_redis) == before
    assert await write_counts(root_redis) == counts


@pytest.mark.asyncio
@pytest.mark.parametrize('state', ['done', 'failed'])
@pytest.mark.parametrize('fault', ['receipt_missing', 'head_revision'])
async def test_stale_restart_witness_cannot_project(alpha, root_redis, state, fault):
    req, store, run, task, binding, witness = await capture_durable_unprojected(alpha, root_redis, state)
    await inject_fault(root_redis, store, run, witness.canonical_head_witness, fault)
    before, counts = await semantic_snapshot(root_redis), await write_counts(root_redis)
    with pytest.raises(RunTerminateAuthorityError, match='canonical_run_terminate_proof_conflict'):
        await RunTerminateProjectionManager(root_redis).project(witness)
    assert await semantic_snapshot(root_redis) == before
    assert await write_counts(root_redis) == counts


@pytest.mark.asyncio
@pytest.mark.parametrize('state', ['done', 'failed'])
@pytest.mark.parametrize('fault', ['receipt_missing', 'head_revision'])
async def test_exact_duplicate_stays_read_only_after_own_proof_movement(alpha, root_redis, state, fault):
    req, store, run, task, binding, witness = await capture_durable_unprojected(alpha, root_redis, state)
    first = await RunTerminateProjectionManager(root_redis).project(witness)
    assert first.projected
    await inject_fault(root_redis, store, run, witness.canonical_head_witness, fault)
    before, counts = await semantic_snapshot(root_redis), await write_counts(root_redis)
    duplicate = await RunTerminateProjectionManager(root_redis).project(witness)
    assert not duplicate.projected and duplicate.stream_entry_id == first.stream_entry_id
    assert await semantic_snapshot(root_redis) == before
    assert await write_counts(root_redis) == counts


@pytest.mark.asyncio
@pytest.mark.parametrize('state', ['done', 'failed'])
async def test_missing_witness_cannot_mutate(alpha, root_redis, state):
    req, store, run, task, binding, witness = await capture_durable_unprojected(alpha, root_redis, state)
    before, counts = await semantic_snapshot(root_redis), await write_counts(root_redis)
    with pytest.raises(RunTerminateAuthorityError, match='canonical_run_terminate_proof_missing'):
        await RunTerminateProjectionManager(root_redis).project(replace(
            witness, canonical_head_keys=(), canonical_head_witness=(),
        ))
    assert await semantic_snapshot(root_redis) == before
    assert await write_counts(root_redis) == counts
