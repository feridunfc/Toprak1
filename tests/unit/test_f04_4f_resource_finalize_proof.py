"""Resource finalize: actual RUN_CREATE owner and contested Lua effect boundary."""
import asyncio
import json

import pytest

from hfa.config.keys import RedisKey
from hfa.governance.admission_resource_reservation import AdmissionResourceReservationManager
from hfa_control.run_create_authority import (
    RunCreateAuthorityBinding, RunCreateAuthorityError, run_create_identity,
)
from tests.unit.test_f02a1_root_binding import alpha, root_redis, request
from tests.unit.test_f04_run_create_projection_proof import FAULTS, inject_fault
from tests.unit.test_f04_task_projection_proof import semantic_snapshot, write_counts


async def prepare(alpha, *, finalized=False):
    """Crash immediately before the real finalize Lua call, after durable admission."""
    req = request()
    req.estimated_cost_cents = 125
    binding = alpha.admission._run_create_authority
    await binding.initialise()
    manager = binding.resource_manager
    original = manager._loader.run
    captured = {}

    async def crash(**kwargs):
        if kwargs['args'][0] == 'finalize':
            captured.update(kwargs)
            raise RuntimeError('controlled crash before resource finalize')
        return await original(**kwargs)

    manager._loader.run = crash
    try:
        with pytest.raises(RunCreateAuthorityError, match='controlled crash'):
            await alpha.admission.admit(req)
    finally:
        manager._loader.run = original
    assert captured['num_keys'] == 10
    own = json.loads(captured['args'][12])
    assert own[4] == '1' and own[10] == 'pending'
    assert captured['keys'][4:] == binding.store.keyspace(run_create_identity(req.run_id).sha256).commit_keys()[:6]
    assert await root_state(manager._redis, captured) == 'RESERVED'
    if finalized:
        assert (await original(**captured))[0] == 'finalized'
    return req, binding, captured


async def root_state(redis, call):
    return await redis.hget(call['keys'][0], 'state')


async def rejected(redis, manager, call):
    before, counts = await semantic_snapshot(redis), await write_counts(redis)
    result = await manager._loader.run(**call)
    assert result == ['canonical_run_create_proof_conflict', '', '0']
    assert await semantic_snapshot(redis) == before
    assert await write_counts(redis) == counts


@pytest.mark.asyncio
@pytest.mark.parametrize('finalized', [False, True], ids=['first-effect', 'duplicate'])
@pytest.mark.parametrize('fault', FAULTS + ['head_moved'])
async def test_proof_loss_after_python_validation(alpha, root_redis, finalized, fault):
    req, binding, call = await prepare(alpha, finalized=finalized)
    await inject_fault(root_redis, binding.store, run_create_identity(req.run_id),
                       json.loads(call['args'][12]), fault)
    await rejected(root_redis, binding.resource_manager, call)


@pytest.mark.asyncio
async def test_valid_finalize_duplicate_and_restart(alpha, root_redis):
    req, binding, call = await prepare(alpha)
    authority = await semantic_snapshot(root_redis, call['keys'][4:])
    counters = await semantic_snapshot(root_redis, call['keys'][1:4])
    manager = binding.resource_manager
    counts = await write_counts(root_redis)
    assert (await manager._loader.run(**call)) == ['finalized', 'FINALIZED', '0']
    if counts is not None:
        after = await write_counts(root_redis)
        assert {cmd: after[cmd] - n for cmd, n in counts.items() if after[cmd] != n} == {'hset': 1, 'persist': 1}
    assert await root_state(root_redis, call) == 'FINALIZED'
    assert await semantic_snapshot(root_redis, call['keys'][4:]) == authority
    assert await semantic_snapshot(root_redis, call['keys'][1:4]) == counters
    assert await root_redis.hget(call['keys'][0], 'finalized_at_ms') == call['args'][7]
    before, counts = await semantic_snapshot(root_redis), await write_counts(root_redis)
    call['args'][7] = str(int(call['args'][7]) + 1000)
    assert (await manager._loader.run(**call)) == ['already_finalized', 'FINALIZED', '0']
    assert await semantic_snapshot(root_redis) == before
    assert await write_counts(root_redis) == counts
    restarted = RunCreateAuthorityBinding(root_redis, AdmissionResourceReservationManager(root_redis), RedisKey.stream_control())
    first = await restarted.admit(req, tenant_inflight_limit=10)
    assert first.first_projection
    before, counts = await semantic_snapshot(root_redis), await write_counts(root_redis)
    duplicate = await restarted.admit(req, tenant_inflight_limit=10)
    assert not duplicate.first_projection
    assert await semantic_snapshot(root_redis) == before
    assert await write_counts(root_redis) == counts
    assert await root_redis.xlen(RedisKey.stream_control()) == 1


@pytest.mark.asyncio
async def test_restart_missing_finalize_materializes_once(alpha, root_redis):
    req, _, call = await prepare(alpha)
    restarted = RunCreateAuthorityBinding(root_redis, AdmissionResourceReservationManager(root_redis), RedisKey.stream_control())
    result = await restarted.admit(req, tenant_inflight_limit=10)
    assert result.first_projection and await root_state(root_redis, call) == 'FINALIZED'
    before, counts = await semantic_snapshot(root_redis), await write_counts(root_redis)
    assert not (await restarted.admit(req, tenant_inflight_limit=10)).first_projection
    assert await semantic_snapshot(root_redis) == before
    assert await write_counts(root_redis) == counts


@pytest.mark.asyncio
@pytest.mark.parametrize('state', ['RESERVED', 'FINALIZED'])
async def test_forged_runtime_receipt_cannot_replace_missing_proof(alpha, root_redis, state):
    _, binding, call = await prepare(alpha)
    await root_redis.hset(call['keys'][0], mapping={'state': state, 'finalized_at_ms': '1234'})
    await root_redis.delete(*call['keys'][4:])
    await rejected(root_redis, binding.resource_manager, call)


@pytest.mark.asyncio
@pytest.mark.parametrize('field', [1, 2, 3, 4, 5])
async def test_source_owned_input_drift(alpha, root_redis, field):
    _, binding, call = await prepare(alpha)
    call['args'][field] = {
        1: 'run-create:v1:' + '0' * 64, 2: 'foreign-run',
        3: 'foreign-tenant', 4: '126', 5: '2',
    }[field]
    before, counts = await semantic_snapshot(root_redis), await write_counts(root_redis)
    assert (await binding.resource_manager._loader.run(**call))[0] in ('canonical_run_create_proof_conflict', 'invalid_input')
    assert await semantic_snapshot(root_redis) == before
    assert await write_counts(root_redis) == counts


@pytest.mark.asyncio
@pytest.mark.parametrize('key', list(range(1, 10)))
async def test_counter_or_proof_destination_drift(alpha, root_redis, key):
    _, binding, call = await prepare(alpha)
    call['keys'][key] += ':foreign'
    await rejected(root_redis, binding.resource_manager, call)


@pytest.mark.asyncio
@pytest.mark.parametrize('raw', ['', '{', 'null', '[]', '[1]', json.dumps([''] * 14)])
async def test_missing_or_malformed_witness(alpha, root_redis, raw):
    _, binding, call = await prepare(alpha)
    call['args'][12] = raw
    await rejected(root_redis, binding.resource_manager, call)


@pytest.mark.asyncio
async def test_foreign_run_proof(alpha, root_redis):
    _, binding, call = await prepare(alpha)
    _, _, foreign = await prepare(alpha)
    call['args'][12] = foreign['args'][12]
    call['keys'][4:] = foreign['keys'][4:]
    await rejected(root_redis, binding.resource_manager, call)


@pytest.mark.asyncio
async def test_later_operation_on_same_run_cannot_authorize_finalize(alpha, root_redis):
    req, binding, call = await prepare(alpha)
    from tests.unit.test_f03_terminal_ordering import terminate_run
    run = run_create_identity(req.run_id)
    await terminate_run(binding.store, run)
    keys, witness = await binding.store._authority_head_guard(run)
    assert witness[4] == '2' and witness[10] == 'done'
    call['keys'][4:] = keys
    call['args'][12] = json.dumps(witness)
    await rejected(root_redis, binding.resource_manager, call)


@pytest.mark.asyncio
async def test_public_finalize_without_witness_fails_closed(alpha, root_redis):
    req, binding, call = await prepare(alpha)
    from hfa.governance.admission_resource_reservation import AdmissionResourceReservationInput
    value = AdmissionResourceReservationInput(call['args'][1], req.run_id, req.tenant_id, req.estimated_cost_cents)
    before, counts = await semantic_snapshot(root_redis), await write_counts(root_redis)
    result = await binding.resource_manager.finalize_once(value)
    assert result.status == 'canonical_run_create_proof_conflict'
    assert await semantic_snapshot(root_redis) == before
    assert await write_counts(root_redis) == counts


@pytest.mark.asyncio
async def test_second_connection_removes_receipt_at_effect_boundary(alpha, root_redis):
    binding = alpha.admission._run_create_authority
    await binding.initialise()
    manager = binding.resource_manager
    original = manager._loader.run
    ready, damaged = asyncio.Event(), asyncio.Event()
    captured = {}

    async def interleave(**kwargs):
        if kwargs['args'][0] == 'finalize':
            captured.update(kwargs)
            ready.set()
            await asyncio.wait_for(damaged.wait(), 5)
            captured['before'] = await semantic_snapshot(root_redis)
            captured['counts'] = await write_counts(root_redis)
        return await original(**kwargs)

    async def damage_from_other_connection():
        await asyncio.wait_for(ready.wait(), 5)
        own = json.loads(captured['args'][12])
        async with root_redis.client() as other:
            await other.hdel(captured['keys'][6], own[2])
        damaged.set()

    manager._loader.run = interleave
    writer = asyncio.create_task(damage_from_other_connection())
    try:
        with pytest.raises(RunCreateAuthorityError, match='resource receipt did not reach FINALIZED'):
            await alpha.admission.admit(request())
        await writer
    finally:
        manager._loader.run = original
        if not writer.done():
            writer.cancel()
            await asyncio.gather(writer, return_exceptions=True)
    assert await semantic_snapshot(root_redis) == captured['before']
    assert await write_counts(root_redis) == captured['counts']
    assert await root_state(root_redis, captured) == 'RESERVED'
