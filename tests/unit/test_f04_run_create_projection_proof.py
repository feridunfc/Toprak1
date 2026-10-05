"""F04.2: production admission, with faults at the projection Lua boundary."""
from dataclasses import replace
import json

import pytest

from hfa.config.keys import RedisKey
from hfa.governance.admission_resource_reservation import AdmissionResourceReservationManager
from hfa_control.run_create_authority import (
    RunCreateAuthorityBinding, RunCreateAuthorityError, RunCreateProjectionManager,
    run_create_identity, run_create_operation_id,
)
from tests.unit.test_f02a1_root_binding import alpha, root_redis, request
from tests.unit.test_f03_terminal_ordering import terminate_run
from tests.unit.test_f04_task_projection_proof import (
    FAULTS as TASK_FAULTS, damage, semantic_snapshot, write_counts,
)


FAULTS = [f for f in TASK_FAULTS if not f.startswith('claim_')] + [
    'receipt_checksum', 'index_checksum', 'envelope_invalid', 'log_tail', 'outbox_tail',
]


async def inject_fault(redis, store, run, own, fault):
    keys = store.keyspace(run.sha256).commit_keys()[:6]
    if fault == 'head_moved':
        # Controlled durable-history movement; no F02B authorization is bypassed
        # in production. This is the existing F03 ordering/recovery fixture.
        await terminate_run(store, run)
        assert (await store.get_aggregate_snapshot(run)).revision == 2
    elif fault in ('log_tail', 'outbox_tail'):
        await redis.xadd(keys[4 if fault == 'log_tail' else 5], {'invalid': 'tail'})
    elif fault in ('receipt_checksum', 'index_checksum', 'envelope_invalid'):
        key = keys[1 if fault == 'index_checksum' else 2]
        field = own[3] if fault == 'index_checksum' else own[2]
        raw = await redis.hget(key, field)
        envelope = json.loads(raw)
        envelope['storage_sha1'] = '0' * 40
        await redis.hset(key, field, '{' if fault == 'envelope_invalid' else json.dumps(envelope))
    else:
        await damage(redis, keys, own, own, fault)


def projection_keys(req, manager):
    return [manager.receipt_key(run_create_operation_id(req.run_id)),
            RedisKey.run_state(req.run_id), RedisKey.stream_control()]


@pytest.mark.asyncio
@pytest.mark.parametrize('fault', FAULTS + ['head_moved'])
async def test_proof_race_rejects_before_any_projection_write(alpha, root_redis, fault):
    req = request()
    binding = alpha.admission._run_create_authority
    await binding.initialise()
    manager, store = binding.projection_manager, binding.store
    run = run_create_identity(req.run_id)
    original = manager._loader.run
    observed = {}

    async def interleave(**kwargs):
        own = json.loads(kwargs['args'][13])
        assert kwargs['keys'][3:] == store.keyspace(run.sha256).commit_keys()[:6]
        assert own[4] == '1' and own[10] == 'pending'
        await inject_fault(root_redis, store, run, own, fault)
        observed['before'] = await semantic_snapshot(root_redis)
        observed['writes'] = await write_counts(root_redis)
        return await original(**kwargs)

    manager._loader.run = interleave
    with pytest.raises(RunCreateAuthorityError, match='canonical_run_create_proof_conflict'):
        await alpha.admission.admit(req)
    assert await semantic_snapshot(root_redis) == observed['before']
    assert await write_counts(root_redis) == observed['writes']
    for key in projection_keys(req, manager):
        assert not await root_redis.exists(key)


async def capture_durable_unprojected(alpha, req):
    binding = alpha.admission._run_create_authority
    manager = binding.projection_manager
    original = manager.project
    captured = {}

    async def crash(value):
        captured['input'] = value
        raise RuntimeError('controlled crash after durable RUN_CREATE')

    manager.project = crash
    try:
        with pytest.raises(RunCreateAuthorityError, match='controlled crash'):
            await alpha.admission.admit(req)
    finally:
        manager.project = original
    return captured['input']


@pytest.mark.asyncio
async def test_restart_materializes_once_and_duplicate_has_zero_effect(alpha, root_redis):
    req = request()
    witness = await capture_durable_unprojected(alpha, req)
    old = alpha.admission._run_create_authority
    authority_before = await semantic_snapshot(root_redis, witness.canonical_head_keys)
    for key in projection_keys(req, old.projection_manager):
        assert not await root_redis.exists(key)
    restarted = RunCreateAuthorityBinding(
        root_redis, AdmissionResourceReservationManager(root_redis), RedisKey.stream_control(),
    )
    first = await restarted.admit(req, tenant_inflight_limit=10)
    assert first.first_projection and first.status == 'run_admitted'
    assert await root_redis.get(RedisKey.run_state(req.run_id)) == 'admitted'
    assert await root_redis.xlen(RedisKey.stream_control()) == 1
    receipt_key = restarted.projection_manager.receipt_key(witness.operation_id)
    assert await root_redis.hget(receipt_key, 'stream_entry_id') == first.stream_entry_id
    assert await semantic_snapshot(root_redis, witness.canonical_head_keys) == authority_before
    before, counts = await semantic_snapshot(root_redis), await write_counts(root_redis)
    second = await restarted.admit(req, tenant_inflight_limit=10)
    assert not second.first_projection and second.status == 'canonical_run_create_already_projected'
    assert second.stream_entry_id == first.stream_entry_id
    assert await semantic_snapshot(root_redis) == before
    assert await write_counts(root_redis) == counts


@pytest.mark.asyncio
@pytest.mark.parametrize('fault', ['receipt_missing', 'head_moved'])
async def test_restart_stale_witness_cannot_project(alpha, root_redis, fault):
    req = request()
    witness = await capture_durable_unprojected(alpha, req)
    binding = alpha.admission._run_create_authority
    await inject_fault(root_redis, binding.store, run_create_identity(req.run_id),
                       witness.canonical_head_witness, fault)
    before, counts = await semantic_snapshot(root_redis), await write_counts(root_redis)
    with pytest.raises(RunCreateAuthorityError, match='canonical_run_create_proof_conflict'):
        await RunCreateProjectionManager(root_redis).project(witness)
    assert await semantic_snapshot(root_redis) == before
    assert await write_counts(root_redis) == counts


@pytest.mark.asyncio
@pytest.mark.parametrize('fault', ['receipt_missing', 'head_moved'])
async def test_exact_projection_receipt_remains_read_only_after_proof_movement(alpha, root_redis, fault):
    req = request()
    witness = await capture_durable_unprojected(alpha, req)
    manager = RunCreateProjectionManager(root_redis)
    first = await manager.project(witness)
    assert first.projected
    binding = alpha.admission._run_create_authority
    await inject_fault(root_redis, binding.store, run_create_identity(req.run_id),
                       witness.canonical_head_witness, fault)
    before, counts = await semantic_snapshot(root_redis), await write_counts(root_redis)
    result = await RunCreateProjectionManager(root_redis).project(witness)
    assert not result.projected and result.stream_entry_id == first.stream_entry_id
    assert await semantic_snapshot(root_redis) == before
    assert await write_counts(root_redis) == counts


@pytest.mark.asyncio
async def test_missing_witness_cannot_mutate(alpha, root_redis):
    req = request()
    witness = await capture_durable_unprojected(alpha, req)
    before, counts = await semantic_snapshot(root_redis), await write_counts(root_redis)
    with pytest.raises(RunCreateAuthorityError, match='canonical_run_create_proof_missing'):
        await RunCreateProjectionManager(root_redis).project(replace(
            witness, canonical_head_keys=(), canonical_head_witness=(),
        ))
    assert await semantic_snapshot(root_redis) == before
    assert await write_counts(root_redis) == counts
