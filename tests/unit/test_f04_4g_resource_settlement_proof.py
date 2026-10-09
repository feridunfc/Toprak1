"""F04.4G: production RUN_TERMINATE owner, atomic resource settlement effects."""
import asyncio
import hashlib
import json

import pytest

from hfa.authority import RedisCanonicalAuthorityStore
from hfa.config.keys import RedisKey
from hfa.governance.admission_resource_reservation import AdmissionResourceSettlementInput
from hfa_control.run_terminate_authority import RunTerminateAuthorityBinding, RunTerminateAuthorityError
from tests.unit.test_f02a1_root_binding import alpha, root_redis
from tests.unit.test_f02b_canonical_run_terminate import domain, finish, forged_runtime
from tests.unit.test_f04_run_create_projection_proof import FAULTS, inject_fault
from tests.unit.test_f04_task_projection_proof import semantic_snapshot, write_counts


async def prepare(alpha, redis, state='done', *, settled=False):
    req, store, run, task, binding = await domain(alpha, redis, state)
    head_before = await redis.hgetall(store.keyspace(run.sha256).aggregate)
    manager, captured = binding.resource_manager, {}
    original = manager._loader.run

    async def crash(**kwargs):
        assert kwargs['args'][0] == 'settle'
        captured.update(kwargs)
        raise RuntimeError('controlled crash before settlement')

    manager._loader.run = crash
    try:
        with pytest.raises(RunTerminateAuthorityError, match='controlled crash'):
            await finish(binding, req)
    finally:
        manager._loader.run = original
    assert captured['num_keys'] == 10 and len(captured['args']) == 22
    assert captured['keys'][4:] == store.keyspace(run.sha256).commit_keys()[:6]
    own, creation = [json.loads(x) for x in captured['args'][20:22]]
    assert own[4] == '2' and own[10] == state and creation[4] == '1'
    assert await redis.hget(captured['keys'][0], 'state') == 'FINALIZED'
    if settled:
        assert await original(**captured) == ['settled', 'SETTLED', '1']
    return req, store, run, task, binding, captured, head_before


async def rejected(redis, manager, call):
    before, counts = await semantic_snapshot(redis), await write_counts(redis)
    assert await manager._loader.run(**call) == ['canonical_run_terminate_proof_conflict', '', '0']
    assert await semantic_snapshot(redis) == before
    assert await write_counts(redis) == counts


@pytest.mark.asyncio
@pytest.mark.parametrize('state', ['done', 'failed'])
@pytest.mark.parametrize('settled', [False, True], ids=['first-effect', 'duplicate'])
@pytest.mark.parametrize('fault', FAULTS + ['head_rewound'])
async def test_own_proof_loss_after_python_validation(alpha, root_redis, state, settled, fault):
    req, store, run, task, binding, call, prior = await prepare(alpha, root_redis, state, settled=settled)
    if fault == 'head_rewound':
        await root_redis.hset(call['keys'][4], mapping=prior)
    else:
        await inject_fault(root_redis, store, run, json.loads(call['args'][20]), fault)
    await rejected(root_redis, binding.resource_manager, call)


@pytest.mark.asyncio
@pytest.mark.parametrize('state', ['done', 'failed'])
@pytest.mark.parametrize('settled', [False, True])
@pytest.mark.parametrize('family', ['record', 'receipt', 'index'])
@pytest.mark.parametrize('fault', ['missing', 'checksum', 'valid-envelope-drift'])
async def test_exact_predecessor_required(alpha, root_redis, state, settled, family, fault):
    _, _, _, _, binding, call, _ = await prepare(alpha, root_redis, state, settled=settled)
    creation = json.loads(call['args'][21])
    key = call['keys'][{'record':7, 'receipt':6, 'index':5}[family]]
    field = creation[3] if family == 'index' else creation[2]
    if fault == 'missing':
        await root_redis.hdel(key, field)
    else:
        envelope = json.loads(await root_redis.hget(key, field))
        if fault == 'checksum':
            envelope['storage_sha1'] = '0' * 40
        else:
            payload = json.loads(envelope['payload'])
            payload['canonical_record_hash'] = '0' * 64
            envelope['payload'] = json.dumps(payload, sort_keys=True, separators=(',', ':'))
            envelope['storage_sha1'] = hashlib.sha1(envelope['payload'].encode()).hexdigest()
        await root_redis.hset(key, field, json.dumps(envelope))
    await rejected(root_redis, binding.resource_manager, call)


@pytest.mark.asyncio
@pytest.mark.parametrize('state', ['done', 'failed'])
async def test_normal_duplicate_and_restart_without_task_reauthorization(alpha, root_redis, monkeypatch, state):
    req, store, run, task, binding, call, _ = await prepare(alpha, root_redis, state)
    manager = binding.resource_manager
    authority = await semantic_snapshot(root_redis, call['keys'][4:])
    assert await manager.get_resource_snapshot(req.tenant_id) == {'concurrent_runs':1, 'budget_reserved_cents':125, 'tenant_inflight':1}
    counts = await write_counts(root_redis)
    assert await manager._loader.run(**call) == ['settled', 'SETTLED', '1']
    if counts is not None:
        after = await write_counts(root_redis)
        assert {k:after[k]-v for k,v in counts.items() if after[k]!=v} == {'decrby':3, 'hset':1, 'persist':1}
    assert await manager.get_resource_snapshot(req.tenant_id) == {'concurrent_runs':0, 'budget_reserved_cents':0, 'tenant_inflight':0}
    assert await semantic_snapshot(root_redis, call['keys'][4:]) == authority
    before, counts = await semantic_snapshot(root_redis), await write_counts(root_redis)
    call['args'][7] = '9999'  # retry wall time must not refresh settlement time/TTL
    assert await manager._loader.run(**call) == ['already_settled', 'SETTLED', '0']
    assert await semantic_snapshot(root_redis) == before
    assert await write_counts(root_redis) == counts
    await forged_runtime(root_redis, req, 'running')
    await root_redis.delete(*store.keyspace(task.sha256).commit_keys())
    async def forbidden(*a, **kw):
        pytest.fail('durable RUN_TERMINATE restart must not reauthorize TASK')
    monkeypatch.setattr(RedisCanonicalAuthorityStore, 'run_terminate_task_guard', forbidden)
    restarted = RunTerminateAuthorityBinding(root_redis, resource_manager=manager)
    await restarted.initialise()
    result = await finish(restarted, req)
    assert result.finalized and await root_redis.get(RedisKey.run_state(req.run_id)) == state
    before, counts = await semantic_snapshot(root_redis), await write_counts(root_redis)
    assert (await finish(restarted, req)).finalized
    assert await semantic_snapshot(root_redis) == before
    assert await write_counts(root_redis) == counts


@pytest.mark.asyncio
@pytest.mark.parametrize('state', ['done', 'failed'])
async def test_restart_materializes_missing_settlement_once(alpha, root_redis, state):
    req, _, _, _, binding, call, _ = await prepare(alpha, root_redis, state)
    restarted = RunTerminateAuthorityBinding(root_redis, resource_manager=binding.resource_manager)
    await restarted.initialise()
    assert (await finish(restarted, req)).finalized
    assert await root_redis.hget(call['keys'][0], 'state') == 'SETTLED'
    before, counts = await semantic_snapshot(root_redis), await write_counts(root_redis)
    assert (await finish(restarted, req)).finalized
    assert await semantic_snapshot(root_redis) == before
    assert await write_counts(root_redis) == counts


@pytest.mark.asyncio
@pytest.mark.parametrize('field', [1,2,3,4,12,13,14,15,16,17,18])
async def test_source_bound_input_drift(alpha, root_redis, field):
    _, _, _, _, binding, call, _ = await prepare(alpha, root_redis)
    call['args'][field] = {1:'run-create:v1:'+'0'*64, 2:'other-run', 3:'other-tenant', 4:'126',
                          12:'run-terminate:v1:'+'0'*64, 13:'0'*64, 14:'foreign-transition',
                          15:'0'*64, 16:'0'*64, 17:'3', 18:'failed'}[field]
    await rejected(root_redis, binding.resource_manager, call)


@pytest.mark.asyncio
async def test_forged_cost_and_runtime_receipt_cannot_rebind_canonical_cost(alpha, root_redis):
    _, _, _, _, binding, call, _ = await prepare(alpha, root_redis)
    call['args'][4] = '124'
    await root_redis.hset(call['keys'][0], 'estimated_cost_cents', '124')
    await rejected(root_redis, binding.resource_manager, call)


@pytest.mark.asyncio
@pytest.mark.parametrize('key', list(range(1,10)))
async def test_counter_or_proof_destination_drift(alpha, root_redis, key):
    _, _, _, _, binding, call, _ = await prepare(alpha, root_redis)
    call['keys'][key] += ':foreign'
    await rejected(root_redis, binding.resource_manager, call)


@pytest.mark.asyncio
@pytest.mark.parametrize('arg', [20,21])
@pytest.mark.parametrize('raw', ['', '{', 'null', '[]', '[1]', json.dumps(['']*14)])
async def test_missing_or_malformed_witness(alpha, root_redis, arg, raw):
    _, _, _, _, binding, call, _ = await prepare(alpha, root_redis)
    call['args'][arg] = raw
    await rejected(root_redis, binding.resource_manager, call)


@pytest.mark.asyncio
@pytest.mark.parametrize('part', ['own', 'creation', 'both'])
async def test_foreign_proof(alpha, root_redis, part):
    _, _, _, _, binding, call, _ = await prepare(alpha, root_redis)
    _, _, _, _, _, foreign, _ = await prepare(alpha, root_redis)
    if part in ('own','both'):
        call['args'][20] = foreign['args'][20]
        call['keys'][4:] = foreign['keys'][4:]
    if part in ('creation','both'):
        call['args'][21] = foreign['args'][21]
    await rejected(root_redis, binding.resource_manager, call)


@pytest.mark.asyncio
@pytest.mark.parametrize('settled', [False,True])
async def test_runtime_receipt_cannot_replace_absent_authority(alpha, root_redis, settled):
    _, _, _, _, binding, call, _ = await prepare(alpha, root_redis, settled=settled)
    await root_redis.delete(*call['keys'][4:])
    await rejected(root_redis, binding.resource_manager, call)


@pytest.mark.asyncio
async def test_public_settle_without_witness_rejects(alpha, root_redis):
    _, _, _, _, binding, call, _ = await prepare(alpha, root_redis)
    a=call['args']
    value=AdmissionResourceSettlementInput(run_create_operation_id=a[1],run_id=a[2],tenant_id=a[3],estimated_cost_cents=int(a[4]),run_create_reservation_proof_sha256=a[6],run_terminate_operation_id=a[12],terminal_proof_sha256=a[13],canonical_transition_id=a[14],canonical_record_hash=a[15],canonical_command_hash=a[16],canonical_revision=int(a[17]),final_state=a[18])
    before, counts=await semantic_snapshot(root_redis),await write_counts(root_redis)
    assert (await binding.resource_manager.settle_once(value)).status=='canonical_run_terminate_proof_conflict'
    assert await semantic_snapshot(root_redis)==before
    assert await write_counts(root_redis)==counts


@pytest.mark.asyncio
@pytest.mark.parametrize('part', ['own', 'creation'])
async def test_second_client_proof_loss_at_settlement_boundary(alpha, root_redis, part):
    req, _, _, _, binding = await domain(alpha, root_redis)
    manager=binding.resource_manager
    original=manager._loader.run
    ready, damaged=asyncio.Event(),asyncio.Event()
    captured={}
    async def interleave(**kwargs):
        captured.update(kwargs)
        ready.set()
        await asyncio.wait_for(damaged.wait(),5)
        captured['before']=await semantic_snapshot(root_redis)
        captured['counts']=await write_counts(root_redis)
        return await original(**kwargs)
    async def other_client():
        await asyncio.wait_for(ready.wait(),5)
        proof=json.loads(captured['args'][20 if part=='own' else 21])
        async with root_redis.client() as other:
            await other.hdel(captured['keys'][6],proof[2])
        damaged.set()
    manager._loader.run=interleave
    writer=asyncio.create_task(other_client())
    try:
        with pytest.raises(RunTerminateAuthorityError,match='canonical_run_terminate_proof_conflict'):
            await finish(binding,req)
        await writer
    finally:
        manager._loader.run=original
        if not writer.done():
            writer.cancel();await asyncio.gather(writer,return_exceptions=True)
    assert await semantic_snapshot(root_redis)==captured['before']
    assert await write_counts(root_redis)==captured['counts']
    assert await root_redis.hget(captured['keys'][0],'state')=='FINALIZED'


@pytest.mark.asyncio
async def test_run_create_proof_cannot_authorize_settlement(alpha, root_redis):
    _, _, _, _, binding, call, _ = await prepare(alpha, root_redis)
    call['args'][20] = call['args'][21]
    await rejected(root_redis, binding.resource_manager, call)
