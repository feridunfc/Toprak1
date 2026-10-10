"""F04.4H: accepted RUN_TERMINATE proof and the actual recovery PEL effect."""
import asyncio
import json
from dataclasses import replace
from types import SimpleNamespace

import pytest

from hfa.authority import RedisCanonicalAuthorityStore
from hfa.lua.loader import LuaScriptLoader
from hfa_worker.consumer import CONSUMER_GROUP
from hfa_worker.recovery_ack import ack_message
from hfa_worker.run_finalizing_runtime import RunFinalizingWorkerConsumer
from tests.unit.test_f02a1_root_binding import alpha, root_redis
from tests.unit.test_f02b_canonical_run_terminate import domain, finish, forged_runtime
from tests.unit.test_f04_run_create_projection_proof import FAULTS, inject_fault
from tests.unit.test_f04_task_projection_proof import semantic_snapshot, write_counts


async def prepare(alpha, redis, state='done'):
    req, store, run, task, binding = await domain(alpha, redis, state)
    prior = await redis.hgetall(store.keyspace(run.sha256).aggregate)
    result = await finish(binding, req)
    assert result.ack_allowed and result.finalized
    assert result.canonical_head_keys == tuple(store.keyspace(run.sha256).commit_keys()[:6])
    assert result.canonical_head_witness[4] == '2'
    stream = 'hfa:f04-4h:delivery:' + req.run_id
    await redis.xgroup_create(stream, CONSUMER_GROUP, id='0', mkstream=True)
    message = await redis.xadd(stream, {'run_id':req.run_id, 'task_id':req.root_task_id, 'tenant_id':req.tenant_id})
    await redis.xreadgroup(CONSUMER_GROUP, 'worker-h', {stream:'>'}, count=1)
    await redis.pexpire(stream, 600000)
    ctx = SimpleNamespace(run_id=req.run_id, tenant_id=req.tenant_id, task_id=req.root_task_id)
    return req, store, run, task, binding, result, stream, message, ctx, prior


async def invoke(redis, result, stream, message, ctx):
    return await ack_message(redis, stream, CONSUMER_GROUP, message, ctx=ctx, run_termination=result)


async def rejected(redis, result, stream, message, ctx):
    before, counts = await semantic_snapshot(redis), await write_counts(redis)
    assert await invoke(redis, result, stream, message, ctx) == 'recovery_ack_proof_conflict'
    assert await semantic_snapshot(redis) == before
    assert await write_counts(redis) == counts


@pytest.mark.asyncio
@pytest.mark.parametrize('state', ['done','failed'])
@pytest.mark.parametrize('acked', [False,True], ids=['pending','ack-retry'])
@pytest.mark.parametrize('fault', FAULTS + ['head_rewound'])
async def test_proof_loss_at_ack_boundary(alpha, root_redis, monkeypatch, state, acked, fault):
    req, store, run, _, _, result, stream, message, ctx, prior = await prepare(alpha, root_redis, state)
    if acked:
        assert await invoke(root_redis, result, stream, message, ctx) == 'acked'
    original, observed = LuaScriptLoader.run, {}
    async def interleave(loader, **call):
        assert loader._path.name == 'worker_recovery_ack.lua'
        assert call['num_keys'] == 7
        if fault == 'head_rewound':
            await root_redis.hset(result.canonical_head_keys[0], mapping=prior)
        else:
            await inject_fault(root_redis, store, run, result.canonical_head_witness, fault)
        observed['before'], observed['counts'] = await semantic_snapshot(root_redis), await write_counts(root_redis)
        return await original(loader, **call)
    monkeypatch.setattr(LuaScriptLoader, 'run', interleave)
    assert await invoke(root_redis, result, stream, message, ctx) == 'recovery_ack_proof_conflict'
    assert await semantic_snapshot(root_redis) == observed['before']
    assert await write_counts(root_redis) == observed['counts']
    assert (await root_redis.xpending(stream, CONSUMER_GROUP))['pending'] == (0 if acked else 1)


@pytest.mark.asyncio
@pytest.mark.parametrize('state', ['done','failed'])
async def test_normal_ack_duplicate_and_restart_without_task_reauthorization(alpha, root_redis, monkeypatch, state):
    req, store, run, task, binding, result, stream, message, ctx, _ = await prepare(alpha, root_redis, state)
    # Durable RUN evidence is authoritative; TASK storage is no longer consulted.
    await forged_runtime(root_redis, req, 'running')
    await root_redis.delete(*store.keyspace(task.sha256).commit_keys())
    async def forbidden(*a, **kw):
        pytest.fail('ACK must not authorize TASK or invoke executor/settlement/projection')
    monkeypatch.setattr(RedisCanonicalAuthorityStore, 'run_terminate_task_guard', forbidden)
    monkeypatch.setattr(binding.resource_manager, 'settle_once', forbidden)
    monkeypatch.setattr(binding.projection_manager, 'project', forbidden)
    before, counts = await semantic_snapshot(root_redis), await write_counts(root_redis)
    assert await invoke(root_redis, result, stream, message, ctx) == 'acked'
    after = await semantic_snapshot(root_redis)
    assert {k:v for k,v in before.items() if k!=stream} == {k:v for k,v in after.items() if k!=stream}
    assert before[stream][2] == after[stream][2]  # absolute TTL unchanged
    assert before[stream][1][0] == after[stream][1][0]  # stream contents unchanged
    assert (await root_redis.xpending(stream, CONSUMER_GROUP))['pending'] == 0
    if counts is not None:
        new = await write_counts(root_redis)
        assert {k:new[k]-v for k,v in counts.items() if new[k]!=v} == {'xack':1}
    # Each invoke constructs a fresh loader; restart has no process-local receipt.
    counts = await write_counts(root_redis)
    assert await invoke(root_redis, result, stream, message, ctx) == 'already_acked'
    assert await semantic_snapshot(root_redis) == after
    assert await write_counts(root_redis) == counts


@pytest.mark.asyncio
@pytest.mark.parametrize('fault', ['none','receipt_missing','record_missing'])
@pytest.mark.parametrize('state', ['done','failed'])
async def test_actual_worker_finalize_then_ack_window(alpha, root_redis, state, fault):
    req, store, run, _, binding, _, stream, message, ctx, _ = await prepare(alpha, root_redis, state)
    observed = {}
    async def finalize(context, **kw):
        result = await finish(binding, req)
        if fault != 'none':
            await inject_fault(root_redis, store, run, result.canonical_head_witness, fault)
        observed['before'], observed['counts'] = await semantic_snapshot(root_redis), await write_counts(root_redis)
        return result
    worker = object.__new__(RunFinalizingWorkerConsumer)
    worker._redis = root_redis
    worker._task_consumer = SimpleNamespace(finalize_terminal_duplicate=finalize)
    await worker._finalize_run_and_ack(ctx, terminal_state=state, msg_id=message, stream=stream)
    assert (await root_redis.xpending(stream, CONSUMER_GROUP))['pending'] == (0 if fault=='none' else 1)
    if fault != 'none':
        assert await semantic_snapshot(root_redis) == observed['before']
        assert await write_counts(root_redis) == observed['counts']


@pytest.mark.asyncio
@pytest.mark.parametrize('field', ['run_id','tenant_id','task_id'])
async def test_foreign_transport_context(alpha, root_redis, field):
    _, _, _, _, _, result, stream, message, ctx, _ = await prepare(alpha, root_redis)
    setattr(ctx, field, 'foreign')
    await rejected(root_redis, result, stream, message, ctx)


@pytest.mark.asyncio
@pytest.mark.parametrize('field', ['run_id','tenant_id','task_id'])
async def test_foreign_stream_entry(alpha, root_redis, field):
    _, _, _, _, _, result, stream, _, ctx, _ = await prepare(alpha, root_redis)
    fields = vars(ctx).copy()
    fields[field] = 'foreign'
    message = await root_redis.xadd(stream, fields)
    await root_redis.xreadgroup(CONSUMER_GROUP, 'worker-h', {stream:'>'}, count=1)
    await rejected(root_redis, result, stream, message, ctx)


@pytest.mark.asyncio
@pytest.mark.parametrize('field', ['final_state','canonical_transition_id','canonical_record_hash','canonical_command_hash','canonical_revision','terminal_proof_sha256'])
async def test_result_binding_drift(alpha, root_redis, field):
    _, _, _, _, _, result, stream, message, ctx, _ = await prepare(alpha, root_redis)
    result = replace(result, **{field: 99 if field=='canonical_revision' else 'foreign'})
    await rejected(root_redis, result, stream, message, ctx)


@pytest.mark.asyncio
async def test_foreign_own_proof(alpha, root_redis):
    _, _, _, _, _, _, stream, message, ctx, _ = await prepare(alpha, root_redis)
    _, _, _, _, _, foreign, _, _, _, _ = await prepare(alpha, root_redis)
    await rejected(root_redis, foreign, stream, message, ctx)


@pytest.mark.asyncio
@pytest.mark.parametrize('footprint', ['exact','partial'])
async def test_runtime_finalized_without_proof_cannot_ack(alpha, root_redis, footprint):
    req, store, run, _, _, result, stream, message, ctx, _ = await prepare(alpha, root_redis)
    await root_redis.delete(*result.canonical_head_keys)
    if footprint == 'partial':
        await forged_runtime(root_redis, req, 'done')
    await rejected(root_redis, result, stream, message, ctx)
    await rejected(root_redis, SimpleNamespace(ack_allowed=True, finalized=True), stream, message, ctx)


@pytest.mark.asyncio
@pytest.mark.parametrize('raw', ['', '{', 'null', '[]', '[1]', json.dumps(['']*14)])
async def test_malformed_witness(alpha, root_redis, monkeypatch, raw):
    _, _, _, _, _, result, stream, message, ctx, _ = await prepare(alpha, root_redis)
    original = LuaScriptLoader.run
    async def alter(loader, **call):
        call['args'][0] = raw
        return await original(loader, **call)
    monkeypatch.setattr(LuaScriptLoader, 'run', alter)
    await rejected(root_redis, result, stream, message, ctx)


@pytest.mark.asyncio
@pytest.mark.parametrize('case', ['missing-stream','wrong-type','trimmed-entry','missing-group','invalid-id'])
async def test_transport_failure_is_read_only(alpha, root_redis, case):
    _, _, _, _, _, result, stream, message, ctx, _ = await prepare(alpha, root_redis)
    if case in {'missing-stream','wrong-type'}:
        await root_redis.delete(stream)
        if case == 'wrong-type':
            await root_redis.set(stream, 'invalid')
    elif case == 'trimmed-entry':
        await root_redis.xdel(stream, message)
    elif case == 'missing-group':
        await root_redis.xgroup_destroy(stream, CONSUMER_GROUP)
    else:
        message = 'invalid'
    await rejected(root_redis, result, stream, message, ctx)


@pytest.mark.asyncio
@pytest.mark.parametrize('state', ['done','failed'])
async def test_second_client_proof_loss(alpha, root_redis, monkeypatch, state):
    _, _, _, _, _, result, stream, message, ctx, _ = await prepare(alpha, root_redis, state)
    if root_redis.__class__.__module__.startswith('fakeredis'):
        import fakeredis.aioredis
        other = fakeredis.aioredis.FakeRedis(connection_pool=root_redis.connection_pool)
    else:
        from redis.asyncio import Redis
        other = Redis.from_url('redis://127.0.0.1:6389/0', decode_responses=True)
    ready, damaged = asyncio.Event(), asyncio.Event()
    async def lose_proof():
        await ready.wait()
        await other.hdel(result.canonical_head_keys[2], result.canonical_head_witness[2])
        damaged.set()
    race = asyncio.create_task(lose_proof())
    original, observed = LuaScriptLoader.run, {}
    async def interleave(loader, **call):
        ready.set()
        await asyncio.wait_for(damaged.wait(), 5)
        observed['before'], observed['counts'] = await semantic_snapshot(root_redis), await write_counts(root_redis)
        return await original(loader, **call)
    monkeypatch.setattr(LuaScriptLoader, 'run', interleave)
    try:
        assert await invoke(root_redis, result, stream, message, ctx) == 'recovery_ack_proof_conflict'
        await race
        assert await semantic_snapshot(root_redis) == observed['before']
        assert await write_counts(root_redis) == observed['counts']
        assert (await root_redis.xpending(stream, CONSUMER_GROUP))['pending'] == 1
    finally:
        if not race.done():
            race.cancel()
        await other.aclose()


@pytest.mark.asyncio
@pytest.mark.parametrize('index', range(6))
async def test_wrong_proof_key_destination(alpha, root_redis, index):
    _, _, _, _, _, result, stream, message, ctx, _ = await prepare(alpha, root_redis)
    keys = list(result.canonical_head_keys)
    keys[index] += ':foreign'
    await rejected(root_redis, replace(result, canonical_head_keys=tuple(keys)), stream, message, ctx)


@pytest.mark.asyncio
async def test_run_create_cannot_authorize_ack(alpha, root_redis):
    from hfa_control.run_create_authority import run_create_operation_id
    _, store, run, _, _, result, stream, message, ctx, _ = await prepare(alpha, root_redis)
    probe = await store.load_receipt_probe(run, run_create_operation_id(ctx.run_id))
    keys, witness = store.authority_head_witness(probe.canonical_store_record, probe.receipt)
    await rejected(root_redis, replace(result, canonical_head_keys=tuple(keys), canonical_head_witness=tuple(witness)), stream, message, ctx)


@pytest.mark.asyncio
async def test_concurrent_exact_ack_retries_have_one_effect(alpha, root_redis):
    _, _, _, _, _, result, stream, message, ctx, _ = await prepare(alpha, root_redis)
    counts = await write_counts(root_redis)
    statuses = await asyncio.gather(*(invoke(root_redis, result, stream, message, ctx) for _ in range(2)))
    assert sorted(statuses) == ['acked','already_acked']
    assert (await root_redis.xpending(stream, CONSUMER_GROUP))['pending'] == 0
    if counts is not None:
        after = await write_counts(root_redis)
        assert {k:after[k]-v for k,v in counts.items() if after[k]!=v} == {'xack':1}
