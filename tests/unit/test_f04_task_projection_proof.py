"""F04.1 proof-loss races at the actual projector Lua boundary (offline)."""
from __future__ import annotations

import hashlib
import json

import pytest

from hfa.config.keys import RedisKey
from hfa.dag.schema import DagRedisKey
from hfa_control.task_terminal_authority import TaskTerminalAuthorityBinding, TaskTerminalAuthorityError
from tests.unit import test_f03_terminal_ordering as f03
from tests.unit.test_f03_terminal_ordering import (
    domain, projection_domain, prepare,
    test_exact_projected_duplicate_is_read_only_through_coordinator,
    test_prior_terminal_missing_projection_is_blocked_after_run_terminal,
    test_run_terminalization_between_projection_guard_read_and_lua_is_read_only,
    test_terminal_projection_drift_is_not_exact_no_op,
)


def text(value):
    return value.decode() if isinstance(value, bytes) else value


async def semantic_snapshot(redis, keys=None):
    """Values, absolute expiration deadlines and PEL; no DUMP byte ordering."""
    result = {}
    for key in sorted(keys if keys is not None else await redis.keys('*')):
        kind = text(await redis.type(key))
        if kind == 'string':
            value = await redis.get(key)
        elif kind == 'hash':
            value = await redis.hgetall(key)
        elif kind == 'set':
            value = await redis.smembers(key)
        elif kind == 'zset':
            value = await redis.zrange(key, 0, -1, withscores=True)
        elif kind == 'stream':
            groups = []
            for group in await redis.xinfo_groups(key):
                name = group['name']
                entries = await redis.xpending_range(key, name, '-', '+', 100)
                groups.append((group, [
                    {k: v for k, v in entry.items() if k != 'time_since_delivered'}
                    for entry in entries
                ]))
            value = (await redis.xrange(key), groups)
        elif kind == 'none':
            value = None
        else:
            raise AssertionError(f'unexpected key type: {kind}')
        result[key] = (kind, value, await redis.pexpiretime(key))
    return result


@pytest.fixture(autouse=True)
def semantic_f03_oracles(monkeypatch):
    # Reuse the accepted F03 behavior, with semantic snapshots on real Redis too.
    monkeypatch.setattr(f03, 'snapshot', semantic_snapshot)


async def write_counts(redis):
    if redis.__class__.__module__.startswith('fakeredis'):
        return None
    commands = {
        'set', 'setex', 'psetex', 'setnx', 'mset', 'hset', 'hsetnx', 'hdel',
        'del', 'unlink', 'expire', 'pexpire', 'expireat', 'pexpireat', 'persist',
        'zadd', 'zrem', 'sadd', 'srem', 'incr', 'decr', 'incrby', 'decrby',
        'xadd', 'xack', 'xdel', 'xclaim', 'xautoclaim', 'lpush', 'rpush',
    }
    stats = await redis.info('commandstats')
    return {cmd: stats.get('cmdstat_' + cmd, {}).get('calls', 0) for cmd in commands}


async def delivery_and_child(redis, task):
    # Sentinel dependency residue tests zero fanout on rejection; no DAG authority.
    child = task.task_id + '-child'
    await redis.sadd(DagRedisKey.task_children(task.task_id), child)
    await redis.set(DagRedisKey.task_state(child), 'pending', px=600000)
    await redis.set(DagRedisKey.task_remaining_deps(child), '1', px=600000)
    stream = 'hfa:f03:delivery:' + task.task_id
    await redis.xgroup_create(stream, 'f04', id='0', mkstream=True)
    await redis.xadd(stream, {'task_id': task.task_id})
    await redis.xreadgroup('f04', 'worker', {stream: '>'}, count=1)
    for key in (DagRedisKey.task_state(task.task_id), DagRedisKey.task_meta(task.task_id)):
        await redis.pexpire(key, 600000)


FAULTS = [
    'receipt_missing', 'record_missing', 'index_missing',
    'receipt_changed', 'record_changed', 'index_changed', 'record_checksum',
    'head_revision', 'head_state', 'head_identity', 'head_operation',
    'head_digest', 'head_transition', 'head_command_hash', 'head_record_hash', 'head_incomplete',
    'type_head', 'type_receipts', 'type_records', 'type_index', 'type_log', 'type_outbox',
    'claim_receipt_changed', 'claim_record_changed', 'claim_index_changed',
]


async def damage(redis, keys, own, claim, fault):
    head_fields = {
        'head_revision': 'revision', 'head_state': 'state',
        'head_identity': 'canonical_aggregate_identity_sha256',
        'head_operation': 'operation_id', 'head_digest': 'operation_digest',
        'head_transition': 'transition_id', 'head_command_hash': 'canonical_command_hash',
        'head_record_hash': 'canonical_record_hash',
    }
    if fault in head_fields:
        await redis.hset(keys[0], head_fields[fault], '999' if fault == 'head_revision' else 'changed')
    elif fault == 'head_incomplete':
        await redis.hdel(keys[0], 'updated_at_ms')
    elif fault.startswith('type_'):
        key = keys[{'head': 0, 'index': 1, 'receipts': 2, 'records': 3, 'log': 4, 'outbox': 5}[fault[5:]]]
        await redis.delete(key)
        await redis.set(key, 'wrong-type')
    else:
        proof = claim if fault.startswith('claim_') else own
        name = fault.removeprefix('claim_').split('_')[0]
        key = keys[{'index': 1, 'receipt': 2, 'record': 3}[name]]
        field = proof[3] if name == 'index' else proof[2]
        if fault.endswith('_missing'):
            await redis.hdel(key, field)
        else:
            envelope = json.loads(await redis.hget(key, field))
            payload = json.loads(envelope['payload'])
            payload['canonical_record_hash'] = '0' * 64
            envelope['payload'] = json.dumps(payload, sort_keys=True, separators=(',', ':'))
            if fault != 'record_checksum':
                envelope['storage_sha1'] = hashlib.sha1(envelope['payload'].encode()).hexdigest()
            await redis.hset(key, field, json.dumps(envelope))


@pytest.mark.asyncio
@pytest.mark.parametrize('state', ['done', 'failed'])
@pytest.mark.parametrize('fault', FAULTS)
async def test_lost_or_changed_proof_cannot_write(projection_domain, state, fault):
    redis, store, run, task, binding = projection_domain
    prepared = await prepare(binding, task, state)
    await delivery_and_child(redis, task)
    manager = binding.projection_manager
    await manager.initialise()
    original = manager._loader.run
    observed = {}

    async def interleave(**kwargs):
        keys = store.keyspace(task.sha256).commit_keys()[:6]
        assert kwargs['keys'][15:] == keys
        own, claim = [json.loads(arg) for arg in kwargs['args'][35:37]]
        await damage(redis, keys, own, claim, fault)
        observed['before'] = await semantic_snapshot(redis)
        observed['writes'] = await write_counts(redis)
        return await original(**kwargs)

    manager._loader.run = interleave
    with pytest.raises(TaskTerminalAuthorityError) as caught:
        await binding._apply_prepared(prepared)
    assert caught.value.status == 'canonical_task_proof_conflict'
    assert await semantic_snapshot(redis) == observed['before']
    assert await write_counts(redis) == observed['writes']
    assert text(await redis.get(DagRedisKey.task_state(task.task_id))) == 'running'


@pytest.mark.asyncio
@pytest.mark.parametrize('state', ['done', 'failed'])
async def test_invalid_own_proof_prevents_even_truth_conflict_event(projection_domain, state):
    redis, store, run, task, binding = projection_domain
    prepared = await prepare(binding, task, state)
    await redis.delete(RedisKey.run_state(run.run_id))
    manager = binding.projection_manager
    await manager.initialise()
    original = manager._loader.run
    observed = {}

    async def interleave(**kwargs):
        own = json.loads(kwargs['args'][35])
        await redis.hdel(kwargs['keys'][17], own[2])
        observed['before'] = await semantic_snapshot(redis)
        observed['writes'] = await write_counts(redis)
        return await original(**kwargs)

    manager._loader.run = interleave
    with pytest.raises(TaskTerminalAuthorityError, match='canonical_task_proof_conflict'):
        await binding._apply_prepared(prepared)
    assert await semantic_snapshot(redis) == observed['before']
    assert await write_counts(redis) == observed['writes']


@pytest.mark.asyncio
@pytest.mark.parametrize('state', ['done', 'failed'])
@pytest.mark.parametrize('fault', ['receipt_missing', 'head_revision'])
async def test_true_exact_duplicate_stays_read_only_when_own_proof_moves(projection_domain, state, fault):
    redis, store, run, task, binding = projection_domain
    prepared = await prepare(binding, task, state)
    assert (await binding._apply_prepared(prepared)).projection_applied
    manager = binding.projection_manager
    original = manager._loader.run
    observed = {}

    async def interleave(**kwargs):
        own, claim = [json.loads(arg) for arg in kwargs['args'][35:37]]
        await damage(redis, kwargs['keys'][15:], own, claim, fault)
        observed['before'] = await semantic_snapshot(redis)
        observed['writes'] = await write_counts(redis)
        return await original(**kwargs)

    manager._loader.run = interleave
    result = await binding._apply_prepared(prepared)
    assert result.exact_no_op and not result.projection_applied
    assert await semantic_snapshot(redis) == observed['before']
    assert await write_counts(redis) == observed['writes']


@pytest.mark.asyncio
@pytest.mark.parametrize('state', ['done', 'failed'])
async def test_restart_projects_once_then_duplicate_is_zero_effect(projection_domain, state):
    redis, store, run, task, binding = projection_domain
    await prepare(binding, task, state)  # durable decision; crash before projection
    authority_keys = store.keyspace(task.sha256).commit_keys()
    authority_before = await semantic_snapshot(redis, authority_keys)
    args = dict(task_id=task.task_id, run_id=run.run_id, tenant_id='tenant',
                worker_instance_id='worker', scheduler_epoch='epoch', claim_epoch=1)
    restarted = TaskTerminalAuthorityBinding(redis)
    first = await restarted.replay_terminal_projection(**args)
    assert first.completed and first.projection_applied and not first.exact_no_op
    assert text(await redis.get(DagRedisKey.task_state(task.task_id))) == state
    assert await semantic_snapshot(redis, authority_keys) == authority_before
    before, counts = await semantic_snapshot(redis), await write_counts(redis)
    duplicate = await TaskTerminalAuthorityBinding(redis).replay_terminal_projection(**args)
    assert duplicate.exact_no_op and not duplicate.projection_applied
    assert await semantic_snapshot(redis) == before
    assert await write_counts(redis) == counts
