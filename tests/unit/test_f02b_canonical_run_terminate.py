"""F02B production admission/termination with real canonical Lua commits."""
from dataclasses import replace

import pytest

from hfa.authority import (
    AuthorityDecisionCode, OperationType, RedisAuthorityCommitStatus,
    RedisCanonicalAuthorityStore, evaluate_authority_commit,
)
from hfa.governance.admission_resource_reservation import RESERVATION_STATE_SETTLED
from hfa.config.keys import RedisKey
from hfa.dag.schema import DagRedisKey
from hfa_control.run_terminate_authority import (
    RunTerminateAuthorityBinding, RunTerminateAuthorityError,
    build_run_terminate_command, build_run_terminate_context,
)
from tests.unit.test_f02a1_root_binding import alpha, root_redis, create, request, seed, snapshot
from tests.unit.test_f03_terminal_ordering import plan, terminal
from tests.integration.test_run_terminate_authority_binding_integration import _set_terminal_event_migration_ready


async def domain(alpha, redis, state='done'):
    req = request()
    req.estimated_cost_cents = 125
    req, store, run = await create(alpha, req)
    task = None
    if state != 'missing':
        from hfa_control.task_admit_authority import build_task_admit_command
        await alpha._scheduler.composition.dag_lua.task_admit(seed(req))
        task = build_task_admit_command(seed(req)).aggregate_identity
        for entry in (
            plan(task, OperationType.TASK_DISPATCH, 'ready', 'scheduled', 1,
                 ['CONTROL_NOTIFICATION', 'TASK_REQUEST_MESSAGE']),
            plan(task, OperationType.TASK_CLAIM, 'scheduled', 'running', 2, ['RUNNING_SET']),
        ):
            assert (await store.commit(entry)).committed
        if state in {'done', 'failed'}:
            assert (await store.commit(terminal(task, state))).committed
    await _set_terminal_event_migration_ready(redis)
    binding = RunTerminateAuthorityBinding(
        redis, store=store, resource_manager=alpha.admission._run_create_authority.resource_manager,
    )
    await binding.initialise()
    return req, store, run, task, binding


async def finish(binding, req, **overrides):
    args = dict(run_id=req.run_id, tenant_id=req.tenant_id, trigger_task_id=req.root_task_id,
                finalized_at_ms=2000, worker_instance_id='f02b', trigger_terminal_state='done')
    args.update(overrides)
    return await binding.terminate(**args)


async def forged_runtime(redis, req, state='done'):
    await redis.delete(DagRedisKey.run_tasks(req.run_id))
    await redis.sadd(DagRedisKey.run_tasks(req.run_id), req.root_task_id, 'forged-other-task')
    for task_id in (req.root_task_id, 'forged-other-task'):
        await redis.set(DagRedisKey.task_state(task_id), state)
        await redis.hset(DagRedisKey.task_meta(task_id), mapping={
            'task_id':task_id, 'run_id':req.run_id, 'tenant_id':req.tenant_id, 'state':state,
        })


@pytest.mark.asyncio
@pytest.mark.parametrize('state', ['running', 'missing'])
async def test_runtime_terminal_cannot_authorize_run(alpha, root_redis, state):
    req, store, run, _, binding = await domain(alpha, root_redis, state)
    await forged_runtime(root_redis, req)
    before = await snapshot(root_redis)
    if state == 'missing':
        with pytest.raises(RunTerminateAuthorityError):
            await finish(binding, req)
    else:
        result = await finish(binding, req)
        assert not result.finalized and not result.ack_allowed
    assert (await store.get_aggregate_snapshot(run)).state == 'pending'
    assert await snapshot(root_redis) == before


@pytest.mark.asyncio
@pytest.mark.parametrize('state', ['done', 'failed'])
@pytest.mark.parametrize('runtime', ['opposite', 'absent', 'forged'])
async def test_success_failure_use_canonical_root_only(alpha, root_redis, state, runtime):
    req, store, run, _, binding = await domain(alpha, root_redis, state)
    if runtime == 'absent':
        await root_redis.delete(DagRedisKey.run_tasks(req.run_id), DagRedisKey.task_state(req.root_task_id), DagRedisKey.task_meta(req.root_task_id))
    else:
        await forged_runtime(root_redis, req, 'failed' if state == 'done' else 'done')
    result = await finish(binding, req, trigger_terminal_state='failed' if state == 'done' else 'done')
    assert result.finalized and result.final_state == state and result.canonical_revision == 2
    head = await store.get_aggregate_snapshot(run)
    record = (await store.load_receipt_probe(run, head.operation_id)).canonical_store_record
    proof = record.authoritative_metadata_changes['terminal_evidence']
    assert [dict(row) for row in proof['tasks']] == [{'task_id':req.root_task_id, 'state':state}]
    assert proof['canonical_task']['state'] == state
    assert await root_redis.get(RedisKey.run_state(req.run_id)) == state
    manager = binding.resource_manager
    from hfa_control.run_create_authority import run_create_operation_id
    assert (await root_redis.hgetall(manager.reservation_receipt_key(run_create_operation_id(req.run_id))))['state'] == RESERVATION_STATE_SETTLED
    assert await manager.get_resource_snapshot(req.tenant_id) == {
        'concurrent_runs':0, 'budget_reserved_cents':0, 'tenant_inflight':0,
    }


@pytest.mark.asyncio
async def test_wrong_trigger_root_rejected(alpha, root_redis):
    req, _, _, _, binding = await domain(alpha, root_redis)
    before = await snapshot(root_redis)
    with pytest.raises(RunTerminateAuthorityError, match='canonical_root_task_mismatch'):
        await finish(binding, req, trigger_task_id='other-root')
    assert await snapshot(root_redis) == before


@pytest.mark.asyncio
@pytest.mark.parametrize('part', ['record', 'receipt'])
@pytest.mark.parametrize('damage', ['missing', 'corrupt'])
async def test_task_proof_damage_fails_closed(alpha, root_redis, part, damage):
    req, store, _, task, binding = await domain(alpha, root_redis)
    head = await store.get_aggregate_snapshot(task)
    keys = store.keyspace(task.sha256)
    key = keys.operation_records if part == 'record' else keys.receipts
    if damage == 'missing':
        await root_redis.hdel(key, head.operation_digest)
    else:
        await root_redis.hset(key, head.operation_digest, '{}')
    before = await snapshot(root_redis)
    with pytest.raises(RunTerminateAuthorityError):
        await finish(binding, req)
    assert await snapshot(root_redis) == before


@pytest.mark.asyncio
@pytest.mark.parametrize('damage', ['receipt', 'record', 'head', 'run_create'])
async def test_task_witness_race_cannot_commit(alpha, root_redis, monkeypatch, damage):
    req, store, run, task, binding = await domain(alpha, root_redis)
    original = store.run_terminate_task_guard
    calls = 0
    expected = None
    async def interleave(run_id):
        nonlocal calls, expected
        result = await original(run_id)
        calls += 1
        # First capture builds the command; second capture is commit preparation.
        if calls == 2:
            head = await store.get_aggregate_snapshot(task)
            keys = store.keyspace(task.sha256)
            if damage == 'head':
                await root_redis.hset(keys.aggregate, 'state', 'running')
            elif damage == 'run_create':
                keys = store.keyspace(run.sha256)
                from hfa_control.run_create_authority import run_create_operation_id
                await root_redis.hdel(keys.receipts, keys.operation_field(run_create_operation_id(run_id)))
            else:
                key = keys.receipts if damage == 'receipt' else keys.operation_records
                await root_redis.hdel(key, head.operation_digest)
            expected = await snapshot(root_redis)
        return result
    monkeypatch.setattr(store, 'run_terminate_task_guard', interleave)
    with pytest.raises(RunTerminateAuthorityError):
        await finish(binding, req)
    assert calls == 2 and expected is not None
    assert (await store.get_aggregate_snapshot(run)).state == 'pending'
    assert await snapshot(root_redis) == expected


@pytest.mark.asyncio
@pytest.mark.parametrize('state', ['done', 'failed'])
async def test_durable_duplicate_is_zero_effect_and_does_not_read_task(alpha, root_redis, monkeypatch, state):
    req, store, run, task, binding = await domain(alpha, root_redis, state)
    assert (await finish(binding, req)).finalized
    head = await store.get_aggregate_snapshot(run)
    probe = await store.load_receipt_probe(run, head.operation_id)
    proof = binding._proof_from_record(probe.canonical_store_record)
    command = build_run_terminate_command(proof)
    # Recreate the original plan; duplicate commit must return before TASK proof.
    evaluated = evaluate_authority_commit(context=build_run_terminate_context(command),
        command=command,current_revision=1,current_state='pending',receipt_probe=None,
        committed_at_ms=proof.finalized_at_ms,correlation_id=None)
    assert evaluated.decision.code is AuthorityDecisionCode.ACCEPTED
    await forged_runtime(root_redis, req, 'running')
    await root_redis.hdel(store.keyspace(task.sha256).receipts,
        (await store.get_aggregate_snapshot(task)).operation_digest)
    async def forbidden(*args, **kwargs):
        pytest.fail('durable RUN_TERMINATE reread TASK witness')
    monkeypatch.setattr(RedisCanonicalAuthorityStore, 'run_terminate_task_guard', forbidden)
    stream = 'f02b:pending-delivery'
    await root_redis.xgroup_create(stream, 'workers', id='0', mkstream=True)
    await root_redis.xadd(stream, {'run_id':req.run_id, 'task_id':req.root_task_id})
    await root_redis.xreadgroup('workers', 'worker', {stream:'>'}, count=1)
    pending = await root_redis.xpending(stream, 'workers')
    assert pending['pending'] == 1
    before = await snapshot(root_redis)
    restarted = RunTerminateAuthorityBinding(root_redis, resource_manager=binding.resource_manager)
    await restarted.initialise()
    result = await finish(restarted, req, finalized_at_ms=9999, trigger_task_id='new-notification')
    assert result.already_finalized and result.canonical_revision == 2
    assert (await store.commit(evaluated.commit_plan)).status is RedisAuthorityCommitStatus.ALREADY_APPLIED
    assert await snapshot(root_redis) == before
    assert await root_redis.xpending(stream, 'workers') == pending


@pytest.mark.asyncio
@pytest.mark.parametrize('state', ['done', 'failed'])
async def test_restart_after_durable_commit_restores_projection_without_task_reads(alpha, root_redis, monkeypatch, state):
    req, store, run, task, binding = await domain(alpha, root_redis, state)
    async def stop_projection(*args, **kwargs):
        raise RuntimeError('crash after canonical durability and resource settlement')
    monkeypatch.setattr(binding.projection_manager, 'project', stop_projection)
    with pytest.raises(RunTerminateAuthorityError):
        await finish(binding, req)
    assert (await store.get_aggregate_snapshot(run)).state == state
    await forged_runtime(root_redis, req, 'running')
    await root_redis.delete(*store.keyspace(task.sha256).commit_keys())
    async def forbidden(*args, **kwargs):
        pytest.fail('restart recaptured TASK')
    monkeypatch.setattr(RedisCanonicalAuthorityStore, 'run_terminate_task_guard', forbidden)
    restarted = RunTerminateAuthorityBinding(root_redis, resource_manager=binding.resource_manager)
    await restarted.initialise()
    result = await finish(restarted, req)
    assert result.finalized and result.final_state == state and result.canonical_revision == 2
    assert await root_redis.get(RedisKey.run_state(req.run_id)) == state


@pytest.mark.asyncio
async def test_forged_terminal_command_rejected_at_canonical_boundary(alpha, root_redis):
    req, store, run, task, binding = await domain(alpha, root_redis, 'running')
    await forged_runtime(root_redis, req)
    _, guard, _ = await store.run_terminate_task_guard(req.run_id)
    # Correct root and exact current head fingerprints; only the claimed
    # terminal outcome is false. This must reach the Lua terminal-state check.
    fake = binding._proof_from_payload({
        'schema_version':1, 'run_id':req.run_id, 'tenant_id':req.tenant_id,
        'tasks':[{'task_id':req.root_task_id, 'state':'done'}],
        'task_count':1, 'done_count':1, 'failed_count':0, 'skipped_count':0, 'final_state':'done',
        'canonical_task':{'identity_sha256':guard[0], 'operation_id':guard[1],
            'transition_id':guard[3], 'revision':int(guard[4]),
            'canonical_command_hash':guard[5], 'canonical_record_hash':guard[6], 'state':guard[10]},
    }, finalized_at_ms=2000,worker_instance_id='f02b',trigger_task_id=req.root_task_id,
        trigger_terminal_state='done',canonical_expected_revision=1,canonical_previous_state='pending')
    command = build_run_terminate_command(fake)
    evaluated = evaluate_authority_commit(context=build_run_terminate_context(command),
        command=command,current_revision=1,current_state='pending',receipt_probe=None,
        committed_at_ms=2000,correlation_id=None)
    before = await snapshot(root_redis)
    result = await store.commit(evaluated.commit_plan)
    assert result.status is RedisAuthorityCommitStatus.ILLEGAL_STATE_TRANSITION
    assert result.detail == 'canonical_root_task_not_terminal'
    assert await snapshot(root_redis) == before


@pytest.mark.asyncio
async def test_other_canonical_task_witness_cannot_authorize_root(alpha, root_redis, monkeypatch):
    req, store, run, _, binding = await domain(alpha, root_redis)
    other, _, _, _, _ = await domain(alpha, root_redis)
    _, _, creation = await store.run_terminate_task_guard(req.run_id)
    wrong_keys, wrong_guard, _ = await store.run_terminate_task_guard(other.run_id)
    async def substituted(run_id):
        return wrong_keys, wrong_guard, creation
    monkeypatch.setattr(store, 'run_terminate_task_guard', substituted)
    before = await snapshot(root_redis)
    with pytest.raises(RunTerminateAuthorityError, match='canonical_root_task_mismatch'):
        await finish(binding, req)
    assert await snapshot(root_redis) == before


@pytest.mark.asyncio
@pytest.mark.parametrize('change', ['task_id', 'outcome', 'record_hash'])
async def test_forged_command_cannot_override_canonical_witness(alpha, root_redis, change):
    req, store, run, _, binding = await domain(alpha, root_redis)
    proof = await binding._capture_canonical_proof(
        snapshot=await store.get_aggregate_snapshot(run),run_id=req.run_id,tenant_id=req.tenant_id,
        finalized_at_ms=2000,worker_instance_id='f02b',trigger_task_id=req.root_task_id,trigger_terminal_state='done')
    if change == 'task_id':
        proof = replace(proof, tasks=(replace(proof.tasks[0],task_id='different-root'),))
    elif change == 'outcome':
        proof = replace(proof, final_state='failed', done_count=0, failed_count=1)
    else:
        proof = replace(proof, canonical_task={**proof.canonical_task,'canonical_record_hash':'0'*64})
    command = build_run_terminate_command(proof)
    evaluated = evaluate_authority_commit(context=build_run_terminate_context(command),command=command,
        current_revision=1,current_state='pending',receipt_probe=None,committed_at_ms=2000,correlation_id=None)
    before = await snapshot(root_redis)
    assert (await store.commit(evaluated.commit_plan)).status is RedisAuthorityCommitStatus.ILLEGAL_STATE_TRANSITION
    assert await snapshot(root_redis) == before


@pytest.mark.asyncio
async def test_lost_commit_response_recovers_exact_receipt(alpha, root_redis, monkeypatch):
    req, store, run, _, binding = await domain(alpha, root_redis)
    original = store.commit
    async def lost(plan, **kwargs):
        assert (await original(plan, **kwargs)).committed
        raise ConnectionError('response lost after durable commit')
    monkeypatch.setattr(store, 'commit', lost)
    result = await finish(binding, req)
    assert result.finalized and result.canonical_revision == 2


@pytest.mark.asyncio
async def test_unbound_history_cannot_create_new_run_termination(alpha, root_redis):
    from types import SimpleNamespace
    req = SimpleNamespace(run_id='historical-unbound',tenant_id='f02a1',agent_type='default',
        priority=0,payload={},estimated_cost_cents=0,preferred_region='',preferred_placement='LEAST_LOADED')
    await alpha.admission._run_create_authority.admit(req, tenant_inflight_limit=10)
    req.root_task_id = 'runtime-only-task'
    await forged_runtime(root_redis, req)
    binding = RunTerminateAuthorityBinding(root_redis)
    await binding.initialise()
    before = await snapshot(root_redis)
    with pytest.raises(RunTerminateAuthorityError, match='root binding'):
        await finish(binding, req)
    assert await snapshot(root_redis) == before


@pytest.mark.asyncio
async def test_historical_durable_run_termination_replays_without_new_root(alpha, root_redis, monkeypatch):
    from types import SimpleNamespace
    from tests.canonical_history import install_historical_run_termination
    from tests.unit.test_run_terminate_authority_binding import proof as historical_proof
    req = SimpleNamespace(run_id='run-1',tenant_id='tenant-a',agent_type='default',priority=0,
        payload={},estimated_cost_cents=0,preferred_region='',preferred_placement='LEAST_LOADED')
    await alpha.admission._run_create_authority.admit(req, tenant_inflight_limit=10)
    req.root_task_id = 'ignored-trigger'
    command = build_run_terminate_command(historical_proof())
    evaluated = evaluate_authority_commit(context=build_run_terminate_context(command),command=command,
        current_revision=1,current_state='pending',receipt_probe=None,committed_at_ms=1000,correlation_id=None)
    store = alpha.admission._run_create_authority.store
    await install_historical_run_termination(root_redis, store, evaluated.commit_plan)
    await _set_terminal_event_migration_ready(root_redis)
    async def forbidden(*args, **kwargs):
        pytest.fail('historical durable retry requested new TASK proof')
    monkeypatch.setattr(RedisCanonicalAuthorityStore, 'run_terminate_task_guard', forbidden)
    binding = RunTerminateAuthorityBinding(root_redis)
    await binding.initialise()
    assert (await finish(binding, req)).finalized
    before = await snapshot(root_redis)
    assert (await finish(binding, req)).already_finalized
    assert await snapshot(root_redis) == before


@pytest.mark.asyncio
@pytest.mark.parametrize('ok', [True, False])
async def test_worker_success_and_failure_preserve_run_finalization(alpha, root_redis, ok):
    from hfa_worker.task_executor import TaskExecutionResult
    from tests.integration.test_worker_run_terminate_canonical_composition_integration import (
        _seed_dispatch, _worker, _TaskExecutor, _enqueue, _text, _pending, _events,
    )
    req, store, run = await create(alpha)
    worker_id, epoch = 'f02b-worker', 'f02b-epoch'
    await _seed_dispatch(root_redis, req.root_task_id, req.run_id, req.tenant_id, worker_id, epoch)
    executor = _TaskExecutor(TaskExecutionResult(ok=ok, output={'answer':42} if ok else {}, error='' if ok else 'failure'))
    worker = await _worker(root_redis, worker_id, executor)
    stream = RedisKey.stream_shard(0)
    message_id, data = await _enqueue(root_redis, stream, 'TaskRequested',req.root_task_id,req.run_id,req.tenant_id,worker_id,epoch)
    await worker._consumer._process_message(_text(message_id),data,stream,0)
    state = 'done' if ok else 'failed'
    head = await store.get_aggregate_snapshot(run)
    assert head.state == state and head.revision == 2
    events = await _events(root_redis, req.run_id)
    assert len(events) == 1 and await _pending(root_redis,stream) == []
    assert len(executor.calls) == 1
    probe = await store.load_receipt_probe(run,head.operation_id)
    assert probe.canonical_store_record.authoritative_metadata_changes['terminal_evidence']['canonical_task']['state'] == state
