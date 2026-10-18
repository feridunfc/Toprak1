"""G1 F07 v2: immutable global TASK owner inventory (single HASH + monotonic header count).

Oracles compare full semantic Redis snapshots (values, absolute expiries, queues, streams, PEL)
and per-key write counts before/after every rejected attempt; an exception alone is never proof.
"""
import asyncio

import fakeredis.aioredis
import pytest

from hfa.authority import (
    RedisCanonicalAuthorityStore, TaskOwnerActivationRefusedError, TaskOwnerVerificationError,
    task_owner_header_key, task_owners_key,
)
from hfa.authority.redis_persistence import task_owner_value
from hfa.dag.schema import DagRedisKey
from hfa_control.dag_lua import DagLua
from hfa_control.task_admit_authority import (
    TaskAdmitAuthorityError, TaskAdmitProjectionConflictError, build_task_admit_command,
)
from tests.unit.test_f02a1_root_binding import (  # noqa: F401
    FRESH_TEST_ATTESTATION, alpha, create, request, root_redis, seed,
)
from tests.unit.test_f04_4a_task_admit_projection_proof import setup
from tests.unit.test_f04_4c_task_claim_projection_proof import snapshot
from tests.unit.test_f04_task_projection_proof import write_counts
from tests.unit.test_f06_task_admit_effect_completeness import capture
from tests.integration.test_task_dispatch_authority_binding_integration import dispatch

NS = 'hfa:authority:v1'
OWNERS, HEADER = task_owners_key(NS), task_owner_header_key(NS)


async def frozen(redis):
    return await snapshot(redis), await write_counts(redis)


async def drop_runtime(redis, item):
    await redis.delete(*(f'hfa:dag:task:{item.task_id}:{s}' for s in ('state', 'meta', 'remaining_deps', 'children', 'ready_emitted')))
    await redis.srem(DagRedisKey.run_tasks(item.run_id), item.task_id)
    await redis.zrem(DagRedisKey.tenant_ready_queue(item.tenant_id), item.task_id)


async def second_run(alpha, item, tenant='f02a1'):
    from uuid import uuid4
    other = request()
    other.root_task_id, other.tenant_id = item.task_id, tenant
    other.run_id = f'run-{tenant}-{uuid4()}'      # run ids encode their tenant
    await create(alpha, other)
    return other


async def agg(store, item):
    return await store.get_aggregate_snapshot(build_task_admit_command(item).aggregate_identity)


async def assert_rejected_clean(dag, store, victim, before, text=None):
    with pytest.raises(TaskAdmitAuthorityError) as err:
        await dag.task_admit(victim)
    if text:
        assert text in str(err.value), str(err.value)
    assert await frozen(dag._redis) == before      # zero writes, TTLs untouched
    assert await agg(store, victim) is None        # NO second canonical TASK commit or receipt
    return err


@pytest.mark.asyncio
async def test_first_admission_creates_one_immutable_owner_and_count(alpha, root_redis):
    _, store, dag, item, task = await setup(alpha)
    assert (await dag.task_admit(item)).admitted
    assert await root_redis.hlen(OWNERS) == 1 and await root_redis.hget(HEADER, 'owner_count') == '1'
    assert await root_redis.pttl(OWNERS) == -1 and await root_redis.pttl(HEADER) == -1
    owner = await store.load_task_owner(item.task_id)
    assert (owner.run_id, owner.tenant_id) == (item.run_id, item.tenant_id)
    probe = await store.load_receipt_probe(task, f'task-admit:v1:{task.sha256}')
    digest = store.keyspace(task.sha256).operation_field(probe.canonical_store_record.operation_id)
    assert await root_redis.hget(OWNERS, item.task_id) == task_owner_value(probe.canonical_store_record, operation_digest=digest)
    await store.verify_task_owner(probe.canonical_store_record)
    assert (await store.get_aggregate_snapshot(task)).revision == 1


@pytest.mark.asyncio
async def test_exact_duplicate_is_zero_write_and_no_ttl_refresh(alpha, root_redis):
    _, store, dag, item, _ = await setup(alpha)
    assert (await dag.task_admit(item)).admitted
    before = await frozen(root_redis)
    again = await dag.task_admit(item)
    assert again.status == 'already_exists' and await frozen(root_redis) == before


@pytest.mark.asyncio
async def test_same_owner_changed_payload_conflicts(alpha, root_redis):
    import dataclasses
    _, store, dag, item, task = await setup(alpha)
    assert (await dag.task_admit(item)).admitted
    owners_before = await root_redis.hgetall(OWNERS)
    keys = store.keyspace(task.sha256).commit_keys()
    with pytest.raises(TaskAdmitAuthorityError):
        await dag.task_admit(dataclasses.replace(item, payload_json='{"changed":true}'))
    # An idempotency conflict may record durable conflict EVIDENCE (accepted F06 behaviour), but
    # never a second canonical TASK commit/receipt/owner.
    assert (await store.get_aggregate_snapshot(task)).revision == 1
    assert await root_redis.hlen(keys[2]) == 1 and await root_redis.hlen(keys[3]) == 1   # receipts, records
    assert await root_redis.hgetall(OWNERS) == owners_before and await root_redis.hget(HEADER, 'owner_count') == '1'


@pytest.mark.asyncio
@pytest.mark.parametrize('tenant', ['f02a1', 'f02a2'])
async def test_different_run_or_tenant_same_task_id_rejected(alpha, root_redis, tenant):
    _, store, dag, item, _ = await setup(alpha)
    assert (await dag.task_admit(item)).admitted
    other = await second_run(alpha, item, tenant)
    await drop_runtime(root_redis, item)   # runtime expiry must not matter: header count still binds
    before = await frozen(root_redis)
    await assert_rejected_clean(dag, store, seed(other), before, 'task_owner_conflict')


@pytest.mark.asyncio
async def test_concurrent_cross_run_race_has_one_owner_one_canonical_admission(alpha, root_redis):
    _, store, dag, item, _ = await setup(alpha)
    other = await second_run(alpha, item)
    results = await asyncio.gather(dag.task_admit(seed(alpha_req := other)), dag.task_admit(item), return_exceptions=True)
    ok = [r for r in results if not isinstance(r, Exception)]
    assert len(ok) == 1 and all(isinstance(r, TaskAdmitAuthorityError) for r in results if isinstance(r, Exception))
    assert await root_redis.hlen(OWNERS) == 1 and await root_redis.hget(HEADER, 'owner_count') == '1'
    committed = [await agg(store, seed(other)), await agg(store, item)]
    assert sum(1 for c in committed if c is not None) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize('loss', ['field', 'whole_hash'])
async def test_P0_owner_loss_then_reallocation_fails_closed(alpha, root_redis, loss):
    """RUN A/TASK X commits; delete global owner X; delete runtime X; RUN B/TASK X must fail closed."""
    _, store, dag, item, task = await setup(alpha)
    assert (await dag.task_admit(item)).admitted
    other = await second_run(alpha, item)
    if loss == 'field':
        assert await root_redis.hdel(OWNERS, item.task_id) == 1
    else:
        assert await root_redis.delete(OWNERS) == 1
    await drop_runtime(root_redis, item)
    before = await frozen(root_redis)
    await assert_rejected_clean(dag, store, seed(other), before, 'task_owner_inventory_loss_detected')
    assert (await store.get_aggregate_snapshot(task)).revision == 1
    assert await root_redis.zscore(DagRedisKey.tenant_ready_queue(other.tenant_id), item.task_id) is None
    assert await root_redis.exists(DagRedisKey.task_state(item.task_id)) == 0   # no runtime/queue/executor effect


@pytest.mark.asyncio
@pytest.mark.parametrize('corrupt', ['delete', 'extra_field', 'ttl', 'wrong_type', 'bad_count', 'bad_state'])
async def test_header_corruption_fails_closed_without_writes(alpha, root_redis, corrupt):
    _, store, dag, item, _ = await setup(alpha)
    await create(alpha, None)  # unrelated RUN keeps fixture shape honest
    if corrupt == 'delete':
        await root_redis.delete(HEADER)
    elif corrupt == 'extra_field':
        await root_redis.hset(HEADER, 'extra', '1')
    elif corrupt == 'ttl':
        await root_redis.expire(HEADER, 1000)
    elif corrupt == 'wrong_type':
        await root_redis.delete(HEADER); await root_redis.set(HEADER, 'x')
    elif corrupt == 'bad_count':
        await root_redis.hset(HEADER, 'owner_count', '-3')
    else:
        await root_redis.hset(HEADER, 'state', 'INACTIVE')
    before = await frozen(root_redis)
    await assert_rejected_clean(dag, store, item, before)


@pytest.mark.asyncio
@pytest.mark.parametrize('how', ['ttl', 'wrong_type'])
async def test_owner_hash_ttl_or_type_fails_closed(alpha, root_redis, how):
    _, store, dag, item, _ = await setup(alpha)
    other = await second_run(alpha, item)
    assert (await dag.task_admit(item)).admitted
    if how == 'ttl':
        await root_redis.expire(OWNERS, 100)
    else:
        await root_redis.delete(OWNERS); await root_redis.set(OWNERS, 'x')
    before = await frozen(root_redis)
    await assert_rejected_clean(dag, store, seed(other), before)


@pytest.mark.asyncio
async def test_owner_only_footprint_is_not_admission(alpha, root_redis):
    _, store, dag, item, _ = await setup(alpha)
    plan = await _plan_for(item)
    value = store.task_owner_projection_inputs(plan.record)[1]
    await root_redis.hset(OWNERS, item.task_id, value)
    await root_redis.hincrby(HEADER, 'owner_count', 1)          # as if the owner write landed without canonical
    before = await frozen(root_redis)
    await assert_rejected_clean(dag, store, item, before, 'task_owner_binding_without_canonical_admission')
    assert await root_redis.exists(DagRedisKey.task_state(item.task_id)) == 0


async def _plan_for(item):
    from hfa.authority import AuthorityDecisionCode, evaluate_authority_commit
    from hfa_control.task_admit_authority import build_task_admit_context
    command = build_task_admit_command(item)
    return evaluate_authority_commit(
        context=build_task_admit_context(command), command=command, current_revision=0, current_state=None,
        receipt_probe=None, committed_at_ms=item.admitted_at, correlation_id=None).commit_plan


@pytest.mark.asyncio
async def test_canonical_without_binding_blocks_exact_duplicate_python_path(alpha, root_redis):
    """Canonical TASK exists, binding removed AND count rewritten consistently: duplicate must not succeed."""
    _, store, dag, item, task = await setup(alpha)
    assert (await dag.task_admit(item)).admitted
    await root_redis.hdel(OWNERS, item.task_id)
    await root_redis.hset(HEADER, 'owner_count', '0')
    before = await frozen(root_redis)
    with pytest.raises(TaskAdmitAuthorityError) as err:
        await dag.task_admit(item)
    assert 'task_owner_binding_missing_for_canonical_task' in str(err.value)
    assert await frozen(root_redis) == before


@pytest.mark.asyncio
async def test_python_duplicate_bypass_closed_for_inventory_loss(alpha, root_redis):
    _, store, dag, item, _ = await setup(alpha)
    assert (await dag.task_admit(item)).admitted
    await root_redis.hdel(OWNERS, item.task_id)                  # count now exceeds HLEN
    before = await frozen(root_redis)
    with pytest.raises(TaskAdmitAuthorityError) as err:
        await dag.task_admit(item)
    assert 'task_owner_inventory_loss_detected' in str(err.value) or 'owner' in str(err.value)
    assert await frozen(root_redis) == before


@pytest.mark.asyncio
async def test_projector_lua_alone_rejects_owner_loss_python_precheck_bypassed(alpha, root_redis, monkeypatch):
    """Python verification disabled: the projector Lua must refuse before the first runtime write."""
    store, dag, item, task, call = (await capture(alpha, root_redis, 'root'))[0:5]
    await root_redis.hdel(OWNERS, item.task_id)
    async def noop(record): return None
    monkeypatch.setattr(dag._task_admit_authority_binding.store, 'verify_task_owner', noop)
    before = await frozen(root_redis)
    with pytest.raises(TaskAdmitProjectionConflictError) as err:
        await dag.task_admit(item)
    assert 'canonical_task_admit_owner_conflict' in str(err.value)
    assert await frozen(root_redis) == before and await root_redis.exists(DagRedisKey.task_state(item.task_id)) == 0


@pytest.mark.asyncio
async def test_owner_changes_between_python_observation_and_projection_toctou(alpha, root_redis):
    store, dag, item, task, call = (await capture(alpha, root_redis, 'root'))[0:5]
    original = dag._admit_loader.run
    async def delete_owner_then_run(**kw):
        await root_redis.hdel(OWNERS, item.task_id)              # after Python verified, before Lua
        return await original(**kw)
    dag._admit_loader.run = delete_owner_then_run
    try:
        before_state = await root_redis.exists(DagRedisKey.task_state(item.task_id))
        with pytest.raises(TaskAdmitProjectionConflictError):
            await dag.task_admit(item)
    finally:
        dag._admit_loader.run = original
    assert before_state == 0 and await root_redis.exists(DagRedisKey.task_state(item.task_id)) == 0


@pytest.mark.asyncio
async def test_lost_ack_unknown_commit_rereads_exact_proof_no_second_owner(alpha, root_redis):
    store, dag, item, task, call = (await capture(alpha, root_redis, 'root'))[0:5]
    revision = (await store.get_aggregate_snapshot(task)).revision
    assert await root_redis.hget(HEADER, 'owner_count') == '1' and await root_redis.hlen(OWNERS) == 1
    out = await dag.task_admit(item)                              # exact retry after the lost response
    assert out.admitted
    assert (await store.get_aggregate_snapshot(task)).revision == revision
    assert await root_redis.hget(HEADER, 'owner_count') == '1' and await root_redis.hlen(OWNERS) == 1
    before = await frozen(root_redis)
    assert (await dag.task_admit(item)).status == 'already_exists' and await frozen(root_redis) == before


@pytest.mark.asyncio
async def test_progressed_exact_duplicate_has_no_rewind_or_requeue(alpha, root_redis):
    _, store, dag, item, _ = await setup(alpha)
    assert (await dag.task_admit(item)).admitted
    assert (await dag.task_dispatch_commit(dispatch(item))).committed
    before = await frozen(root_redis)
    assert (await dag.task_admit(item)).status == 'already_exists'
    assert await frozen(root_redis) == before
    assert await root_redis.zscore(DagRedisKey.tenant_ready_queue(item.tenant_id), item.task_id) is None


@pytest.mark.asyncio
async def test_internal_restored_admission_gets_same_enforcement(alpha, root_redis):
    _, store, dag, item, _ = await setup(alpha)
    assert (await dag.task_admit(item)).admitted
    other = await second_run(alpha, item)
    await drop_runtime(root_redis, item)          # runtime gone: only the owner inventory can block
    internal = DagLua(root_redis, canonical_task_admit_binding=True, require_task_owner_binding=True)
    before = await frozen(root_redis)
    await assert_rejected_clean(internal, store, seed(other), before, 'task_owner_conflict')


@pytest.mark.asyncio
async def test_losing_run_create_is_preserved_and_reported_incomplete(alpha, root_redis):
    _, store, dag, item, _ = await setup(alpha)
    assert (await dag.task_admit(item)).admitted
    other = await second_run(alpha, item)
    run_b = alpha.admission._run_create_authority.store
    from hfa.authority import AggregateType, CanonicalAggregateIdentity
    run_id = CanonicalAggregateIdentity(aggregate_type=AggregateType.RUN, run_id=other.run_id)
    before_run = await run_b.get_aggregate_snapshot(run_id)
    with pytest.raises(TaskAdmitAuthorityError):
        await dag.task_admit(seed(other))
    assert await run_b.get_aggregate_snapshot(run_id) == before_run      # RUN_CREATE preserved, not released/erased
    assert await agg(store, seed(other)) is None


# ----------------------------- activation / legacy fencing -----------------------------



@pytest.mark.asyncio
async def test_unactivated_namespace_blocks_fresh_admission_without_writes():
    redis = fakeredis.aioredis.FakeRedis(decode_responses=True)
    store = RedisCanonicalAuthorityStore(redis)
    await store.initialise()
    item = seed(request())
    plan = await _plan_for(item)
    before = await frozen(redis)
    res = await store.commit(plan, require_task_owner_binding=True)
    assert res.status.value == 'ILLEGAL_STATE_TRANSITION' and 'task_owner_namespace_not_activated' in res.detail
    assert await frozen(redis) == before and await redis.dbsize() == 0


@pytest.mark.asyncio
async def test_constructors_never_activate(root_redis, monkeypatch):
    for name in ('RUN_CREATE', 'TASK_ADMIT', 'TASK_DISPATCH', 'TASK_CLAIM', 'TASK_TERMINAL', 'TASK_REQUEUE'):
        monkeypatch.setenv(f'HFA_CANONICAL_{name}_BINDING', 'true')
    from hfa_control.models import ControlPlaneConfig
    from hfa_control.service import ControlPlaneService
    ControlPlaneService(root_redis, ControlPlaneConfig(instance_id='x', product_mode='SINGLE_TASK_ALPHA', strict_cas_mode=True,
                                                       tenant_identity_boundary='TRUSTED_GATEWAY_HEADER'))
    DagLua(root_redis, canonical_task_admit_binding=True, require_task_owner_binding=True)
    RedisCanonicalAuthorityStore(root_redis)
    assert await root_redis.dbsize() == 0 and await root_redis.exists(HEADER) == 0


BAD_ATTESTATIONS = [
    None, {}, {**FRESH_TEST_ATTESTATION, 'writers_isolated': False}, {**FRESH_TEST_ATTESTATION, 'fresh_domain_id': 'other'},
    {**FRESH_TEST_ATTESTATION, 'evidence_ref': ' '}, {**FRESH_TEST_ATTESTATION, 'attested_by': ''},
]


@pytest.mark.asyncio
@pytest.mark.parametrize('attestation', BAD_ATTESTATIONS)
async def test_activation_without_valid_isolation_attestation_is_migration_required(root_redis, attestation):
    store = RedisCanonicalAuthorityStore(root_redis)
    with pytest.raises(TaskOwnerActivationRefusedError, match='MIGRATION_ACCEPTANCE_REQUIRED'):
        await store.activate_task_owner_namespace(namespace_id='f02a1-fresh-test-namespace', activated_at_ms=1,
                                                  isolation_attestation=attestation)
    assert await root_redis.dbsize() == 0


@pytest.mark.asyncio
@pytest.mark.parametrize('seeded', [
    ('hset', 'hfa:authority:v1:abc:aggregate'), ('xadd', 'hfa:authority:v1:abc:outbox'),
    ('hset', 'hfa:authority:v1:abc:conflicts'), ('hset', 'hfa:authority:v1:operator-audits'),
    ('set', 'hfa:dag:task:legacy-task:state'), ('sadd', 'hfa:dag:run:legacy-run:tasks'),
])
async def test_activation_refuses_any_historical_evidence_family(root_redis, seeded):
    kind, key = seeded
    if kind == 'hset': await root_redis.hset(key, 'f', 'v')
    elif kind == 'xadd': await root_redis.xadd(key, {'f': 'v'})
    elif kind == 'sadd': await root_redis.sadd(key, 'm')
    else: await root_redis.set(key, 'v')
    before = await frozen(root_redis)
    with pytest.raises(TaskOwnerActivationRefusedError, match='MIGRATION_ACCEPTANCE_REQUIRED'):
        await RedisCanonicalAuthorityStore(root_redis).activate_task_owner_namespace(
            namespace_id='f02a1-fresh-test-namespace', activated_at_ms=1, isolation_attestation=FRESH_TEST_ATTESTATION)
    assert await frozen(root_redis) == before and await root_redis.exists(HEADER) == 0


@pytest.mark.asyncio
async def test_activation_success_is_idempotent_and_never_rewrites(root_redis):
    store = RedisCanonicalAuthorityStore(root_redis)
    first = await store.activate_task_owner_namespace(namespace_id='f02a1-fresh-test-namespace', activated_at_ms=1,
                                                      isolation_attestation=FRESH_TEST_ATTESTATION)
    assert first['status'] == 'ACTIVATED' and await store.task_owner_count() == 0
    assert await root_redis.pttl(HEADER) == -1 and await root_redis.hlen(HEADER) == 7
    before = await frozen(root_redis)
    again = await store.activate_task_owner_namespace(namespace_id='f02a1-fresh-test-namespace', activated_at_ms=999,
                                                      isolation_attestation=FRESH_TEST_ATTESTATION)
    assert again['status'] == 'ALREADY_ACTIVE' and await frozen(root_redis) == before


@pytest.mark.asyncio
async def test_non_owner_aware_writer_is_fenced_after_activation(alpha, root_redis):
    _, store, dag, item, _ = await setup(alpha)
    legacy = DagLua(root_redis, canonical_task_admit_binding=True)
    await legacy.initialise()
    before = await frozen(root_redis)
    await assert_rejected_clean(legacy, store, item, before, 'task_owner_legacy_writer_fenced')


@pytest.mark.asyncio
async def test_alpha_composition_requires_owner_binding(alpha):
    assert alpha._scheduler.composition.dag_lua._require_task_owner_binding is True
