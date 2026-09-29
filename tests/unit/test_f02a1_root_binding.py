"""Actual production admission and Lua oracles; offline fixture by default."""
from dataclasses import replace
import hashlib
from types import SimpleNamespace
from uuid import uuid4

import fakeredis
import fakeredis.aioredis
import pytest
import pytest_asyncio

from hfa.authority import (
    AggregateType, AuthorityDecisionCode, CanonicalAggregateIdentity,
    OperationType, RedisAuthorityCommitStatus, RedisCanonicalAuthorityStore,
    evaluate_authority_commit,
)
from hfa.dag.schema import DagRedisKey, DagTaskSeed
from hfa_control.admission import AdmissionController
from hfa_control.dag_lua import DagLua
from hfa_control.models import ControlPlaneConfig
from hfa_control.run_create_authority import (
    RunCreateAuthorityError, build_run_create_command, run_create_operation_id,
)
from hfa_control.run_submission import SingleTaskRunSubmission
from hfa_control.service import ControlPlaneService
from hfa_control.task_admit_authority import (
    TaskAdmitAuthorityError, build_task_admit_command, build_task_admit_context,
)
from tests.unit.test_f03_terminal_ordering import plan
from tests.unit.test_run_create_authority_contract import value


@pytest_asyncio.fixture
async def root_redis():
    server = fakeredis.FakeServer(version=(7, 4))
    redis = fakeredis.aioredis.FakeRedis(server=server, decode_responses=True)
    await redis.eval('return 1', 0)
    server._lua_runtime.globals().redis.sha1hex = (
        lambda raw: hashlib.sha1(raw).hexdigest().encode('ascii')
    )
    try:
        yield redis
    finally:
        await redis.aclose()


@pytest.fixture
def alpha(root_redis, monkeypatch):
    for name in ('RUN_CREATE', 'TASK_ADMIT', 'TASK_DISPATCH', 'TASK_CLAIM',
                 'TASK_TERMINAL', 'TASK_REQUEUE'):
        monkeypatch.setenv(f'HFA_CANONICAL_{name}_BINDING', 'true')
    service = ControlPlaneService(root_redis, ControlPlaneConfig(
        instance_id='f02a1', product_mode='SINGLE_TASK_ALPHA',
        strict_cas_mode=True, tenant_identity_boundary='TRUSTED_GATEWAY_HEADER',
    ))
    assert service._product_profile.single_task_alpha
    dag = service._scheduler.composition.dag_lua
    assert type(dag) is DagLua and service._run_submission._dag_lua is dag
    assert dag._require_root_task_binding is True
    return service


def request(*, root=True):
    fields = dict(
        run_id=f'run-f02a1-{uuid4()}', tenant_id='f02a1', agent_type='default',
        priority=0, payload={}, estimated_cost_cents=0,
        preferred_region='', preferred_placement='LEAST_LOADED',
    )
    if root:
        fields['root_task_id'] = f'task-{uuid4()}'
    return SimpleNamespace(**fields)


def seed(req, task_id=None):
    return DagTaskSeed(
        task_id=task_id or req.root_task_id, run_id=req.run_id,
        tenant_id=req.tenant_id, agent_type=req.agent_type, priority=0,
        admitted_at=1000, dependency_count=0, child_task_ids=(), parent_task_ids=[],
        payload_json='{}',
    )


def admit_plan(item):
    command = build_task_admit_command(item)
    evaluated = evaluate_authority_commit(
        context=build_task_admit_context(command), command=command,
        current_revision=0, current_state=None, receipt_probe=None,
        committed_at_ms=item.admitted_at, correlation_id=None,
    )
    assert evaluated.decision.code is AuthorityDecisionCode.ACCEPTED
    return evaluated.commit_plan


async def create(alpha, req=None):
    req = req or request()
    # No raw RUN_CREATE command: the real production AdmissionController owns it.
    assert await alpha.admission.admit(req) == req.run_id
    store = alpha.admission._run_create_authority.store
    run = CanonicalAggregateIdentity(aggregate_type=AggregateType.RUN, run_id=req.run_id)
    head = await store.get_aggregate_snapshot(run)
    probe = await store.load_receipt_probe(run, run_create_operation_id(req.run_id))
    assert head.state == 'pending' and head.revision == 1
    assert probe.canonical_store_record.authoritative_metadata_changes['root_task_id'] == req.root_task_id
    assert probe.receipt.canonical_command_hash == probe.canonical_store_record.canonical_command_hash
    await alpha.admission._run_create_authority._validate_exact_head(probe.canonical_store_record, probe.receipt)
    return req, store, run


async def snapshot(redis):
    # Redis hash-table rehashing can reorder DUMP on reads. Compare exact value
    # bytes with normalized collection ordering, plus absolute TTL deadlines.
    def raw(value):
        return value.encode('utf-8') if isinstance(value, str) else value

    result = {}
    for key in sorted(await redis.keys('*')):
        kind = await redis.type(key)
        if kind == 'hash':
            payload = tuple(sorted((raw(k), raw(v)) for k, v in (await redis.hgetall(key)).items()))
        elif kind == 'string':
            payload = raw(await redis.get(key))
        elif kind == 'set':
            payload = tuple(sorted(raw(v) for v in await redis.smembers(key)))
        elif kind == 'zset':
            payload = tuple((raw(v), score) for v, score in await redis.zrange(key, 0, -1, withscores=True))
        elif kind == 'stream':
            payload = tuple((raw(i), tuple(sorted((raw(k), raw(v)) for k, v in fields.items())))
                            for i, fields in await redis.xrange(key))
        else:
            raise AssertionError(f'unexpected snapshot key type: {kind}')
        result[raw(key)] = (kind, payload, await redis.pexpiretime(key))
    return result


@pytest.mark.asyncio
@pytest.mark.parametrize('membership', ['unchanged', 'empty', 'forged'])
async def test_original_blocker_now_rejects_second_task(alpha, root_redis, membership):
    req, store, run = await create(alpha)
    dag = alpha._scheduler.composition.dag_lua
    allowed = seed(req)
    assert (await dag.task_admit(allowed)).status == 'seeded_root'
    root = build_task_admit_command(allowed).aggregate_identity
    assert (await store.get_aggregate_snapshot(root)).revision == 1
    wrong = seed(req, f'task-{uuid4()}')
    if membership == 'empty':
        await root_redis.delete(DagRedisKey.run_tasks(req.run_id))
    elif membership == 'forged':
        await root_redis.sadd(DagRedisKey.run_tasks(req.run_id), wrong.task_id)
    before = await snapshot(root_redis)
    with pytest.raises(TaskAdmitAuthorityError):
        await dag.task_admit(wrong)
    # Also reach the canonical boundary directly, even if forged membership
    # was rejected earlier by legacy-footprint detection in the gateway.
    rejected = await store.commit(admit_plan(wrong), require_task_admit_root_binding=True)
    assert rejected.status is RedisAuthorityCommitStatus.ILLEGAL_STATE_TRANSITION
    assert rejected.detail == 'parent_run_root_task_mismatch'
    assert await store.get_aggregate_snapshot(build_task_admit_command(wrong).aggregate_identity) is None
    assert (await store.get_aggregate_snapshot(run)).state == 'pending'
    assert await snapshot(root_redis) == before


@pytest.mark.asyncio
async def test_bound_parent_enforced_even_on_internal_gateway(alpha, root_redis):
    req, store, _ = await create(alpha)
    internal = DagLua(root_redis, canonical_task_admit_binding=True)
    with pytest.raises(TaskAdmitAuthorityError, match='parent_run_root_task_mismatch'):
        await internal.task_admit(seed(req, 'wrong-internal-task'))
    assert await store.get_aggregate_snapshot(build_task_admit_command(seed(req, 'wrong-internal-task')).aggregate_identity) is None


@pytest.mark.asyncio
@pytest.mark.parametrize('part', ['record', 'receipt'])
@pytest.mark.parametrize('damage', ['missing', 'corrupt'])
async def test_matching_root_rejects_damaged_parent(alpha, root_redis, part, damage):
    req, store, run = await create(alpha)
    keys = store.keyspace(run.sha256)
    key = keys.operation_records if part == 'record' else keys.receipts
    field = keys.operation_field(run_create_operation_id(req.run_id))
    if damage == 'missing':
        await root_redis.hdel(key, field)
    else:
        await root_redis.hset(key, field, '{}')
    before = await snapshot(root_redis)
    with pytest.raises(TaskAdmitAuthorityError):
        await alpha._scheduler.composition.dag_lua.task_admit(seed(req))
    assert await store.get_aggregate_snapshot(build_task_admit_command(seed(req)).aggregate_identity) is None
    assert await snapshot(root_redis) == before


@pytest.mark.asyncio
async def test_alpha_task_requires_parent(alpha, root_redis):
    req = request()
    before = await snapshot(root_redis)
    with pytest.raises(TaskAdmitAuthorityError, match='parent_run_root_binding_required'):
        await alpha._scheduler.composition.dag_lua.task_admit(seed(req))
    assert await snapshot(root_redis) == before


@pytest.mark.asyncio
@pytest.mark.parametrize('race', ['terminalize', 'delete_receipt'])
async def test_parent_change_after_witness_cannot_commit(alpha, root_redis, monkeypatch, race):
    req, store, run = await create(alpha)
    original = RedisCanonicalAuthorityStore.parent_run_guard
    observed = []
    expected = None

    async def interleave(self, run_id, **kwargs):
        nonlocal expected
        witness = await original(self, run_id, **kwargs)
        if not observed:
            assert witness[1][10] == 'pending'
            observed.append(True)
            if race == 'terminalize':
                assert (await store.commit(plan(run, OperationType.RUN_TERMINATE,
                    'pending', 'done', 1, ['RUN_RESULT_PROJECTION']))).committed
            else:
                keys = store.keyspace(run.sha256)
                await root_redis.hdel(keys.receipts, keys.operation_field(run_create_operation_id(req.run_id)))
            expected = await snapshot(root_redis)
        return witness

    monkeypatch.setattr(RedisCanonicalAuthorityStore, 'parent_run_guard', interleave)
    with pytest.raises(TaskAdmitAuthorityError):
        await alpha._scheduler.composition.dag_lua.task_admit(seed(req))
    assert observed
    assert await store.get_aggregate_snapshot(build_task_admit_command(seed(req)).aggregate_identity) is None
    assert await snapshot(root_redis) == expected


@pytest.mark.asyncio
@pytest.mark.parametrize('damage_parent', [False, True])
async def test_exact_root_duplicate_is_receipt_first_and_read_only(alpha, root_redis, monkeypatch, damage_parent):
    req, store, run = await create(alpha)
    dag = alpha._scheduler.composition.dag_lua
    item = seed(req)
    assert (await dag.task_admit(item)).admitted
    if damage_parent:
        keys = store.keyspace(run.sha256)
        await root_redis.hdel(keys.receipts, keys.operation_field(run_create_operation_id(req.run_id)))

    async def forbidden(*args, **kwargs):
        pytest.fail('exact TASK_ADMIT duplicate recaptured parent authority')

    monkeypatch.setattr(RedisCanonicalAuthorityStore, 'parent_run_guard', forbidden)
    before = await snapshot(root_redis)
    assert (await dag.task_admit(item)).status == 'already_exists'
    duplicate = await store.commit(admit_plan(item), require_task_admit_root_binding=True)
    assert duplicate.status is RedisAuthorityCommitStatus.ALREADY_APPLIED
    assert await snapshot(root_redis) == before


@pytest.mark.asyncio
async def test_historical_unbound_run_and_duplicate_preserved(alpha, root_redis):
    historical = AdmissionController(root_redis, ControlPlaneConfig(instance_id='historical'),
        canonical_run_create_binding=True, run_create_authority=alpha.admission._run_create_authority)
    req = request(root=False)
    assert await historical.admit(req) == req.run_id
    store = historical._run_create_authority.store
    run = CanonicalAggregateIdentity(aggregate_type=AggregateType.RUN, run_id=req.run_id)
    before = await store.get_aggregate_snapshot(run)
    probe = await store.load_receipt_probe(run, run_create_operation_id(req.run_id))
    assert 'root_task_id' not in probe.canonical_store_record.authoritative_metadata_changes
    assert await historical.admit(req) == req.run_id
    assert await store.get_aggregate_snapshot(run) == before
    internal = DagLua(root_redis, canonical_task_admit_binding=True)
    for task in ('historical-first', 'historical-second'):
        assert (await internal.task_admit(seed(req, task))).admitted
    with pytest.raises(TaskAdmitAuthorityError, match='root_binding_required'):
        await alpha._scheduler.composition.dag_lua.task_admit(seed(req, 'alpha-cannot-adopt-history'))
    req.root_task_id = 'cannot-upgrade-durable-history'
    with pytest.raises(RunCreateAuthorityError):
        await historical.admit(req)
    assert await store.get_aggregate_snapshot(run) == before


@pytest.mark.asyncio
async def test_binding_cannot_change_on_run_create_retry(alpha):
    req, store, run = await create(alpha)
    before = await store.get_aggregate_snapshot(run)
    req.root_task_id = 'different-root'
    with pytest.raises(RunCreateAuthorityError):
        await alpha.admission.admit(req)
    assert await store.get_aggregate_snapshot(run) == before


@pytest.mark.asyncio
@pytest.mark.parametrize('root', [None, '', '   '])
async def test_alpha_run_requires_root_before_writes(alpha, root_redis, root):
    req = request()
    req.root_task_id = root
    before = await snapshot(root_redis)
    with pytest.raises(ValueError, match='requires root_task_id'):
        await alpha.admission.admit(req)
    assert await snapshot(root_redis) == before


@pytest.mark.asyncio
async def test_normal_alpha_submission_binds_generated_task_and_seeds_root(alpha):
    result = await alpha.submit_single_task_run(SingleTaskRunSubmission(
        tenant_id='f02a1', payload={'root_task_id': 'untrusted-payload'},
        idempotency_key='f02a1-normal',
    ))
    assert result['status'] == 'ACCEPTED', result
    store = alpha.admission._run_create_authority.store
    run = CanonicalAggregateIdentity(aggregate_type=AggregateType.RUN, run_id=result['run_id'])
    probe = await store.load_receipt_probe(run, run_create_operation_id(result['run_id']))
    assert probe.canonical_store_record.authoritative_metadata_changes['root_task_id'] == result['task_id']
    task = CanonicalAggregateIdentity(aggregate_type=AggregateType.TASK,
        run_id=result['run_id'], task_id=result['task_id'])
    assert (await store.get_aggregate_snapshot(task)).state == 'ready'
    assert (await store.get_aggregate_snapshot(task)).revision == 1
    assert result['task_admitted'] and result['task_ready']


def test_historical_v1_command_hash_golden_and_bound_semantics():
    old = build_run_create_command(value())
    assert old.canonical_command_hash == 'aeab565a4cb7bee3383bebcab90c38cb97aa33501df610cc208527161bfdec9a'
    assert old.operation_id == 'run-create:v1:542c988293f09df0a63f78c3846b50446060984e1c3bdf7a7d0ba84a77b336fb'
    bound = build_run_create_command(replace(value(), root_task_id='root-task'))
    assert bound.operation_id == old.operation_id
    assert bound.canonical_command_hash != old.canonical_command_hash
    assert bound.authoritative_metadata_changes['root_task_id'] == 'root-task'
