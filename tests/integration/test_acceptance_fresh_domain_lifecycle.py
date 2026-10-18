"""R2 foundation on creator-owned Redis only. No shared TCP fixture is used."""
import json

import pytest
import redis.asyncio as redis
from redis.exceptions import AuthenticationError, ResponseError

from tests.acceptance_fresh_domain import (  # noqa: F401
    owned_domain, integration_redis_stack, strict_mode_env, flush_redis,
)
from scripts.acceptance_fresh_domain import (
    HEADER, OWNERS, PREFIX, Hold, Phase, digest, semantic_snapshot,
)
from hfa.authority import (
    RedisCanonicalAuthorityStore, evaluate_authority_commit, RedisAuthorityCommitStatus,
    RedisAuthorityPersistenceError,
)
from hfa_control.run_create_authority import build_run_create_command, build_run_create_context
from hfa_control.dag_lua import DagLua
from tests.unit.test_f02a1_root_binding import request, seed, admit_plan
from tests.unit.test_run_create_authority_contract import value

pytestmark = [pytest.mark.asyncio, pytest.mark.integration]


async def admit(domain):
    r = domain.client
    req = request()
    command = build_run_create_command(value(run_id=req.run_id, tenant_id=req.tenant_id,
                                            root_task_id=req.root_task_id, created_at_ms=1000))
    evaluated = evaluate_authority_commit(context=build_run_create_context(command), command=command,
        current_revision=0, current_state=None, receipt_probe=None, committed_at_ms=1000,
        correlation_id=None)
    store = RedisCanonicalAuthorityStore(r)
    await store.initialise()
    assert (await store.commit(evaluated.commit_plan)).committed
    dag = DagLua(r, canonical_task_admit_binding=True, require_root_task_binding=True,
                 require_task_owner_binding=True)
    item = seed(req)
    assert (await dag.task_admit(item)).admitted
    return store, dag, item


async def assert_retained_refusal_unchanged(d):
    before = await semantic_snapshot(d._client)
    with pytest.raises((Hold, RedisAuthorityPersistenceError)):
        await d.verify_retained()
    assert await semantic_snapshot(d._client) == before
    assert d._activation_attempts == 1


async def test_fresh_activation_exact_header_and_external_ownership(owned_domain):
    d = owned_domain
    assert d.phase == Phase.WRITERS_ISOLATED and d._activation_attempts == 0
    assert d.verify_ownership()["tcp_listeners"] == 1
    await d.activate_fresh()
    assert d.phase == Phase.DOMAIN_RELEASED and d._activation_attempts == 1
    assert await d.client.hgetall(HEADER) == d._header
    assert await d.client.pttl(HEADER) == -1
    assert await d.client.exists(OWNERS) == 0
    assert d._header["attestation_sha256"] == digest(d._attestation)
    receipts = [json.loads(p.read_text()) for p, _ in d._receipts]
    phases = [r["kind"] for r in receipts if r["kind"] in {p.value for p in Phase}]
    assert phases == [p.value for p in (Phase.CREATED, Phase.OWNERSHIP_VERIFIED,
        Phase.WRITERS_ISOLATED, Phase.FULL_DB_INVENTORY_COMPLETE, Phase.EXTERNALLY_ATTESTED,
        Phase.ACTIVATION_CALLED_ONCE, Phase.HEADER_VERIFIED, Phase.DOMAIN_RELEASED)]
    assert any(r["kind"] == "inventory" and r["value"]["cursor_complete"]
               and r["value"]["keys"] == [] for r in receipts)


async def test_uncredentialled_writer_and_reset_commands_are_excluded(owned_domain):
    d = owned_domain
    unauthenticated = redis.Redis(host='127.0.0.1', port=d._port, socket_timeout=2)
    try:
        with pytest.raises(AuthenticationError):
            await unauthenticated.set("foreign", "writer")
    finally:
        await unauthenticated.aclose()
    await d.activate_fresh()
    for command in ("FLUSHDB", "FLUSHALL", "CONFIG GET *", "ACL USERS"):
        with pytest.raises(ResponseError):
            await d.client.execute_command(*command.split())
    assert await d.client.exists("foreign") == 0


async def test_real_owner_duplicate_retained_and_reconstruction(owned_domain, monkeypatch):
    d = owned_domain
    await d.activate_fresh()
    store, dag, item = await admit(d)
    assert await d.client.hlen(OWNERS) == 1
    assert await d.client.hget(HEADER, "owner_count") == "1"
    assert await d.client.pttl(OWNERS) == -1
    before = await semantic_snapshot(d._client)
    assert (await dag.task_admit(item)).status == "already_exists"
    assert await semantic_snapshot(d._client) == before
    # Normal object reconstruction: no alpha configuration or startup patch.
    from hfa_control.service import ControlPlaneService
    from hfa_control.models import ControlPlaneConfig
    async def forbidden(*args, **kwargs):
        pytest.fail("object restart attempted activation")
    monkeypatch.setattr(RedisCanonicalAuthorityStore, "activate_task_owner_namespace", forbidden)
    first = ControlPlaneService(d.client, ControlPlaneConfig(instance_id="r2-first"))
    second = ControlPlaneService(d.client, ControlPlaneConfig(instance_id="r2-restart"))
    rebuilt = DagLua(d.client, canonical_task_admit_binding=True, require_root_task_binding=True,
                     require_task_owner_binding=True)
    assert (await rebuilt.task_admit(item)).status == "already_exists"
    assert first is not second
    assert await semantic_snapshot(d._client) == before
    await d.quiesce()
    assert await semantic_snapshot(d._client) == before
    await d.verify_retained()
    await d.verify_retained()
    assert await semantic_snapshot(d._client) == before
    assert d._activation_attempts == 1
    await d.resume_verified()
    assert (await DagLua(d.client, canonical_task_admit_binding=True,
        require_task_owner_binding=True).task_admit(item)).status == "already_exists"
    assert await semantic_snapshot(d._client) == before


async def test_owned_child_disposal_does_not_issue_reset(owned_domain, monkeypatch):
    d = owned_domain
    await d.activate_fresh()
    process, storage = d._process, d._root
    async def forbidden(*args, **kwargs):
        pytest.fail("disposal issued FLUSHDB")
    monkeypatch.setattr(d._client, "flushdb", forbidden)
    await d.dispose()
    assert d.phase == Phase.DISPOSED and process.poll() is not None and not storage.exists()
    with pytest.raises(Hold, match="already disposed"):
        await d.dispose()


async def test_occupied_endpoint_is_not_contacted_or_terminated(tmp_path, monkeypatch):
    import os
    import socket
    from pathlib import Path
    from scripts.acceptance_fresh_domain import OwnedFreshDomain
    d = OwnedFreshDomain(Path(os.environ['HFA_ACCEPTANCE_REDIS_BINARY']), tmp_path)
    occupant = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    occupant.bind(('127.0.0.1', 0))
    occupant.listen(1)
    occupant.settimeout(0.05)
    port = occupant.getsockname()[1]
    try:
        with pytest.raises(Hold, match='occupied'):
            await d.create(port=port)
        with pytest.raises(TimeoutError):
            occupant.accept()
        assert d._process is None and d._client is None
        assert occupant.getsockname()[1] == port
    finally:
        occupant.close()


async def test_foreign_process_identity_refuses_before_scenario_connection(owned_domain, monkeypatch):
    import scripts.acceptance_fresh_domain as module
    d = owned_domain
    original = module.process_identity
    monkeypatch.setattr(module, 'process_identity', lambda pid: {**original(pid), 'start_ticks': 'foreign'})
    try:
        with pytest.raises(Hold, match='foreign'):
            await d.activate_fresh()
        assert d._activation_attempts == 0 and await d._client.dbsize() == 0
    finally:
        monkeypatch.undo()


async def test_module_local_overrides_prevent_shared_reset_or_docker(request):
    for name in ('integration_redis_stack', 'strict_mode_env', 'flush_redis'):
        assert request._fixture_defs[name].func.__module__ == 'tests.acceptance_fresh_domain'
    assert 'redis_client' not in request.fixturenames


@pytest.mark.parametrize("key", [PREFIX + ":x:aggregate", PREFIX + ":x:receipts",
    PREFIX + ":x:operation-records", PREFIX + ":x:transition-indexes", PREFIX + ":x:log",
    PREFIX + ":x:outbox", PREFIX + ":x:conflicts", PREFIX + ":operator-audits",
    "hfa:dag:task:legacy:state", "hfa:dag:run:legacy:tasks", "hfa:run:state:legacy",
    "unrelated", "other:authority:record", HEADER, OWNERS])
async def test_any_preexisting_evidence_refuses_fresh_without_erasure(owned_domain, key):
    d = owned_domain
    await d._client.set(key, "prior evidence")
    before = await semantic_snapshot(d._client)
    with pytest.raises(Hold, match="pre-existing"):
        await d.activate_fresh()
    assert d._activation_attempts == 0 and d.phase == Phase.HOLD
    assert await semantic_snapshot(d._client) == before


@pytest.mark.parametrize("fault", ["missing-isolation", "extra-client", "partial-scan", "scan-error",
                                  "inventory-race", "receipt-forged", "owner-acl-nopass"])
async def test_pre_activation_boundary_faults(owned_domain, monkeypatch, fault):
    d = owned_domain
    extra = None
    if fault == "missing-isolation":
        monkeypatch.setattr(d, "verify_ownership", lambda: (_ for _ in ()).throw(Hold("isolation missing")))
    elif fault == "extra-client":
        extra = redis.Redis(host='127.0.0.1', port=d._port, username="owner",
                            password=d._owner_password, decode_responses=True)
        await extra.ping()
    elif fault in {"partial-scan", "scan-error"}:
        await d._client.set("unread", "evidence")
        async def broken(**kwargs):
            if fault == "scan-error":
                raise OSError("incomplete inventory")
            return 0, []
        monkeypatch.setattr(d._client, "scan", broken)
    elif fault == "inventory-race":
        original = d._inventory
        async def race():
            result = await original()
            await d._client.set("concurrent", "writer")
            return result
        monkeypatch.setattr(d, "_inventory", race)
    elif fault == 'owner-acl-nopass':
        await d._client.execute_command('ACL', 'SETUSER', 'owner', 'nopass')
    else:
        path = d._receipts[0][0]
        path.chmod(0o600)
        path.write_text('{"owned":true}')
        path.chmod(0o400)
    try:
        with pytest.raises((Hold, OSError)):
            await d.activate_fresh()
        assert d._activation_attempts == 0
        assert await d._client.exists(HEADER) == 0
    finally:
        if extra is not None:
            await extra.aclose()
        monkeypatch.undo()


@pytest.mark.parametrize("outcome", ["already-active", "timeout-before", "lost-response"])
async def test_unknown_or_already_active_never_retries(owned_domain, monkeypatch, outcome):
    d = owned_domain
    original = RedisCanonicalAuthorityStore.activate_task_owner_namespace
    calls = []
    async def uncertain(self, **kwargs):
        calls.append(kwargs)
        if outcome == "already-active":
            return {"status": "ALREADY_ACTIVE"}
        if outcome == "lost-response":
            await original(self, **kwargs)
        raise TimeoutError("unknown activation outcome")
    monkeypatch.setattr(RedisCanonicalAuthorityStore, "activate_task_owner_namespace", uncertain)
    with pytest.raises((Hold, TimeoutError)):
        await d.activate_fresh()
    assert len(calls) == 1 and d._activation_attempts == 1 and d.phase == Phase.HOLD
    before = await semantic_snapshot(d._client)
    with pytest.raises(Hold):
        await d.activate_fresh()
    assert await semantic_snapshot(d._client) == before and len(calls) == 1


async def test_actual_activation_lua_exists_race_is_not_fresh_success(owned_domain, monkeypatch):
    d = owned_domain
    original = d._client.eval
    injected = []
    async def raced(script, *args, **kwargs):
        if not injected:
            # Deliberately violate exclusion inside the owned test boundary at
            # the API's scan-to-EVAL gap. No accepted owner proof is fabricated.
            await d._client.hset(OWNERS, 'foreign', 'unaccepted fault injection')
            injected.append(True)
        return await original(script, *args, **kwargs)
    monkeypatch.setattr(d._client, 'eval', raced)
    with pytest.raises(Hold, match='ALREADY_ACTIVE'):
        await d.activate_fresh()
    assert d.phase == Phase.HOLD and d._activation_attempts == 1
    assert await d._client.exists(HEADER) == 0
    assert await d._client.hgetall(OWNERS) == {'foreign': 'unaccepted fault injection'}
    response = [json.loads(p.read_text())['value'] for p, _ in d._receipts
                if 'activation_response' in p.name]
    assert response[0]['status'] == 'ALREADY_ACTIVE'


@pytest.mark.parametrize('damage', ['ttl', 'foreign-id', 'unexpected-owner'])
async def test_fresh_readback_does_not_release_corrupt_domain(owned_domain, monkeypatch, damage):
    d = owned_domain
    original = RedisCanonicalAuthorityStore.activate_task_owner_namespace
    async def corrupt(self, **kwargs):
        response = await original(self, **kwargs)
        if damage == 'ttl':
            await d._client.pexpire(HEADER, 600000)
        elif damage == 'foreign-id':
            await d._client.hset(HEADER, 'namespace_id', 'foreign')
        else:
            await d._client.hset(OWNERS, 'unaccepted', 'corrupt')
        return response
    monkeypatch.setattr(RedisCanonicalAuthorityStore, 'activate_task_owner_namespace', corrupt)
    with pytest.raises(Hold):
        await d.activate_fresh()
    assert d.phase == Phase.HOLD and d._scenario is None
    assert await d._client.acl_users() == ['default', 'owner']


@pytest.mark.parametrize("fault", ["missing", "wrong-type", "ttl", "count", "id", "hash",
                                  "timestamp", "extra-field"])
async def test_header_corruption_refuses_retention_without_repair(owned_domain, fault):
    d = owned_domain
    await d.activate_fresh()
    await d.quiesce()
    if fault in {"missing", "wrong-type"}:
        await d._client.delete(HEADER)
        if fault == "wrong-type":
            await d._client.set(HEADER, "corrupt")
    elif fault == "ttl":
        await d._client.pexpire(HEADER, 600000)
    else:
        field, val = {"count": ("owner_count", "-1"), "id": ("namespace_id", "foreign"),
            "hash": ("attestation_sha256", "bad"), "timestamp": ("activated_at_ms", "999"),
            "extra-field": ("extra", "field")}[fault]
        await d._client.hset(HEADER, field, val)
    await assert_retained_refusal_unchanged(d)


@pytest.mark.parametrize("fault", ["lost-field", "lost-hash", "substituted", "owner-without-record",
                                  "record-without-owner", "stale-checkpoint", "no-checkpoint"])
async def test_owner_and_partial_snapshot_boundary(owned_domain, fault):
    d = owned_domain
    await d.activate_fresh()
    store, dag, item = await admit(d)
    if fault == 'stale-checkpoint':
        await d.client.set('checkpointed', 'latest acknowledged value')
    await d.quiesce()
    if fault == "lost-field":
        await d._client.hdel(OWNERS, item.task_id)
    elif fault == "lost-hash":
        await d._client.delete(OWNERS)
    elif fault == "substituted":
        await d._client.hset(OWNERS, item.task_id, "foreign provenance")
    elif fault == "owner-without-record":
        from hfa_control.task_admit_authority import build_task_admit_command
        identity = build_task_admit_command(item).aggregate_identity
        await d._client.delete(store.keyspace(identity.sha256).operation_records)
    elif fault == "record-without-owner":
        await d._client.delete(OWNERS)
        await d._client.hset(HEADER, "owner_count", "0")
    elif fault == "stale-checkpoint":
        # Simulate loss of a checkpointed value without importing/restoring a snapshot.
        await d._client.delete('checkpointed')
    else:
        d._checkpoint = None
    await assert_retained_refusal_unchanged(d)


async def test_retained_reset_reactivation_and_revoked_client_refuse(owned_domain):
    d = owned_domain
    await d.activate_fresh()
    old = d.client
    await old.set("persistent", "evidence")
    await d.quiesce()
    before = await semantic_snapshot(d._client)
    with pytest.raises(Hold):
        await d.reset()
    with pytest.raises(Hold):
        await d.activate_fresh()
    with pytest.raises(AuthenticationError):
        await old.set("late", "writer")
    assert await semantic_snapshot(d._client) == before
    await old.aclose()


async def test_missing_owner_activation_blocks_actual_admission(owned_domain):
    d = owned_domain
    store = RedisCanonicalAuthorityStore(d._client)
    await store.initialise()
    before = await semantic_snapshot(d._client)
    result = await store.commit(admit_plan(seed(request())), require_task_owner_binding=True)
    assert result.status == RedisAuthorityCommitStatus.ILLEGAL_STATE_TRANSITION
    assert result.detail == "task_owner_namespace_not_activated"
    assert await semantic_snapshot(d._client) == before


async def test_queue_pel_ttl_checkpoint_survives_read_only_verification(owned_domain):
    d = owned_domain
    await d.activate_fresh()
    r = d.client
    await r.set("ttl-probe", "value", px=600000)
    await r.zadd("queue", {"task": 1})
    await r.xadd("delivery", {"task_id": "delivery-only"})
    await r.xgroup_create("delivery", "workers", id="0")
    await r.xreadgroup("workers", "consumer", {"delivery": ">"}, count=1)
    before = await semantic_snapshot(d._client)
    await d.quiesce()
    await d.verify_retained()
    assert await semantic_snapshot(d._client) == before


async def test_new_admission_after_retained_resume_only_grows_owner_inventory(owned_domain):
    d = owned_domain
    await d.activate_fresh()
    await admit(d)
    original = await d.client.hgetall(OWNERS)
    await d.quiesce()
    await d.resume_verified()
    await admit(d)
    current = await d.client.hgetall(OWNERS)
    assert len(current) == 2 and all(current[k] == v for k, v in original.items())
    assert await d.client.hget(HEADER, 'owner_count') == '2'
    await d.quiesce()
    await d.verify_retained()


async def test_quiescence_cannot_replace_a_prior_owner_checkpoint_after_loss(owned_domain):
    d = owned_domain
    await d.activate_fresh()
    await admit(d)
    await d.quiesce()
    previous = d._checkpoint
    await d.resume_verified()
    # Fault injection only: remove the owned domain's complete authority/runtime
    # evidence except the original immutable header; do not invoke RESTORE/FLUSHDB.
    keys = [key async for key in d.client.scan_iter() if key != HEADER]
    await d.client.delete(*keys)
    await d.client.hset(HEADER, 'owner_count', '0')
    before = await semantic_snapshot(d._client)
    with pytest.raises(Hold, match='rolled back'):
        await d.quiesce()
    assert d.phase == Phase.HOLD and d._checkpoint is previous
    assert await semantic_snapshot(d._client) == before
