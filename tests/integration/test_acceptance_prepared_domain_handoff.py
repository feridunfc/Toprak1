"""D2A only: verified local children, no original builder/CLI/workflow cut-in."""
import asyncio
from dataclasses import dataclass
import json
import os
from pathlib import Path
import time

import pytest
import pytest_asyncio
from redis.exceptions import AuthenticationError, ResponseError

from tests.acceptance_fresh_domain import (  # noqa: F401
    owned_domain, integration_redis_stack, strict_mode_env, flush_redis,
)
from scripts.acceptance_fresh_domain import HEADER, OWNERS, Hold, Phase, semantic_snapshot
import scripts.acceptance_prepared_domain as prepared
from scripts.acceptance_prepared_domain import PreparedDomainIssuer, PreparedGrant

pytestmark = [pytest.mark.asyncio, pytest.mark.integration]


def consumer():
    """Exact audited in-process consumer identity for these foundation probes."""


@pytest_asyncio.fixture
async def domain_issuer(owned_domain):
    d = owned_domain
    await d.activate_prepared()
    issuer = PreparedDomainIssuer(d)
    try:
        yield d, issuer
    finally:
        if not issuer._closed.is_set():
            await issuer.abort()
        assert d.phase == Phase.DISPOSED
        if issuer._error:
            raise issuer._error


async def issue(issuer, *, target=consumer, timeout=20):
    return await issuer.issue(scenario='d2a', acceptance_id='d2a-proof',
                              consumer=target, timeout=timeout)


async def redeem(issuer, grant, *, scenario='d2a', acceptance_id='d2a-proof', target=consumer):
    return await issuer.redeem(grant, scenario=scenario, acceptance_id=acceptance_id,
                                consumer=target)


async def test_real_issue_one_use_byte_dump_and_lease_local_close(domain_issuer):
    d, issuer = domain_issuer
    grant = await issue(issuer)
    assert prepared._ISSUERS[issuer._key] is issuer
    assert issuer._entry['binding']['creation'] == d._receipts[0][1]
    assert issuer._entry['binding']['host'] == d._host
    lease = await redeem(issuer, grant)
    assert lease.client.connection_pool is not d.client.connection_pool
    assert lease.client.connection_pool.connection_kwargs['decode_responses'] is False
    assert d.client.connection_pool.connection_kwargs['decode_responses'] is True
    await lease.client.set('byte-probe', b'payload')
    assert await lease.client.get('byte-probe') == b'payload'
    assert isinstance(await lease.client.dump('byte-probe'), bytes)
    with pytest.raises(Hold):
        await redeem(issuer, grant)
    with pytest.raises(Hold):
        await issue(issuer)
    await lease.aclose()
    assert d._process.poll() is None and await d.client.ping()
    with pytest.raises(Hold):
        lease.client
    await issuer.finish()
    assert d.phase == Phase.DISPOSED and not d._root.exists()


@pytest.mark.parametrize('foreign', ['missing', 'json', 'copied', 'dataclass'])
async def test_missing_forged_copied_grant_refuses_before_scenario_write(domain_issuer, foreign):
    d, issuer = domain_issuer
    grant = await issue(issuer)
    @dataclass
    class Forged:
        nonce: str
    copied_receipt = json.loads(d._receipts[0][0].read_text())
    supplied = {'missing': None, 'json': copied_receipt | {'nonce': issuer._entry['nonce']},
                'copied': object.__new__(PreparedGrant),
                'dataclass': Forged(issuer._entry['nonce'])}[foreign]
    before = await semantic_snapshot(d._client)
    with pytest.raises(Hold):
        await redeem(issuer, supplied)
    assert not issuer._entry['used'] and issuer._lease is None
    assert await semantic_snapshot(d._client) == before


@pytest.mark.parametrize('fault', ['scenario', 'acceptance', 'consumer', 'registry', 'deadline'])
async def test_live_binding_mismatch_refuses(domain_issuer, fault):
    d, issuer = domain_issuer
    grant = await issue(issuer)
    before = await semantic_snapshot(d._client)
    kwargs = {}
    if fault == 'scenario': kwargs['scenario'] = 'foreign'
    if fault == 'acceptance': kwargs['acceptance_id'] = 'foreign'
    if fault == 'consumer': kwargs['target'] = test_live_binding_mismatch_refuses
    if fault == 'registry': prepared._ISSUERS.pop(issuer._key)
    if fault == 'deadline': issuer._entry['deadline'] = time.monotonic() - 1
    with pytest.raises(Hold):
        await redeem(issuer, grant, **kwargs)
    assert issuer._lease is None and not issuer._entry['used']
    assert await semantic_snapshot(d._client) == before


@pytest.mark.parametrize('fault', ['pid', 'listener', 'storage', 'header', 'activation-receipt', 'acl'])
async def test_changed_domain_provenance_refuses_without_product_writes(domain_issuer, monkeypatch, fault):
    d, issuer = domain_issuer
    grant = await issue(issuer)
    if fault == 'pid':
        original = d._verify_process
        monkeypatch.setattr(d, '_verify_process', lambda: (_ for _ in ()).throw(Hold('foreign PID')))
    elif fault == 'listener':
        original = d._listener
        d._listener = {**original, 'socket_inode': 'foreign'}
    elif fault == 'storage':
        original = d._storage_identity
        d._storage_identity = (0, 0)
    elif fault == 'header':
        await d._client.hset(HEADER, 'namespace_id', 'foreign')
    elif fault == 'activation-receipt':
        path = next(p for p, _ in d._receipts if 'activation_response' in p.name)
        raw = path.read_bytes()
        (d.evidence / 'negative-original-activation-receipt.bin').write_bytes(raw)
        path.chmod(0o600); path.write_text('{"status":"ACTIVATED"}'); path.chmod(0o400)
    else:
        await d._client.execute_command('ACL', 'SETUSER', 'scenario', '+flushdb')
    before = await semantic_snapshot(d._client)
    try:
        with pytest.raises(Hold):
            await redeem(issuer, grant)
        assert not issuer._entry['used'] and issuer._lease is None
        assert await semantic_snapshot(d._client) == before
    finally:
        monkeypatch.undo()
        if fault == 'listener': d._listener = original
        if fault == 'storage': d._storage_identity = original
        # Original activation-receipt corruption remains archived and rejected.
        # Host proof, not the deliberately invalid chain, authorizes owned abort.


async def test_explicit_terminal_setup_exact_header_and_no_retained_repair(owned_domain):
    from hfa.config.keys import RedisKey
    from hfa_control.run_terminal_event_evidence import validate_index_contract
    d = owned_domain
    await d.activate_prepared(terminal_events=True)
    assert d.phase == Phase.HEADER_VERIFIED and d._scenario is None
    assert d._activation_attempts == 1
    key = RedisKey.run_terminal_event_index()
    shell = await d._client.hgetall(key)
    validate_index_contract(shell)
    assert len(shell) == 2 and await d._client.pttl(key) == -1
    assert await d._client.hgetall(HEADER) == d._header
    assert d._header['attestation_sha256'] == prepared.digest(d._attestation)
    issuer = PreparedDomainIssuer(d)
    grant = await issuer.issue(scenario='terminal', acceptance_id='d2a-terminal',
        consumer=consumer, timeout=20, terminal_events=True)
    lease = await issuer.redeem(grant, scenario='terminal', acceptance_id='d2a-terminal', consumer=consumer)
    assert await lease.client.pttl(key) == -1
    await issuer.finish(retain=True)
    assert d.phase == Phase.VERIFIED_RETAINED
    with pytest.raises(Hold):
        await d.activate_prepared(terminal_events=True)
    before = await semantic_snapshot(d._client)
    await d.verify_retained()
    assert await semantic_snapshot(d._client) == before
    await d.dispose()


async def test_missing_setup_does_not_release_grant(domain_issuer):
    d, issuer = domain_issuer
    with pytest.raises(Hold):
        await issuer.issue(scenario='terminal', acceptance_id='d2a', consumer=consumer,
                           timeout=20, terminal_events=True)
    assert d._scenario is None and issuer._entry is None and not d._prepared_issued


async def test_probe_grant_cannot_be_redeemed_as_an_in_process_lease(domain_issuer):
    d, issuer = domain_issuer
    grant = await issue(issuer, target=prepared._SOURCE)
    before = await semantic_snapshot(d._client)
    with pytest.raises(Hold):
        await issuer.redeem(grant, scenario='d2a', acceptance_id='d2a-proof', consumer=prepared._SOURCE)
    assert not issuer._entry['used'] and issuer._lease is None
    assert await semantic_snapshot(d._client) == before


async def test_unexpected_already_active_has_no_prepared_grant(owned_domain, monkeypatch):
    from hfa.authority import RedisCanonicalAuthorityStore
    async def unexpected(*args, **kwargs): return {'status': 'ALREADY_ACTIVE'}
    monkeypatch.setattr(RedisCanonicalAuthorityStore, 'activate_task_owner_namespace', unexpected)
    with pytest.raises(Hold):
        await owned_domain.activate_prepared()
    assert owned_domain._activation_attempts == 1 and owned_domain._scenario is None
    with pytest.raises(Hold):
        PreparedDomainIssuer(owned_domain)


async def test_byte_lease_real_canonical_admission_duplicate_and_restart_preserve_evidence(owned_domain, monkeypatch):
    from types import SimpleNamespace
    from tests.integration.test_acceptance_fresh_domain_lifecycle import admit
    from hfa_control.dag_lua import DagLua
    d = owned_domain
    await d.activate_prepared()
    issuer = PreparedDomainIssuer(d)
    grant = await issue(issuer)
    lease = await redeem(issuer, grant)
    _, dag, item = await admit(SimpleNamespace(client=lease.client))
    assert await d._client.hget(HEADER, 'owner_count') == '1'
    owners = await d._client.hgetall(OWNERS)
    before = await semantic_snapshot(d._client)
    from hfa.authority import RedisCanonicalAuthorityStore
    from hfa_control.service import ControlPlaneService
    from hfa_control.models import ControlPlaneConfig
    async def forbidden(*args, **kwargs): pytest.fail('restart attempted activation')
    monkeypatch.setattr(RedisCanonicalAuthorityStore, 'activate_task_owner_namespace', forbidden)
    first = ControlPlaneService(lease.client, ControlPlaneConfig(instance_id='d2a-first'))
    second = ControlPlaneService(lease.client, ControlPlaneConfig(instance_id='d2a-restart'))
    assert first is not second
    rebuilt = DagLua(lease.client, canonical_task_admit_binding=True,
                    require_root_task_binding=True, require_task_owner_binding=True)
    assert (await rebuilt.task_admit(item)).status == 'already_exists'
    assert await semantic_snapshot(d._client) == before
    await issuer.finish(retain=True)
    await d.verify_retained()
    assert d._activation_attempts == 1 and await d._client.hgetall(OWNERS) == owners
    assert await semantic_snapshot(d._client) == before
    await d.dispose()


@pytest.mark.parametrize('command', ['FLUSHDB', 'FLUSHALL', 'ACL USERS', 'CONFIG GET *'])
async def test_scenario_reset_and_admin_attempts_are_denied(domain_issuer, command):
    d, issuer = domain_issuer
    lease = await redeem(issuer, await issue(issuer))
    before = await semantic_snapshot(d._client)
    with pytest.raises(ResponseError):
        await lease.client.execute_command(*command.split())
    assert await semantic_snapshot(d._client) == before


async def test_provenance_tamper_is_detected_without_claiming_hostile_acl_immunity(owned_domain):
    d = owned_domain
    await d.activate_prepared()
    issuer = PreparedDomainIssuer(d)
    lease = await redeem(issuer, await issue(issuer))
    # Deliberate owned-domain negative: ACL is not a canonical provenance firewall.
    await lease.client.hset(HEADER, 'namespace_id', 'tampered')
    with pytest.raises(Hold):
        await issuer.finish(retain=True)
    with pytest.raises(Hold):
        await issuer.wait_closed()
    assert d.phase == Phase.DISPOSED and d._process.poll() is not None
    receipts = [json.loads(p.read_text()) for p, _ in d._receipts]
    assert not any(r['kind'] == 'VERIFIED_RETAINED' for r in receipts)
    assert any(r['kind'] == 'issuer_hold' for r in receipts)


async def test_live_deadline_revokes_raw_credential_and_disposes(domain_issuer):
    d, issuer = domain_issuer
    lease = await redeem(issuer, await issue(issuer, timeout=0.8))
    raw = lease.client
    await asyncio.wait_for(issuer.wait_closed(), 5)
    assert d.phase == Phase.DISPOSED and d._process.poll() is not None
    with pytest.raises(Hold):
        lease.client
    assert issuer._entry['used']


async def test_real_private_probe_child_handoff_and_exact_cleanup(domain_issuer):
    d, issuer = domain_issuer
    grant = await issue(issuer, target=prepared._SOURCE)
    assert await issuer.run_probe(grant, scenario='d2a', acceptance_id='d2a-proof') == {
        'status': 'PASS', 'probe_only': True, 'dump_bytes': True}
    assert issuer._child.returncode == 0 and d.phase == Phase.DISPOSED
    kinds = [json.loads(p.read_text())['kind'] for p, _ in d._receipts]
    assert kinds.count('probe_created') == kinds.count('probe_reaped') == 1
    assert kinds.count('grant_consumed') == kinds.count('scenario_revoked') == 1


@pytest.mark.parametrize('behavior', ['exception', 'timeout', 'eof', 'replay'])
async def test_probe_failure_timeout_eof_replay_are_reaped_and_disposed(domain_issuer, behavior):
    d, issuer = domain_issuer
    grant = await issue(issuer, target=prepared._SOURCE, timeout=3 if behavior == 'timeout' else 20)
    with pytest.raises(Hold):
        await issuer.run_probe(grant, scenario='d2a', acceptance_id='d2a-proof', behavior=behavior)
    assert issuer._child.poll() is not None and d.phase == Phase.DISPOSED
    assert not d._root.exists()
    if behavior in {'eof', 'replay'}:
        assert not issuer._entry['used']
    else:
        assert issuer._probe_ready  # The intended exception/timeout follows real corroboration.
    if behavior == 'timeout':
        receipts = [json.loads(p.read_text()) for p, _ in d._receipts]
        assert any(r['kind'] == 'owned_abort' and r['value']['reason'] == 'deadline' for r in receipts)


async def test_probe_cancellation_still_reaps_only_owned_children(domain_issuer):
    d, issuer = domain_issuer
    grant = await issue(issuer, target=prepared._SOURCE)
    task = asyncio.create_task(issuer.run_probe(grant, scenario='d2a', acceptance_id='d2a-proof', behavior='timeout'))
    async with asyncio.timeout(5):
        while not issuer._probe_ready:
            await asyncio.sleep(0.01)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert issuer._child.poll() is not None and d.phase == Phase.DISPOSED


async def test_probe_wrong_source_is_refused_before_credential_delivery(domain_issuer, tmp_path):
    d, issuer = domain_issuer
    foreign = tmp_path / 'foreign.py'
    foreign.write_text('raise RuntimeError("not audited probe")')
    grant = await issue(issuer, target=foreign)
    with pytest.raises(Hold):
        await issuer.run_probe(grant, scenario='d2a', acceptance_id='d2a-proof')
    assert issuer._child is None and not issuer._entry['used']
    assert d.phase == Phase.DISPOSED


async def test_foreign_redis_generation_during_abort_is_hold_without_signal(owned_domain, monkeypatch):
    d = owned_domain
    await d.activate_prepared()
    process, root = d._process, d._root
    original = d._host
    d._host = {**original, 'start_ticks': 'foreign'}
    with pytest.raises(Hold):
        await d.abort_owned(reason='negative_identity_test')
    assert process.poll() is None and root.exists() and d.phase == Phase.HOLD
    assert any('owned_abort_hold' in path.name for path, _ in d._receipts)
    d._host = original
    await d.abort_owned(reason='verified_identity_restored_for_owned_test_cleanup')
    assert d.phase == Phase.DISPOSED


async def test_secret_canaries_do_not_escape_protocol_repr_or_receipts(domain_issuer, capsys, caplog):
    d, issuer = domain_issuer
    grant = await issue(issuer, target=prepared._SOURCE)
    secrets = [d._owner_password, d._scenario_password]
    await issuer.run_probe(grant, scenario='d2a', acceptance_id='d2a-proof')
    captured = capsys.readouterr()
    text = captured.out + captured.err + caplog.text
    text += repr(issuer) + repr(grant) + repr(issuer._child.args)
    text += ''.join(p.read_text() for p in d.evidence.iterdir() if p.is_file())
    assert all(secret not in text for secret in secrets)
    assert '--redis-url' not in issuer._child.args


@pytest.mark.parametrize('fault', ['child-generation', 'source', 'copied-nonce',
                                   'expired-envelope', 'replaced-pipe', 'parent-eof',
                                   'credential-nonce'])
async def test_child_revalidates_private_handoff_before_writes(domain_issuer, monkeypatch, fault):
    d, issuer = domain_issuer
    grant = await issue(issuer, target=prepared._SOURCE)
    send = prepared._send
    async def damaged(fd, value, deadline):
        value = json.loads(json.dumps(value))
        if value['kind'] == 'challenge':
            if fault == 'child-generation': value['child']['start_ticks'] = 'foreign'
            if fault == 'source': value['consumer']['sha256'] = 'foreign'
            if fault == 'copied-nonce': value['nonce'] = 'copied'
            if fault == 'expired-envelope': value['deadline'] = time.monotonic() - 1
            if fault == 'replaced-pipe': value['channels']['read_inode'] = -1
            if fault == 'parent-eof':
                os.close(fd)
                issuer._fds.pop(fd)
                return
        if value['kind'] == 'credential' and fault == 'credential-nonce':
            value['nonce'] = 'replaced'
        await send(fd, value, deadline)
    monkeypatch.setattr(prepared, '_send', damaged)
    with pytest.raises(Hold):
        await issuer.run_probe(grant, scenario='d2a', acceptance_id='d2a-proof')
    assert issuer._child.poll() is not None and d.phase == Phase.DISPOSED
    if fault != 'credential-nonce':
        assert not issuer._entry['used']
    # No accepted authority admission or owner entry was created by the probe.
    assert all('TASK_ADMIT' not in p.read_text() for p, _ in d._receipts)


async def test_wrong_probe_generation_cleanup_holds_until_original_identity_is_verified(owned_domain):
    d = owned_domain
    await d.activate_prepared()
    issuer = PreparedDomainIssuer(d)
    grant = await issue(issuer, target=prepared._SOURCE, timeout=3)
    task = asyncio.create_task(issuer.run_probe(grant, scenario='d2a',
        acceptance_id='d2a-proof', behavior='timeout'))
    async with asyncio.timeout(5):
        while not issuer._probe_ready:
            await asyncio.sleep(0.01)
    original = issuer._child_identity
    issuer._child_identity = {**original, 'start_ticks': 'reused'}
    try:
        with pytest.raises(Hold):
            await issuer.abort()
        assert issuer._child.poll() is None  # Identity failure must not signal this PID.
        assert d.phase == Phase.DISPOSED  # Independently verified Redis still cleaned.
    finally:
        issuer._child_identity = original
        await issuer._reap_child()  # Explicit owned-test cleanup after removing the injected fault.
        with pytest.raises(Hold):
            await task
    assert issuer._child.poll() is not None


async def test_cleanup_error_is_not_suppressed_or_leaked(owned_domain, monkeypatch):
    d = owned_domain
    await d.activate_prepared()
    issuer = PreparedDomainIssuer(d)
    lease = await redeem(issuer, await issue(issuer))
    secret = d._scenario_password
    # Use a failing owner command to prove abort still stops the exact child and
    # reports cleanup failure, without serializing the exception message.
    async def failing_revoke(*args):
        raise RuntimeError(secret)
    monkeypatch.setattr(d._client, 'acl_deluser', failing_revoke)
    with pytest.raises(Hold) as error:
        await issuer.abort(reason=secret)
    assert secret not in repr(error.value)
    assert d.phase == Phase.DISPOSED and d._process.poll() is not None
    assert all(secret not in p.read_text() for p in d.evidence.iterdir() if p.is_file())


async def test_retained_finish_revokes_credential_without_disposing_child(owned_domain):
    d = owned_domain
    await d.activate_prepared()
    issuer = PreparedDomainIssuer(d)
    lease = await redeem(issuer, await issue(issuer))
    raw = lease.client
    assert await raw.ping()
    before = await semantic_snapshot(d._client)
    await issuer.finish(retain=True)
    assert d.phase == Phase.VERIFIED_RETAINED and d._process.poll() is None
    d.verify_ownership()
    with pytest.raises(AuthenticationError):
        await raw.ping()
    await raw.aclose()
    await d.verify_retained()
    assert await semantic_snapshot(d._client) == before
    assert d._activation_attempts == 1
    await d.dispose()


async def test_retained_terminal_loss_is_refused_without_preparation(owned_domain, monkeypatch):
    from hfa.config.keys import RedisKey
    import hfa_control.run_terminal_event_evidence as terminal
    d = owned_domain
    await d.activate_prepared(terminal_events=True)
    issuer = PreparedDomainIssuer(d)
    grant = await issuer.issue(scenario='terminal', acceptance_id='d2a', consumer=consumer,
                               timeout=20, terminal_events=True)
    await issuer.redeem(grant, scenario='terminal', acceptance_id='d2a', consumer=consumer)
    await issuer.finish(retain=True)
    key = RedisKey.run_terminal_event_index()
    await d._client.delete(key)  # Deliberate owned retained-domain evidence loss.
    async def forbidden(*args): pytest.fail('retained verification attempted preparation')
    monkeypatch.setattr(terminal, 'ensure_terminal_event_index', forbidden)
    before = await semantic_snapshot(d._client)
    with pytest.raises(Hold):
        await d.verify_retained()
    with pytest.raises(Hold):
        await d.activate_prepared(terminal_events=True)
    assert await semantic_snapshot(d._client) == before and await d._client.exists(key) == 0
    await d.abort_owned(reason='scenario_failure')


@pytest.mark.parametrize('fault', ['ttl', 'wrong-type', 'extra-field', 'unrelated-key'])
async def test_terminal_setup_readback_refuses_corruption(owned_domain, monkeypatch, fault):
    from hfa.config.keys import RedisKey
    import hfa_control.run_terminal_event_evidence as terminal
    original = terminal.ensure_terminal_event_index
    async def damaged(client):
        result = await original(client)
        key = RedisKey.run_terminal_event_index()
        if fault == 'ttl': await client.pexpire(key, 60000)
        if fault == 'wrong-type':
            await client.delete(key)
            await client.set(key, 'wrong-type')
        if fault == 'extra-field': await client.hset(key, 'unaccepted', 'evidence')
        if fault == 'unrelated-key': await client.set('concurrent-writer', 'unaccepted')
        return result
    monkeypatch.setattr(terminal, 'ensure_terminal_event_index', damaged)
    with pytest.raises(ResponseError if fault == 'wrong-type' else Hold):
        await owned_domain.activate_prepared(terminal_events=True)
    assert owned_domain.phase == Phase.HOLD and owned_domain._scenario is None
    assert owned_domain._activation_attempts == 1
    with pytest.raises(Hold):
        PreparedDomainIssuer(owned_domain)


async def test_setup_loss_before_grant_cannot_be_repaired_by_issuer(owned_domain):
    from hfa.config.keys import RedisKey
    d = owned_domain
    await d.activate_prepared(terminal_events=True)
    key = RedisKey.run_terminal_event_index()
    await d._client.delete(key)
    issuer = PreparedDomainIssuer(d)
    try:
        before = await semantic_snapshot(d._client)
        with pytest.raises(Hold):
            await issuer.issue(scenario='terminal', acceptance_id='d2a', consumer=consumer,
                               timeout=20, terminal_events=True)
        assert d._scenario is None and not d._prepared_issued
        assert await semantic_snapshot(d._client) == before
    finally:
        await issuer.abort()


async def test_expired_finish_cannot_win_race_to_retained_acceptance(owned_domain):
    d = owned_domain
    await d.activate_prepared()
    issuer = PreparedDomainIssuer(d)
    await redeem(issuer, await issue(issuer))
    # Exercise the independent completion-boundary check even if timer dispatch
    # was delayed; the separate live-deadline test exercises actual timer cleanup.
    issuer._timer.cancel()
    issuer._entry['deadline'] = time.monotonic() - 1
    with pytest.raises(Hold, match='expired'):
        await issuer.finish(retain=True)
    assert d.phase == Phase.DISPOSED
    receipts = [json.loads(p.read_text()) for p, _ in d._receipts]
    assert not any(r['kind'] == 'VERIFIED_RETAINED' for r in receipts)
    assert any(r['kind'] == 'owned_abort' and r['value']['reason'] == 'deadline' for r in receipts)


async def test_only_module_local_fixtures_are_used(request):
    for name in ('integration_redis_stack', 'strict_mode_env', 'flush_redis'):
        assert request._fixture_defs[name].func.__module__ == 'tests.acceptance_fresh_domain'
    assert 'redis_client' not in request.fixturenames


@pytest.mark.parametrize('fault', [
    'before-acl', 'after-acl-setuser', 'after-acl-readback',
    'during-grant-receipt', 'after-receipt-before-timer',
])
async def test_issue_release_failure_always_disposes_known_owned_child(
    owned_domain, monkeypatch, fault,
):
    """No grant/credential survives a partial _release under a trusted creator."""
    d = owned_domain
    await d.activate_prepared()
    issuer = PreparedDomainIssuer(d)
    canary = 'POST_ACL_FAILURE_CANARY_MUST_NOT_ESCAPE'

    if fault == 'before-acl':
        async def reject():
            raise RuntimeError('pre-ACL failure')
        monkeypatch.setattr(d, '_release', reject)
    elif fault == 'after-acl-setuser':
        original = d._client.execute_command
        async def fail_after_write(*args, **kwargs):
            result = await original(*args, **kwargs)
            if args[:2] == ('ACL', 'SETUSER'):
                # The ACL change is real; the caller sees a lost response.
                raise RuntimeError(canary)
            return result
        monkeypatch.setattr(d._client, 'execute_command', fail_after_write)
    elif fault == 'after-acl-readback':
        original = d._client.acl_getuser
        count = 0
        async def fail_second_read(*args, **kwargs):
            nonlocal count
            result = await original(*args, **kwargs)
            if args and args[0] == 'scenario':
                count += 1
                if count == 2:
                    raise RuntimeError(canary)
            return result
        monkeypatch.setattr(d._client, 'acl_getuser', fail_second_read)
    elif fault == 'during-grant-receipt':
        original = d._record
        def fail_receipt(kind, *args, **kwargs):
            if kind == 'grant_issued':
                raise RuntimeError(canary)
            return original(kind, *args, **kwargs)
        monkeypatch.setattr(d, '_record', fail_receipt)
    else:
        original = asyncio.create_task
        def fail_expire_task(coroutine, *args, **kwargs):
            if getattr(getattr(coroutine, 'cr_code', None), 'co_name', '') == '_expire':
                coroutine.close()
                raise RuntimeError(canary)
            return original(coroutine, *args, **kwargs)
        monkeypatch.setattr(prepared.asyncio, 'create_task', fail_expire_task)

    with pytest.raises(Hold, match='grant release failed') as error:
        await issue(issuer)
    assert canary not in str(error.value)
    assert issuer._entry is not None and not issuer._entry['used']
    assert prepared._ISSUERS.get(issuer._key) is None
    assert issuer._closed.is_set() and d.phase == Phase.DISPOSED
    assert d._process.poll() is not None and not d._root.exists()
    assert not any('VERIFIED_RETAINED' in path.name for path, _ in d._receipts)
    assert all(canary not in path.read_text() and d._scenario_password not in path.read_text()
               for path in d.evidence.iterdir() if path.is_file())


async def test_cancellation_during_post_acl_release_waits_for_verified_abort(owned_domain, monkeypatch):
    d = owned_domain
    await d.activate_prepared()
    issuer = PreparedDomainIssuer(d)
    release_done, hold_release = asyncio.Event(), asyncio.Event()
    original = d._release

    async def blocked_after_acl():
        await original()
        release_done.set()
        await hold_release.wait()

    monkeypatch.setattr(d, '_release', blocked_after_acl)
    task = asyncio.create_task(issue(issuer))
    await asyncio.wait_for(release_done.wait(), 5)
    assert d.phase == Phase.DOMAIN_RELEASED
    task.cancel()
    hold_release.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert d.phase == Phase.DISPOSED and d._process.poll() is not None
    assert not d._root.exists() and issuer._closed.is_set()
    assert prepared._ISSUERS.get(issuer._key) is None
    assert not issuer._entry['used']


async def test_partial_grant_release_with_lost_child_identity_holds_without_signal(
    owned_domain, monkeypatch,
):
    d = owned_domain
    await d.activate_prepared()
    issuer = PreparedDomainIssuer(d)
    original_release, original_identity = d._release, d.verify_ownership

    async def foreign_after_release():
        await original_release()
        monkeypatch.setattr(d, 'verify_ownership',
                            lambda: (_ for _ in ()).throw(Hold('foreign generation')))
        raise RuntimeError('post-ACL response lost')

    monkeypatch.setattr(d, '_release', foreign_after_release)
    try:
        with pytest.raises(Hold, match='cleanup HOLD'):
            await issue(issuer)
        assert issuer._closed.is_set()
        assert issuer._error is not None
        assert d.phase == Phase.HOLD and d._process.poll() is None
        assert d._root.exists()
        assert prepared._ISSUERS.get(issuer._key) is None
    finally:
        monkeypatch.setattr(d, 'verify_ownership', original_identity)
        if d.phase != Phase.DISPOSED:
            await d.abort_owned(reason='validation_failure')
    assert d.phase == Phase.DISPOSED


@pytest.mark.parametrize('mode', ['abort', 'finish'])
async def test_cancelled_public_close_still_completes_owned_cleanup(
    owned_domain, monkeypatch, mode,
):
    d = owned_domain
    await d.activate_prepared()
    issuer = PreparedDomainIssuer(d)
    await issue(issuer)
    close_entered, continue_close = asyncio.Event(), asyncio.Event()
    original = issuer._reap_child

    async def gated_reap():
        close_entered.set()
        await continue_close.wait()
        await original()

    monkeypatch.setattr(issuer, '_reap_child', gated_reap)
    closer = issuer.abort if mode == 'abort' else issuer.finish
    task = asyncio.create_task(closer())
    await asyncio.wait_for(close_entered.wait(), 5)
    task.cancel()
    continue_close.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert issuer._closed.is_set()
    assert prepared._ISSUERS.get(issuer._key) is None
    assert d.phase == Phase.DISPOSED and d._process.poll() is not None
    assert not d._root.exists()


@pytest.mark.parametrize('mode', ['abort', 'finish'])
async def test_repeated_caller_cancel_cannot_cancel_public_owned_cleanup(
    owned_domain, monkeypatch, mode,
):
    d = owned_domain
    await d.activate_prepared()
    issuer = PreparedDomainIssuer(d)
    await issue(issuer)
    entered, release = asyncio.Event(), asyncio.Event()
    original = issuer._reap_child

    async def gated_reap():
        entered.set()
        await release.wait()
        await original()

    monkeypatch.setattr(issuer, '_reap_child', gated_reap)
    task = asyncio.create_task(issuer.abort() if mode == 'abort' else issuer.finish())
    try:
        await asyncio.wait_for(entered.wait(), 5)
        task.cancel()
        await asyncio.sleep(0)
        task.cancel()
        await asyncio.sleep(0)
        assert not issuer._closed.is_set()  # Gated cleanup still runs.
    finally:
        release.set()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(task, 5)
    assert issuer._closed.is_set() and issuer._error is None
    assert d.phase == Phase.DISPOSED and d._process.poll() is not None
    assert not d._root.exists() and prepared._ISSUERS.get(issuer._key) is None


async def test_repeated_cancel_cannot_interrupt_post_acl_release_abort(
    owned_domain, monkeypatch,
):
    d = owned_domain
    await d.activate_prepared()
    issuer = PreparedDomainIssuer(d)
    acl_released, blocked_release = asyncio.Event(), asyncio.Event()
    entered_cleanup, allow_cleanup = asyncio.Event(), asyncio.Event()
    original_release, original_reap = d._release, issuer._reap_child

    async def blocked_after_acl():
        await original_release()
        acl_released.set()
        await blocked_release.wait()

    async def gated_reap():
        entered_cleanup.set()
        await allow_cleanup.wait()
        await original_reap()

    monkeypatch.setattr(d, '_release', blocked_after_acl)
    monkeypatch.setattr(issuer, '_reap_child', gated_reap)
    task = asyncio.create_task(issue(issuer))
    try:
        await asyncio.wait_for(acl_released.wait(), 5)
        task.cancel()  # Cancels the release-stage operation.
        await asyncio.wait_for(entered_cleanup.wait(), 5)
        task.cancel()  # Attempts to cancel the already-running owned abort.
        await asyncio.sleep(0)
        assert not issuer._closed.is_set()
    finally:
        blocked_release.set()
        allow_cleanup.set()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(task, 5)
    assert issuer._closed.is_set() and issuer._error is None
    assert d.phase == Phase.DISPOSED and d._process.poll() is not None
    assert not d._root.exists() and prepared._ISSUERS.get(issuer._key) is None


async def test_repeated_cancel_cleanup_hold_takes_precedence_over_cancellation(
    owned_domain, monkeypatch,
):
    d = owned_domain
    await d.activate_prepared()
    issuer = PreparedDomainIssuer(d)
    await issue(issuer)
    entered, release = asyncio.Event(), asyncio.Event()

    async def failing_reap():
        entered.set()
        await release.wait()
        raise RuntimeError('deliberate cleanup oracle failure')

    monkeypatch.setattr(issuer, '_reap_child', failing_reap)
    task = asyncio.create_task(issuer.finish())
    try:
        await asyncio.wait_for(entered.wait(), 5)
        task.cancel()
        await asyncio.sleep(0)
        task.cancel()
        await asyncio.sleep(0)
    finally:
        release.set()
    with pytest.raises(Hold, match='cleanup/validation failed'):
        await asyncio.wait_for(task, 5)
    assert issuer._closed.is_set() and issuer._error is not None
    assert d.phase == Phase.DISPOSED and d._process.poll() is not None
    assert not d._root.exists()
    with pytest.raises(Hold):
        await issuer.wait_closed()
