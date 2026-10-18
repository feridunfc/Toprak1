"""Real Redis P1 boundary; never inherit the destructive shared fixture."""
import asyncio
import json
import os
from pathlib import Path
import signal
import socket

import pytest
import pytest_asyncio
import redis.asyncio as redis

from scripts import acceptance_ci_legacy_environment as legacy
from scripts.acceptance_fresh_domain import Hold, file_digest
from tests.acceptance_fresh_domain import (  # noqa: F401
    integration_redis_stack, strict_mode_env, flush_redis,
)

pytestmark = [pytest.mark.asyncio, pytest.mark.integration]


@pytest_asyncio.fixture
async def legacy_factory(tmp_path):
    domains = []
    async def create(*, port=0):
        d = legacy.OwnedLegacyEnvironment(Path(os.environ['HFA_ACCEPTANCE_REDIS_BINARY']),
            expected_sha256=os.environ['HFA_CI_LEGACY_REVIEWED_SHA256'],
            evidence_parent=Path(os.environ.get('HFA_CI_LEGACY_EVIDENCE_DIR', str(tmp_path))), port=port)
        domains.append(d)
        await d.create()
        return d
    yield create
    for d in domains:
        if not d._closed:
            await d.close()
        assert d.process.poll() is not None and not d.root.exists()


def probe(tmp_path, body):
    path = tmp_path/'test_owned_legacy_probe.py'
    path.write_text('import asyncio, os, pytest\nimport redis.asyncio as redis\n'
        'from scripts.acceptance_ci_legacy_environment import legacy_endpoint\n'
        'from scripts.acceptance_fresh_domain import Hold\n' + body)
    return [str(path), '-q', '--tb=short', '-p', 'no:cacheprovider']


async def test_real_creation_digest_acl_and_disposal(legacy_factory):
    d = await legacy_factory()
    assert file_digest(d.binary) == d.expected_sha256
    assert d.verify() == d.listener
    assert await d.client.pttl(legacy.HEADER) == -2
    checks = json.loads(d._records[-1].read_text())['value']['acl']
    assert checks == {'flushdb': 'OK', 'flushall': 'DENIED', 'acl': 'DENIED', 'info': 'OK'}
    assert set(await d.client.acl_users()) == {'default', 'legacy_creator', 'legacy_fixture'}
    await d.close()
    assert d.process.poll() is not None and not d.root.exists() and not d._credential_live


async def test_private_handoff_supports_module_class_and_constructor_clients(legacy_factory, tmp_path):
    d = await legacy_factory()
    args = probe(tmp_path, '''
@pytest.mark.asyncio
async def test_clients():
    from urllib.parse import urlsplit
    u=urlsplit(legacy_endpoint())
    clients=[redis.from_url(legacy_endpoint(),decode_responses=True),
             redis.Redis.from_url(legacy_endpoint(),decode_responses=False),
             redis.Redis(host=u.hostname,port=u.port,db=0)]
    try:
        for c in clients:
            await c.set('legacy-test','yes')
            assert await c.flushdb()
            assert await c.dbsize()==0
    finally:
        for c in clients: await c.aclose()
''')
    result = await d.run_pytest(args, cwd=tmp_path)
    assert result.returncode == 0, result.stdout + result.stderr
    observation = json.loads((d.evidence/'pytest-observation.json').read_text())
    assert len([r for r in observation['events'] if r['kind']=='FLUSHDB_COMPLETED']) == 3
    assert all(r['outcome']=='passed' for r in observation['phases'])
    assert observation['nodes'] == ['test_owned_legacy_probe.py::test_clients']


@pytest.mark.parametrize('credential', ['wrong', 'missing-channel', 'prepared'])
async def test_wrong_missing_or_prepared_credential_refused(legacy_factory, tmp_path, credential):
    d = await legacy_factory()
    if credential == 'missing-channel':
        env = os.environ.copy()
        env[legacy.READ_FD] = '9999'
        with pytest.raises(Hold, match='FD flags'):
            await d.run_pytest([], environment=env)
        assert d.child is None and await d.client.dbsize()==0
        return
    args = probe(tmp_path, f'''
def test_foreign_credential():
    with pytest.raises(Hold, match='credential'):
        redis.from_url(legacy_endpoint(),username={'scenario' if credential=='prepared' else 'legacy_fixture'!r},password='foreign-secret')
''')
    result = await d.run_pytest(args, cwd=tmp_path)
    assert result.returncode == 0, result.stdout + result.stderr
    assert not any(r['kind']=='FLUSHDB_COMPLETED' for r in json.loads((d.evidence/'pytest-observation.json').read_text())['events'])


@pytest.mark.parametrize('pipeline', [False, True], ids=['direct', 'pipeline'])
async def test_wrong_domain_flush_refuses_without_connecting(legacy_factory, tmp_path, pipeline):
    d = await legacy_factory()
    sentinel = socket.socket()
    sentinel.bind(('127.0.0.1', 0))
    sentinel.listen()
    port = sentinel.getsockname()[1]
    try:
        args = probe(tmp_path, f'''
@pytest.mark.asyncio
async def test_no_foreign_reset():
    c=redis.Redis(host='127.0.0.1',port={port},db=0)
    try:
        with pytest.raises(Hold,match='unverified'):
            {'await c.pipeline().flushdb().execute()' if pipeline else 'await c.flushdb()'}
    finally: await c.aclose()
''')
        result = await d.run_pytest(args, cwd=tmp_path)
        assert result.returncode == 0, result.stdout + result.stderr
        sentinel.settimeout(.05)
        with pytest.raises(TimeoutError):
            sentinel.accept()
    finally:
        sentinel.close()


async def test_pipeline_legacy_reset_and_owner_evidence_boundary(legacy_factory, tmp_path):
    d = await legacy_factory()
    args = probe(tmp_path, '''
@pytest.mark.asyncio
async def test_reset():
    from scripts.acceptance_fresh_domain import HEADER
    c=redis.from_url(legacy_endpoint())
    try:
        assert await c.pipeline().set('legacy','1').flushdb().execute()==[True,True]
        await c.hset(HEADER,mapping={'negative-test-marker':'never-repair'})
        before=await c.dump(HEADER)
        with pytest.raises(Hold,match='owner evidence'): await c.flushdb()
        assert await c.dump(HEADER)==before
    finally: await c.aclose()
''')
    result = await d.run_pytest(args, cwd=tmp_path)
    assert result.returncode == 0, result.stdout + result.stderr


@pytest.mark.parametrize('fault', ['pid', 'listener', 'storage'])
async def test_foreign_identity_cleanup_never_signals_or_deletes(legacy_factory, monkeypatch, fault):
    d = await legacy_factory()
    with monkeypatch.context() as m:
        if fault == 'pid':
            real = legacy.process_identity
            m.setattr(legacy, 'process_identity', lambda pid: {**real(pid), 'start_ticks':'foreign'})
        elif fault == 'listener':
            real = legacy.listener_evidence
            m.setattr(legacy, 'listener_evidence', lambda p,n: {**real(p,n),'socket_inode':'foreign'})
        else:
            real = legacy._identity
            m.setattr(legacy, '_identity', lambda p: (0,0) if p==d.root else real(p))
        def forbidden(*a, **k):
            pytest.fail('unverified resource signaled/reset/deleted')
        m.setattr(legacy.signal, 'pidfd_send_signal', forbidden)
        m.setattr(legacy.shutil, 'rmtree', forbidden)
        with pytest.raises(Hold, match='HOLD'):
            await d.close()
        assert d.process.poll() is None and d.root.exists()
    await d.close()


async def test_occupied_endpoint_is_bind_only_no_adoption(tmp_path, monkeypatch):
    listener = socket.socket()
    listener.bind(('127.0.0.1',0))
    listener.listen()
    d = legacy.OwnedLegacyEnvironment(Path(os.environ['HFA_ACCEPTANCE_REDIS_BINARY']),
        expected_sha256=os.environ['HFA_CI_LEGACY_REVIEWED_SHA256'], evidence_parent=tmp_path,
        port=listener.getsockname()[1])
    try:
        def forbidden(*a, **k): pytest.fail('occupied endpoint connected/signaled')
        with monkeypatch.context() as m:
            m.setattr(legacy.redis, 'Redis', forbidden)
            m.setattr(legacy.signal, 'pidfd_send_signal', forbidden)
            with pytest.raises(Hold, match='occupied'):
                await d.create()
        assert d.process is None and d.root is None
        listener.settimeout(.05)
        with pytest.raises(TimeoutError): listener.accept()
    finally:
        listener.close()


@pytest.mark.parametrize('behavior,code', [('test-failure',1), ('collection-error',2), ('usage-error',4)])
async def test_original_child_failure_and_order_exit_are_preserved(legacy_factory, tmp_path, behavior, code):
    d = await legacy_factory()
    body = '''
def test_first(): assert False, 'original-negative-failure'
def test_second(): assert True
'''
    if behavior == 'collection-error': body = 'raise RuntimeError("original-collection-error")\n'
    args = probe(tmp_path, body)
    if behavior == 'usage-error': args += ['--p1-deliberately-invalid-option']
    result = await d.run_pytest(args, cwd=tmp_path)
    assert result.returncode == code
    created=next(json.loads(p.read_text())['value'] for p in d._records
                 if json.loads(p.read_text())['kind']=='PYTEST_CREATED')
    assert created['arguments']==args
    if behavior == 'test-failure':
        assert 'original-negative-failure' in result.stdout
        obs=json.loads((d.evidence/'pytest-observation.json').read_text())
        calls=[r for r in obs['phases'] if r['when']=='call']
        assert [r['node'].split('::')[-1] for r in calls]==['test_first','test_second']
        assert [r['outcome'] for r in calls]==['failed','passed']
    elif behavior == 'collection-error': assert 'original-collection-error' in result.stdout
    else:
        assert 'unrecognized arguments' in result.stderr
        assert not any(json.loads(p.read_text())['kind']=='CREDENTIAL_RELEASED' for p in d._records)


async def test_timeout_reaps_only_original_pytest_and_redis(legacy_factory, tmp_path):
    d = await legacy_factory()
    args = probe(tmp_path, 'def test_wait():\n    import time\n    time.sleep(30)\n')
    with pytest.raises(TimeoutError):
        await d.run_pytest(args, cwd=tmp_path, timeout=2)
    assert d.child.poll() is not None and d.process.poll() is not None and not d.root.exists()


async def test_repeated_cancellation_finishes_real_known_owned_cleanup(legacy_factory, tmp_path, monkeypatch):
    d = await legacy_factory()
    args = probe(tmp_path, 'def test_wait():\n    import time\n    time.sleep(30)\n')
    started, release = asyncio.Event(), asyncio.Event()
    original = d._cleanup_body
    async def delayed():
        started.set()
        await release.wait()
        await original()
    monkeypatch.setattr(d, '_cleanup_body', delayed)
    task = asyncio.create_task(d.run_pytest(args, cwd=tmp_path))
    while d.child is None:
        await asyncio.sleep(.01)
    task.cancel()
    await started.wait()
    task.cancel()
    await asyncio.sleep(0)
    assert not task.done() and d.process.poll() is None
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert d.child.poll() is not None and d.process.poll() is not None and not d.root.exists()


async def test_real_cleanup_failure_has_hold_precedence_and_no_success_receipt(legacy_factory, monkeypatch):
    d = await legacy_factory()
    started, release = asyncio.Event(), asyncio.Event()
    original = d.client.acl_deluser
    async def fail(*a, **k):
        started.set()
        await release.wait()
        raise RuntimeError('injected-acl-revocation-failure')
    monkeypatch.setattr(d.client, 'acl_deluser', fail)
    task = asyncio.create_task(d.close())
    await started.wait()
    task.cancel()
    await asyncio.sleep(0)
    task.cancel()
    release.set()
    with pytest.raises(Hold, match='HOLD'):
        await task
    assert d._closed and d.process.poll() is not None and not d.root.exists()
    with pytest.raises(Hold, match='HOLD'):
        await d.close()
    assert json.loads(d._records[-1].read_text())['kind']=='CLEANUP_HOLD'
    monkeypatch.setattr(d.client, 'acl_deluser', original)


async def test_real_credentials_absent_from_result_junit_and_receipts(legacy_factory, tmp_path):
    d = await legacy_factory()
    args = probe(tmp_path, '''
def test_credential_canary():
    from scripts.acceptance_ci_legacy_environment import _ADAPTER
    secret=_ADAPTER.credential['password']
    print(secret)
    assert False, secret
''') + ['--junitxml='+str(tmp_path/'canary.xml')]
    result = await d.run_pytest(args, cwd=tmp_path)
    assert result.returncode == 1
    public=result.stdout+result.stderr+repr(result.args)+(tmp_path/'canary.xml').read_text()
    public+=''.join(p.read_text() for p in d.evidence.glob('*.json'))
    for secret in (d._creator_password,d._legacy_password): assert secret not in public
    assert '<redacted>' in public


@pytest.mark.parametrize('fault', ['missing-password', 'wrong-password', 'prepared-user', 'replayed-nonce'])
async def test_corrupted_private_credential_frame_cannot_authorize(legacy_factory, tmp_path, monkeypatch, fault):
    d = await legacy_factory()
    args = probe(tmp_path, 'def test_never_authorized():\n    assert False, "must not execute"\n')
    original = legacy._send
    async def corrupt(fd, frame, deadline):
        if frame.get('kind') == 'LEGACY_CREDENTIAL':
            frame = dict(frame)
            if fault == 'missing-password': frame.pop('password')
            elif fault == 'wrong-password': frame['password']='0'*64
            elif fault == 'prepared-user': frame['username']='scenario'
            else: frame['nonce']='foreign'
        await original(fd, frame, deadline)
    monkeypatch.setattr(legacy, '_send', corrupt)
    with pytest.raises(Hold):
        await d.run_pytest(args, cwd=tmp_path)
    assert d.process.poll() is not None and d.child.poll() is not None and not d.root.exists()
    assert not any(json.loads(p.read_text())['kind']=='CREDENTIAL_RELEASED' for p in d._records)


async def test_foreign_pytest_generation_holds_before_any_cleanup_effect(legacy_factory, tmp_path, monkeypatch):
    d = await legacy_factory()
    args = probe(tmp_path, 'def test_never_authorized(): assert False\n')
    with monkeypatch.context() as m:
        identity = legacy.process_identity
        calls = []
        def changed(pid):
            observed = identity(pid)
            if d.child is not None and pid==d.child.pid:
                calls.append(pid)
                if len(calls)>1: return {**observed, 'start_ticks':'foreign'}
            return observed
        m.setattr(legacy, 'process_identity', changed)
        def forbidden(*a, **k): pytest.fail('unverified cleanup effect')
        m.setattr(legacy.signal, 'pidfd_send_signal', forbidden)
        m.setattr(legacy.shutil, 'rmtree', forbidden)
        with pytest.raises(Hold): await d.run_pytest(args, cwd=tmp_path)
        assert d.process.poll() is None and d.root.exists()
    # Original registration remains intact; restore the independent observer.
    await d.close()


async def test_genuine_prepared_credential_is_rejected_at_legacy_boundary(legacy_factory, tmp_path):
    d = await legacy_factory()
    args = probe(tmp_path, '''
@pytest.mark.asyncio
async def test_separate_prepared_boundary():
    from pathlib import Path
    import sys
    from scripts.acceptance_ci_legacy_environment import ROOT
    sys.path.insert(0,str(ROOT/'hfa-core/src'))
    from scripts.acceptance_fresh_domain import OwnedFreshDomain
    p=OwnedFreshDomain(Path(os.environ['HFA_ACCEPTANCE_REDIS_BINARY']),Path(os.environ['HFA_ACCEPTANCE_EVIDENCE_DIR']))
    try:
        await p.create()
        await p.activate_fresh()
        assert p._port != int(legacy_endpoint().split(':')[-1].split('/')[0])
        with pytest.raises(Hold,match='credential'):
            redis.from_url(legacy_endpoint(),username='scenario',password=p._scenario_password)
        assert await p.client.ping()
        from scripts.acceptance_fresh_domain import HEADER
        assert await p.client.pttl(HEADER)==-1
    finally: await p.dispose()
''')
    result = await d.run_pytest(args, cwd=tmp_path)
    assert result.returncode == 0, result.stdout + result.stderr


async def test_changed_creation_receipt_holds_without_reset_signal_or_delete(legacy_factory, monkeypatch):
    d = await legacy_factory()
    path = d._records[1]
    original = path.read_bytes()
    changed = json.loads(original)
    changed['value']['host']['start_ticks']='foreign'
    corrupted=(json.dumps(changed,sort_keys=True)+'\n').encode()
    (d.evidence/'negative-receipt-input.json').write_bytes(corrupted)
    path.write_bytes(corrupted)
    try:
        with monkeypatch.context() as m:
            def forbidden(*a, **k): pytest.fail('corrupt receipt authorized a cleanup effect')
            m.setattr(legacy.signal,'pidfd_send_signal',forbidden)
            m.setattr(legacy.shutil,'rmtree',forbidden)
            with pytest.raises(Hold,match='receipt'): d.verify()
            with pytest.raises(Hold,match='HOLD'): await d.close()
            assert d.process.poll() is None and d.root.exists()
    finally:
        # Restore the exact independently retained original bytes, never rebuild
        # provenance from current Redis state or accept a replacement receipt.
        path.write_bytes(original)
    await d.close()
