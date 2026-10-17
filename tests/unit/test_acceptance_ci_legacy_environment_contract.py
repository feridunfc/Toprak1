"""Pure contract checks; synthetic executables below are not Redis evidence."""
import asyncio
import hashlib
from pathlib import Path

import pytest

from scripts import acceptance_ci_legacy_environment as legacy
from scripts.acceptance_fresh_domain import Hold


@pytest.fixture
def reviewed_stub(tmp_path):
    binary = tmp_path/'version-only-test-stub'
    binary.write_text('#!/bin/sh\necho "Redis server v=7.4.10 unit-test-only"\n')
    binary.chmod(0o700)
    return binary, hashlib.sha256(binary.read_bytes()).hexdigest()


def test_reviewed_digest_is_required_before_executable_invocation(reviewed_stub, monkeypatch):
    binary, digest = reviewed_stub
    def forbidden(*a, **k):
        pytest.fail('unreviewed bytes were executed')
    monkeypatch.setattr(legacy.subprocess, 'run', forbidden)
    with pytest.raises(Hold, match='digest mismatch'):
        legacy.reviewed_binary(binary, '0'*64)


@pytest.mark.parametrize('digest', [None, '', '0'*63, 'G'*64, '0'*65])
def test_absent_or_invalid_reviewed_digest_rejected(reviewed_stub, digest):
    with pytest.raises(Hold):
        legacy.reviewed_binary(reviewed_stub[0], digest)


@pytest.mark.parametrize('variant', ['relative', 'missing', 'directory', 'symlink', 'not-executable'])
def test_binary_path_contract(reviewed_stub, tmp_path, variant):
    binary, digest = reviewed_stub
    if variant == 'relative':
        binary = Path('redis-server')
    elif variant == 'missing':
        binary = tmp_path/'absent'
    elif variant == 'directory':
        binary = tmp_path
    elif variant == 'symlink':
        link = tmp_path/'link'
        link.symlink_to(binary)
        binary = link
    else:
        binary.chmod(0o600)
    with pytest.raises(Hold):
        legacy.reviewed_binary(binary, digest)


def test_wrong_version_even_with_matching_reviewed_bytes_fails(reviewed_stub):
    binary, _ = reviewed_stub
    binary.write_text('#!/bin/sh\necho "Redis server v=7.2.0 unit-test-only"\n')
    with pytest.raises(Hold, match='7.4.10'):
        legacy.reviewed_binary(binary, hashlib.sha256(binary.read_bytes()).hexdigest())


def test_non_linux_host_rejected_before_execution(reviewed_stub, monkeypatch):
    monkeypatch.setattr(legacy.sys, 'platform', 'win32')
    with pytest.raises(Hold, match='Linux'):
        legacy.reviewed_binary(*reviewed_stub)


def test_no_binary_substitution_or_digest_inference(reviewed_stub):
    binary, digest = reviewed_stub
    result = legacy.reviewed_binary(binary, digest)
    assert result['path'] == str(binary)
    assert result['reviewed_sha256'] == result['sha256'] == digest


@pytest.mark.parametrize('port', [-1, 65536, True, '6389'])
def test_invalid_port_does_not_create_lifecycle(tmp_path, port):
    with pytest.raises(Hold):
        legacy.OwnedLegacyEnvironment(tmp_path/'redis', expected_sha256='0'*64,
                                     evidence_parent=tmp_path, port=port)


def test_no_live_handoff_can_be_invented_with_a_url_or_environment(monkeypatch):
    monkeypatch.setenv(legacy.READ_FD, '9999')
    with pytest.raises(Hold, match='live inherited'):
        legacy.legacy_endpoint()


def test_receipts_and_repr_refuse_credentials(tmp_path):
    d = legacy.OwnedLegacyEnvironment(tmp_path/'redis', expected_sha256='0'*64,
                                    evidence_parent=tmp_path)
    for secret in (d._creator_password, d._legacy_password):
        assert secret not in repr(d)
        with pytest.raises(Hold, match='credential forbidden'):
            d._record('CANARY', {'value': secret})
    assert not d._records and d.root is None


@pytest.mark.asyncio
async def test_repeated_cancellation_waits_for_cleanup_and_then_preserves_cancel(tmp_path, monkeypatch):
    d = legacy.OwnedLegacyEnvironment(tmp_path/'redis', expected_sha256='0'*64,
                                    evidence_parent=tmp_path)
    started, release = asyncio.Event(), asyncio.Event()
    completed = []
    async def cleanup():
        started.set()
        await release.wait()
        completed.append(True)
    monkeypatch.setattr(d, '_cleanup', cleanup)
    task = asyncio.create_task(d.close())
    await started.wait()
    task.cancel()
    await asyncio.sleep(0)
    task.cancel()
    await asyncio.sleep(0)
    assert not task.done() and not completed
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert completed == [True]


@pytest.mark.asyncio
async def test_cleanup_hold_precedes_repeated_cancellation(tmp_path, monkeypatch):
    d = legacy.OwnedLegacyEnvironment(tmp_path/'redis', expected_sha256='0'*64,
                                    evidence_parent=tmp_path)
    started, release = asyncio.Event(), asyncio.Event()
    async def cleanup():
        started.set()
        await release.wait()
        raise Hold('real-cleanup-failure')
    monkeypatch.setattr(d, '_cleanup', cleanup)
    task = asyncio.create_task(d.close())
    await started.wait()
    task.cancel()
    await asyncio.sleep(0)
    task.cancel()
    release.set()
    with pytest.raises(Hold, match='real-cleanup-failure'):
        await task


@pytest.mark.asyncio
@pytest.mark.parametrize('arguments,timeout', [([1], 1), (['ok'], 0), (['ok'], float('inf'))])
async def test_invalid_execution_arguments_refuse_before_resource_access(tmp_path, arguments, timeout):
    d = legacy.OwnedLegacyEnvironment(tmp_path/'redis', expected_sha256='0'*64,
                                    evidence_parent=tmp_path)
    with pytest.raises(Hold, match='argument'):
        await d.run_pytest(arguments, timeout=timeout)
    assert d.child is d.process is None
