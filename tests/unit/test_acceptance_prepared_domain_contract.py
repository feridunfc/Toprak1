"""Prepared interface contracts; no external Redis or product composition."""
import argparse
import asyncio
import copy
import os
from pathlib import Path
import time

import pytest

from scripts.acceptance_fresh_domain import Hold, OwnedFreshDomain, Phase
from scripts.acceptance_prepared_domain import (
    PreparedDomainIssuer, PreparedGrant, ScenarioLease, _consumer, _pipe,
    _probe_child, _receive, _send,
)


def test_opaque_grant_has_no_caller_constructor():
    with pytest.raises(Hold):
        PreparedGrant()
    forged = object.__new__(PreparedGrant)
    assert repr(forged) == '<PreparedGrant opaque>'
    assert not hasattr(forged, '__dict__')


def test_copying_a_grant_cannot_copy_registry_authority():
    grant = object.__new__(PreparedGrant)
    for copier in (copy.copy, copy.deepcopy):
        with pytest.raises(Hold):
            copier(grant)


@pytest.mark.parametrize('phase', [None, *Phase])
def test_unprepared_state_or_values_never_import_creator_authority(tmp_path, phase):
    d = OwnedFreshDomain(Path('/bin/true'), tmp_path)
    d.phase = phase
    with pytest.raises(Hold):
        PreparedDomainIssuer(d)
    assert d._process is None and d._activation_attempts == 0


@pytest.mark.parametrize('foreign', [None, {}, {'owned': True}, 'redis://127.0.0.1:6389/0'])
def test_json_url_or_missing_lifecycle_is_not_ownership(foreign):
    with pytest.raises(Hold):
        PreparedDomainIssuer(foreign)


@pytest.mark.parametrize('consumer', [None, {}, 'caller.py', object()])
def test_consumer_values_are_not_exact_source_identity(consumer):
    with pytest.raises(Hold):
        _consumer(consumer)


def test_consumer_identity_binds_function_object_code_and_source():
    identity = _consumer(test_consumer_identity_binds_function_object_code_and_source)
    assert identity['object_id'] == id(test_consumer_identity_binds_function_object_code_and_source)
    assert identity['code_id'] == id(test_consumer_identity_binds_function_object_code_and_source.__code__)
    assert identity['source'] == str(Path(__file__).resolve())
    assert len(identity['sha256']) == 64


@pytest.mark.parametrize('fd', [-1, 0, 1, 2, '4', None])
def test_missing_or_invalid_fd_is_not_private_channel(fd):
    with pytest.raises(Hold):
        _pipe(fd, write=False)


def test_regular_file_and_closed_fd_are_not_private_channel(tmp_path):
    with (tmp_path / 'caller.json').open('w+') as f:
        with pytest.raises(Hold):
            _pipe(f.fileno(), write=False)
    read, write = os.pipe()
    os.close(read)
    os.close(write)
    with pytest.raises(Hold):
        _pipe(read, write=False)


@pytest.mark.asyncio
async def test_private_frame_roundtrip_is_directional():
    read, write = os.pipe()
    try:
        with pytest.raises(Hold):
            _pipe(read, write=True)
        await _send(write, {'challenge': 'one-use'}, time.monotonic() + 1)
        assert await _receive(read, time.monotonic() + 1) == {'challenge': 'one-use'}
    finally:
        os.close(read)
        os.close(write)


@pytest.mark.parametrize('payload', [b'[]\n', b'not-json\n', b'{}\n{}\n', b'x' * 16385])
@pytest.mark.asyncio
async def test_replaced_or_malformed_private_frames_fail(payload):
    read, write = os.pipe()
    try:
        # Small pipe capacity is not a protocol authority; feed large frames concurrently.
        async def feed():
            await asyncio.to_thread(os.write, write, payload)
        sender = asyncio.create_task(feed())
        with pytest.raises(Hold):
            await _receive(read, time.monotonic() + 1)
        await sender
    finally:
        os.close(read)
        os.close(write)


@pytest.mark.asyncio
async def test_channel_eof_and_deadline_fail_closed():
    read, write = os.pipe()
    try:
        with pytest.raises(TimeoutError):
            await _receive(read, time.monotonic() + 0.02)
        os.close(write)
        write = None
        with pytest.raises(Hold, match='EOF'):
            await _receive(read, time.monotonic() + 1)
    finally:
        os.close(read)
        if write is not None:
            os.close(write)


@pytest.mark.asyncio
async def test_probe_rejects_forged_parent_envelope_before_redis():
    read, write = os.pipe()
    reply_read, reply_write = os.pipe()
    try:
        await _send(write, {'kind': 'challenge', 'issuer': {'pid': os.getpid()},
                           'password': 'must-not-be-authority'}, time.monotonic() + 1)
        with pytest.raises(Hold):
            await _probe_child(argparse.Namespace(read_fd=read, write_fd=reply_write,
                                                  scenario='s', acceptance_id='a'))
    finally:
        for fd in (read, write, reply_read, reply_write):
            os.close(fd)


@pytest.mark.asyncio
async def test_lease_closes_only_its_client_and_redacts_repr():
    class Client:
        closed = False
        async def aclose(self):
            self.closed = True
    client = Client()
    lease = ScenarioLease(client, lambda: None)
    assert lease.client is client
    assert repr(lease) == '<ScenarioLease byte-mode>'
    for name in ('domain', 'reset', 'activate_fresh', 'dispose', 'owner_password'):
        assert not hasattr(lease, name)
    await lease.aclose()
    assert client.closed
    with pytest.raises(Hold):
        lease.client
