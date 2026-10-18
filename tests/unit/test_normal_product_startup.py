"""Normal entrypoint gates and retained evidence checks; no new Redis lifecycle."""
import os
from types import SimpleNamespace
from unittest.mock import AsyncMock

import fakeredis.aioredis
import pytest
import pytest_asyncio

from hfa.config.keys import RedisKey
from hfa_control.product_startup import verify_terminal_event_readiness, redis_client_from_environment
from hfa_control.run_terminal_event_evidence import ensure_terminal_event_index, backfill_terminal_event_evidence


@pytest_asyncio.fixture
async def prepared():
    client = fakeredis.aioredis.FakeRedis(decode_responses=True, version=(7, 4))
    # fakeredis reports None for the ID of a genuinely empty stream; Redis reports 0-0.
    original_parser = client.response_callbacks['XINFO STREAM']
    def stream_info(response, **kwargs):
        info = original_parser(response, **kwargs)
        if info['length'] == 0 and info['last-generated-id'] is None:
            info['last-generated-id'] = '0-0'
        return info
    client.set_response_callback('XINFO STREAM', stream_info)
    await ensure_terminal_event_index(client)
    await client.xgroup_create(RedisKey.stream_results(), 'normal-startup-unit', id='0', mkstream=True)
    await backfill_terminal_event_evidence(client, completed_at_ms=1000)
    try:
        yield client
    finally:
        await client.aclose()


async def dumps(client):
    return {key: (await client.dump(key), await client.pexpiretime(key)) async for key in client.scan_iter()}


@pytest.mark.asyncio
async def test_valid_prepared_readiness_is_read_only(prepared):
    before = await dumps(prepared)
    assert (await verify_terminal_event_readiness(prepared))['__migration__:status'] == 'ready'
    assert await dumps(prepared) == before


@pytest.mark.asyncio
@pytest.mark.parametrize('fault', ['index_missing', 'index_type', 'index_ttl', 'stream_missing', 'stream_type', 'stream_ttl', 'readiness_missing', 'orphan_evidence'])
async def test_invalid_retained_evidence_is_read_only_fail_closed(prepared, fault):
    index, stream = RedisKey.run_terminal_event_index(), RedisKey.stream_results()
    if fault.endswith('_missing') and fault in {'index_missing', 'stream_missing'}:
        await prepared.delete(index if fault.startswith('index') else stream)
    elif fault.endswith('_type'):
        key = index if fault.startswith('index') else stream
        await prepared.delete(key)
        await prepared.set(key, 'negative-wrong-type')
    elif fault.endswith('_ttl'):
        await prepared.pexpire(index if fault.startswith('index') else stream, 3600000)
    elif fault == 'readiness_missing':
        await prepared.hdel(index, '__migration__:status')
    else:
        await prepared.hset(index, 'run:foreign', 'invalid-evidence')
    before = await dumps(prepared)
    with pytest.raises(RuntimeError, match='startup blocked'):
        await verify_terminal_event_readiness(prepared)
    assert await dumps(prepared) == before


@pytest.mark.asyncio
async def test_incomplete_readiness_is_rejected_without_repair(prepared):
    await prepared.hdel(RedisKey.run_terminal_event_index(), '__migration__:completed_at_ms')
    before = await dumps(prepared)
    with pytest.raises(RuntimeError, match='startup blocked'):
        await verify_terminal_event_readiness(prepared)
    assert await dumps(prepared) == before


@pytest.mark.asyncio
async def test_worker_gate_precedes_constructor_and_closes_redis(monkeypatch):
    import hfa_worker.process_root as root
    redis = SimpleNamespace(aclose=AsyncMock())
    called = []
    async def gate(value):
        assert value is redis
        called.append('gate')
        raise RuntimeError('startup blocked')
    monkeypatch.setattr(root, 'verify_terminal_event_readiness', gate)
    monkeypatch.setattr(root, 'WorkerService', lambda *a: pytest.fail('service constructed before gate'))
    with pytest.raises(RuntimeError, match='startup blocked'):
        await root.run_worker_process(redis_factory=lambda _: redis, config={'product_mode':'SINGLE_TASK_ALPHA'})
    assert called == ['gate']
    redis.aclose.assert_awaited_once()


@pytest.mark.asyncio
async def test_control_gate_precedes_constructor_and_closes_redis(monkeypatch):
    from hfa_control.main import build_app
    import hfa_control.service as service
    import hfa_control.product_startup as startup
    redis = SimpleNamespace(aclose=AsyncMock())
    async def gate(value):
        assert value is redis
        raise RuntimeError('startup blocked')
    monkeypatch.setattr(startup, 'verify_terminal_event_readiness', gate)
    monkeypatch.setattr(service, 'ControlPlaneService', lambda *a: pytest.fail('service constructed before gate'))
    app = build_app(redis, SimpleNamespace(product_mode='SINGLE_TASK_ALPHA'))
    with pytest.raises(RuntimeError, match='startup blocked'):
        async with app.router.lifespan_context(app):
            pytest.fail('invalid startup became active')
    redis.aclose.assert_awaited_once()
    assert not hasattr(app.state, 'cp')


def test_password_pipe_does_not_put_secret_in_url_or_environment(monkeypatch):
    read_fd, write_fd = os.pipe()
    os.write(write_fd, b'unit-canary\n')
    os.close(write_fd)
    monkeypatch.setenv('HFA_REDIS_PASSWORD_FD', str(read_fd))
    client = redis_client_from_environment('redis://scenario@127.0.0.1:1/0')
    assert client.connection_pool.connection_kwargs['password'] == 'unit-canary'
    assert 'HFA_REDIS_PASSWORD_FD' not in os.environ
    with pytest.raises(OSError):
        os.fstat(read_fd)


@pytest.mark.parametrize('fd', ['bad', '-1', '999999'])
def test_invalid_password_descriptor_fails_closed(monkeypatch, fd):
    monkeypatch.setenv('HFA_REDIS_PASSWORD_FD', fd)
    with pytest.raises(RuntimeError, match='private Redis password descriptor'):
        redis_client_from_environment('redis://scenario@127.0.0.1:1/0')


@pytest.mark.asyncio
@pytest.mark.parametrize('issuer', [None, object()])
async def test_fresh_product_preparation_requires_original_issuer(issuer):
    from scripts.acceptance_prepared_domain import prepare_fresh_product_domain
    from scripts.acceptance_fresh_domain import Hold
    with pytest.raises(Hold, match='original fresh issuer'):
        await prepare_fresh_product_domain(grant=None, issuer=issuer, consumer=test_fresh_product_preparation_requires_original_issuer,
            scenario='normal', acceptance_id='unit')


@pytest.mark.asyncio
async def test_forged_issuer_cannot_enter_fresh_preparation():
    from scripts.acceptance_prepared_domain import PreparedDomainIssuer, prepare_fresh_product_domain
    from scripts.acceptance_fresh_domain import Hold
    with pytest.raises(Hold, match='original fresh issuer'):
        await prepare_fresh_product_domain(grant=None, issuer=object.__new__(PreparedDomainIssuer),
            consumer=test_forged_issuer_cannot_enter_fresh_preparation, scenario='normal', acceptance_id='unit')


def test_requeue_asset_resolves_in_installed_layout(tmp_path, monkeypatch):
    import hfa_control.task_requeue_authority as binding
    original = binding._lua_path().read_bytes()
    installed = tmp_path / 'site-packages'
    asset = installed / 'hfa/lua/task_requeue.lua'
    asset.parent.mkdir(parents=True)
    asset.write_bytes(original)
    module = installed / 'hfa_control/task_requeue_authority.py'
    module.parent.mkdir()
    monkeypatch.setattr(binding, '__file__', str(module))
    assert binding._lua_path() == asset
    assert binding._lua_path().read_bytes() == original
