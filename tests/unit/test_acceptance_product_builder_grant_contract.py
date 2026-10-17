"""Builder authority boundaries; synthetic scenario tests are projection-only."""
import argparse
import ast
import asyncio
from pathlib import Path
from unittest.mock import AsyncMock

import pytest

import scripts.acceptance_prepared_domain as prepared
import scripts.runtime_alpha_acceptance_83_7 as s7
import scripts.runtime_alpha_acceptance_83_8 as s8
from scripts.acceptance_fresh_domain import Hold


@pytest.mark.asyncio
@pytest.mark.parametrize('module', [s7, s8], ids=['83.7', '83.8'])
@pytest.mark.parametrize('case', ['missing', 'url', 'reset', 'forged', 'foreign-issuer'])
async def test_invalid_authorization_never_connects_or_starts_product(module, case, monkeypatch):
    def forbidden(*args, **kwargs):
        pytest.fail('unauthorized Redis connection or product startup')
    monkeypatch.setattr(prepared.redis, 'Redis', forbidden)
    monkeypatch.setattr(module, 'ControlPlaneService', forbidden)
    args = {'acceptance_id': 'd2b-unit'}
    if case == 'url':
        args['redis_url'] = 'redis://protected.invalid:6379/0'
    elif case == 'reset':
        args['reset_test_db'] = True
    elif case == 'forged':
        args['prepared_grant'] = object.__new__(prepared.PreparedGrant)
    elif case == 'foreign-issuer':
        args.update(prepared_grant=object.__new__(prepared.PreparedGrant),
                    prepared_issuer=object.__new__(prepared.PreparedDomainIssuer))
    with pytest.raises(Hold):
        await module.build_acceptance_report(**args)


@pytest.mark.parametrize('module', [s7, s8], ids=['83.7', '83.8'])
def test_builder_has_no_activation_reset_or_url_connection(module):
    tree = ast.parse(Path(module.__file__).read_text())
    builder = next(n for n in tree.body if isinstance(n, ast.AsyncFunctionDef)
                   and n.name == 'build_acceptance_report')
    calls = [n.func.attr for n in ast.walk(builder) if isinstance(n, ast.Call)
             and isinstance(n.func, ast.Attribute)]
    assert not set(calls) & {'flushdb', 'flushall', 'activate_task_owner_namespace',
                            'activate_prepared', 'from_url', 'create', 'reset'}
    assert 'aclose' in calls


@pytest.mark.asyncio
@pytest.mark.parametrize('module', [s7, s8], ids=['83.7', '83.8'])
@pytest.mark.parametrize('failure', [False, True], ids=['projection-only', 'scenario-error'])
async def test_builder_borrows_once_and_closes_only_lease(module, failure, monkeypatch):
    class Lease:
        client = object()
        aclose = AsyncMock()
    lease = Lease()
    borrow = AsyncMock(return_value=lease)
    monkeypatch.setattr(module, 'borrow_builder_lease', borrow)
    scenario = AsyncMock(side_effect=RuntimeError('scenario-original') if failure else None,
                         return_value={'status': 'PASS', 'projection_only': True})
    monkeypatch.setattr(module, '_product_alpha_scenario' if module is s7 else '_scenario', scenario)
    if failure:
        with pytest.raises(RuntimeError, match='scenario-original'):
            await module.build_acceptance_report(acceptance_id='d2b-unit')
    else:
        report = await module.build_acceptance_report(acceptance_id='d2b-unit')
        assert report['production_ready'] is False
        if module is s7:
            assert report['redis_url'] is None and report['redis_db_reset'] is False
            assert report['redis_domain_mode'] == 'OWNED_DISPOSABLE'
    borrow.assert_awaited_once()
    scenario.assert_awaited_once_with(lease.client, acceptance_id='d2b-unit')
    lease.aclose.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize('case', ['missing', 'url', 'reset', 'one-fd', 'mixed'])
async def test_cli_requires_explicit_launcher_or_complete_private_handoff(case):
    args = argparse.Namespace(owned_fresh_domain=False, prepared_read_fd=None,
        prepared_write_fd=None, redis_url=None, reset_test_db=False)
    if case == 'url': args.redis_url = 'redis://protected.invalid:6379/0'
    if case == 'reset': args.reset_test_db = True
    if case == 'one-fd': args.prepared_read_fd = 99
    if case == 'mixed':
        args.owned_fresh_domain = True
        args.prepared_read_fd = 99
    with pytest.raises(Hold):
        await prepared.dispatch_product_cli(args, source=Path(s7.__file__), scenario='s83-7',
            consumer=s7.build_acceptance_report, execute=s7._async_main)


def test_cli_parent_environment_never_forwards_endpoint_credentials(monkeypatch):
    from tests.acceptance_fresh_domain import product_cli_environment
    monkeypatch.setenv('REDIS_URL', 'redis://owner:CANARY@protected.invalid/0')
    monkeypatch.setenv('HFA_ACCEPTANCE_REDIS_BINARY', '/owned/binary')
    monkeypatch.setenv('HFA_ACCEPTANCE_EVIDENCE_DIR', '/owned/evidence')
    value = product_cli_environment()
    assert 'REDIS_URL' not in value and 'CANARY' not in repr(value)
    assert value['HFA_ACCEPTANCE_REDIS_BINARY'] == '/owned/binary'
