"""D2B: real owned Redis; fixture CLI success is explicitly not product success."""
from pathlib import Path
import argparse
import asyncio
import json
import logging
import sys

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path: sys.path.insert(0, str(ROOT))
import scripts.runtime_alpha_acceptance_83_7 as s7
import scripts.runtime_alpha_acceptance_83_8 as s8
import scripts.acceptance_prepared_domain as prepared
from scripts.acceptance_fresh_domain import HEADER, Phase, Hold, semantic_snapshot
from scripts.acceptance_prepared_domain import PreparedDomainIssuer
from tests.acceptance_fresh_domain import (  # noqa: F401
    integration_redis_stack, strict_mode_env, flush_redis, owned_domain, prepared_product,
)
import pytest

pytestmark = [pytest.mark.asyncio, pytest.mark.integration]


@pytest.mark.parametrize('module,scenario', [(s7, 's83-7'), (s8, 's83-8')], ids=['83.7', '83.8'])
async def test_genuine_builder_entry_reaches_original_r3_validation(prepared_product, module, scenario):
    grant, issuer = await prepared_product(module.build_acceptance_report,
        scenario=scenario, acceptance_id='d2b-real')
    d = issuer._domain
    before = await semantic_snapshot(d._client)
    header = await d._client.hgetall(HEADER)
    with pytest.raises(ValueError) as error:
        await module.build_acceptance_report(prepared_grant=grant, prepared_issuer=issuer,
                                             acceptance_id='d2b-real')
    for binding in ('canonical_run_create_binding', 'canonical_task_dispatch_binding',
                    'canonical_task_requeue_binding'):
        assert binding in str(error.value)
    assert issuer._entry['used'] and issuer._lease._ScenarioLease__closed
    assert d.phase == Phase.DOMAIN_RELEASED and d._activation_attempts == 1
    assert await semantic_snapshot(d._client) == before
    assert await d._client.hgetall(HEADER) == header


@pytest.mark.parametrize('variant', ['reused', 'foreign', 'expired', 'missing'])
async def test_real_invalid_builder_grants_leave_prepared_domain_unchanged(
    prepared_product, monkeypatch, variant,
):
    grant, issuer = await prepared_product(s7.build_acceptance_report,
        scenario='s83-7', acceptance_id='d2b-real')
    d = issuer._domain
    before = await semantic_snapshot(d._client)
    if variant == 'reused':
        lease = await issuer.redeem(grant, scenario='s83-7',
            acceptance_id='d2b-real', consumer=s7.build_acceptance_report)
        await lease.aclose()
    elif variant == 'foreign':
        issuer._entry['acceptance_id'] = 'foreign'
    elif variant == 'expired':
        issuer._entry['deadline'] = 0
    else:
        grant = None
    def no_connection(*a, **k): pytest.fail('invalid grant attempted a Redis connection')
    monkeypatch.setattr(prepared.redis, 'Redis', no_connection)
    with pytest.raises(Hold):
        await s7.build_acceptance_report(prepared_grant=grant, prepared_issuer=issuer,
                                         acceptance_id='d2b-real')
    monkeypatch.undo()
    assert await semantic_snapshot(d._client) == before
    assert d._activation_attempts == 1


async def test_byte_builder_handoff_and_restart_preserve_provenance(prepared_product, monkeypatch):
    grant, issuer = await prepared_product(s7.build_acceptance_report,
        scenario='s83-7', acceptance_id='d2b-real')
    d = issuer._domain
    header = await d._client.hgetall(HEADER)
    async def inspection(client, *, acceptance_id):
        assert client.connection_pool.connection_kwargs['decode_responses'] is False
        assert client.connection_pool is not d._client.connection_pool
        assert isinstance(await client.dump(HEADER), bytes)
        # In-process client reconstruction, not a claim of full product startup.
        from hfa_control.models import ControlPlaneConfig
        from hfa_control.service import ControlPlaneService
        for suffix in ('initial', 'restart'):
            control = ControlPlaneService(client, ControlPlaneConfig(instance_id=suffix))
            await control.close()
        assert await d._client.hgetall(HEADER) == header
        assert d._activation_attempts == 1
        return {'status': 'PASS', 'handoff_inspection_only': True}
    monkeypatch.setattr(s7, '_product_alpha_scenario', inspection)
    report = await s7.build_acceptance_report(prepared_grant=grant, prepared_issuer=issuer,
                                             acceptance_id='d2b-real')
    assert report['scenarios'][0]['handoff_inspection_only'] is True
    assert report['redis_url'] is None and report['redis_db_reset'] is False
    assert d.phase == Phase.DOMAIN_RELEASED


async def fixture_builder(*, grant, acceptance_id):
    return await prepared.borrow_builder_lease(grant=grant, issuer=None,
        consumer=fixture_builder, scenario='fixture', acceptance_id=acceptance_id)


async def fixture_execute(args, grant, issuer):
    lease = await fixture_builder(grant=grant, acceptance_id=args.acceptance_id)
    try:
        assert isinstance(await lease.client.dump(HEADER), bytes)
        if args.acceptance_id == 'timeout':
            await asyncio.sleep(60)
        if args.acceptance_id == 'error':
            raise ValueError('original-fixture-error')
        if args.acceptance_id == 'canary':
            secret = lease.client.connection_pool.connection_kwargs['password']
            print(secret)
            logging.getLogger('d2b-redaction').warning(secret)
            raise ValueError(secret)
        Path(args.out).write_text(json.dumps({'fixture_only': True, 'dump_bytes': True},sort_keys=True))
        print('FIXTURE_ONLY PASS')
        return 0
    finally:
        await lease.aclose()


@pytest.mark.parametrize('behavior', ['normal', 'error', 'timeout', 'canary'])
async def test_owned_private_cli_normal_error_timeout_and_redaction(owned_domain, tmp_path, behavior):
    d = owned_domain
    await d.activate_prepared(terminal_events=True)
    issuer = PreparedDomainIssuer(d)
    grant = await issuer.issue(scenario='fixture', acceptance_id=behavior,
        consumer=Path(__file__), timeout=3 if behavior == 'timeout' else 20, terminal_events=True)
    secrets = [d._owner_password, d._scenario_password]
    try:
        if behavior == 'timeout':
            with pytest.raises(Hold):
                await issuer.run_cli(grant, scenario='fixture', acceptance_id=behavior,
                    consumer=Path(__file__), output=tmp_path/'fixture.json')
        else:
            completed = await issuer.run_cli(grant, scenario='fixture', acceptance_id=behavior,
                consumer=Path(__file__), output=tmp_path/'fixture.json')
            assert completed.returncode == (0 if behavior == 'normal' else 1)
            if behavior == 'error': assert 'ValueError: original-fixture-error' in completed.stderr
            if behavior == 'canary': assert '<redacted>' in completed.stderr
            for secret in secrets:
                assert secret not in completed.stdout + completed.stderr + repr(completed.args)
        assert d.phase == Phase.DISPOSED and d._process.poll() is not None
        assert issuer._child.poll() is not None and not d._root.exists()
        assert all(secret not in ''.join(p.read_text() for p,_ in d._receipts) for secret in secrets)
    finally:
        if not issuer._closed.is_set(): await issuer.abort()


@pytest.mark.parametrize('module,scenario', [(s7,'s83-7'),(s8,'s83-8')], ids=['83.7','83.8'])
async def test_real_product_cli_preserves_r3_error_and_disposes(owned_domain, tmp_path, module, scenario):
    d=owned_domain
    await d.activate_prepared(terminal_events=True)
    issuer=PreparedDomainIssuer(d)
    grant=await issuer.issue(scenario=scenario,acceptance_id='d2b-cli',
        consumer=Path(module.__file__),timeout=30,terminal_events=True)
    try:
        completed=await asyncio.wait_for(issuer.run_cli(grant,scenario=scenario,
            acceptance_id='d2b-cli',consumer=Path(module.__file__),output=tmp_path/'product.json'),90)
        assert completed.returncode == 1
        assert 'ValueError' in completed.stderr
        for binding in ('canonical_run_create_binding','canonical_task_dispatch_binding',
                        'canonical_task_requeue_binding'):
            assert binding in completed.stderr
        assert not (tmp_path/'product.json').exists()
        assert d.phase == Phase.DISPOSED and not d._root.exists()
        assert issuer._child.poll() is not None
    finally:
        if not issuer._closed.is_set(): await issuer.abort()


async def test_fixture_origins_are_module_local(request):
    for name in ('integration_redis_stack','strict_mode_env','flush_redis'):
        assert request._fixture_defs[name].func.__module__ == 'tests.acceptance_fresh_domain'
    assert 'redis_client' not in request.fixturenames


async def test_foreign_cli_source_refused_before_spawn(owned_domain, tmp_path, monkeypatch):
    d = owned_domain
    await d.activate_prepared(terminal_events=True)
    issuer = PreparedDomainIssuer(d)
    grant = await issuer.issue(scenario='s83-7', acceptance_id='d2b-cli',
        consumer=Path(s7.__file__), timeout=20, terminal_events=True)
    def no_spawn(*a, **k): pytest.fail('foreign source attempted child spawn')
    monkeypatch.setattr(prepared.subprocess, 'Popen', no_spawn)
    with pytest.raises(Hold):
        await issuer.run_cli(grant, scenario='s83-7', acceptance_id='d2b-cli',
            consumer=Path(__file__), output=tmp_path/'foreign.json')
    assert issuer._child is None and not issuer._entry['used']
    assert d.phase == Phase.DISPOSED


async def test_deterministic_projection_uses_two_genuinely_distinct_domains(
    owned_domain, monkeypatch, request,
):
    # Projection contract only; original product success remains blocked by R3.
    from scripts.acceptance_fresh_domain import OwnedFreshDomain
    reports, ids = [], []
    first = owned_domain
    for d in (first, OwnedFreshDomain(first._binary, first.evidence.parent)):
        issuer = None
        try:
            if d is not first:
                await d.create()
                creation, creation_sha256 = d._receipts[0]
                d._record('test_node', {'nodeid': request.node.nodeid,
                    'creation_receipt_sha256': creation_sha256,
                    'creation_receipt_hex': creation.read_bytes().hex()})
            await d.activate_prepared(terminal_events=True)
            issuer = PreparedDomainIssuer(d)
            grant = await issuer.issue(scenario='s83-7', acceptance_id='deterministic',
                consumer=s7.build_acceptance_report, timeout=20, terminal_events=True)
            async def observed(client, *, acceptance_id):
                assert isinstance(await client.dump(HEADER), bytes)
                return {'status': 'PASS', 'projection_only': True}
            monkeypatch.setattr(s7, '_product_alpha_scenario', observed)
            reports.append(await s7.build_acceptance_report(prepared_grant=grant,
                prepared_issuer=issuer, acceptance_id='deterministic'))
            ids.append((d.domain_id, d._run_id, d._process.pid, d._port))
        finally:
            if issuer is not None and not issuer._closed.is_set(): await issuer.abort()
            elif issuer is None and d.phase != Phase.DISPOSED: await d.abort_owned()
    assert ids[0] != ids[1] and ids[0][0] != ids[1][0]
    assert json.dumps(reports[0], sort_keys=True) == json.dumps(reports[1], sort_keys=True)
    raw = json.dumps(reports)
    assert all(str(part) not in raw for identity in ids for part in identity)


if __name__ == '__main__':
    parser=argparse.ArgumentParser()
    parser.add_argument('--prepared-read-fd',type=int,required=True)
    parser.add_argument('--prepared-write-fd',type=int,required=True)
    parser.add_argument('--acceptance-id',required=True)
    parser.add_argument('--out',required=True)
    parser.add_argument('--json',action='store_true')
    args=parser.parse_args()
    raise SystemExit(asyncio.run(prepared.product_cli_child(args,consumer=fixture_builder,
        scenario='fixture',execute=fixture_execute)))
