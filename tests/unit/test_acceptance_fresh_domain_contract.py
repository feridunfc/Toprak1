"""Deterministic R2 foundation policies; no external Redis connection."""
import json
from pathlib import Path

import fakeredis.aioredis
import pytest

from scripts.acceptance_fresh_domain import (
    Hold, OwnedFreshDomain, Phase, digest, listener_evidence, semantic_snapshot,
)
from hfa.authority import RedisCanonicalAuthorityStore, TaskOwnerActivationRefusedError


def domain(tmp_path):
    return OwnedFreshDomain(Path("/bin/true"), tmp_path)


def test_constructor_cannot_import_ownership(tmp_path, monkeypatch):
    monkeypatch.setenv("REDIS_URL", "redis://protected.invalid:6379/0")
    monkeypatch.setenv("USE_EXISTING_REDIS", "1")
    d = domain(tmp_path)
    assert d.phase is None and d._process is None and d._client is None
    assert d._activation_attempts == 0
    assert not d._receipts
    with pytest.raises(AttributeError):
        d.domain_id = 'caller-supplied-identity'


@pytest.mark.parametrize("argument", ["endpoint", "pid", "attestation", "checkpoint", "redis_url"])
def test_foreign_endpoint_or_json_is_not_a_constructor_capability(tmp_path, argument):
    with pytest.raises(TypeError):
        OwnedFreshDomain(Path("/bin/true"), tmp_path, **{argument: {"owned": True}})


@pytest.mark.asyncio
async def test_wrong_executable_fails_before_child_creation(tmp_path):
    d = domain(tmp_path)
    with pytest.raises(Hold, match="7.4.10"):
        await d.create()
    assert d._process is None and d._client is None


@pytest.mark.parametrize("phase", [None, *[p for p in Phase if p != Phase.WRITERS_ISOLATED]])
@pytest.mark.asyncio
async def test_activation_has_no_implicit_state_transition(tmp_path, phase):
    d = domain(tmp_path)
    d.phase = phase
    with pytest.raises(Hold):
        await d.activate_fresh()
    assert d._activation_attempts == 0 and d._client is None


@pytest.mark.parametrize("phase", [None, *Phase])
@pytest.mark.asyncio
async def test_reset_is_never_authorized(tmp_path, phase):
    d = domain(tmp_path)
    d.phase = phase
    with pytest.raises(Hold, match="reset"):
        await d.reset()


@pytest.mark.parametrize("phase", [None, Phase.CREATED, Phase.WRITERS_ISOLATED, Phase.HOLD,
                                  Phase.QUIESCENT, Phase.VERIFIED_RETAINED, Phase.DISPOSED])
def test_unreleased_or_retained_domain_has_no_scenario_client(tmp_path, phase):
    d = domain(tmp_path)
    d.phase = phase
    with pytest.raises(Hold):
        d.client


def test_external_receipt_tampering_is_not_a_grant(tmp_path):
    d = domain(tmp_path)
    d._record("creation", {"observed": "host"})
    d._verify_receipts()
    path = d._receipts[0][0]
    path.chmod(0o600)
    path.write_text(json.dumps({"owned": True}))
    path.chmod(0o400)
    with pytest.raises(Hold, match="changed"):
        d._verify_receipts()


def test_missing_receipt_is_not_a_grant(tmp_path):
    d = domain(tmp_path)
    with pytest.raises(Hold, match="missing"):
        d._verify_receipts()


def test_unverified_process_cannot_supply_listener_evidence():
    with pytest.raises((Hold, OSError)):
        listener_evidence(99999999, 6379)


@pytest.mark.parametrize("attestation", [None, {}, {"fresh_domain_id": "x"},
    {"fresh_domain_id": "x", "writers_isolated": False, "attested_by": "x", "evidence_ref": "x"},
    {"fresh_domain_id": "x", "writers_isolated": 1, "attested_by": "x", "evidence_ref": "x"},
    {"fresh_domain_id": "foreign", "writers_isolated": True, "attested_by": "x", "evidence_ref": "x"},
    {"fresh_domain_id": "x", "writers_isolated": True, "attested_by": "", "evidence_ref": "x"},
    {"fresh_domain_id": "x", "writers_isolated": True, "attested_by": "x", "evidence_ref": " "}])
@pytest.mark.asyncio
async def test_existing_api_rejects_invalid_attestation_without_writes(attestation):
    r = fakeredis.aioredis.FakeRedis(decode_responses=True)
    try:
        with pytest.raises(TaskOwnerActivationRefusedError):
            await RedisCanonicalAuthorityStore(r).activate_task_owner_namespace(
                namespace_id="x", activated_at_ms=1, isolation_attestation=attestation)
        assert await r.dbsize() == 0
    finally:
        await r.aclose()


def test_digest_matches_existing_api_serialization():
    value = {"writers_isolated": True, "fresh_domain_id": "ü", "evidence_ref": "external"}
    import hashlib
    assert digest(value) == hashlib.sha256(json.dumps(value, sort_keys=True).encode()).hexdigest()


@pytest.mark.asyncio
async def test_snapshot_does_not_accept_partial_inventory():
    class Incomplete:
        async def scan(self, **kwargs):
            return 0, []

        async def dbsize(self):
            return 1
    with pytest.raises(Hold, match="incomplete"):
        await semantic_snapshot(Incomplete())


@pytest.mark.asyncio
async def test_retained_verification_cannot_manufacture_checkpoint(tmp_path):
    d = domain(tmp_path)
    d.phase = Phase.QUIESCENT
    with pytest.raises(Hold, match="checkpoint unavailable"):
        await d.verify_retained()
