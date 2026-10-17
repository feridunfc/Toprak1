"""Explicit fixtures for R2 foundation only; no Docker or shared DB reset.

The environment selects an executable/evidence destination, never an endpoint or
ownership grant. The launcher creates and verifies every process itself. Missing
prerequisites are failures, not skip/xfail. Existing integration fixtures elsewhere
are unchanged. Import these overrides only into the new lifecycle test module.
"""
import os
import json
from pathlib import Path

import pytest
import pytest_asyncio

from scripts.acceptance_fresh_domain import OwnedFreshDomain, Phase


@pytest.fixture(scope="session", autouse=True)
def integration_redis_stack():
    # This module's tests acquire only launcher-created authenticated loopback children.
    yield


@pytest.fixture(scope="session", autouse=True)
def strict_mode_env():
    # Do not inherit a TCP REDIS_URL or alter canonical composition flags.
    yield


@pytest.fixture(autouse=True)
def flush_redis():
    # Replace the inherited destructive fixture for this module only.
    yield


@pytest_asyncio.fixture
async def owned_domain(tmp_path, request):
    binary = Path(os.environ["HFA_ACCEPTANCE_REDIS_BINARY"])
    evidence = Path(os.environ.get("HFA_ACCEPTANCE_EVIDENCE_DIR", str(tmp_path)))
    domain = OwnedFreshDomain(binary, evidence)
    try:
        await domain.create()
        # Observational evidence only: never accepted as an ownership grant.
        # Keep the original bytes even when a negative test corrupts its receipt.
        creation, creation_sha256 = domain._receipts[0]
        domain._record('test_node', {'nodeid': request.node.nodeid,
            'creation_receipt_sha256': creation_sha256,
            'creation_receipt_hex': creation.read_bytes().hex()})
        yield domain
    finally:
        if domain.phase == Phase.DISPOSED:
            assert domain._process.poll() is not None and not domain._root.exists()
        else:
            await domain.dispose()
