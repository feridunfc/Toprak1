"""F04.2 production admission oracles on an owned disposable Redis 7.4.10."""
import os

import pytest

from tests.integration.test_f02a1_root_binding_integration import root_redis, flush_redis
from tests.unit.test_f04_run_create_projection_proof import (
    alpha,
    test_proof_race_rejects_before_any_projection_write,
    test_restart_materializes_once_and_duplicate_has_zero_effect,
    test_restart_stale_witness_cannot_project,
    test_exact_projection_receipt_remains_read_only_after_proof_movement,
    test_missing_witness_cannot_mutate,
)

pytestmark = pytest.mark.integration


@pytest.fixture(scope='session')
def integration_redis_stack():
    if os.environ.get('USE_EXISTING_REDIS') != '1' or os.environ.get('HFA_F04_2_LIVE_ACCEPTANCE') != '1':
        pytest.skip('requires ownership of disposable Redis 7.4.10 at 6389/0')
    yield
