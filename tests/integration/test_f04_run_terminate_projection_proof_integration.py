"""F04.3 oracles on an explicitly owned disposable Redis 7.4.10."""
import os

import pytest

from tests.integration.test_f02a1_root_binding_integration import root_redis, flush_redis
from tests.unit.test_f04_run_terminate_projection_proof import (
    alpha,
    test_own_run_proof_race_blocks_all_projection_writes,
    test_restart_projects_once_without_reauthorizing_task,
    test_stale_restart_witness_cannot_project,
    test_exact_duplicate_stays_read_only_after_own_proof_movement,
    test_missing_witness_cannot_mutate,
)

pytestmark = pytest.mark.integration


@pytest.fixture(scope='session')
def integration_redis_stack():
    if os.environ.get('USE_EXISTING_REDIS') != '1' or os.environ.get('HFA_F04_3_LIVE_ACCEPTANCE') != '1':
        pytest.skip('requires ownership of disposable Redis 7.4.10 at 6389/0')
    yield
