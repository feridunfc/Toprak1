"""F04.4B oracles on explicitly owned standalone Redis 7.4.10."""
import os

import pytest

from tests.integration.test_f02a1_root_binding_integration import root_redis, flush_redis
from tests.unit.test_f04_4b_task_dispatch_projection_proof import (
    alpha,
    test_normal_first_projection_and_exact_duplicate_zero_effect,
    test_proof_loss_or_corruption_rejected_at_actual_lua,
    test_restart_materializes_once,
    test_runtime_cannot_hide_missing_proof_even_on_duplicate,
    test_exact_record_binds_projection_inputs,
    test_no_witness_cannot_fall_back_to_runtime,
    test_concurrent_proof_loss_between_preparation_and_eval,
    test_stale_dispatch_witness_after_actual_claim,
    test_missing_proof_cannot_emit_runtime_truth_conflict,
)

pytestmark = pytest.mark.integration


@pytest.fixture(scope='session')
def integration_redis_stack():
    if os.environ.get('USE_EXISTING_REDIS') != '1' or os.environ.get('HFA_F04_4B_LIVE_ACCEPTANCE') != '1':
        pytest.skip('requires ownership of disposable Redis 7.4.10 at 6389/0')
    yield
