"""F04.4C product oracles on owned standalone Redis 7.4.10."""
import os

import pytest

from tests.integration.test_f02a1_root_binding_integration import root_redis, flush_redis
from tests.unit.test_f04_4c_task_claim_projection_proof import (
    alpha,
    test_normal_claim_and_exact_duplicate_zero_effect,
    test_proof_loss_or_corruption_rejected_at_actual_lua,
    test_restart_projects_once,
    test_runtime_footprint_never_substitutes_for_proof,
    test_exact_claim_record_binds_inputs_and_effects,
    test_no_proof_transport_cannot_fall_back,
    test_separate_client_proof_loss_after_preparation,
    test_stale_claim_after_real_terminal_transition,
    test_missing_own_proof_cannot_emit_truth_conflict,
)

pytestmark = pytest.mark.integration


@pytest.fixture(scope='session')
def integration_redis_stack():
    if os.environ.get('USE_EXISTING_REDIS') != '1' or os.environ.get('HFA_F04_4C_LIVE_ACCEPTANCE') != '1':
        pytest.skip('requires ownership of disposable Redis 7.4.10 at 6389/0')
    yield
