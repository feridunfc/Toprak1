"""F04.4F semantic oracles on owned standalone Redis 7.4.10."""
import os

import pytest

from tests.integration.test_f02a1_root_binding_integration import root_redis, flush_redis
from tests.unit.test_f04_4f_resource_finalize_proof import (
    alpha,
    test_proof_loss_after_python_validation,
    test_valid_finalize_duplicate_and_restart,
    test_restart_missing_finalize_materializes_once,
    test_forged_runtime_receipt_cannot_replace_missing_proof,
    test_source_owned_input_drift,
    test_counter_or_proof_destination_drift,
    test_missing_or_malformed_witness,
    test_foreign_run_proof,
    test_later_operation_on_same_run_cannot_authorize_finalize,
    test_public_finalize_without_witness_fails_closed,
    test_second_connection_removes_receipt_at_effect_boundary,
)

pytestmark = pytest.mark.integration


@pytest.fixture(scope='session')
def integration_redis_stack():
    if os.environ.get('USE_EXISTING_REDIS') != '1' or os.environ.get('HFA_F04_4F_LIVE_ACCEPTANCE') != '1':
        pytest.skip('requires ownership of disposable Redis 7.4.10 at 6389/0')
    yield
