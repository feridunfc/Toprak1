"""F04.4G semantic proof/effect oracles on owned Redis 7.4.10 standalone."""
import os
import pytest
from tests.integration.test_f02a1_root_binding_integration import root_redis, flush_redis
from tests.unit.test_f04_4g_resource_settlement_proof import (
    alpha,
    test_own_proof_loss_after_python_validation,
    test_exact_predecessor_required,
    test_normal_duplicate_and_restart_without_task_reauthorization,
    test_restart_materializes_missing_settlement_once,
    test_source_bound_input_drift,
    test_forged_cost_and_runtime_receipt_cannot_rebind_canonical_cost,
    test_counter_or_proof_destination_drift,
    test_missing_or_malformed_witness,
    test_foreign_proof,
    test_runtime_receipt_cannot_replace_absent_authority,
    test_public_settle_without_witness_rejects,
    test_second_client_proof_loss_at_settlement_boundary,
    test_run_create_proof_cannot_authorize_settlement,
)
pytestmark = pytest.mark.integration

@pytest.fixture(scope='session')
def integration_redis_stack():
    if os.environ.get('USE_EXISTING_REDIS') != '1' or os.environ.get('HFA_F04_4G_LIVE_ACCEPTANCE') != '1':
        pytest.skip('requires ownership of disposable Redis 7.4.10 at 6389/0')
    yield
