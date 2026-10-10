"""F04.4H production ACK Lua on explicitly owned Redis 7.4.10 standalone."""
import os
import pytest
from tests.integration.test_f02a1_root_binding_integration import root_redis, flush_redis
from tests.unit.test_f04_4h_worker_recovery_ack_proof import (
    alpha,
    test_proof_loss_at_ack_boundary,
    test_normal_ack_duplicate_and_restart_without_task_reauthorization,
    test_actual_worker_finalize_then_ack_window,
    test_foreign_transport_context,
    test_foreign_stream_entry,
    test_result_binding_drift,
    test_foreign_own_proof,
    test_runtime_finalized_without_proof_cannot_ack,
    test_malformed_witness,
    test_transport_failure_is_read_only,
    test_second_client_proof_loss,
    test_wrong_proof_key_destination,
    test_run_create_cannot_authorize_ack,
    test_concurrent_exact_ack_retries_have_one_effect,
)
pytestmark = pytest.mark.integration

@pytest.fixture(scope='session')
def integration_redis_stack():
    if os.environ.get('USE_EXISTING_REDIS') != '1' or os.environ.get('HFA_F04_4H_LIVE_ACCEPTANCE') != '1':
        pytest.skip('requires ownership of disposable Redis 7.4.10 at 6389/0')
    yield
