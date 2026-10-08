"""F04.4E semantic oracles on owned standalone Redis 7.4.10."""
import os

import pytest

from tests.integration.test_f02a1_root_binding_integration import root_redis, flush_redis
from tests.unit.test_f04_4e_task_requeue_delivery_proof import (
    alpha,
    test_first_delivery_and_exact_duplicate,
    test_own_proof_fault_after_python_validation,
    test_runtime_notification_cannot_replace_proof,
    test_exact_delivery_record_binds_inputs,
    test_delivery_destination_and_proof_key_binding,
    test_missing_or_malformed_proof_transport,
    test_foreign_canonical_witness,
    test_second_connection_proof_loss,
    test_stale_head_rejected_then_fresh_historical_delivery_allowed,
    test_historical_own_proof_still_required_under_valid_later_head,
    test_missing_proof_blocks_conflict_writers,
    test_historical_fingerprint_cannot_diverge_from_exact_record,
    test_different_operation_same_task_cannot_authorize_delivery,
    test_restart_after_projected_requeue_never_duplicates_delivery,
)

pytestmark = pytest.mark.integration


@pytest.fixture(scope='session')
def integration_redis_stack():
    if os.environ.get('USE_EXISTING_REDIS') != '1' or os.environ.get('HFA_F04_4E_LIVE_ACCEPTANCE') != '1':
        pytest.skip('requires ownership of disposable Redis 7.4.10 at 6389/0')
    yield
