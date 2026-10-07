"""F04.4D semantic matrix on owned standalone Redis 7.4.10."""
import os

import pytest

from tests.integration.test_f02a1_root_binding_integration import root_redis, flush_redis
from tests.unit.test_f04_4d_task_requeue_projection_proof import (
    alpha,
    test_complete_foreign_canonical_witness_rejected,
    test_missing_or_malformed_witness_transport,
    test_normal_projection_duplicate_and_delivery_remains_separate,
    test_proof_fault_after_python_validation,
    test_restart_materializes_once,
    test_runtime_cannot_substitute_for_missing_proof,
    test_exact_record_binds_projection_input,
    test_foreign_destination_or_proof_key,
    test_separate_connection_barrier,
    test_actual_later_dispatch_rejects_stale_requeue,
    test_missing_proof_blocks_error_evidence_writers,
)

pytestmark = pytest.mark.integration


@pytest.fixture(scope='session')
def integration_redis_stack():
    if os.environ.get('USE_EXISTING_REDIS') != '1' or os.environ.get('HFA_F04_4D_LIVE_ACCEPTANCE') != '1':
        pytest.skip('requires ownership of disposable Redis 7.4.10 at 6389/0')
    yield
