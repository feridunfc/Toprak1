"""F06 exact effects/retries using exclusively owned Redis 7.4.10 standalone."""
import os
import pytest
from tests.integration.test_f02a1_root_binding_integration import root_redis, flush_redis
from tests.unit.test_f06_task_admit_effect_completeness import (
    alpha,
    test_first_exact_duplicate_lost_ack_and_restored_empty_retry,
    test_each_destination_wrong_type_before_first_write,
    test_invalid_late_command_arguments_do_not_write,
    test_partial_footprint_is_visible_manual_conflict,
    test_concurrent_duplicate_materializes_once,
    test_actual_lifecycle_progress_does_not_recreate_admission,
    test_progressed_duplicate_requires_exact_history_and_current_head,
    test_error_after_each_effect_never_makes_partial_retry_success,
    test_submission_conflict_is_terminal_operator_visible_not_invisible_pending,
    test_cross_run_task_id_explicit_f07_disposition,
    test_valid_current_head_progress_after_python_witness_cannot_slip_through,
)
pytestmark=pytest.mark.integration

@pytest.fixture(scope='session')
def integration_redis_stack():
    if os.environ.get('USE_EXISTING_REDIS')!='1' or os.environ.get('HFA_F06_LIVE_ACCEPTANCE')!='1':
        pytest.skip('requires ownership of disposable Redis 7.4.10 at 6389/0')
    yield
