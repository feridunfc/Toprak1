"""F02B oracles on an explicitly owned disposable Redis 7.4.10."""
import os
import pytest
from tests.unit.test_f02b_canonical_run_terminate import (
    alpha,
    test_runtime_terminal_cannot_authorize_run,
    test_success_failure_use_canonical_root_only,
    test_wrong_trigger_root_rejected,
    test_task_proof_damage_fails_closed,
    test_task_witness_race_cannot_commit,
    test_durable_duplicate_is_zero_effect_and_does_not_read_task,
    test_restart_after_durable_commit_restores_projection_without_task_reads,
    test_forged_terminal_command_rejected_at_canonical_boundary,
    test_other_canonical_task_witness_cannot_authorize_root,
    test_forged_command_cannot_override_canonical_witness,
    test_lost_commit_response_recovers_exact_receipt,
    test_unbound_history_cannot_create_new_run_termination,
    test_historical_durable_run_termination_replays_without_new_root,
    test_worker_success_and_failure_preserve_run_finalization,
)
from tests.integration.test_f02a1_root_binding_integration import root_redis, flush_redis

pytestmark = pytest.mark.integration


@pytest.fixture(scope='session')
def integration_redis_stack():
    if (os.environ.get('USE_EXISTING_REDIS') != '1'
            or os.environ.get('HFA_F02B_LIVE_ACCEPTANCE') != '1'):
        pytest.skip('requires explicit ownership of disposable Redis 7.4.10 at 6389/0')
    yield
