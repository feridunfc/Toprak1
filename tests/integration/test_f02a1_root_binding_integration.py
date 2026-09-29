"""F02A1 production-path oracles on explicitly owned disposable Redis 7.4.10."""
import os

import pytest
import pytest_asyncio
from redis.asyncio import Redis

from tests.unit.test_f02a1_root_binding import (
    alpha,
    test_alpha_run_requires_root_before_writes,
    test_alpha_task_requires_parent,
    test_binding_cannot_change_on_run_create_retry,
    test_bound_parent_enforced_even_on_internal_gateway,
    test_exact_root_duplicate_is_receipt_first_and_read_only,
    test_historical_unbound_run_and_duplicate_preserved,
    test_matching_root_rejects_damaged_parent,
    test_normal_alpha_submission_binds_generated_task_and_seeds_root,
    test_original_blocker_now_rejects_second_task,
    test_parent_change_after_witness_cannot_commit,
)

pytestmark = pytest.mark.integration


@pytest.fixture(scope='session')
def integration_redis_stack():
    if (os.environ.get('USE_EXISTING_REDIS') != '1'
            or os.environ.get('HFA_F02A1_LIVE_ACCEPTANCE') != '1'):
        pytest.skip('requires explicit ownership of disposable Redis 7.4.10 at 6389/0')
    yield


@pytest.fixture(autouse=True)
def flush_redis(integration_redis_stack):
    # Replace the inherited automatic flush: the owned fixture verifies first.
    yield


@pytest_asyncio.fixture
async def root_redis(integration_redis_stack):
    client = Redis.from_url('redis://127.0.0.1:6389/0', decode_responses=True)
    owned = False
    try:
        assert (await client.info('server'))['redis_version'] == '7.4.10'
        assert await client.dbsize() == 0, 'refuse nonempty Redis; exclusive test instance required'
        owned = True
        yield client
    finally:
        if owned:
            await client.flushdb()
            assert await client.dbsize() == 0
        await client.aclose()
