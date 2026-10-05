"""F04.1 oracles on an explicitly owned disposable Redis 7.4.10."""
import os
from uuid import uuid4

import pytest
import pytest_asyncio
from redis.asyncio import Redis

from hfa.authority import AggregateType, CanonicalAggregateIdentity
from hfa.config.keys import RedisKey
from hfa.dag.schema import DagRedisKey
from tests.unit.test_f03_terminal_ordering import seed_domain
from tests.unit.test_f04_task_projection_proof import (
    projection_domain, semantic_f03_oracles,
    test_lost_or_changed_proof_cannot_write,
    test_invalid_own_proof_prevents_even_truth_conflict_event,
    test_true_exact_duplicate_stays_read_only_when_own_proof_moves,
    test_restart_projects_once_then_duplicate_is_zero_effect,
    test_exact_projected_duplicate_is_read_only_through_coordinator,
    test_prior_terminal_missing_projection_is_blocked_after_run_terminal,
    test_run_terminalization_between_projection_guard_read_and_lua_is_read_only,
    test_terminal_projection_drift_is_not_exact_no_op,
)

pytestmark = pytest.mark.integration


@pytest.fixture(scope='session')
def integration_redis_stack():
    if os.environ.get('USE_EXISTING_REDIS') != '1' or os.environ.get('HFA_F04_LIVE_ACCEPTANCE') != '1':
        pytest.skip('requires explicit ownership of disposable Redis 7.4.10 at 6389/0')
    yield


@pytest.fixture(autouse=True)
def flush_redis(integration_redis_stack):
    # No inherited pre-test flush: refuse foreign state instead.
    yield


@pytest_asyncio.fixture
async def domain(integration_redis_stack):
    redis = Redis.from_url('redis://127.0.0.1:6389/0', decode_responses=True)
    try:
        assert (await redis.info('server'))['redis_version'] == '7.4.10'
        assert await redis.dbsize() == 0, 'requires empty exclusive disposable DB'
        values = await seed_domain(redis, prefix='f04-' + uuid4().hex)
        _, store, run, task = values
        projected_run = CanonicalAggregateIdentity(aggregate_type=AggregateType.RUN,
                                                    run_id=run.run_id + '-projection')
        projected_task = CanonicalAggregateIdentity(aggregate_type=AggregateType.TASK,
                            run_id=projected_run.run_id, task_id=task.task_id + '-projection')
        owned = set()
        for identity in (run, task, projected_run, projected_task):
            owned.update(store.keyspace(identity.sha256).commit_keys())
        for identity in (run, projected_run):
            owned.add(RedisKey.run_state(identity.run_id))
        for factory in (DagRedisKey.task_state, DagRedisKey.task_meta,
                        DagRedisKey.task_output, DagRedisKey.task_children):
            owned.add(factory(projected_task.task_id))
        for factory in (DagRedisKey.task_state, DagRedisKey.task_remaining_deps,
                        DagRedisKey.task_ready_emitted):
            owned.add(factory(projected_task.task_id + '-child'))
        owned.update({DagRedisKey.tenant_ready_queue('tenant'),
                      DagRedisKey.task_running_zset('tenant'),
                      'hfa:f03:delivery:' + projected_task.task_id})
        try:
            yield values
        finally:
            actual = {key async for key in redis.scan_iter()}
            assert actual <= owned, 'unexpected keys: retain evidence, do not clean unknown state'
            if actual:
                await redis.delete(*actual)
            assert await redis.dbsize() == 0
    finally:
        await redis.aclose()
