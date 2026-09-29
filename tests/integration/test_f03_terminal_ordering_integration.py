"""Operator-owned Redis 7.4.10 execution of the F03 semantic oracles.

Requires the explicitly authorized, empty acceptance-test Redis at 6389/0.
Never starts Docker or provisions a Redis domain. Not executed by the worker.
"""
from __future__ import annotations

import os
from uuid import uuid4

import pytest
import pytest_asyncio
from redis.asyncio import Redis

from hfa.authority import AggregateType, CanonicalAggregateIdentity
from hfa.config.keys import RedisKey
from hfa.dag.schema import DagRedisKey
from tests.unit.test_f03_terminal_ordering import (
    projection_domain,
    seed_domain,
    test_exact_projected_duplicate_is_read_only_through_coordinator,
    test_missing_parent_cannot_fall_back_to_runtime_state,
    test_new_terminal_after_run_terminal_is_rejected_without_task_writes,
    test_parent_proof_deleted_after_python_read_rejects_without_task_write,
    test_prior_terminal_missing_projection_is_blocked_after_run_terminal,
    test_run_commit_between_prevalidation_and_task_lua_cannot_slip_through,
    test_run_terminalization_between_projection_guard_read_and_lua_is_read_only,
    test_task_first_then_run_preserves_receipt_first_exact_commit_duplicate,
    test_terminal_projection_drift_is_not_exact_no_op,
)

pytestmark = pytest.mark.integration


@pytest.fixture(scope="session")
def integration_redis_stack():
    # Override the parent fixture: no automatic container start/stop is allowed.
    if os.environ.get("USE_EXISTING_REDIS") != "1" or os.environ.get("HFA_F03_LIVE_ACCEPTANCE") != "1":
        pytest.fail("operator must set USE_EXISTING_REDIS=1 and HFA_F03_LIVE_ACCEPTANCE=1")
    yield


@pytest_asyncio.fixture
async def domain(integration_redis_stack):
    redis = Redis.from_url("redis://127.0.0.1:6389/0", decode_responses=False)
    try:
        assert (await redis.info("server"))["redis_version"] == "7.4.10"
        assert await redis.dbsize() == 0, "F03 requires an empty, exclusive disposable test DB"
        prefix = "f03-" + uuid4().hex
        values = await seed_domain(redis, prefix=prefix)
        _, store, run, task = values
        projection_run = CanonicalAggregateIdentity(
            aggregate_type=AggregateType.RUN, run_id=f"{run.run_id}-projection",
        )
        projection_task = CanonicalAggregateIdentity(
            aggregate_type=AggregateType.TASK, run_id=projection_run.run_id,
            task_id=f"{task.task_id}-projection",
        )
        owned = set()
        for identity in (run, task, projection_run, projection_task):
            owned.update(key.encode() for key in store.keyspace(identity.sha256).commit_keys())
        for identity in (run, projection_run):
            owned.add(RedisKey.run_state(identity.run_id).encode())
        for factory in (DagRedisKey.task_state, DagRedisKey.task_meta, DagRedisKey.task_output):
            owned.add(factory(projection_task.task_id).encode())
        owned.update({
            DagRedisKey.tenant_ready_queue("tenant").encode(),
            DagRedisKey.task_running_zset("tenant").encode(),
            f"hfa:f03:delivery:{projection_task.task_id}".encode(),
        })
        try:
            yield values
        finally:
            actual = {key async for key in redis.scan_iter()}
            assert actual <= owned, "unexpected keys: retain evidence and stop; do not clean unknown state"
            if actual:
                await redis.delete(*actual)
            assert await redis.dbsize() == 0
    finally:
        await redis.aclose()
