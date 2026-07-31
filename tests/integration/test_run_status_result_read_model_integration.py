from __future__ import annotations

import json
import pytest

from hfa.config.keys import RedisKey
from hfa_control.run_status_read_model import (
    DurableRunStatusResultReader,
    ExternalRunStatus,
    ReadCompleteness,
)
from hfa_control.service import ControlPlaneService
from hfa_control.models import ControlPlaneConfig


async def seed_terminal(real_redis, run_id: str, state: str = "done"):
    event_id = f"event:{run_id}"
    await real_redis.set(RedisKey.run_state(run_id), state, ex=86400)
    await real_redis.hset(RedisKey.run_meta(run_id), mapping={
        "run_id": run_id,
        "tenant_id": "tenant-1",
        "state": state,
        "finalized_at_ms": "2000",
        "finalization_operation": "RUN_TERMINATE",
        "finalization_source": "terminal_task_aggregate",
        "task_count": "1",
        "done_count": "1" if state == "done" else "0",
        "failed_count": "1" if state == "failed" else "0",
        "skipped_count": "0",
        "result_event_id": event_id,
    })
    await real_redis.expire(RedisKey.run_meta(run_id), 86400)
    await real_redis.hset(RedisKey.run_result(run_id), mapping={
        "run_id": run_id,
        "tenant_id": "tenant-1",
        "status": state,
        "payload": json.dumps({"task_count": 1}, sort_keys=True, separators=(",", ":")),
        "error": "" if state == "done" else "aggregate_task_failure",
        "completed_at": "2.0",
        "finalized_at_ms": "2000",
        "finalization_operation": "RUN_TERMINATE",
        "finalization_source": "terminal_task_aggregate",
        "task_count": "1",
        "done_count": "1" if state == "done" else "0",
        "failed_count": "1" if state == "failed" else "0",
        "skipped_count": "0",
        "result_event_id": event_id,
    })
    await real_redis.expire(RedisKey.run_result(run_id), 86400)


@pytest.mark.asyncio
async def test_real_redis_running_view(real_redis, unique_ns):
    run_id = f"{unique_ns}:running"
    await real_redis.set(RedisKey.run_state(run_id), "running", ex=86400)
    await real_redis.hset(RedisKey.run_meta(run_id), mapping={"run_id": run_id, "state": "running"})
    await real_redis.expire(RedisKey.run_meta(run_id), 86400)
    view = await DurableRunStatusResultReader(real_redis).read(run_id)
    assert view.status is ExternalRunStatus.RUNNING
    assert view.completeness is ReadCompleteness.RUNNING_WITHOUT_RESULT


@pytest.mark.asyncio
async def test_real_redis_sprint83_2_completed_result_consumable(real_redis, unique_ns):
    run_id = f"{unique_ns}:done"
    await seed_terminal(real_redis, run_id, "done")
    view = await DurableRunStatusResultReader(real_redis).read(run_id)
    assert view.status is ExternalRunStatus.COMPLETED
    assert view.result.payload == {"task_count": 1}
    assert view.task_counts == {"task_count": 1, "done_count": 1, "failed_count": 0, "skipped_count": 0}


@pytest.mark.asyncio
async def test_real_redis_failed_result_consumable(real_redis, unique_ns):
    run_id = f"{unique_ns}:failed"
    await seed_terminal(real_redis, run_id, "failed")
    view = await DurableRunStatusResultReader(real_redis).read(run_id)
    assert view.status is ExternalRunStatus.FAILED
    assert view.error.code == "aggregate_task_failure"


@pytest.mark.asyncio
async def test_real_redis_missing_terminal_result_is_not_proven_expired(real_redis, unique_ns):
    run_id = f"{unique_ns}:missing-result"
    await real_redis.set(RedisKey.run_state(run_id), "done", ex=86400)
    await real_redis.hset(RedisKey.run_meta(run_id), mapping={"run_id":run_id,"state":"done","result_event_id":"e"})
    await real_redis.expire(RedisKey.run_meta(run_id), 86400)
    view = await DurableRunStatusResultReader(real_redis).read(run_id)
    assert view.completeness is ReadCompleteness.TERMINAL_WITHOUT_RESULT
    assert view.completeness_reason == "RESULT_MISSING_OR_EXPIRED"
    assert view.result_ttl_seconds == -2


@pytest.mark.asyncio
async def test_control_plane_combined_query(real_redis, unique_ns):
    run_id = f"{unique_ns}:service"
    await seed_terminal(real_redis, run_id, "done")
    service = ControlPlaneService(real_redis, ControlPlaneConfig(instance_id="status-query-test"))
    result = await service.get_run_status_result(run_id)
    assert result["schema_version"] == 1
    assert result["status"] == "COMPLETED"
    assert result["completeness"] == "TERMINAL_WITH_RESULT"
    assert "projection_revision" not in result


@pytest.mark.asyncio
async def test_query_does_not_mutate_redis(real_redis, unique_ns):
    run_id = f"{unique_ns}:readonly"
    await seed_terminal(real_redis, run_id, "done")
    before = await real_redis.dump(RedisKey.run_result(run_id))
    before_state = await real_redis.dump(RedisKey.run_state(run_id))
    await DurableRunStatusResultReader(real_redis).read(run_id)
    assert await real_redis.dump(RedisKey.run_result(run_id)) == before
    assert await real_redis.dump(RedisKey.run_state(run_id)) == before_state
