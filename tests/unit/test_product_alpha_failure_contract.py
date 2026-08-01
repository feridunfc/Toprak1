from __future__ import annotations

import json

import pytest

from hfa.config.keys import RedisKey
from hfa.dag.schema import DagRedisKey
from hfa_control.run_status_read_model import (
    DurableRunStatusResultReader,
    ExternalRunStatus,
    public_executor_failure_payload,
    public_executor_failure_view,
)
from hfa_control.service import ControlPlaneService
from hfa_control.user_facing_run_result import (
    TaskOutputStatus,
    UserFacingSingleTaskRunReader,
)


SECRET = (
    "redis://admin:super-secret@private-host:6379 "
    "C:\\private\\provider\\trace.py "
    "provider_body={\"api_key\":\"sk-live-secret\"}"
)


class FakeRedis:
    def __init__(self) -> None:
        self.values = {}
        self.hashes = {}
        self.sets = {}
        self.ttls = {}
        self.read_calls = []
        self.write_calls = 0

    async def type(self, key):
        self.read_calls.append(("type", key))
        if key in self.values:
            return "string"
        if key in self.hashes:
            return "hash"
        if key in self.sets:
            return "set"
        return "none"

    async def get(self, key):
        self.read_calls.append(("get", key))
        return self.values.get(key)

    async def hgetall(self, key):
        self.read_calls.append(("hgetall", key))
        return dict(self.hashes.get(key, {}))

    async def smembers(self, key):
        self.read_calls.append(("smembers", key))
        return set(self.sets.get(key, set()))

    async def ttl(self, key):
        self.read_calls.append(("ttl", key))
        return self.ttls.get(key, -2)

    async def set(self, *args, **kwargs):
        self.write_calls += 1
        raise AssertionError("public reader attempted SET")

    async def hset(self, *args, **kwargs):
        self.write_calls += 1
        raise AssertionError("public reader attempted HSET")


def seed_run(
    redis: FakeRedis,
    *,
    run_id: str = "run-tenant-r1",
    state: str = "failed",
    error: str = SECRET,
) -> None:
    redis.values[RedisKey.run_state(run_id)] = state
    redis.hashes[RedisKey.run_meta(run_id)] = {
        "run_id": run_id,
        "tenant_id": "tenant",
        "state": state,
        "result_event_id": "event-1",
    }
    redis.hashes[RedisKey.run_result(run_id)] = {
        "run_id": run_id,
        "tenant_id": "tenant",
        "status": state,
        "payload": json.dumps(
            {
                "task_count": 1,
                "done_count": 1 if state == "done" else 0,
                "failed_count": 1 if state == "failed" else 0,
                "skipped_count": 0,
            }
        ),
        "result_event_id": "event-1",
        "completed_at": "2.0",
        "error": error,
    }
    for key in (
        RedisKey.run_state(run_id),
        RedisKey.run_meta(run_id),
        RedisKey.run_result(run_id),
    ):
        redis.ttls[key] = 3600


def seed_task(
    redis: FakeRedis,
    *,
    run_id: str = "run-tenant-r1",
    task_id: str = "task-1",
    state: str = "failed",
    output=SECRET,
) -> None:
    redis.sets[DagRedisKey.run_tasks(run_id)] = {task_id}
    redis.hashes[DagRedisKey.task_meta(task_id)] = {
        "task_id": task_id,
        "run_id": run_id,
        "tenant_id": "tenant",
    }
    redis.values[DagRedisKey.task_state(task_id)] = state
    if output is not None:
        redis.values[DagRedisKey.task_output(task_id)] = (
            output
            if isinstance(output, str)
            else json.dumps(output)
        )


def test_public_executor_failure_contract_is_exact():
    assert public_executor_failure_payload() == {
        "code": "EXECUTOR_FAILED",
        "message": "Task execution failed.",
        "retryable": False,
    }
    view = public_executor_failure_view()
    assert view.summary == view.message
    assert "summary" not in view.to_dict()


@pytest.mark.asyncio
async def test_failed_run_raw_error_is_sanitized():
    redis = FakeRedis()
    seed_run(redis)

    view = await DurableRunStatusResultReader(redis).read(
        "run-tenant-r1"
    )

    assert view.status is ExternalRunStatus.FAILED
    assert view.error is not None
    assert view.error.to_dict() == public_executor_failure_payload()
    assert SECRET not in view.to_canonical_json()


@pytest.mark.asyncio
async def test_failed_run_without_internal_error_still_has_public_error():
    redis = FakeRedis()
    seed_run(redis, error="")

    view = await DurableRunStatusResultReader(redis).read(
        "run-tenant-r1"
    )

    assert view.error is not None
    assert view.error.code == "EXECUTOR_FAILED"


@pytest.mark.asyncio
async def test_completed_run_never_exposes_stray_internal_error():
    redis = FakeRedis()
    seed_run(redis, state="done", error=SECRET)

    view = await DurableRunStatusResultReader(redis).read(
        "run-tenant-r1"
    )

    assert view.status is ExternalRunStatus.COMPLETED
    assert view.error is None
    assert SECRET not in view.to_canonical_json()


@pytest.mark.asyncio
async def test_failed_task_output_is_public_and_never_read():
    redis = FakeRedis()
    seed_run(redis)
    seed_task(redis, output={"provider_body": SECRET})
    output_key = DagRedisKey.task_output("task-1")

    view = await UserFacingSingleTaskRunReader(redis).read(
        "run-tenant-r1"
    )

    assert view.task_output_status is TaskOutputStatus.AVAILABLE
    assert view.task_output == public_executor_failure_payload()
    assert ("type", output_key) not in redis.read_calls
    assert ("get", output_key) not in redis.read_calls
    assert SECRET not in view.to_canonical_json()
    assert redis.write_calls == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("raw_output", ["", "{", SECRET])
async def test_failed_task_malformed_or_secret_output_is_ignored(
    raw_output,
):
    redis = FakeRedis()
    seed_run(redis)
    seed_task(redis, output=raw_output)

    view = await UserFacingSingleTaskRunReader(redis).read(
        "run-tenant-r1"
    )

    assert view.task_output == public_executor_failure_payload()


@pytest.mark.asyncio
async def test_successful_task_output_remains_actual_output():
    redis = FakeRedis()
    seed_run(redis, state="done", error="")
    seed_task(
        redis,
        state="done",
        output={"output_text": "ALPHA_SUCCESS"},
    )

    view = await UserFacingSingleTaskRunReader(redis).read(
        "run-tenant-r1"
    )

    assert view.task_output == {"output_text": "ALPHA_SUCCESS"}


@pytest.mark.asyncio
async def test_legacy_result_service_sanitizes_internal_error(
    monkeypatch,
):
    raw_result = {
        "run_id": "run-tenant-r1",
        "tenant_id": "tenant",
        "status": "failed",
        "error": SECRET,
        "payload": {},
    }

    class StateStoreProbe:
        def __init__(self, redis) -> None:
            self.redis = redis

        async def get_result(self, run_id):
            return dict(raw_result)

    import hfa.runtime.state_store as state_store_module

    monkeypatch.setattr(
        state_store_module,
        "StateStore",
        StateStoreProbe,
    )
    service = object.__new__(ControlPlaneService)
    service._redis = object()

    result = await service.get_run_result("run-tenant-r1")

    assert result is not None
    assert result["error"] == "Task execution failed."
    assert SECRET not in repr(result)
