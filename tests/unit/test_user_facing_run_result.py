from __future__ import annotations

import json

import pytest

from hfa.config.keys import RedisKey
from hfa.dag.schema import DagRedisKey
from hfa_control.user_facing_run_result import (
    TaskOutputStatus,
    UserFacingSingleTaskRunReader,
)


class FakeRedis:
    def __init__(self) -> None:
        self.values = {}
        self.hashes = {}
        self.sets = {}
        self.ttls = {}
        self.write_calls = 0
        self.read_calls = []

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
        raise AssertionError("reader attempted Redis SET")

    async def hset(self, *args, **kwargs):
        self.write_calls += 1
        raise AssertionError("reader attempted Redis HSET")

    async def sadd(self, *args, **kwargs):
        self.write_calls += 1
        raise AssertionError("reader attempted Redis SADD")

    async def delete(self, *args, **kwargs):
        self.write_calls += 1
        raise AssertionError("reader attempted Redis DELETE")

    async def xadd(self, *args, **kwargs):
        self.write_calls += 1
        raise AssertionError("reader attempted Redis XADD")


def seed_run(
    redis,
    *,
    run_id="run-tenant-r1",
    state="done",
    result=True,
):
    redis.values[RedisKey.run_state(run_id)] = state
    redis.hashes[RedisKey.run_meta(run_id)] = {
        "run_id": run_id,
        "state": state,
        "result_event_id": "event-1",
    }
    redis.ttls[RedisKey.run_state(run_id)] = 3600
    redis.ttls[RedisKey.run_meta(run_id)] = 3600
    if result:
        redis.hashes[RedisKey.run_result(run_id)] = {
            "run_id": run_id,
            "status": state,
            "payload": json.dumps(
                {
                    "task_count": 1,
                    "done_count":
                        1 if state == "done" else 0,
                    "failed_count":
                        1 if state == "failed" else 0,
                    "skipped_count": 0,
                }
            ),
            "result_event_id": "event-1",
            "completed_at": "2.0",
            "error":
                ""
                if state == "done"
                else "aggregate_task_failure",
        }
        redis.ttls[RedisKey.run_result(run_id)] = 3600


def seed_task(
    redis,
    *,
    run_id="run-tenant-r1",
    task_id="task-1",
    state="done",
    output=None,
):
    redis.sets[DagRedisKey.run_tasks(run_id)] = {
        task_id
    }
    redis.hashes[DagRedisKey.task_meta(task_id)] = {
        "task_id": task_id,
        "run_id": run_id,
        "tenant_id": "tenant",
    }
    redis.values[DagRedisKey.task_state(task_id)] = (
        state
    )
    if output is not None:
        redis.values[
            DagRedisKey.task_output(task_id)
        ] = json.dumps(output)


@pytest.mark.asyncio
async def test_unknown_run_stops_before_dag_reads():
    redis = FakeRedis()

    view = await UserFacingSingleTaskRunReader(
        redis
    ).read("missing")

    assert (
        view.task_output_status
        is TaskOutputStatus.RUN_UNKNOWN
    )
    assert all(
        "hfa:dag:" not in key
        for _method, key in redis.read_calls
    )


@pytest.mark.asyncio
async def test_nonterminal_run_stops_before_dag_reads():
    redis = FakeRedis()
    seed_run(redis, state="running", result=False)

    view = await UserFacingSingleTaskRunReader(
        redis
    ).read("run-tenant-r1")

    assert (
        view.task_output_status
        is TaskOutputStatus.NOT_TERMINAL
    )
    assert all(
        "hfa:dag:" not in key
        for _method, key in redis.read_calls
    )


@pytest.mark.asyncio
async def test_incomplete_terminal_run_fails_closed():
    redis = FakeRedis()
    seed_run(redis, result=False)

    view = await UserFacingSingleTaskRunReader(
        redis
    ).read("run-tenant-r1")

    assert (
        view.task_output_status
        is TaskOutputStatus.RUN_EVIDENCE_INCOMPLETE
    )


@pytest.mark.asyncio
async def test_missing_task_membership():
    redis = FakeRedis()
    seed_run(redis)

    view = await UserFacingSingleTaskRunReader(
        redis
    ).read("run-tenant-r1")

    assert (
        view.task_output_status
        is TaskOutputStatus.TASK_MEMBERSHIP_MISSING
    )


@pytest.mark.asyncio
async def test_wrong_type_task_membership():
    redis = FakeRedis()
    seed_run(redis)
    redis.values[
        DagRedisKey.run_tasks("run-tenant-r1")
    ] = "task-1"

    view = await UserFacingSingleTaskRunReader(
        redis
    ).read("run-tenant-r1")

    assert (
        view.task_output_status
        is TaskOutputStatus.TASK_MEMBERSHIP_WRONG_TYPE
    )


@pytest.mark.asyncio
async def test_empty_task_membership():
    redis = FakeRedis()
    seed_run(redis)
    redis.sets[
        DagRedisKey.run_tasks("run-tenant-r1")
    ] = set()

    view = await UserFacingSingleTaskRunReader(
        redis
    ).read("run-tenant-r1")

    assert (
        view.task_output_status
        is TaskOutputStatus.TASK_MEMBERSHIP_MISSING
    )


@pytest.mark.asyncio
async def test_multiple_tasks_are_not_single_task_run():
    redis = FakeRedis()
    seed_run(redis)
    redis.sets[
        DagRedisKey.run_tasks("run-tenant-r1")
    ] = {"task-2", "task-1"}

    view = await UserFacingSingleTaskRunReader(
        redis
    ).read("run-tenant-r1")

    assert (
        view.task_output_status
        is TaskOutputStatus.NOT_A_SINGLE_TASK_RUN
    )
    assert view.task_id is None


@pytest.mark.asyncio
async def test_missing_task_meta():
    redis = FakeRedis()
    seed_run(redis)
    redis.sets[
        DagRedisKey.run_tasks("run-tenant-r1")
    ] = {"task-1"}

    view = await UserFacingSingleTaskRunReader(
        redis
    ).read("run-tenant-r1")

    assert (
        view.task_output_status
        is TaskOutputStatus.TASK_IDENTITY_UNAVAILABLE
    )
    assert view.task_id == "task-1"


@pytest.mark.asyncio
async def test_missing_task_identity_fields():
    redis = FakeRedis()
    seed_run(redis)
    seed_task(redis)
    redis.hashes[
        DagRedisKey.task_meta("task-1")
    ].pop("run_id")

    view = await UserFacingSingleTaskRunReader(
        redis
    ).read("run-tenant-r1")

    assert (
        view.task_output_status
        is TaskOutputStatus.TASK_IDENTITY_UNAVAILABLE
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("run_id", "run-other"),
        ("task_id", "task-other"),
    ],
)
async def test_task_identity_conflict(field, value):
    redis = FakeRedis()
    seed_run(redis)
    seed_task(redis)
    redis.hashes[
        DagRedisKey.task_meta("task-1")
    ][field] = value

    view = await UserFacingSingleTaskRunReader(
        redis
    ).read("run-tenant-r1")

    assert (
        view.task_output_status
        is TaskOutputStatus.TASK_IDENTITY_CONFLICT
    )


@pytest.mark.asyncio
async def test_missing_task_state():
    redis = FakeRedis()
    seed_run(redis)
    seed_task(redis)
    redis.values.pop(
        DagRedisKey.task_state("task-1")
    )

    view = await UserFacingSingleTaskRunReader(
        redis
    ).read("run-tenant-r1")

    assert (
        view.task_output_status
        is TaskOutputStatus.TASK_STATE_UNAVAILABLE
    )


@pytest.mark.asyncio
async def test_completed_run_requires_done_task():
    redis = FakeRedis()
    seed_run(redis)
    seed_task(redis, state="running")

    view = await UserFacingSingleTaskRunReader(
        redis
    ).read("run-tenant-r1")

    assert (
        view.task_output_status
        is TaskOutputStatus.TASK_STATE_CONFLICT
    )
    assert view.task_state == "running"


@pytest.mark.asyncio
async def test_failed_run_requires_failed_task():
    redis = FakeRedis()
    seed_run(redis, state="failed")
    seed_task(redis, state="done")

    view = await UserFacingSingleTaskRunReader(
        redis
    ).read("run-tenant-r1")

    assert (
        view.task_output_status
        is TaskOutputStatus.TASK_STATE_CONFLICT
    )


@pytest.mark.asyncio
async def test_terminal_output_missing():
    redis = FakeRedis()
    seed_run(redis)
    seed_task(redis)

    view = await UserFacingSingleTaskRunReader(
        redis
    ).read("run-tenant-r1")

    assert (
        view.task_output_status
        is TaskOutputStatus.TERMINAL_OUTPUT_MISSING
    )


@pytest.mark.asyncio
async def test_task_output_wrong_type():
    redis = FakeRedis()
    seed_run(redis)
    seed_task(redis)
    redis.hashes[
        DagRedisKey.task_output("task-1")
    ] = {"bad": "type"}

    view = await UserFacingSingleTaskRunReader(
        redis
    ).read("run-tenant-r1")

    assert (
        view.task_output_status
        is TaskOutputStatus.OUTPUT_WRONG_TYPE
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("raw", ["", "{"])
async def test_task_output_malformed(raw):
    redis = FakeRedis()
    seed_run(redis)
    seed_task(redis)
    redis.values[
        DagRedisKey.task_output("task-1")
    ] = raw

    view = await UserFacingSingleTaskRunReader(
        redis
    ).read("run-tenant-r1")

    assert (
        view.task_output_status
        is TaskOutputStatus.OUTPUT_MALFORMED
    )


@pytest.mark.asyncio
async def test_valid_terminal_output_is_available():
    redis = FakeRedis()
    seed_run(redis)
    seed_task(
        redis,
        output={
            "output_text": "SPRINT83_6_OK",
            "nested": {"b": 2, "a": 1},
        },
    )

    view = await UserFacingSingleTaskRunReader(
        redis
    ).read("run-tenant-r1")
    data = view.to_dict()

    assert (
        view.task_output_status
        is TaskOutputStatus.AVAILABLE
    )
    assert view.task_id == "task-1"
    assert view.task_state == "done"
    assert view.task_output == {
        "output_text": "SPRINT83_6_OK",
        "nested": {"b": 2, "a": 1},
    }
    assert data["status"] == "COMPLETED"
    assert data["task_output_status"] == "AVAILABLE"
    assert data["task_output"] == view.task_output


@pytest.mark.asyncio
async def test_failed_task_output_is_sanitized():
    redis = FakeRedis()
    seed_run(redis, state="failed")
    seed_task(
        redis,
        state="failed",
        output={"error": "executor_failed"},
    )

    view = await UserFacingSingleTaskRunReader(
        redis
    ).read("run-tenant-r1")

    assert (
        view.task_output_status
        is TaskOutputStatus.AVAILABLE
    )
    assert view.task_output == {
        "code": "EXECUTOR_FAILED",
        "message": "Task execution failed.",
        "retryable": False,
    }


@pytest.mark.asyncio
async def test_serialization_is_deterministic():
    redis = FakeRedis()
    seed_run(redis)
    seed_task(
        redis,
        output={"z": 3, "a": 1},
    )
    reader = UserFacingSingleTaskRunReader(redis)

    first = (
        await reader.read("run-tenant-r1")
    ).to_canonical_json()
    second = (
        await reader.read("run-tenant-r1")
    ).to_canonical_json()

    assert first == second


@pytest.mark.asyncio
async def test_read_path_performs_zero_writes():
    redis = FakeRedis()
    seed_run(redis)
    seed_task(
        redis,
        output={"output_text": "ok"},
    )

    await UserFacingSingleTaskRunReader(
        redis
    ).read("run-tenant-r1")

    assert redis.write_calls == 0
