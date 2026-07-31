from __future__ import annotations

import json
import pytest

from hfa.config.keys import RedisKey
from hfa_control.run_status_read_model import (
    DurableRunStatusResultReader,
    ExternalRunStatus,
    ProjectionCompleteness,
    ProjectionFreshness,
)


class FakeRedis:
    def __init__(self):
        self.values = {}
        self.hashes = {}
        self.ttls = {}
        self.write_calls = 0

    async def type(self, key):
        if key in self.values:
            return "string"
        if key in self.hashes:
            return "hash"
        return "none"

    async def get(self, key):
        return self.values.get(key)

    async def hgetall(self, key):
        return dict(self.hashes.get(key, {}))

    async def ttl(self, key):
        return self.ttls.get(key, -2)

    async def set(self, *args, **kwargs):
        self.write_calls += 1
        raise AssertionError("read model attempted a write")

    async def hset(self, *args, **kwargs):
        self.write_calls += 1
        raise AssertionError("read model attempted a write")


def seed(redis, run_id="r1", state="running", meta=None, result=None, ttl=3600):
    redis.values[RedisKey.run_state(run_id)] = state
    redis.hashes[RedisKey.run_meta(run_id)] = {"run_id": run_id, "state": state, **(meta or {})}
    redis.ttls[RedisKey.run_state(run_id)] = ttl
    redis.ttls[RedisKey.run_meta(run_id)] = ttl
    if result is not None:
        redis.hashes[RedisKey.run_result(run_id)] = result
        redis.ttls[RedisKey.run_result(run_id)] = ttl


@pytest.mark.asyncio
async def test_unknown_run():
    view = await DurableRunStatusResultReader(FakeRedis()).read("missing")
    assert view.status is ExternalRunStatus.UNKNOWN
    assert view.completeness is ProjectionCompleteness.UNKNOWN_RUN


@pytest.mark.asyncio
@pytest.mark.parametrize("state", ["admitted", "queued", "pending", "scheduled", "rescheduled"])
async def test_queued_vocabulary(state):
    redis = FakeRedis(); seed(redis, state=state)
    view = await DurableRunStatusResultReader(redis).read("r1")
    assert view.status is ExternalRunStatus.QUEUED
    assert view.completeness is ProjectionCompleteness.RUNNING_WITHOUT_RESULT


@pytest.mark.asyncio
async def test_running_without_result():
    redis = FakeRedis(); seed(redis)
    view = await DurableRunStatusResultReader(redis).read("r1")
    assert view.status is ExternalRunStatus.RUNNING
    assert view.result is None and not view.terminal


@pytest.mark.asyncio
async def test_completed_with_result():
    redis = FakeRedis(); seed(redis, state="done", meta={"result_event_id":"e1","finalized_at_ms":"2000"}, result={"run_id":"r1","status":"done","payload":"{\"ok\":true}","completed_at":"2.0","result_event_id":"e1","cost_cents":"0","tokens_used":"0","error":""})
    view = await DurableRunStatusResultReader(redis).read("r1")
    assert view.status is ExternalRunStatus.COMPLETED
    assert view.outcome == "SUCCESS"
    assert view.result.payload == {"ok": True}
    assert view.completeness is ProjectionCompleteness.TERMINAL_WITH_RESULT


@pytest.mark.asyncio
async def test_failed_with_error():
    redis = FakeRedis(); seed(redis, state="failed", meta={"result_event_id":"e1"}, result={"run_id":"r1","status":"failed","payload":"{}","completed_at":"2.0","result_event_id":"e1","error":"aggregate_task_failure"})
    view = await DurableRunStatusResultReader(redis).read("r1")
    assert view.status is ExternalRunStatus.FAILED
    assert view.error.code == "aggregate_task_failure"


@pytest.mark.asyncio
async def test_cancelled_mapping():
    redis = FakeRedis(); seed(redis, state="cancelled")
    view = await DurableRunStatusResultReader(redis).read("r1")
    assert view.status is ExternalRunStatus.CANCELLED


@pytest.mark.asyncio
async def test_terminal_without_result():
    redis = FakeRedis(); seed(redis, state="done")
    view = await DurableRunStatusResultReader(redis).read("r1")
    assert view.completeness is ProjectionCompleteness.TERMINAL_WITHOUT_RESULT


@pytest.mark.asyncio
async def test_result_expired_is_distinct():
    redis = FakeRedis(); seed(redis, state="done", meta={"result_event_id":"e1"})
    view = await DurableRunStatusResultReader(redis).read("r1")
    assert view.completeness is ProjectionCompleteness.RESULT_EXPIRED


@pytest.mark.asyncio
async def test_result_before_terminal_conflicts():
    redis = FakeRedis(); seed(redis, state="running", result={"status":"done","payload":"{}","result_event_id":"e1"})
    view = await DurableRunStatusResultReader(redis).read("r1")
    assert view.completeness is ProjectionCompleteness.CONFLICTING_EVIDENCE
    assert "TERMINAL_RESULT_BEFORE_TERMINAL_STATE" in view.conflicts


@pytest.mark.asyncio
async def test_changed_terminal_status_conflicts():
    redis = FakeRedis(); seed(redis, state="done", result={"status":"failed","payload":"{}","result_event_id":"e1"})
    view = await DurableRunStatusResultReader(redis).read("r1")
    assert "RUN_STATE_RESULT_STATUS_MISMATCH" in view.conflicts


@pytest.mark.asyncio
async def test_malformed_payload_conflicts():
    redis = FakeRedis(); seed(redis, state="done", result={"status":"done","payload":"{","result_event_id":"e1"})
    view = await DurableRunStatusResultReader(redis).read("r1")
    assert "RUN_RESULT_PAYLOAD_INVALID" in view.conflicts


@pytest.mark.asyncio
async def test_wrong_key_type_conflicts():
    redis = FakeRedis(); redis.hashes[RedisKey.run_state("r1")] = {"bad":"type"}
    view = await DurableRunStatusResultReader(redis).read("r1")
    assert "RUN_STATE_WRONG_TYPE" in view.conflicts


@pytest.mark.asyncio
async def test_expiring_freshness():
    redis = FakeRedis(); seed(redis, ttl=30)
    view = await DurableRunStatusResultReader(redis).read("r1")
    assert view.freshness is ProjectionFreshness.EXPIRING


@pytest.mark.asyncio
async def test_deterministic_serialization():
    redis = FakeRedis(); seed(redis, state="done", meta={"result_event_id":"e1"}, result={"status":"done","payload":json.dumps({"b":2,"a":1}),"result_event_id":"e1"})
    reader = DurableRunStatusResultReader(redis)
    first = (await reader.read("r1")).to_canonical_json()
    second = (await reader.read("r1")).to_canonical_json()
    assert first == second


@pytest.mark.asyncio
async def test_read_path_performs_zero_writes():
    redis = FakeRedis(); seed(redis)
    await DurableRunStatusResultReader(redis).read("r1")
    assert redis.write_calls == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("bad", ["", "  ", None, 1, True])
async def test_invalid_run_id_rejected(bad):
    with pytest.raises(ValueError):
        await DurableRunStatusResultReader(FakeRedis()).read(bad)
