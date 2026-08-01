from __future__ import annotations

import json
import pytest

from hfa.config.keys import RedisKey
from hfa_control.run_status_read_model import (
    DurableRunStatusResultReader,
    ExternalRunStatus,
    ReadCompleteness,
    ReadFreshness,
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
    assert view.completeness is ReadCompleteness.UNKNOWN_RUN


@pytest.mark.asyncio
async def test_admitted_maps_to_queued():
    redis = FakeRedis(); seed(redis, state="admitted")
    view = await DurableRunStatusResultReader(redis).read("r1")
    assert view.status is ExternalRunStatus.QUEUED
    assert view.completeness is ReadCompleteness.RUNNING_WITHOUT_RESULT


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
    assert view.completeness is ReadCompleteness.TERMINAL_WITH_RESULT


@pytest.mark.asyncio
async def test_failed_with_error():
    redis = FakeRedis(); seed(redis, state="failed", meta={"result_event_id":"e1"}, result={"run_id":"r1","status":"failed","payload":"{}","completed_at":"2.0","result_event_id":"e1","error":"aggregate_task_failure"})
    view = await DurableRunStatusResultReader(redis).read("r1")
    assert view.status is ExternalRunStatus.FAILED
    assert view.error.code == "EXECUTOR_FAILED"
    assert view.error.message == "Task execution failed."
    assert view.error.retryable is False
    assert view.error.summary == "Task execution failed."


@pytest.mark.asyncio
async def test_terminal_without_result():
    redis = FakeRedis(); seed(redis, state="done")
    view = await DurableRunStatusResultReader(redis).read("r1")
    assert view.completeness is ReadCompleteness.TERMINAL_WITHOUT_RESULT
    assert view.completeness_reason == "RESULT_NOT_PRESENT"


@pytest.mark.asyncio
async def test_expected_result_absence_is_not_proven_expiry():
    redis = FakeRedis(); seed(redis, state="done", meta={"result_event_id":"e1"})
    view = await DurableRunStatusResultReader(redis).read("r1")
    assert view.completeness is ReadCompleteness.TERMINAL_WITHOUT_RESULT
    assert view.completeness_reason == "RESULT_MISSING_OR_EXPIRED"


@pytest.mark.asyncio
async def test_result_before_terminal_conflicts():
    redis = FakeRedis(); seed(redis, state="running", result={"status":"done","payload":"{}","result_event_id":"e1"})
    view = await DurableRunStatusResultReader(redis).read("r1")
    assert view.completeness is ReadCompleteness.CONFLICTING_EVIDENCE
    assert "TERMINAL_RESULT_BEFORE_TERMINAL_STATE" in view.issues


@pytest.mark.asyncio
async def test_done_with_failed_result_conflicts():
    redis = FakeRedis(); seed(redis, state="done", result={"status":"failed","payload":"{}","result_event_id":"e1"})
    view = await DurableRunStatusResultReader(redis).read("r1")
    assert view.completeness is ReadCompleteness.CONFLICTING_EVIDENCE
    assert "RUN_STATE_RESULT_STATUS_MISMATCH" in view.issues


@pytest.mark.asyncio
async def test_failed_with_done_result_conflicts():
    redis = FakeRedis(); seed(redis, state="failed", result={"status":"done","payload":"{}","result_event_id":"e1"})
    view = await DurableRunStatusResultReader(redis).read("r1")
    assert view.completeness is ReadCompleteness.CONFLICTING_EVIDENCE


@pytest.mark.asyncio
async def test_malformed_payload_is_incomplete_not_conflicting():
    redis = FakeRedis(); seed(redis, state="done", result={"status":"done","payload":"{","result_event_id":"e1"})
    view = await DurableRunStatusResultReader(redis).read("r1")
    assert view.completeness is ReadCompleteness.EVIDENCE_INCOMPLETE
    assert "RUN_RESULT_PAYLOAD_INVALID" in view.issues


@pytest.mark.asyncio
async def test_wrong_key_type_is_incomplete():
    redis = FakeRedis(); redis.hashes[RedisKey.run_state("r1")] = {"bad":"type"}
    view = await DurableRunStatusResultReader(redis).read("r1")
    assert view.completeness is ReadCompleteness.EVIDENCE_INCOMPLETE
    assert "RUN_STATE_WRONG_TYPE" in view.issues


@pytest.mark.asyncio
async def test_unknown_state_is_preserved_and_incomplete():
    redis = FakeRedis(); seed(redis, state="cancelled")
    view = await DurableRunStatusResultReader(redis).read("r1")
    assert view.status is ExternalRunStatus.UNKNOWN
    assert view.internal_state == "cancelled"
    assert view.completeness is ReadCompleteness.EVIDENCE_INCOMPLETE
    assert "RUN_STATE_UNKNOWN" in view.issues


@pytest.mark.asyncio
async def test_expiring_freshness():
    redis = FakeRedis(); seed(redis, ttl=30)
    view = await DurableRunStatusResultReader(redis).read("r1")
    assert view.freshness is ReadFreshness.EXPIRING


@pytest.mark.asyncio
async def test_terminal_missing_result_can_have_current_surviving_evidence():
    redis = FakeRedis(); seed(redis, state="done", meta={"result_event_id":"e1"}, ttl=3600)
    view = await DurableRunStatusResultReader(redis).read("r1")
    assert view.freshness is ReadFreshness.CURRENT
    assert view.result_ttl_seconds == -2
    assert view.completeness is ReadCompleteness.TERMINAL_WITHOUT_RESULT


@pytest.mark.asyncio
async def test_surviving_ttl_values_are_exposed():
    redis = FakeRedis(); seed(redis, ttl=120)
    view = await DurableRunStatusResultReader(redis).read("r1")
    assert view.state_ttl_seconds == 120
    assert view.meta_ttl_seconds == 120
    assert view.result_ttl_seconds == -2


@pytest.mark.asyncio
async def test_deterministic_serialization():
    redis = FakeRedis(); seed(redis, state="done", meta={"result_event_id":"e1"}, result={"status":"done","payload":json.dumps({"b":2,"a":1}),"result_event_id":"e1"})
    reader = DurableRunStatusResultReader(redis)
    first = (await reader.read("r1")).to_canonical_json()
    second = (await reader.read("r1")).to_canonical_json()
    assert first == second


@pytest.mark.asyncio
async def test_schema_contains_no_false_projection_or_revision_fields():
    redis = FakeRedis(); seed(redis)
    data = (await DurableRunStatusResultReader(redis).read("r1")).to_dict()
    assert "projection_revision" not in data
    assert "canonical_revision" not in data
    assert "source_transition_id" not in data


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
