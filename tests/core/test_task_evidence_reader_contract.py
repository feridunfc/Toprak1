
from __future__ import annotations

import json
from pathlib import Path

import pytest

from hfa.dag.schema import DagRedisKey
from hfa_control.api.task_evidence import read_task_evidence


class RecordingReadOnlyRedis:
    def __init__(self) -> None:
        self.values: dict[str, object] = {}
        self.hashes: dict[str, dict[object, object]] = {}
        self.calls: list[tuple[str, str]] = []
        self.mutation_calls: list[str] = []

    async def get(self, key: str):
        self.calls.append(("get", key))
        return self.values.get(key)

    async def hgetall(self, key: str):
        self.calls.append(("hgetall", key))
        return self.hashes.get(key, {})

    async def set(self, *args, **kwargs):
        self.mutation_calls.append("set")
        raise AssertionError("read-only evidence reader must not SET")

    async def hset(self, *args, **kwargs):
        self.mutation_calls.append("hset")
        raise AssertionError("read-only evidence reader must not HSET")

    async def delete(self, *args, **kwargs):
        self.mutation_calls.append("delete")
        raise AssertionError("read-only evidence reader must not DELETE")

    async def xadd(self, *args, **kwargs):
        self.mutation_calls.append("xadd")
        raise AssertionError("read-only evidence reader must not XADD")

    async def xack(self, *args, **kwargs):
        self.mutation_calls.append("xack")
        raise AssertionError("read-only evidence reader must not XACK")


@pytest.mark.asyncio
async def test_read_task_evidence_reads_state_meta_and_output_without_mutation() -> None:
    redis = RecordingReadOnlyRedis()
    task_id = "task-evidence-1"

    redis.values[DagRedisKey.task_state(task_id)] = b"done"
    redis.values[DagRedisKey.task_output(task_id)] = json.dumps(
        {"done": True, "answer": 42},
        sort_keys=True,
    ).encode()
    redis.hashes[DagRedisKey.task_meta(task_id)] = {
        b"run_id": b"run-evidence-1",
        b"tenant_id": b"tenant-a",
        b"worker_instance_id": b"worker-a",
        b"scheduler_epoch": b"sched-1",
        b"claim_epoch": b"3",
        b"completed_at_ms": b"123456",
        b"reason_code": b"completed",
    }

    evidence = await read_task_evidence(redis, task_id)

    assert evidence["task_id"] == task_id
    assert evidence["run_id"] == "run-evidence-1"
    assert evidence["tenant_id"] == "tenant-a"
    assert evidence["found"] is True
    assert evidence["state"] == "done"
    assert evidence["terminal_state"] == "done"
    assert evidence["worker_instance_id"] == "worker-a"
    assert evidence["scheduler_epoch"] == "sched-1"
    assert evidence["claim_epoch"] == "3"
    assert evidence["completed_at_ms"] == "123456"
    assert evidence["reason_code"] == "completed"
    assert evidence["output"] == {"done": True, "answer": 42}
    assert evidence["output_found"] is True

    assert evidence["evidence"]["state_key"] == DagRedisKey.task_state(task_id)
    assert evidence["evidence"]["meta_key"] == DagRedisKey.task_meta(task_id)
    assert evidence["evidence"]["output_key"] == DagRedisKey.task_output(task_id)
    assert evidence["evidence"]["meta_found"] is True
    assert evidence["evidence"]["state_found"] is True

    assert evidence["safety"] == {
        "read_only": True,
        "redis_mutation_attempted": False,
        "runtime_mutation_attempted": False,
        "retry_or_reclaim_attempted": False,
        "production_ready_claim": False,
    }
    assert redis.mutation_calls == []


@pytest.mark.asyncio
async def test_read_task_evidence_missing_task_is_explicit_unknown_without_mutation() -> None:
    redis = RecordingReadOnlyRedis()

    evidence = await read_task_evidence(redis, "missing-task")

    assert evidence["found"] is False
    assert evidence["state"] == "unknown"
    assert evidence["tenant_id"] == ""
    assert evidence["worker_instance_id"] == ""
    assert evidence["scheduler_epoch"] == ""
    assert evidence["claim_epoch"] == ""
    assert evidence["output"] is None
    assert evidence["output_found"] is False
    assert redis.mutation_calls == []


def test_task_evidence_reader_static_contract_is_read_only() -> None:
    source = Path("hfa-control/src/hfa_control/api/task_evidence.py").read_text(encoding="utf-8")

    assert "await redis.get(" in source
    assert "await redis.hgetall(" in source
    assert ".set(" not in source
    assert ".hset(" not in source
    assert ".delete(" not in source
    assert ".xadd(" not in source
    assert ".xack(" not in source
    assert ".xclaim(" not in source
    assert "production_ready_claim" in source


def test_task_evidence_endpoint_is_operator_only_and_read_only() -> None:
    source = Path("hfa-control/src/hfa_control/api/router.py").read_text(encoding="utf-8")
    marker = '@router.get("/tasks/{task_id}/evidence")'
    assert marker in source

    body = source[source.index(marker):]

    assert "_require_operator(x_cp_auth)" in body
    assert "read_task_evidence(request.app.state.redis, task_id)" in body
    assert "await request.app.state.redis.xadd" not in body
    assert "await request.app.state.redis.xack" not in body
    assert "reclaim" in body.lower()
    assert "production" in body.lower()
