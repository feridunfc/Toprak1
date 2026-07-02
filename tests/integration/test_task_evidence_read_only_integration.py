
from __future__ import annotations

import json

import pytest

from hfa.dag.schema import DagRedisKey
from hfa_control.api.task_evidence import read_task_evidence

pytestmark = pytest.mark.asyncio


async def test_read_task_evidence_real_redis_preserves_runtime_keys(redis_client) -> None:
    task_id = "task-evidence-real-redis-66"

    state_key = DagRedisKey.task_state(task_id)
    meta_key = DagRedisKey.task_meta(task_id)
    output_key = DagRedisKey.task_output(task_id)

    output = {"done": True, "task_id": task_id}
    await redis_client.set(state_key, "done")
    await redis_client.hset(
        meta_key,
        mapping={
            "run_id": "run-evidence-real-redis-66",
            "tenant_id": "tenant-evidence",
            "worker_instance_id": "worker-evidence",
            "scheduler_epoch": "sched-evidence",
            "claim_epoch": "7",
            "completed_at_ms": "987654",
            "reason_code": "completed",
        },
    )
    await redis_client.set(output_key, json.dumps(output, sort_keys=True))

    before_state = await redis_client.get(state_key)
    before_meta = await redis_client.hgetall(meta_key)
    before_output = await redis_client.get(output_key)

    evidence = await read_task_evidence(redis_client, task_id)

    after_state = await redis_client.get(state_key)
    after_meta = await redis_client.hgetall(meta_key)
    after_output = await redis_client.get(output_key)

    assert after_state == before_state
    assert after_meta == before_meta
    assert after_output == before_output

    assert evidence["found"] is True
    assert evidence["state"] == "done"
    assert evidence["terminal_state"] == "done"
    assert evidence["run_id"] == "run-evidence-real-redis-66"
    assert evidence["tenant_id"] == "tenant-evidence"
    assert evidence["worker_instance_id"] == "worker-evidence"
    assert evidence["scheduler_epoch"] == "sched-evidence"
    assert evidence["claim_epoch"] == "7"
    assert evidence["completed_at_ms"] == "987654"
    assert evidence["reason_code"] == "completed"
    assert evidence["output"] == output
    assert evidence["safety"]["read_only"] is True
    assert evidence["safety"]["production_ready_claim"] is False
