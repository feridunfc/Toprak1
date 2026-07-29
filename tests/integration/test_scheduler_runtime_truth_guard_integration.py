from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json

import pytest

from hfa.config.keys import RedisKey
from hfa.dag.schema import DagRedisKey
from hfa_control.dag_lua import DagLua

pytestmark = [pytest.mark.asyncio, pytest.mark.integration]


@dataclass
class Dispatch:
    task_id: str = "task-truth-1"
    run_id: str = "run-truth-1"
    tenant_id: str = "tenant-truth-1"
    agent_type: str = "agent"
    worker_group: str = "workers"
    shard: int = 0
    priority: int = 5
    admitted_at: int = 1000
    scheduled_at: int = 2000
    scheduler_epoch: str = "epoch-1"
    payload_json: str = "{}"
    trace_parent: str = ""
    trace_state: str = ""
    policy: str = "LEAST_LOADED"
    region: str = "eu"
    scheduled_zset: str = ""
    running_zset: str = ""
    control_stream: str = ""
    shard_stream: str = ""


async def _dag(redis_client) -> DagLua:
    dag = DagLua(redis_client)
    await dag.initialise()
    return dag


async def _seed_dispatchable(redis_client, dispatch: Dispatch) -> None:
    await redis_client.set(DagRedisKey.task_state(dispatch.task_id), "ready")
    await redis_client.hset(
        DagRedisKey.task_meta(dispatch.task_id),
        mapping={"task_id": dispatch.task_id, "run_id": dispatch.run_id},
    )
    await redis_client.zadd(
        DagRedisKey.tenant_ready_queue(dispatch.tenant_id),
        {dispatch.task_id: float(dispatch.admitted_at)},
    )


async def _lifecycle_snapshot(redis_client, dispatch: Dispatch) -> dict:
    return {
        "task_state": await redis_client.get(DagRedisKey.task_state(dispatch.task_id)),
        "task_meta": await redis_client.hgetall(DagRedisKey.task_meta(dispatch.task_id)),
        "ready": await redis_client.zscore(
            DagRedisKey.tenant_ready_queue(dispatch.tenant_id), dispatch.task_id
        ),
        "scheduled": await redis_client.zscore(
            DagRedisKey.task_scheduled_zset(dispatch.tenant_id), dispatch.task_id
        ),
        "running": await redis_client.zscore(
            DagRedisKey.task_running_zset(dispatch.tenant_id), dispatch.task_id
        ),
        "control_len": await redis_client.xlen(RedisKey.stream_control()),
        "shard_len": await redis_client.xlen(RedisKey.stream_shard(dispatch.shard)),
    }


def _length_prefix(value: str) -> str:
    return f"{len(value)}:{value}"


def _conflict_id(
    operation: str,
    run_id: str,
    task_id: str,
    status: str,
    detail_code: str,
    observed_run_state: str,
) -> str:
    material = "".join(
        _length_prefix(value)
        for value in (
            operation,
            run_id,
            task_id,
            status,
            detail_code,
            observed_run_state,
        )
    )
    return hashlib.sha1(material.encode("utf-8")).hexdigest()


async def _single_truth_observation(redis_client) -> tuple[str, dict, dict]:
    index = await redis_client.hgetall(RedisKey.runtime_truth_conflict_index())
    conflict_ids = [key for key in index if key != "__runtime_truth_conflict_count"]
    assert len(conflict_ids) == 1
    conflict_id = conflict_ids[0]
    payload = json.loads(index[conflict_id])
    rows = await redis_client.xrange(RedisKey.runtime_truth_conflict_stream())
    assert len(rows) == 1
    return conflict_id, payload, rows[0][1]


async def _assert_one_truth_observation(redis_client) -> None:
    index = await redis_client.hgetall(RedisKey.runtime_truth_conflict_index())
    assert int(index["__runtime_truth_conflict_count"]) == 1
    assert len(index) == 2
    assert await redis_client.xlen(RedisKey.runtime_truth_conflict_stream()) == 1


async def _assert_blocked_without_lifecycle_mutation(redis_client, dag, dispatch, status):
    before = await _lifecycle_snapshot(redis_client, dispatch)
    result = await dag.task_dispatch_commit(dispatch)
    after = await _lifecycle_snapshot(redis_client, dispatch)
    assert result.committed is False
    assert result.status == status
    assert after == before
    await _assert_one_truth_observation(redis_client)
    return result


@pytest.mark.parametrize("run_state", ["admitted", "queued", "scheduled", "running", "rescheduled"])
async def test_each_known_nonterminal_run_truth_permits_existing_dispatch(redis_client, run_state):
    dispatch = Dispatch(task_id=f"task-{run_state}", run_id=f"run-{run_state}")
    await _seed_dispatchable(redis_client, dispatch)
    await redis_client.set(RedisKey.run_state(dispatch.run_id), run_state)
    dag = await _dag(redis_client)

    result = await dag.task_dispatch_commit(dispatch)

    assert result.committed is True
    assert result.status == "committed"
    assert await redis_client.get(DagRedisKey.task_state(dispatch.task_id)) == "scheduled"
    assert await redis_client.zscore(
        DagRedisKey.tenant_ready_queue(dispatch.tenant_id), dispatch.task_id
    ) is None
    assert await redis_client.zscore(
        DagRedisKey.task_scheduled_zset(dispatch.tenant_id), dispatch.task_id
    ) is not None
    assert await redis_client.xlen(RedisKey.stream_control()) == 1
    assert await redis_client.xlen(RedisKey.stream_shard(dispatch.shard)) == 1
    assert await redis_client.exists(RedisKey.runtime_truth_conflict_index()) == 0


async def test_missing_run_truth_blocks_every_dispatch_mutation_and_records_once(redis_client):
    dispatch = Dispatch(task_id="task-missing", run_id="run-missing")
    await _seed_dispatchable(redis_client, dispatch)
    dag = await _dag(redis_client)
    await _assert_blocked_without_lifecycle_mutation(
        redis_client, dag, dispatch, "run_truth_missing"
    )


@pytest.mark.parametrize("run_state", ["done", "failed", "rejected", "dead_lettered"])
async def test_terminal_run_truth_blocks_every_dispatch_mutation(redis_client, run_state):
    dispatch = Dispatch(task_id=f"task-terminal-{run_state}", run_id=f"run-terminal-{run_state}")
    await _seed_dispatchable(redis_client, dispatch)
    await redis_client.set(RedisKey.run_state(dispatch.run_id), run_state)
    dag = await _dag(redis_client)
    await _assert_blocked_without_lifecycle_mutation(
        redis_client, dag, dispatch, "run_truth_terminal_conflict"
    )


async def test_unknown_run_truth_is_corruption_and_blocks_dispatch(redis_client):
    dispatch = Dispatch(task_id="task-unknown", run_id="run-unknown")
    await _seed_dispatchable(redis_client, dispatch)
    await redis_client.set(RedisKey.run_state(dispatch.run_id), "mystery")
    dag = await _dag(redis_client)
    result = await _assert_blocked_without_lifecycle_mutation(
        redis_client, dag, dispatch, "run_truth_corruption_conflict"
    )
    assert result.reason == "run_state_unknown"


async def test_wrong_type_run_truth_is_corruption_and_blocks_dispatch(redis_client):
    dispatch = Dispatch(task_id="task-wrong-type", run_id="run-wrong-type")
    await _seed_dispatchable(redis_client, dispatch)
    await redis_client.hset(RedisKey.run_state(dispatch.run_id), mapping={"state": "running"})
    dag = await _dag(redis_client)
    result = await _assert_blocked_without_lifecycle_mutation(
        redis_client, dag, dispatch, "run_truth_corruption_conflict"
    )
    assert result.reason == "run_state_key_type_mismatch"


async def test_duplicate_same_truth_conflict_is_deduplicated_first_payload_wins(redis_client):
    dispatch = Dispatch(task_id="task-dedup", run_id="run-dedup", scheduled_at=2000)
    await _seed_dispatchable(redis_client, dispatch)
    dag = await _dag(redis_client)
    first = await dag.task_dispatch_commit(dispatch)
    first_index = await redis_client.hgetall(RedisKey.runtime_truth_conflict_index())

    dispatch.scheduled_at = 3000
    second = await dag.task_dispatch_commit(dispatch)
    second_index = await redis_client.hgetall(RedisKey.runtime_truth_conflict_index())

    assert first.status == second.status == "run_truth_missing"
    assert first_index == second_index
    conflict_id, payload, stream = await _single_truth_observation(redis_client)
    assert payload["operation"] == "TASK_DISPATCH"
    assert stream["operation"] == "TASK_DISPATCH"
    assert conflict_id == _conflict_id(
        "TASK_DISPATCH",
        dispatch.run_id,
        dispatch.task_id,
        "run_truth_missing",
        "run_state_missing",
        "",
    )
    assert conflict_id != _conflict_id(
        "TASK_CLAIM",
        dispatch.run_id,
        dispatch.task_id,
        "run_truth_missing",
        "run_state_missing",
        "",
    )
    await _assert_one_truth_observation(redis_client)


@pytest.mark.parametrize("wrong_truth", ["terminal", "missing", "corrupt", "unknown"])
async def test_wrong_explicit_run_id_is_rejected_before_wrong_run_truth_is_read_or_observed(
    redis_client, wrong_truth
):
    authoritative_run_id = f"run-authoritative-{wrong_truth}"
    dispatch = Dispatch(
        task_id=f"task-identity-mismatch-{wrong_truth}",
        run_id=f"run-explicit-wrong-{wrong_truth}",
    )
    await redis_client.set(DagRedisKey.task_state(dispatch.task_id), "ready")
    await redis_client.hset(
        DagRedisKey.task_meta(dispatch.task_id),
        mapping={"task_id": dispatch.task_id, "run_id": authoritative_run_id},
    )
    await redis_client.zadd(
        DagRedisKey.tenant_ready_queue(dispatch.tenant_id),
        {dispatch.task_id: float(dispatch.admitted_at)},
    )
    wrong_run_key = RedisKey.run_state(dispatch.run_id)
    if wrong_truth == "terminal":
        await redis_client.set(wrong_run_key, "done")
    elif wrong_truth == "corrupt":
        await redis_client.hset(wrong_run_key, mapping={"state": "running"})
    elif wrong_truth == "unknown":
        await redis_client.set(wrong_run_key, "mystery")

    dag = await _dag(redis_client)
    before = await _lifecycle_snapshot(redis_client, dispatch)
    result = await dag.task_dispatch_commit(dispatch)
    after = await _lifecycle_snapshot(redis_client, dispatch)

    assert result.committed is False
    assert result.status == "identity_run_id_mismatch"
    assert result.reason == authoritative_run_id
    assert after == before
    assert await redis_client.exists(RedisKey.runtime_truth_conflict_index()) == 0
    assert await redis_client.exists(RedisKey.runtime_truth_conflict_stream()) == 0


@pytest.mark.parametrize(
    "break_pair,expected_detail",
    [
        ("index_wrong_type", "truth_conflict_index_type_mismatch"),
        ("stream_wrong_type", "truth_conflict_stream_type_mismatch"),
        ("index_only", "truth_conflict_pair_missing_member"),
        ("stream_only", "truth_conflict_pair_missing_member"),
        ("cardinality", "truth_conflict_pair_cardinality_mismatch"),
    ],
)
async def test_unavailable_truth_conflict_store_fails_closed_without_lifecycle_mutation(
    redis_client, break_pair, expected_detail
):
    dispatch = Dispatch(task_id=f"task-pair-{break_pair}", run_id=f"run-pair-{break_pair}")
    await _seed_dispatchable(redis_client, dispatch)
    index = RedisKey.runtime_truth_conflict_index()
    stream = RedisKey.runtime_truth_conflict_stream()
    if break_pair == "index_wrong_type":
        await redis_client.set(index, "wrong")
    elif break_pair == "stream_wrong_type":
        await redis_client.set(stream, "wrong")
    elif break_pair == "index_only":
        await redis_client.hset(index, mapping={"__runtime_truth_conflict_count": "0"})
    elif break_pair == "stream_only":
        await redis_client.xadd(stream, {"seed": "1"})
    else:
        await redis_client.hset(index, mapping={"__runtime_truth_conflict_count": "0"})
        await redis_client.xadd(stream, {"seed": "1"})

    dag = await _dag(redis_client)
    before = await _lifecycle_snapshot(redis_client, dispatch)
    result = await dag.task_dispatch_commit(dispatch)
    after = await _lifecycle_snapshot(redis_client, dispatch)

    assert result.status == "truth_conflict_evidence_store_unavailable"
    assert result.reason == expected_detail
    assert after == before
