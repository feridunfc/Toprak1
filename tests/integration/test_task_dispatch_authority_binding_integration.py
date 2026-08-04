from __future__ import annotations

import pytest

from hfa.config.keys import RedisKey
from hfa.dag.schema import (
    DagRedisKey,
    DagTaskDispatchInput,
    DagTaskSeed,
)
from hfa_control.dag_lua import DagLua
from hfa_control.task_dispatch_authority import (
    TaskDispatchAuthorityBinding,
    TaskDispatchAuthorityConflictError,
    TaskDispatchLegacyStateConflictError,
    TaskDispatchProjectionPendingError,
    build_task_dispatch_command,
)

pytestmark = [
    pytest.mark.asyncio,
    pytest.mark.integration,
]


def seed(**changes) -> DagTaskSeed:
    values = dict(
        task_id="s84-1-task",
        run_id="s84-1-run",
        tenant_id="s84-1-tenant",
        agent_type="default",
        priority=5,
        admitted_at=1000,
        dependency_count=0,
        payload_json='{"x":1}',
        region="eu",
        policy="LEAST_LOADED",
    )
    values.update(changes)
    return DagTaskSeed(**values)


def dispatch(item: DagTaskSeed, **changes):
    values = dict(
        task_id=item.task_id,
        run_id=item.run_id,
        tenant_id=item.tenant_id,
        worker_id="worker-1",
        worker_group="group-1",
        agent_type=item.agent_type,
        shard=2,
        priority=item.priority,
        admitted_at=int(item.admitted_at),
        scheduled_at=2000,
        scheduled_zset=(
            DagRedisKey.task_scheduled_zset(
                item.tenant_id
            )
        ),
        running_zset=(
            DagRedisKey.task_running_zset(
                item.tenant_id
            )
        ),
        control_stream=RedisKey.stream_control(),
        shard_stream=RedisKey.stream_shard(2),
        region=item.region,
        policy=item.policy,
        payload_json=item.payload_json,
        scheduler_epoch="epoch-7",
        attempt=1,
    )
    values.update(changes)
    return DagTaskDispatchInput(**values)


async def admitted_task(redis_client, **changes):
    item = seed(**changes)
    dag = DagLua(
        redis_client,
        canonical_task_admit_binding=True,
        canonical_task_dispatch_binding=True,
    )
    await redis_client.set(
        RedisKey.run_state(item.run_id),
        "admitted",
    )
    admitted = await dag.task_admit(item)
    assert admitted.admitted is True
    assert admitted.ready is True
    return dag, item


async def test_flag_on_dispatch_commits_authority_and_projection(
    redis_client,
):
    dag, item = await admitted_task(redis_client)
    value = dispatch(item)

    result = await dag.task_dispatch_commit(value)

    assert result.committed is True
    assert result.status == "committed"
    assert (
        await redis_client.get(
            DagRedisKey.task_state(item.task_id)
        )
        == "scheduled"
    )
    meta = await redis_client.hgetall(
        DagRedisKey.task_meta(item.task_id)
    )
    assert meta["dispatch_attempt"] == "1"
    assert meta["dispatch_worker_id"] == "worker-1"
    assert meta["canonical_transition_id"]
    assert meta["canonical_record_hash"]
    assert meta["canonical_command_hash"]
    assert meta["canonical_revision"] == "2"

    binding = dag._task_dispatch_authority_binding
    assert binding is not None
    command = build_task_dispatch_command(
        value,
        expected_revision=1,
    )
    snapshot = await binding.store.get_aggregate_snapshot(
        command.aggregate_identity
    )
    assert snapshot is not None
    assert snapshot.revision == 2
    assert snapshot.state == "scheduled"
    assert (
        await redis_client.xlen(value.control_stream)
        == 1
    )
    assert (
        await redis_client.xlen(value.shard_stream)
        == 1
    )


async def test_exact_retry_is_noop_without_stream_reemission(
    redis_client,
):
    dag, item = await admitted_task(
        redis_client,
        task_id="s84-1-retry",
    )
    value = dispatch(item)

    first = await dag.task_dispatch_commit(value)
    control_len = await redis_client.xlen(
        value.control_stream
    )
    shard_len = await redis_client.xlen(
        value.shard_stream
    )
    second = await dag.task_dispatch_commit(
        dispatch(
            item,
            scheduled_at=9999,
        )
    )

    assert first.status == "committed"
    assert second.committed is True
    assert second.status == "already_projected"
    assert (
        await redis_client.xlen(value.control_stream)
        == control_len
    )
    assert (
        await redis_client.xlen(value.shard_stream)
        == shard_len
    )


async def test_same_attempt_changed_worker_conflicts(
    redis_client,
):
    dag, item = await admitted_task(
        redis_client,
        task_id="s84-1-worker-conflict",
    )
    value = dispatch(item)
    await dag.task_dispatch_commit(value)
    before = await redis_client.hgetall(
        DagRedisKey.task_meta(item.task_id)
    )
    control_len = await redis_client.xlen(
        value.control_stream
    )
    shard_len = await redis_client.xlen(
        value.shard_stream
    )

    with pytest.raises(
        TaskDispatchAuthorityConflictError
    ) as raised:
        await dag.task_dispatch_commit(
            dispatch(
                item,
                worker_id="worker-2",
                scheduled_at=3000,
            )
        )

    assert raised.value.status == "IDEMPOTENCY_CONFLICT"
    assert (
        await redis_client.hgetall(
            DagRedisKey.task_meta(item.task_id)
        )
        == before
    )
    assert (
        await redis_client.xlen(value.control_stream)
        == control_len
    )
    assert (
        await redis_client.xlen(value.shard_stream)
        == shard_len
    )


async def test_legacy_task_without_canonical_admit_is_blocked(
    redis_client,
):
    item = seed(task_id="s84-1-legacy")
    await redis_client.set(
        RedisKey.run_state(item.run_id),
        "admitted",
    )
    legacy = DagLua(redis_client)
    admitted = await legacy.task_admit(item)
    assert admitted.admitted is True

    canonical = DagLua(
        redis_client,
        canonical_task_admit_binding=True,
        canonical_task_dispatch_binding=True,
    )
    with pytest.raises(
        TaskDispatchLegacyStateConflictError
    ):
        await canonical.task_dispatch_commit(
            dispatch(item)
        )


async def test_projection_failure_keeps_commit_and_retry_projects(
    redis_client,
    monkeypatch,
):
    dag, item = await admitted_task(
        redis_client,
        task_id="s84-1-projection-retry",
    )
    binding = TaskDispatchAuthorityBinding(
        redis_client,
        dag._task_dispatch_canonical_projection,
    )
    dag._task_dispatch_authority_binding = binding
    original = binding.legacy_dispatch
    calls = 0

    async def flaky(value):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RuntimeError("projection unavailable")
        return await original(value)

    monkeypatch.setattr(
        binding,
        "legacy_dispatch",
        flaky,
    )
    value = dispatch(item)

    with pytest.raises(
        TaskDispatchProjectionPendingError
    ) as raised:
        await dag.task_dispatch_commit(value)

    assert raised.value.canonical_commit_durable is True
    assert raised.value.release_reservation is False
    assert (
        await redis_client.get(
            DagRedisKey.task_state(item.task_id)
        )
        == "ready"
    )

    result = await dag.task_dispatch_commit(
        dispatch(
            item,
            scheduled_at=7777,
        )
    )
    assert result.status == "committed"
    assert (
        await redis_client.get(
            DagRedisKey.task_state(item.task_id)
        )
        == "scheduled"
    )

    command = build_task_dispatch_command(
        value,
        expected_revision=1,
    )
    keyspace = binding.store.keyspace(
        command.aggregate_identity.sha256
    )
    assert (
        await redis_client.hlen(
            keyspace.operation_records
        )
        == 2
    )
    assert (
        await redis_client.hlen(keyspace.receipts)
        == 2
    )
    assert (
        await redis_client.xlen(
            keyspace.transition_log
        )
        == 2
    )


async def test_projection_regression_after_requeue_fails_closed(
    redis_client,
):
    dag, item = await admitted_task(
        redis_client,
        task_id="s84-1-regressed",
    )
    value = dispatch(item)
    await dag.task_dispatch_commit(value)
    control_len = await redis_client.xlen(
        value.control_stream
    )
    shard_len = await redis_client.xlen(
        value.shard_stream
    )

    await redis_client.set(
        DagRedisKey.task_state(item.task_id),
        "ready",
    )
    await redis_client.zadd(
        DagRedisKey.task_ready_queue(item.tenant_id),
        {item.task_id: 1},
    )

    with pytest.raises(
        TaskDispatchProjectionPendingError
    ) as raised:
        await dag.task_dispatch_commit(value)

    assert (
        raised.value.status
        == "canonical_projection_regressed"
    )
    assert raised.value.retry_safe is False
    assert (
        await redis_client.xlen(value.control_stream)
        == control_len
    )
    assert (
        await redis_client.xlen(value.shard_stream)
        == shard_len
    )


async def test_new_attempt_without_canonical_requeue_is_blocked(
    redis_client,
):
    dag, item = await admitted_task(
        redis_client,
        task_id="s84-1-attempt-two",
    )
    await dag.task_dispatch_commit(dispatch(item))
    await redis_client.set(
        DagRedisKey.task_state(item.task_id),
        "ready",
    )

    with pytest.raises(
        TaskDispatchAuthorityConflictError
    ) as raised:
        await dag.task_dispatch_commit(
            dispatch(
                item,
                attempt=2,
                scheduled_at=3000,
            )
        )

    assert (
        raised.value.status
        == "ILLEGAL_STATE_TRANSITION"
    )
