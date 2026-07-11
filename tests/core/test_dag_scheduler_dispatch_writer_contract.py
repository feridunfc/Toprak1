from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

import hfa_control.dag_scheduler_bridge as bridge
from hfa.dag.schema import DagTaskDispatchInput
from hfa_control.dag_lua import TaskDispatchCommitResult


def _dispatch_input() -> DagTaskDispatchInput:
    return DagTaskDispatchInput(
        task_id="task-meta-77",
        run_id="run-meta-77",
        tenant_id="tenant-77",
        worker_id="",
        worker_group="group-meta-77",
        agent_type="default",
        shard=1,
        priority=5,
        admitted_at=1000.0,
        scheduled_at=2000.0,
        payload_json='{"sprint":77}',
    )


def _writer():
    writer_type = getattr(
        bridge,
        "DagSchedulerDispatchWriter",
        None,
    )

    assert writer_type is not None

    ready_queue = AsyncMock()
    dag_lua = AsyncMock()

    writer = writer_type(
        ready_queue=ready_queue,
        dag_lua=dag_lua,
    )

    return writer, ready_queue, dag_lua


def test_canonical_scheduler_dispatch_writer_exists() -> None:
    assert hasattr(
        bridge,
        "DagSchedulerDispatchWriter",
    )


@pytest.mark.asyncio
async def test_writer_propagates_incoming_identity_worker_and_epoch() -> None:
    writer, ready_queue, dag_lua = _writer()

    ready_queue.rebuild_dispatch_input.return_value = (
        _dispatch_input()
    )

    committed = TaskDispatchCommitResult(
        committed=True,
        status="committed",
        task_id="task-77",
        reason="ready",
    )
    dag_lua.task_dispatch_commit.return_value = committed

    result = await writer(
        task_id="task-77",
        worker_id="worker-77",
        scheduler_epoch="epoch-77",
        dispatch_payload={
            "task_id": "task-77",
            "run_id": "run-message-77",
            "tenant_id": "tenant-77",
            "worker_group": "group-77",
            "shard": 3,
            "region": "eu-77",
        },
    )

    assert result is committed

    ready_queue.rebuild_dispatch_input.assert_awaited_once()

    committed_input = (
        dag_lua.task_dispatch_commit.await_args.args[0]
    )

    # Incoming scheduler identity must reach Lua unchanged.
    # It must not be replaced by identity read from task_meta.
    assert committed_input.task_id == "task-77"
    assert committed_input.run_id == "run-message-77"
    assert committed_input.run_id != "run-meta-77"

    assert committed_input.worker_id == "worker-77"
    assert committed_input.scheduler_epoch == "epoch-77"
    assert committed_input.worker_group == "group-77"
    assert committed_input.shard == 3


@pytest.mark.asyncio
async def test_writer_missing_task_meta_fails_without_lua_commit() -> None:
    writer, ready_queue, dag_lua = _writer()

    ready_queue.rebuild_dispatch_input.return_value = None

    result = await writer(
        task_id="task-77",
        worker_id="worker-77",
        scheduler_epoch="epoch-77",
        dispatch_payload={
            "run_id": "run-77",
            "tenant_id": "tenant-77",
        },
    )

    assert result.committed is False
    assert result.status == "missing_task_meta"
    assert result.task_id == "task-77"

    dag_lua.task_dispatch_commit.assert_not_awaited()


@pytest.mark.asyncio
async def test_writer_rejects_missing_explicit_task_id() -> None:
    writer, ready_queue, dag_lua = _writer()

    result = await writer(
        task_id="",
        worker_id="worker-77",
        scheduler_epoch="epoch-77",
        dispatch_payload={
            "run_id": "run-77",
            "tenant_id": "tenant-77",
        },
    )

    assert result.committed is False
    assert result.status == "dispatch_task_id_missing"

    ready_queue.rebuild_dispatch_input.assert_not_awaited()
    dag_lua.task_dispatch_commit.assert_not_awaited()


@pytest.mark.asyncio
async def test_writer_rejects_missing_explicit_run_id() -> None:
    writer, ready_queue, dag_lua = _writer()

    result = await writer(
        task_id="task-77",
        worker_id="worker-77",
        scheduler_epoch="epoch-77",
        dispatch_payload={
            "tenant_id": "tenant-77",
        },
    )

    assert result.committed is False
    assert result.status == "dispatch_run_id_missing"

    ready_queue.rebuild_dispatch_input.assert_not_awaited()
    dag_lua.task_dispatch_commit.assert_not_awaited()


@pytest.mark.asyncio
async def test_writer_rejects_missing_scheduler_epoch() -> None:
    writer, ready_queue, dag_lua = _writer()

    result = await writer(
        task_id="task-77",
        worker_id="worker-77",
        scheduler_epoch="",
        dispatch_payload={
            "run_id": "run-77",
            "tenant_id": "tenant-77",
        },
    )

    assert result.committed is False
    assert result.status == "dispatch_scheduler_epoch_missing"

    ready_queue.rebuild_dispatch_input.assert_not_awaited()
    dag_lua.task_dispatch_commit.assert_not_awaited()
