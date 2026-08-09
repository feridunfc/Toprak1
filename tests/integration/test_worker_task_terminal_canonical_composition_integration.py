from __future__ import annotations

import json
from dataclasses import dataclass

import pytest

from hfa.authority import (
    AggregateType,
    AuthorityCommand,
    AuthorityDecisionCode,
    AuthorityEntryContext,
    CanonicalAggregateIdentity,
    OperationType,
    RedisAuthorityCommitStatus,
    RedisCanonicalAuthorityStore,
    evaluate_authority_commit,
)
from hfa.config.keys import RedisKey
from hfa.dag.schema import DagRedisKey
from hfa.events.codec import serialize_event
from hfa.events.schema import RunRequestedEvent
from hfa_control.task_terminal_authority import (
    TASK_TERMINAL_PROJECTION_PENDING_STATUS,
    TaskTerminalAuthorityError,
)
from hfa_worker.consumer import CONSUMER_GROUP
from hfa_worker.main import WorkerService
from hfa_worker.redis_utils import ensure_consumer_group
from hfa_worker.task_executor import TaskExecutionResult, TaskExecutor

pytestmark = [pytest.mark.asyncio, pytest.mark.integration]


@dataclass
class _ExecutionResult:
    status: str = "done"
    payload: dict | None = None
    error: str = ""


class _Executor:
    async def execute(self, event):
        return _ExecutionResult(payload={"run_id": event.run_id})


class _TaskExecutor(TaskExecutor):
    def __init__(self, result: TaskExecutionResult) -> None:
        self._result = result
        self.calls = []

    async def execute(self, ctx) -> TaskExecutionResult:
        self.calls.append(ctx)
        return self._result


def _text(value) -> str:
    if isinstance(value, bytes):
        return value.decode("utf-8")
    return "" if value is None else str(value)


def _context(command: AuthorityCommand) -> AuthorityEntryContext:
    return AuthorityEntryContext(
        authenticated_writer_id=f"test/{command.operation_type.value}",
        allowed_operations=frozenset({command.operation_type}),
        target_aggregate_identity_sha256=command.aggregate_identity.sha256,
        fence_required=command.operation_type is not OperationType.TASK_ADMIT,
        fence_valid=True,
    )


async def _commit(
    store: RedisCanonicalAuthorityStore,
    command: AuthorityCommand,
    *,
    revision: int,
    state: str | None,
    committed_at_ms: int,
):
    evaluation = evaluate_authority_commit(
        context=_context(command),
        command=command,
        current_revision=revision,
        current_state=state,
        receipt_probe=None,
        committed_at_ms=committed_at_ms,
        correlation_id=None,
    )
    assert evaluation.decision.code is AuthorityDecisionCode.ACCEPTED
    assert evaluation.commit_plan is not None
    result = await store.commit(evaluation.commit_plan)
    assert result.status is RedisAuthorityCommitStatus.COMMITTED
    return evaluation.commit_plan.record


async def _seed_canonical_dispatch(
    redis_client,
    *,
    task_id: str,
    run_id: str,
    tenant_id: str,
    worker_id: str,
    scheduler_epoch: str,
):
    store = RedisCanonicalAuthorityStore(redis_client)
    await store.initialise()
    identity = CanonicalAggregateIdentity(
        aggregate_type=AggregateType.TASK,
        run_id=run_id,
        task_id=task_id,
    )

    admit = AuthorityCommand(
        aggregate_identity=identity,
        operation_type=OperationType.TASK_ADMIT,
        operation_id=f"task-admit:v1:{identity.sha256}",
        expected_revision=0,
        intended_previous_state=None,
        intended_next_state="ready",
        authoritative_payload={"task_id": task_id, "run_id": run_id},
        authoritative_metadata_changes={
            "task_id": task_id,
            "run_id": run_id,
            "tenant_id": tenant_id,
        },
        requested_child_effects={},
        requested_projection_intents=(
            {
                "kind": "READY_QUEUE_IF_READY",
                "tenant_id": tenant_id,
                "task_id": task_id,
            },
        ),
    )
    await _commit(store, admit, revision=0, state=None, committed_at_ms=100)

    metadata = {
        "task_id": task_id,
        "run_id": run_id,
        "tenant_id": tenant_id,
        "worker_id": worker_id,
        "scheduler_epoch": scheduler_epoch,
        "dispatch_attempt": 1,
        "scheduled_at_ms": 200,
    }
    dispatch = AuthorityCommand(
        aggregate_identity=identity,
        operation_type=OperationType.TASK_DISPATCH,
        operation_id=f"task-dispatch:v1:{identity.sha256}:attempt:1",
        expected_revision=1,
        intended_previous_state="ready",
        intended_next_state="scheduled",
        authoritative_payload=metadata,
        authoritative_metadata_changes=metadata,
        requested_child_effects={
            "worker_reservation": {
                "worker_id": worker_id,
                "task_id": task_id,
                "scheduler_epoch": scheduler_epoch,
            }
        },
        requested_projection_intents=(
            {"kind": "CONTROL_NOTIFICATION", "stream": "control"},
            {"kind": "TASK_REQUEST_MESSAGE", "stream": "shard"},
        ),
    )
    record = await _commit(
        store,
        dispatch,
        revision=1,
        state="ready",
        committed_at_ms=200,
    )

    await redis_client.set(DagRedisKey.task_state(task_id), "scheduled")
    await redis_client.hset(
        DagRedisKey.task_meta(task_id),
        mapping={
            "task_id": task_id,
            "run_id": run_id,
            "tenant_id": tenant_id,
            "scheduler_epoch": scheduler_epoch,
            "dispatch_attempt": "1",
            "dispatch_worker_id": worker_id,
            "claim_epoch": "0",
            "canonical_transition_id": record.transition_id,
            "canonical_record_hash": record.canonical_record_hash,
            "canonical_command_hash": record.canonical_command_hash,
            "canonical_revision": str(record.to_revision),
            "canonical_operation_id": record.operation_id,
        },
    )
    await redis_client.set(RedisKey.run_state(run_id), "running")
    await redis_client.zadd(
        DagRedisKey.task_scheduled_zset(tenant_id),
        {task_id: 200.0},
    )
    reservation = {
        "worker_id": worker_id,
        "task_id": task_id,
        "scheduler_epoch": scheduler_epoch,
        "reserved_at_ms": "190",
        "scheduler_id": "scheduler-84-7c2",
    }
    await redis_client.hset(
        DagRedisKey.worker_reservation(worker_id),
        mapping=reservation,
    )
    await redis_client.hset(
        DagRedisKey.task_reservation_owner(task_id),
        mapping=reservation,
    )
    await redis_client.expire(DagRedisKey.worker_reservation(worker_id), 60)
    await redis_client.expire(DagRedisKey.task_reservation_owner(task_id), 60)
    return store, identity


async def _read_one(redis_client, *, stream: str, worker_id: str):
    messages = await redis_client.xreadgroup(
        groupname=CONSUMER_GROUP,
        consumername=worker_id,
        streams={stream: ">"},
        count=1,
        block=100,
    )
    assert messages, "expected one worker stream message"
    _stream_name, entries = messages[0]
    assert len(entries) == 1
    return entries[0]


async def _canonical_worker(
    redis_client,
    *,
    worker_id: str,
    executor: _TaskExecutor,
) -> WorkerService:
    worker = WorkerService(
        redis_client,
        {
            "production": True,
            "worker_id": worker_id,
            "worker_group": "group-84-7c2",
            "region": "integration",
            "shards": [0],
            "capacity": 1,
            "executor": _Executor(),
            "task_executor": executor,
            "canonical_task_admit_binding": True,
            "canonical_task_dispatch_binding": True,
            "canonical_task_claim_binding": True,
            "canonical_task_terminal_binding": True,
        },
    )
    assert worker.canonical_task_claim_binding_enabled is True
    assert worker.canonical_task_terminal_binding_enabled is True
    assert worker._dag_lua is not None
    assert worker._task_terminal_authority_binding is not None
    assert worker._task_terminal_completion_gateway is not None
    assert worker._task_consumer is not None
    assert (
        worker._task_consumer._completion_manager
        is worker._task_terminal_completion_gateway
    )
    await worker._dag_lua.initialise()
    await worker._task_terminal_authority_binding.initialise()
    return worker


async def _forbid_legacy_terminal(worker: WorkerService) -> None:
    async def forbidden_task_complete(*args, **kwargs):
        raise AssertionError(
            "DagLua.task_complete must be unreachable when canonical TASK terminal binding is enabled"
        )

    assert worker._dag_lua is not None
    worker._dag_lua.task_complete = forbidden_task_complete


async def _enqueue(
    redis_client,
    *,
    stream: str,
    event_type: str,
    task_id: str,
    run_id: str,
    tenant_id: str,
    worker_id: str,
    scheduler_epoch: str,
):
    await ensure_consumer_group(
        redis_client,
        stream,
        CONSUMER_GROUP,
        start_id="0",
        mkstream=True,
    )
    event = RunRequestedEvent(
        event_type=event_type,
        task_id=task_id,
        run_id=run_id,
        tenant_id=tenant_id,
        agent_type="terminal-composition-proof",
        payload={"prompt": "terminal composition"},
        scheduler_epoch=scheduler_epoch,
    )
    await redis_client.xadd(stream, serialize_event(event))
    return await _read_one(redis_client, stream=stream, worker_id=worker_id)


async def _assert_acked(redis_client, *, stream: str) -> None:
    pending = await redis_client.xpending_range(
        stream,
        CONSUMER_GROUP,
        min="-",
        max="+",
        count=10,
    )
    assert pending == []


async def _assert_unacked(redis_client, *, stream: str) -> None:
    pending = await redis_client.xpending_range(
        stream,
        CONSUMER_GROUP,
        min="-",
        max="+",
        count=10,
    )
    assert len(pending) == 1


@pytest.mark.integration
async def test_task_requested_success_uses_canonical_complete_then_acks(redis_client) -> None:
    task_id = "c2-success-task"
    run_id = "c2-success-run"
    tenant_id = "c2-success-tenant"
    worker_id = "c2-success-worker"
    scheduler_epoch = "c2-success-epoch"
    child = "c2-success-child"
    stream = RedisKey.stream_shard(0)

    store, identity = await _seed_canonical_dispatch(
        redis_client,
        task_id=task_id,
        run_id=run_id,
        tenant_id=tenant_id,
        worker_id=worker_id,
        scheduler_epoch=scheduler_epoch,
    )
    await redis_client.sadd(DagRedisKey.task_children(task_id), child)
    await redis_client.set(DagRedisKey.task_state(child), "pending")
    await redis_client.set(DagRedisKey.task_remaining_deps(child), 1)

    executor = _TaskExecutor(
        TaskExecutionResult(ok=True, output={"answer": 42})
    )
    worker = await _canonical_worker(
        redis_client,
        worker_id=worker_id,
        executor=executor,
    )
    await _forbid_legacy_terminal(worker)
    msg_id, data = await _enqueue(
        redis_client,
        stream=stream,
        event_type="TaskRequested",
        task_id=task_id,
        run_id=run_id,
        tenant_id=tenant_id,
        worker_id=worker_id,
        scheduler_epoch=scheduler_epoch,
    )

    await worker._consumer._process_message(_text(msg_id), data, stream, 0)

    snapshot = await store.get_aggregate_snapshot(identity)
    assert snapshot is not None
    assert snapshot.revision == 4
    assert snapshot.state == "done"
    probe = await store.load_receipt_probe(identity, snapshot.operation_id)
    assert probe is not None and probe.canonical_store_record is not None
    assert probe.canonical_store_record.operation_type == OperationType.TASK_COMPLETE.value
    assert _text(await redis_client.get(DagRedisKey.task_state(task_id))) == "done"
    output = json.loads(_text(await redis_client.get(DagRedisKey.task_output(task_id))))
    assert output == {"answer": 42}
    assert _text(await redis_client.get(DagRedisKey.task_state(child))) == "ready"
    assert _text(await redis_client.get(DagRedisKey.task_remaining_deps(child))) == "0"
    assert await redis_client.zscore(DagRedisKey.tenant_ready_queue(tenant_id), child) is not None
    assert _text(await redis_client.get(RedisKey.run_state(run_id))) == "running"
    await _assert_acked(redis_client, stream=stream)


@pytest.mark.integration
async def test_run_requested_failure_uses_canonical_fail_then_acks(redis_client) -> None:
    task_id = "c2-fail-task"
    run_id = "c2-fail-run"
    tenant_id = "c2-fail-tenant"
    worker_id = "c2-fail-worker"
    scheduler_epoch = "c2-fail-epoch"
    child = "c2-fail-child"
    stream = RedisKey.stream_shard(0)

    store, identity = await _seed_canonical_dispatch(
        redis_client,
        task_id=task_id,
        run_id=run_id,
        tenant_id=tenant_id,
        worker_id=worker_id,
        scheduler_epoch=scheduler_epoch,
    )
    await redis_client.sadd(DagRedisKey.task_children(task_id), child)
    await redis_client.set(DagRedisKey.task_state(child), "ready")
    await redis_client.zadd(
        DagRedisKey.tenant_ready_queue(tenant_id),
        {child: 1.0},
    )

    executor = _TaskExecutor(
        TaskExecutionResult(ok=False, output={"ignored": True}, error="boom")
    )
    worker = await _canonical_worker(
        redis_client,
        worker_id=worker_id,
        executor=executor,
    )
    await _forbid_legacy_terminal(worker)
    msg_id, data = await _enqueue(
        redis_client,
        stream=stream,
        event_type="RunRequested",
        task_id=task_id,
        run_id=run_id,
        tenant_id=tenant_id,
        worker_id=worker_id,
        scheduler_epoch=scheduler_epoch,
    )

    await worker._consumer._process_message(_text(msg_id), data, stream, 0)

    snapshot = await store.get_aggregate_snapshot(identity)
    assert snapshot is not None
    assert snapshot.revision == 4
    assert snapshot.state == "failed"
    probe = await store.load_receipt_probe(identity, snapshot.operation_id)
    assert probe is not None and probe.canonical_store_record is not None
    assert probe.canonical_store_record.operation_type == OperationType.TASK_FAIL.value
    assert _text(await redis_client.get(DagRedisKey.task_state(task_id))) == "failed"
    assert await redis_client.get(DagRedisKey.task_output(task_id)) is None
    assert _text(await redis_client.get(DagRedisKey.task_state(child))) == "blocked_by_failure"
    assert await redis_client.zscore(DagRedisKey.tenant_ready_queue(tenant_id), child) is None
    assert _text(await redis_client.get(RedisKey.run_state(run_id))) == "running"
    await _assert_acked(redis_client, stream=stream)


@pytest.mark.integration
async def test_terminal_authority_commit_projection_failure_remains_unacked(
    redis_client,
    monkeypatch,
) -> None:
    task_id = "c2-projection-pending-task"
    run_id = "c2-projection-pending-run"
    tenant_id = "c2-projection-pending-tenant"
    worker_id = "c2-projection-pending-worker"
    scheduler_epoch = "c2-projection-pending-epoch"
    stream = RedisKey.stream_shard(0)

    store, identity = await _seed_canonical_dispatch(
        redis_client,
        task_id=task_id,
        run_id=run_id,
        tenant_id=tenant_id,
        worker_id=worker_id,
        scheduler_epoch=scheduler_epoch,
    )
    executor = _TaskExecutor(
        TaskExecutionResult(ok=True, output={"durable": True})
    )
    worker = await _canonical_worker(
        redis_client,
        worker_id=worker_id,
        executor=executor,
    )
    await _forbid_legacy_terminal(worker)
    binding = worker._task_terminal_authority_binding
    assert binding is not None and binding.projection_manager is not None

    async def fail_projection(_projection):
        raise TaskTerminalAuthorityError(
            status=TASK_TERMINAL_PROJECTION_PENDING_STATUS,
            detail="deterministic C2 projection failure",
            canonical_commit_durable=True,
        )

    monkeypatch.setattr(binding.projection_manager, "project", fail_projection)
    msg_id, data = await _enqueue(
        redis_client,
        stream=stream,
        event_type="TaskRequested",
        task_id=task_id,
        run_id=run_id,
        tenant_id=tenant_id,
        worker_id=worker_id,
        scheduler_epoch=scheduler_epoch,
    )

    await worker._consumer._process_message(_text(msg_id), data, stream, 0)

    snapshot = await store.get_aggregate_snapshot(identity)
    assert snapshot is not None
    assert snapshot.revision == 4
    assert snapshot.state == "done"
    probe = await store.load_receipt_probe(identity, snapshot.operation_id)
    assert probe is not None and probe.canonical_store_record is not None
    assert probe.canonical_store_record.operation_type == OperationType.TASK_COMPLETE.value
    assert _text(await redis_client.get(DagRedisKey.task_state(task_id))) == "running"
    assert await redis_client.get(DagRedisKey.task_output(task_id)) is None
    assert _text(await redis_client.get(RedisKey.run_state(run_id))) == "running"
    await _assert_unacked(redis_client, stream=stream)
