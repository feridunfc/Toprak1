from __future__ import annotations

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
from hfa_control.task_claim_authority import (
    TASK_CLAIM_DUPLICATE_STATUS,
    TASK_CLAIM_PROJECTED_STATUS,
)
from hfa_worker.consumer import CONSUMER_GROUP
from hfa_worker.main import WorkerService
from hfa_worker.redis_utils import ensure_consumer_group
from hfa_worker.task_executor import TaskExecutionResult, TaskExecutor

pytestmark = pytest.mark.asyncio


@dataclass
class _ExecutionResult:
    status: str = "done"
    payload: dict | None = None
    error: str = ""


class _Executor:
    async def execute(self, event):
        return _ExecutionResult(payload={"run_id": event.run_id})


class _StopAfterCanonicalClaim(RuntimeError):
    pass


class _ClaimSentinelExecutor(TaskExecutor):
    def __init__(self) -> None:
        self.calls = []

    async def execute(self, ctx) -> TaskExecutionResult:
        self.calls.append(ctx)
        raise _StopAfterCanonicalClaim("stop after canonical claim")


def _context(command: AuthorityCommand) -> AuthorityEntryContext:
    return AuthorityEntryContext(
        authenticated_writer_id=f"test/{command.operation_type.value}",
        allowed_operations=frozenset({command.operation_type}),
        target_aggregate_identity_sha256=(
            command.aggregate_identity.sha256
        ),
        fence_required=(
            command.operation_type is not OperationType.TASK_ADMIT
        ),
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
    await _commit(
        store,
        admit,
        revision=0,
        state=None,
        committed_at_ms=100,
    )

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
        operation_id=(
            f"task-dispatch:v1:{identity.sha256}:attempt:1"
        ),
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
        "scheduler_id": "scheduler-84-7b",
    }
    await redis_client.hset(
        DagRedisKey.worker_reservation(worker_id),
        mapping=reservation,
    )
    await redis_client.hset(
        DagRedisKey.task_reservation_owner(task_id),
        mapping=reservation,
    )
    await redis_client.expire(
        DagRedisKey.worker_reservation(worker_id), 60
    )
    await redis_client.expire(
        DagRedisKey.task_reservation_owner(task_id), 60
    )
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


def _decode(value) -> str:
    if isinstance(value, bytes):
        return value.decode("utf-8")
    return str(value)


async def _canonical_worker(
    redis_client,
    *,
    worker_id: str,
    executor: _ClaimSentinelExecutor,
) -> WorkerService:
    worker = WorkerService(
        redis_client,
        {
            "production": True,
            "worker_id": worker_id,
            "worker_group": "group-84-7b",
            "region": "integration",
            "shards": [0],
            "capacity": 1,
            "executor": _Executor(),
            "task_executor": executor,
            "canonical_task_admit_binding": True,
            "canonical_task_dispatch_binding": True,
            "canonical_task_claim_binding": True,
        },
    )
    assert worker.canonical_task_claim_binding_enabled is True
    assert worker._consumer._canonical_task_claim_binding_enabled is True
    assert worker._task_claim_manager is not None
    assert worker._task_claim_manager._canonical_task_claim_binding_enabled is True
    assert worker._dag_lua is not None
    await worker._dag_lua.initialise()
    return worker


async def _forbid_legacy_claim_path(worker: WorkerService) -> None:
    async def allow_legacy_precheck(*args, **kwargs):
        return True

    async def forbidden_try_claim(*args, **kwargs):
        raise AssertionError(
            "IdempotencyGuard.try_claim_and_mark_running must be unreachable "
            "when canonical TASK_CLAIM routing is enabled"
        )

    worker._consumer._guard.should_execute = allow_legacy_precheck
    worker._consumer._guard.try_claim_and_mark_running = forbidden_try_claim


@pytest.mark.integration
async def test_task_requested_worker_stream_routes_to_canonical_task_claim(
    redis_client,
) -> None:
    task_id = "worker-stream-task-requested-84-7b"
    run_id = "run-worker-stream-task-requested-84-7b"
    tenant_id = "tenant-84-7b"
    worker_id = "worker-task-requested-84-7b"
    scheduler_epoch = "epoch-task-requested-84-7b"
    stream = RedisKey.stream_shard(0)

    store, identity = await _seed_canonical_dispatch(
        redis_client,
        task_id=task_id,
        run_id=run_id,
        tenant_id=tenant_id,
        worker_id=worker_id,
        scheduler_epoch=scheduler_epoch,
    )
    sentinel = _ClaimSentinelExecutor()
    worker = await _canonical_worker(
        redis_client,
        worker_id=worker_id,
        executor=sentinel,
    )
    await _forbid_legacy_claim_path(worker)
    await ensure_consumer_group(
        redis_client,
        stream,
        CONSUMER_GROUP,
        start_id="0",
        mkstream=True,
    )

    event = RunRequestedEvent(
        event_type="TaskRequested",
        task_id=task_id,
        run_id=run_id,
        tenant_id=tenant_id,
        agent_type="claim-route-proof",
        payload={"prompt": "stop after claim"},
        scheduler_epoch=scheduler_epoch,
    )
    await redis_client.xadd(stream, serialize_event(event))
    msg_id, data = await _read_one(
        redis_client,
        stream=stream,
        worker_id=worker_id,
    )

    await worker._consumer._process_message(
        _decode(msg_id),
        data,
        stream,
        0,
    )

    assert len(sentinel.calls) == 1
    assert sentinel.calls[0].task_id == task_id
    assert sentinel.calls[0].run_id == run_id

    snapshot = await store.get_aggregate_snapshot(identity)
    assert snapshot is not None
    assert snapshot.revision == 3
    assert snapshot.state == "running"
    assert await redis_client.get(DagRedisKey.task_state(task_id)) == "running"


@pytest.mark.integration
async def test_canonical_claim_enabled_run_requested_never_uses_idempotency_guard(
    redis_client,
) -> None:
    task_id = "worker-stream-run-requested-84-7b"
    run_id = "run-worker-stream-run-requested-84-7b"
    tenant_id = "tenant-run-requested-84-7b"
    worker_id = "worker-run-requested-84-7b"
    scheduler_epoch = "epoch-run-requested-84-7b"
    stream = RedisKey.stream_shard(0)

    store, identity = await _seed_canonical_dispatch(
        redis_client,
        task_id=task_id,
        run_id=run_id,
        tenant_id=tenant_id,
        worker_id=worker_id,
        scheduler_epoch=scheduler_epoch,
    )
    sentinel = _ClaimSentinelExecutor()
    worker = await _canonical_worker(
        redis_client,
        worker_id=worker_id,
        executor=sentinel,
    )
    await _forbid_legacy_claim_path(worker)
    await ensure_consumer_group(
        redis_client,
        stream,
        CONSUMER_GROUP,
        start_id="0",
        mkstream=True,
    )

    event = RunRequestedEvent(
        event_type="RunRequested",
        task_id=task_id,
        run_id=run_id,
        tenant_id=tenant_id,
        agent_type="claim-route-proof",
        payload={"prompt": "legacy guard must be unreachable"},
        scheduler_epoch=scheduler_epoch,
    )
    await redis_client.xadd(stream, serialize_event(event))
    msg_id, data = await _read_one(
        redis_client,
        stream=stream,
        worker_id=worker_id,
    )

    await worker._consumer._process_message(
        _decode(msg_id),
        data,
        stream,
        0,
    )

    assert len(sentinel.calls) == 1
    snapshot = await store.get_aggregate_snapshot(identity)
    assert snapshot is not None
    assert snapshot.revision == 3
    assert snapshot.state == "running"
