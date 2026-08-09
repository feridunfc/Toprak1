from __future__ import annotations

import asyncio
from dataclasses import dataclass

import pytest

import hfa_worker.consumer as consumer_module
import hfa_worker.run_finalizing_runtime as finalizing_module
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
from hfa_control.run_create_authority import (
    RunCreateAuthorityInput,
    build_run_create_command,
    build_run_create_context,
)
from hfa_control.run_terminate_authority import run_terminate_identity
from hfa_control.task_recovery import TaskRecoveryManager
from hfa_control.task_terminal_authority import (
    TASK_TERMINAL_PROJECTION_PENDING_STATUS,
    TaskTerminalAuthorityError,
)
from hfa_worker.consumer import CONSUMER_GROUP
from hfa_worker.main import WorkerService
from hfa_worker.task_executor import TaskExecutionResult, TaskExecutor

pytestmark = [pytest.mark.asyncio, pytest.mark.integration]


@pytest.fixture(autouse=True)
def _forbid_task_requeue(monkeypatch):
    async def forbidden_requeue(*_args, **_kwargs):
        raise AssertionError("TASK_REQUEUE must be unreachable in Sprint 84.7E")

    monkeypatch.setattr(
        TaskRecoveryManager,
        "requeue_stale_task",
        forbidden_requeue,
    )


@dataclass
class _ExecutionResult:
    status: str = "done"
    payload: dict | None = None
    error: str = ""


class _ForbiddenLegacyExecutor:
    product_executor_capability = "executor:deterministic"

    async def execute(self, _event):
        raise AssertionError("legacy BaseExecutor.execute must be unreachable")


class _CountingTaskExecutor(TaskExecutor):
    def __init__(
        self,
        calls: list[str],
        *,
        result: TaskExecutionResult | None = None,
        raise_error: BaseException | None = None,
        forbid: bool = False,
    ) -> None:
        self.calls = calls
        self.result = result or TaskExecutionResult(ok=True, output={"ok": True})
        self.raise_error = raise_error
        self.forbid = forbid

    async def execute(self, ctx):
        if self.forbid:
            raise AssertionError("TASK executor must not run on redelivery")
        self.calls.append(ctx.worker_instance_id)
        if self.raise_error is not None:
            raise self.raise_error
        return self.result


def _text(value) -> str:
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return "" if value is None else str(value)


def _ctx(command: AuthorityCommand) -> AuthorityEntryContext:
    return AuthorityEntryContext(
        authenticated_writer_id=f"test/{command.operation_type.value}",
        allowed_operations=frozenset({command.operation_type}),
        target_aggregate_identity_sha256=command.aggregate_identity.sha256,
        fence_required=command.operation_type is not OperationType.TASK_ADMIT,
        fence_valid=True,
    )


async def _commit(store, command, revision, state, at):
    evaluation = evaluate_authority_commit(
        context=_ctx(command),
        command=command,
        current_revision=revision,
        current_state=state,
        receipt_probe=None,
        committed_at_ms=at,
        correlation_id=None,
    )
    assert evaluation.decision.code is AuthorityDecisionCode.ACCEPTED
    assert evaluation.commit_plan is not None
    persisted = await store.commit(evaluation.commit_plan)
    assert persisted.status is RedisAuthorityCommitStatus.COMMITTED
    return evaluation.commit_plan.record


async def _seed_run(redis_client, *, run_id: str, tenant_id: str):
    store = RedisCanonicalAuthorityStore(redis_client)
    await store.initialise()
    command = build_run_create_command(
        RunCreateAuthorityInput(
            run_id=run_id,
            tenant_id=tenant_id,
            agent_type="research",
            priority=5,
            payload={"prompt": "84.7e"},
            estimated_cost_cents=100,
            preferred_region="eu-west-1",
            preferred_placement="LEAST_LOADED",
            created_at_ms=50,
            control_stream=RedisKey.stream_control(),
        )
    )
    evaluation = evaluate_authority_commit(
        context=build_run_create_context(command),
        command=command,
        current_revision=0,
        current_state=None,
        receipt_probe=None,
        committed_at_ms=50,
        correlation_id=None,
    )
    assert evaluation.commit_plan is not None
    persisted = await store.commit(evaluation.commit_plan)
    assert persisted.status is RedisAuthorityCommitStatus.COMMITTED
    await redis_client.set(RedisKey.run_state(run_id), "running")
    await redis_client.hset(
        RedisKey.run_meta(run_id),
        mapping={"run_id": run_id, "tenant_id": tenant_id, "state": "running"},
    )
    await redis_client.zadd(RedisKey.cp_running(), {run_id: 50})
    return store


async def _seed_dispatch(
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
            {"kind": "READY_QUEUE_IF_READY", "tenant_id": tenant_id, "task_id": task_id},
        ),
    )
    await _commit(store, admit, 0, None, 100)
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
    record = await _commit(store, dispatch, 1, "ready", 200)
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
    await redis_client.zadd(
        DagRedisKey.task_scheduled_zset(tenant_id),
        {task_id: 200.0},
    )
    reservation = {
        "worker_id": worker_id,
        "task_id": task_id,
        "scheduler_epoch": scheduler_epoch,
        "reserved_at_ms": "190",
        "scheduler_id": "scheduler-84-7e",
    }
    await redis_client.hset(
        DagRedisKey.worker_reservation(worker_id), mapping=reservation
    )
    await redis_client.hset(
        DagRedisKey.task_reservation_owner(task_id), mapping=reservation
    )
    await redis_client.expire(DagRedisKey.worker_reservation(worker_id), 60)
    await redis_client.expire(DagRedisKey.task_reservation_owner(task_id), 60)
    await redis_client.sadd(DagRedisKey.run_tasks(run_id), task_id)
    return store, identity


async def _migration_ready(redis_client) -> None:
    await redis_client.hset(
        RedisKey.run_terminal_event_index(),
        mapping={
            "__contract__:schema_version": "1",
            "__contract__:producer_contract_version": "1",
            "__migration__:status": "ready",
            "__migration__:results_stream_key": RedisKey.stream_results(),
            "__migration__:source_history_complete": "1",
        },
    )
    await redis_client.persist(RedisKey.run_terminal_event_index())


async def _worker(
    redis_client,
    *,
    worker_id: str,
    task_executor: TaskExecutor,
    reclaim_idle_ms: int = 60_000,
) -> WorkerService:
    await _migration_ready(redis_client)
    service = WorkerService(
        redis_client,
        {
            "production": True,
            "worker_id": worker_id,
            "worker_group": "group-84-7e",
            "region": "integration",
            "shards": [0],
            "capacity": 1,
            "executor": _ForbiddenLegacyExecutor(),
            "task_executor": task_executor,
            "canonical_task_admit_binding": True,
            "canonical_task_dispatch_binding": True,
            "canonical_task_claim_binding": True,
            "canonical_task_terminal_binding": True,
            "run_termination_binding_enabled": True,
            "reclaim_idle_ms": reclaim_idle_ms,
        },
    )
    await service.start()

    async def no_task(*_args, **_kwargs):
        raise AssertionError("DagLua.task_complete must be unreachable in canonical Profile D")

    async def no_run(*_args, **_kwargs):
        raise AssertionError("legacy run_terminate_from_tasks.lua must be unreachable in canonical Profile D")

    service._dag_lua.task_complete = no_task
    service._run_termination_coordinator._loader.run = no_run
    return service


async def _enqueue(
    redis_client,
    *,
    task_id: str,
    run_id: str,
    tenant_id: str,
    scheduler_epoch: str,
) -> tuple[str, str]:
    stream = RedisKey.stream_shard(0)
    event = RunRequestedEvent(
        event_type="TaskRequested",
        task_id=task_id,
        run_id=run_id,
        tenant_id=tenant_id,
        agent_type="worker-crash-retry-proof",
        payload={"prompt": "84.7e"},
        scheduler_epoch=scheduler_epoch,
    )
    message_id = await redis_client.xadd(stream, serialize_event(event))
    return stream, _text(message_id)


async def _pending(redis_client, stream: str) -> list[dict[str, object]]:
    rows = await redis_client.xpending_range(
        stream,
        CONSUMER_GROUP,
        min="-",
        max="+",
        count=20,
    )
    normalized = []
    for row in rows:
        if isinstance(row, dict):
            normalized.append({
                "message_id": _text(row.get("message_id")),
                "consumer": _text(row.get("consumer")),
                "time_since_delivered": int(row.get("time_since_delivered", 0) or 0),
                "times_delivered": int(row.get("times_delivered", 0) or 0),
            })
        else:
            normalized.append({
                "message_id": _text(row[0]),
                "consumer": _text(row[1]),
                "time_since_delivered": int(row[2]),
                "times_delivered": int(row[3]),
            })
    return normalized


async def _wait_until(check, *, timeout: float = 5.0):
    deadline = asyncio.get_running_loop().time() + timeout
    last = None
    while asyncio.get_running_loop().time() < deadline:
        last = await check()
        if last:
            return last
        await asyncio.sleep(0.02)
    raise AssertionError(f"timed out waiting for condition; last={last!r}")


async def _wait_pending(redis_client, stream: str, count: int):
    async def check():
        rows = await _pending(redis_client, stream)
        return rows if len(rows) == count else None
    if count == 0:
        async def empty_check():
            rows = await _pending(redis_client, stream)
            return True if not rows else None
        return await _wait_until(empty_check)
    return await _wait_until(check)


async def _wait_task_state(redis_client, task_id: str, state: str):
    async def check():
        observed = _text(await redis_client.get(DagRedisKey.task_state(task_id)))
        return observed == state
    return await _wait_until(check)


async def _wait_run_state(redis_client, run_id: str, state: str):
    async def check():
        observed = _text(await redis_client.get(RedisKey.run_state(run_id)))
        return observed == state
    return await _wait_until(check)


async def _close(service: WorkerService) -> None:
    await service.close(drain_timeout=0.1)


def _terminal_operation_id(identity: CanonicalAggregateIdentity) -> str:
    return f"task-terminal:v1:{identity.sha256}:claim:1"


async def test_a_claim_commit_projection_failure_restart_executes_once_and_finishes(redis_client, monkeypatch):
    task_id, run_id, tenant_id = "e-a-task", "e-a-run", "e-a-tenant"
    worker_id, epoch = "e-a-worker", "e-a-epoch"
    run_store = await _seed_run(redis_client, run_id=run_id, tenant_id=tenant_id)
    task_store, identity = await _seed_dispatch(
        redis_client, task_id=task_id, run_id=run_id, tenant_id=tenant_id,
        worker_id=worker_id, scheduler_epoch=epoch,
    )
    calls: list[str] = []
    first = await _worker(
        redis_client,
        worker_id=worker_id,
        task_executor=_CountingTaskExecutor(calls),
    )

    async def fail_claim_projection(_projection):
        raise RuntimeError("forced claim projection failure")

    monkeypatch.setattr(first._dag_lua, "task_claim_canonical_projection", fail_claim_projection)
    stream, _message_id = await _enqueue(
        redis_client, task_id=task_id, run_id=run_id, tenant_id=tenant_id,
        scheduler_epoch=epoch,
    )
    await _wait_pending(redis_client, stream, 1)

    async def claim_durable():
        snap = await task_store.get_aggregate_snapshot(identity)
        return snap if snap is not None and snap.revision == 3 and snap.state == "running" else None

    claim_snapshot = await _wait_until(claim_durable)
    assert calls == []
    assert _text(await redis_client.get(DagRedisKey.task_state(task_id))) == "scheduled"
    await _close(first)

    second = await _worker(
        redis_client,
        worker_id=worker_id,
        task_executor=_CountingTaskExecutor(calls),
        reclaim_idle_ms=0,
    )
    await _wait_pending(redis_client, stream, 0)
    await _wait_task_state(redis_client, task_id, "done")
    await _wait_run_state(redis_client, run_id, "done")
    terminal = await task_store.get_aggregate_snapshot(identity)
    run_terminal = await run_store.get_aggregate_snapshot(run_terminate_identity(run_id))
    terminal_probe = await task_store.load_receipt_probe(identity, terminal.operation_id)
    assert terminal.revision == 4 and terminal.state == "done"
    assert terminal_probe.canonical_store_record.from_revision == claim_snapshot.revision
    assert terminal_probe.canonical_store_record.authoritative_metadata_changes["claim_operation_id"] == claim_snapshot.operation_id
    assert len(calls) == 1
    assert run_terminal.revision == 2 and run_terminal.state == "done"
    await _close(second)


async def test_b_projected_claim_executor_crash_restart_never_executes_again_or_acks(redis_client):
    task_id, run_id, tenant_id = "e-b-task", "e-b-run", "e-b-tenant"
    worker_id, epoch = "e-b-worker", "e-b-epoch"
    run_store = await _seed_run(redis_client, run_id=run_id, tenant_id=tenant_id)
    task_store, identity = await _seed_dispatch(
        redis_client, task_id=task_id, run_id=run_id, tenant_id=tenant_id,
        worker_id=worker_id, scheduler_epoch=epoch,
    )
    calls: list[str] = []
    first = await _worker(
        redis_client,
        worker_id=worker_id,
        task_executor=_CountingTaskExecutor(calls, raise_error=RuntimeError("executor crash")),
    )
    stream, _ = await _enqueue(
        redis_client, task_id=task_id, run_id=run_id, tenant_id=tenant_id,
        scheduler_epoch=epoch,
    )
    await _wait_pending(redis_client, stream, 1)
    await _wait_task_state(redis_client, task_id, "running")
    await _wait_until(lambda: _async_truth(len(calls) == 1))
    before = await task_store.get_aggregate_snapshot(identity)
    assert before.revision == 3 and before.state == "running"
    await _close(first)

    second = await _worker(
        redis_client,
        worker_id=worker_id,
        task_executor=_CountingTaskExecutor(calls, forbid=True),
        reclaim_idle_ms=0,
    )
    await asyncio.sleep(0.25)
    pending = await _pending(redis_client, stream)
    after = await task_store.get_aggregate_snapshot(identity)
    run_after = await run_store.get_aggregate_snapshot(run_terminate_identity(run_id))
    assert len(calls) == 1
    assert len(pending) == 1 and pending[0]["times_delivered"] >= 2
    assert after.revision == before.revision == 3 and after.operation_id == before.operation_id
    assert await task_store.load_receipt_probe(identity, _terminal_operation_id(identity)) is None
    assert run_after.revision == 1 and run_after.state == "pending"
    assert _text(await redis_client.get(DagRedisKey.task_state(task_id))) == "running"
    await _close(second)


async def _async_truth(value):
    return bool(value)


async def test_c_executor_result_terminal_authority_missing_restart_does_not_guess_result(redis_client, monkeypatch):
    task_id, run_id, tenant_id = "e-c-task", "e-c-run", "e-c-tenant"
    worker_id, epoch = "e-c-worker", "e-c-epoch"
    run_store = await _seed_run(redis_client, run_id=run_id, tenant_id=tenant_id)
    task_store, identity = await _seed_dispatch(
        redis_client, task_id=task_id, run_id=run_id, tenant_id=tenant_id,
        worker_id=worker_id, scheduler_epoch=epoch,
    )
    calls: list[str] = []
    first = await _worker(
        redis_client,
        worker_id=worker_id,
        task_executor=_CountingTaskExecutor(
            calls, result=TaskExecutionResult(ok=True, output={"lost": "in-memory"})
        ),
    )

    async def fail_before_terminal(**_kwargs):
        raise RuntimeError("terminal gateway unavailable before authority commit")

    monkeypatch.setattr(first._task_terminal_completion_gateway, "task_complete", fail_before_terminal)
    stream, _ = await _enqueue(
        redis_client, task_id=task_id, run_id=run_id, tenant_id=tenant_id,
        scheduler_epoch=epoch,
    )
    await _wait_pending(redis_client, stream, 1)
    await _wait_until(lambda: _async_truth(len(calls) == 1))
    before = await task_store.get_aggregate_snapshot(identity)
    assert before.revision == 3 and before.state == "running"
    assert await task_store.load_receipt_probe(identity, _terminal_operation_id(identity)) is None
    await _close(first)

    second = await _worker(
        redis_client,
        worker_id=worker_id,
        task_executor=_CountingTaskExecutor(calls, forbid=True),
        reclaim_idle_ms=0,
    )
    await asyncio.sleep(0.25)
    after = await task_store.get_aggregate_snapshot(identity)
    run_after = await run_store.get_aggregate_snapshot(run_terminate_identity(run_id))
    assert len(calls) == 1
    assert len(await _pending(redis_client, stream)) == 1
    assert after.revision == before.revision == 3 and after.state == "running"
    assert await task_store.load_receipt_probe(identity, _terminal_operation_id(identity)) is None
    assert run_after.revision == 1 and run_after.state == "pending"
    await _close(second)


async def test_d_task_terminal_commit_projection_failure_cross_worker_replays_projection_only(redis_client, monkeypatch):
    task_id, run_id, tenant_id = "e-d-task", "e-d-run", "e-d-tenant"
    owner, replacement, epoch = "e-d-owner", "e-d-replacement", "e-d-epoch"
    run_store = await _seed_run(redis_client, run_id=run_id, tenant_id=tenant_id)
    task_store, identity = await _seed_dispatch(
        redis_client, task_id=task_id, run_id=run_id, tenant_id=tenant_id,
        worker_id=owner, scheduler_epoch=epoch,
    )
    calls: list[str] = []
    first = await _worker(
        redis_client,
        worker_id=owner,
        task_executor=_CountingTaskExecutor(calls),
    )

    async def fail_terminal_projection(_projection):
        raise TaskTerminalAuthorityError(
            status=TASK_TERMINAL_PROJECTION_PENDING_STATUS,
            detail="forced terminal projection failure",
            canonical_commit_durable=True,
        )

    monkeypatch.setattr(
        first._task_terminal_authority_binding.projection_manager,
        "project",
        fail_terminal_projection,
    )
    stream, _ = await _enqueue(
        redis_client, task_id=task_id, run_id=run_id, tenant_id=tenant_id,
        scheduler_epoch=epoch,
    )
    await _wait_pending(redis_client, stream, 1)

    async def terminal_durable():
        snap = await task_store.get_aggregate_snapshot(identity)
        return snap if snap is not None and snap.revision == 4 and snap.state == "done" else None

    before_task = await _wait_until(terminal_durable)
    before_run = await run_store.get_aggregate_snapshot(run_terminate_identity(run_id))
    assert _text(await redis_client.get(DagRedisKey.task_state(task_id))) == "running"
    assert before_run.revision == 1 and before_run.state == "pending"
    assert len(calls) == 1
    pending_before = await _pending(redis_client, stream)
    assert pending_before[0]["consumer"] == owner
    await _close(first)

    second = await _worker(
        redis_client,
        worker_id=replacement,
        task_executor=_CountingTaskExecutor(calls, forbid=True),
        reclaim_idle_ms=0,
    )
    await _wait_pending(redis_client, stream, 0)
    await _wait_task_state(redis_client, task_id, "done")
    await _wait_run_state(redis_client, run_id, "done")
    after_task = await task_store.get_aggregate_snapshot(identity)
    after_run = await run_store.get_aggregate_snapshot(run_terminate_identity(run_id))
    assert len(calls) == 1
    assert after_task.revision == before_task.revision == 4
    assert after_task.operation_id == before_task.operation_id
    assert after_run.revision == 2 and after_run.state == "done"
    await _close(second)


async def test_e_run_commit_projection_failure_restart_replays_run_only(redis_client, monkeypatch):
    task_id, run_id, tenant_id = "e-e-task", "e-e-run", "e-e-tenant"
    owner, replacement, epoch = "e-e-owner", "e-e-replacement", "e-e-epoch"
    run_store = await _seed_run(redis_client, run_id=run_id, tenant_id=tenant_id)
    task_store, identity = await _seed_dispatch(
        redis_client, task_id=task_id, run_id=run_id, tenant_id=tenant_id,
        worker_id=owner, scheduler_epoch=epoch,
    )
    calls: list[str] = []
    first = await _worker(
        redis_client,
        worker_id=owner,
        task_executor=_CountingTaskExecutor(calls),
    )

    async def fail_run_projection(_projection):
        raise RuntimeError("forced RUN projection failure")

    monkeypatch.setattr(
        first._run_terminate_authority_binding.projection_manager,
        "project",
        fail_run_projection,
    )
    stream, _ = await _enqueue(
        redis_client, task_id=task_id, run_id=run_id, tenant_id=tenant_id,
        scheduler_epoch=epoch,
    )
    await _wait_pending(redis_client, stream, 1)
    await _wait_task_state(redis_client, task_id, "done")

    async def run_authority_durable():
        snap = await run_store.get_aggregate_snapshot(run_terminate_identity(run_id))
        return snap if snap is not None and snap.revision == 2 and snap.state == "done" else None

    before_run = await _wait_until(run_authority_durable)
    before_task = await task_store.get_aggregate_snapshot(identity)
    assert _text(await redis_client.get(RedisKey.run_state(run_id))) == "running"
    assert len(calls) == 1
    await _close(first)

    second = await _worker(
        redis_client,
        worker_id=replacement,
        task_executor=_CountingTaskExecutor(calls, forbid=True),
        reclaim_idle_ms=0,
    )
    await _wait_pending(redis_client, stream, 0)
    await _wait_run_state(redis_client, run_id, "done")
    after_task = await task_store.get_aggregate_snapshot(identity)
    after_run = await run_store.get_aggregate_snapshot(run_terminate_identity(run_id))
    assert len(calls) == 1
    assert after_task.revision == before_task.revision and after_task.operation_id == before_task.operation_id
    assert after_run.revision == before_run.revision == 2 and after_run.operation_id == before_run.operation_id
    await _close(second)


async def test_f_ack_loss_restart_reuses_terminal_truth_and_only_acks(redis_client, monkeypatch):
    task_id, run_id, tenant_id = "e-f-task", "e-f-run", "e-f-tenant"
    owner, replacement, epoch = "e-f-owner", "e-f-replacement", "e-f-epoch"
    run_store = await _seed_run(redis_client, run_id=run_id, tenant_id=tenant_id)
    task_store, identity = await _seed_dispatch(
        redis_client, task_id=task_id, run_id=run_id, tenant_id=tenant_id,
        worker_id=owner, scheduler_epoch=epoch,
    )
    calls: list[str] = []
    first = await _worker(
        redis_client,
        worker_id=owner,
        task_executor=_CountingTaskExecutor(calls),
    )
    original_consumer_ack = consumer_module.ack_message
    original_finalizing_ack = finalizing_module.ack_message

    async def lose_ack(*_args, **_kwargs):
        raise RuntimeError("simulated worker death before XACK")

    monkeypatch.setattr(consumer_module, "ack_message", lose_ack)
    monkeypatch.setattr(finalizing_module, "ack_message", lose_ack)
    stream, _ = await _enqueue(
        redis_client, task_id=task_id, run_id=run_id, tenant_id=tenant_id,
        scheduler_epoch=epoch,
    )
    await _wait_pending(redis_client, stream, 1)
    await _wait_task_state(redis_client, task_id, "done")
    await _wait_run_state(redis_client, run_id, "done")

    async def both_terminal():
        task = await task_store.get_aggregate_snapshot(identity)
        run = await run_store.get_aggregate_snapshot(run_terminate_identity(run_id))
        return (task, run) if task.revision == 4 and run.revision == 2 else None

    before_task, before_run = await _wait_until(both_terminal)
    assert len(calls) == 1
    await _close(first)
    monkeypatch.setattr(consumer_module, "ack_message", original_consumer_ack)
    monkeypatch.setattr(finalizing_module, "ack_message", original_finalizing_ack)

    second = await _worker(
        redis_client,
        worker_id=replacement,
        task_executor=_CountingTaskExecutor(calls, forbid=True),
        reclaim_idle_ms=0,
    )
    await _wait_pending(redis_client, stream, 0)
    after_task = await task_store.get_aggregate_snapshot(identity)
    after_run = await run_store.get_aggregate_snapshot(run_terminate_identity(run_id))
    assert len(calls) == 1
    assert after_task.revision == before_task.revision == 4 and after_task.operation_id == before_task.operation_id
    assert after_run.revision == before_run.revision == 2 and after_run.operation_id == before_run.operation_id
    await _close(second)


async def test_g_different_worker_reclaims_running_claim_but_cannot_execute_ack_or_requeue(redis_client):
    task_id, run_id, tenant_id = "e-g-task", "e-g-run", "e-g-tenant"
    owner, replacement, epoch = "e-g-owner", "e-g-replacement", "e-g-epoch"
    run_store = await _seed_run(redis_client, run_id=run_id, tenant_id=tenant_id)
    task_store, identity = await _seed_dispatch(
        redis_client, task_id=task_id, run_id=run_id, tenant_id=tenant_id,
        worker_id=owner, scheduler_epoch=epoch,
    )
    calls: list[str] = []
    first = await _worker(
        redis_client,
        worker_id=owner,
        task_executor=_CountingTaskExecutor(calls, raise_error=RuntimeError("ambiguous execution")),
    )
    stream, _ = await _enqueue(
        redis_client, task_id=task_id, run_id=run_id, tenant_id=tenant_id,
        scheduler_epoch=epoch,
    )
    await _wait_pending(redis_client, stream, 1)
    await _wait_task_state(redis_client, task_id, "running")
    await _wait_until(lambda: _async_truth(len(calls) == 1))
    before = await task_store.get_aggregate_snapshot(identity)
    await _close(first)

    second = await _worker(
        redis_client,
        worker_id=replacement,
        task_executor=_CountingTaskExecutor(calls, forbid=True),
        reclaim_idle_ms=0,
    )

    async def reclaimed_and_pending():
        rows = await _pending(redis_client, stream)
        if len(rows) != 1:
            return None
        row = rows[0]
        if row["consumer"] != replacement or row["times_delivered"] < 2:
            return None
        return row

    row = await _wait_until(reclaimed_and_pending)
    after = await task_store.get_aggregate_snapshot(identity)
    run_after = await run_store.get_aggregate_snapshot(run_terminate_identity(run_id))
    assert row["consumer"] == replacement
    assert len(calls) == 1
    assert after.revision == before.revision == 3 and after.operation_id == before.operation_id
    assert after.state == "running"
    assert _text(await redis_client.get(DagRedisKey.task_state(task_id))) == "running"
    assert await task_store.load_receipt_probe(identity, _terminal_operation_id(identity)) is None
    assert run_after.revision == 1 and run_after.state == "pending"
    await _close(second)
