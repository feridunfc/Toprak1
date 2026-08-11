from __future__ import annotations

import asyncio

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
from hfa.dag.heartbeat import HeartbeatPolicy
from hfa.dag.schema import DagRedisKey
from hfa_control.task_recovery import TaskRecoveryManager
from hfa_control.task_requeue_authority import (
    TASK_REQUEUE_DELIVERY_PENDING_STATUS,
    TASK_REQUEUE_DUPLICATE_STATUS,
    TASK_REQUEUE_EVIDENCE_CONFLICT_STATUS,
    TASK_REQUEUE_PROJECTION_PENDING_STATUS,
    TASK_REQUEUE_PROJECTED_STATUS,
    TaskRequeueAuthorityBinding,
    TaskRequeueAuthorityError,
)
from hfa_control.task_terminal_authority import TaskTerminalAuthorityBinding

pytestmark = [pytest.mark.asyncio, pytest.mark.integration]


def _text(value) -> str:
    if isinstance(value, bytes):
        return value.decode("utf-8")
    return "" if value is None else str(value)


def _context(command: AuthorityCommand) -> AuthorityEntryContext:
    return AuthorityEntryContext(
        authenticated_writer_id=f"test/{command.operation_type.value}",
        allowed_operations=frozenset({command.operation_type}),
        target_aggregate_identity_sha256=command.aggregate_identity.sha256,
        fence_required=False,
        fence_valid=True,
    )


async def _commit(store, command, *, revision: int, state: str | None, at_ms: int):
    evaluation = evaluate_authority_commit(
        context=_context(command),
        command=command,
        current_revision=revision,
        current_state=state,
        receipt_probe=None,
        committed_at_ms=at_ms,
        correlation_id=None,
    )
    assert evaluation.decision.code is AuthorityDecisionCode.ACCEPTED
    assert evaluation.commit_plan is not None
    result = await store.commit(evaluation.commit_plan)
    assert result.status is RedisAuthorityCommitStatus.COMMITTED
    return evaluation.commit_plan.record


async def _seed(redis_client, suffix: str, *, claim_epoch: int = 1):
    task_id = f"s84-9-race-task-{suffix}"
    run_id = f"s84-9-race-run-{suffix}"
    tenant_id = f"s84-9-race-tenant-{suffix}"
    worker_id = f"s84-9-race-worker-{suffix}"
    scheduler_epoch = "11"
    store = RedisCanonicalAuthorityStore(redis_client)
    await store.initialise()
    identity = CanonicalAggregateIdentity(AggregateType.TASK, run_id, task_id)

    admit = AuthorityCommand(
        aggregate_identity=identity,
        operation_type=OperationType.TASK_ADMIT,
        operation_id=f"task-admit:v1:{identity.sha256}",
        expected_revision=0,
        intended_previous_state=None,
        intended_next_state="ready",
        authoritative_payload={"task_id": task_id},
        authoritative_metadata_changes={"task_id": task_id, "run_id": run_id, "tenant_id": tenant_id},
        requested_child_effects={},
        requested_projection_intents=(
            {"kind": "READY_QUEUE_IF_READY", "task_id": task_id, "tenant_id": tenant_id},
        ),
    )
    await _commit(store, admit, revision=0, state=None, at_ms=100)

    attempt = claim_epoch
    dispatch_meta = {
        "task_id": task_id,
        "run_id": run_id,
        "tenant_id": tenant_id,
        "worker_id": worker_id,
        "scheduler_epoch": scheduler_epoch,
        "dispatch_attempt": attempt,
    }
    dispatch = AuthorityCommand(
        aggregate_identity=identity,
        operation_type=OperationType.TASK_DISPATCH,
        operation_id=f"task-dispatch:v1:{identity.sha256}:attempt:{attempt}",
        expected_revision=1,
        intended_previous_state="ready",
        intended_next_state="scheduled",
        authoritative_payload=dispatch_meta,
        authoritative_metadata_changes=dispatch_meta,
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
    dispatch_record = await _commit(store, dispatch, revision=1, state="ready", at_ms=200)

    claim_meta = {
        "task_id": task_id,
        "run_id": run_id,
        "tenant_id": tenant_id,
        "worker_instance_id": worker_id,
        "scheduler_epoch": scheduler_epoch,
        "claimed_at_ms": 300,
        "dispatch_attempt": attempt,
        "previous_claim_epoch": claim_epoch - 1,
        "claim_epoch": claim_epoch,
        "dispatch_transition_id": dispatch_record.transition_id,
        "dispatch_record_hash": dispatch_record.canonical_record_hash,
        "dispatch_command_hash": dispatch_record.canonical_command_hash,
        "dispatch_operation_id": dispatch_record.operation_id,
        "dispatch_revision": dispatch_record.to_revision,
    }
    claim = AuthorityCommand(
        aggregate_identity=identity,
        operation_type=OperationType.TASK_CLAIM,
        operation_id=f"task-claim:v1:{identity.sha256}:attempt:{attempt}",
        expected_revision=2,
        intended_previous_state="scheduled",
        intended_next_state="running",
        authoritative_payload=claim_meta,
        authoritative_metadata_changes=claim_meta,
        requested_child_effects={
            "worker_reservation": {
                "action": "CONSUME",
                "task_id": task_id,
                "worker_id": worker_id,
                "scheduler_epoch": scheduler_epoch,
            }
        },
        requested_projection_intents=(
            {"kind": "RUNNING_SET", "tenant_id": tenant_id, "task_id": task_id},
        ),
        causation_id=dispatch_record.transition_id,
    )
    claim_record = await _commit(store, claim, revision=2, state="scheduled", at_ms=300)

    await redis_client.set(DagRedisKey.task_state(task_id), "running")
    await redis_client.hset(
        DagRedisKey.task_meta(task_id),
        mapping={
            "task_id": task_id,
            "run_id": run_id,
            "tenant_id": tenant_id,
            "worker_instance_id": worker_id,
            "scheduler_epoch": scheduler_epoch,
            "claim_epoch": str(claim_epoch),
            "requeue_count": str(claim_epoch - 1),
            "heartbeat_at_ms": "1000",
            "last_heartbeat_at_ms": "1000",
            "canonical_transition_id": claim_record.transition_id,
            "canonical_record_hash": claim_record.canonical_record_hash,
            "canonical_command_hash": claim_record.canonical_command_hash,
            "canonical_revision": str(claim_record.to_revision),
            "canonical_operation_id": claim_record.operation_id,
            "claim_canonical_transition_id": claim_record.transition_id,
            "claim_canonical_record_hash": claim_record.canonical_record_hash,
            "claim_canonical_command_hash": claim_record.canonical_command_hash,
            "claim_canonical_revision": str(claim_record.to_revision),
            "claim_canonical_operation_id": claim_record.operation_id,
            "dispatch_attempt": str(attempt),
        },
    )
    await redis_client.set(RedisKey.run_state(run_id), "running")
    await redis_client.sadd(DagRedisKey.run_tasks(run_id), task_id)
    await redis_client.zadd(DagRedisKey.task_running_zset(tenant_id), {task_id: 1000.0})
    return {
        "store": store,
        "identity": identity,
        "task_id": task_id,
        "run_id": run_id,
        "tenant_id": tenant_id,
        "worker_id": worker_id,
        "scheduler_epoch": scheduler_epoch,
        "claim_epoch": claim_epoch,
        "claim_record": claim_record,
    }


async def _call(binding, seeded, *, at_ms=5000, reason="TASK_STALE_DETECTED"):
    return await binding.requeue(
        task_id=seeded["task_id"],
        run_id=seeded["run_id"],
        tenant_id=seeded["tenant_id"],
        observed_at_ms=at_ms,
        max_requeue_count=3,
        reason_code=reason,
        worker_instance_id=seeded["worker_id"],
        scheduler_epoch=seeded["scheduler_epoch"],
        claim_epoch=seeded["claim_epoch"],
    )


async def _snapshot(seeded):
    return await seeded["store"].get_aggregate_snapshot(seeded["identity"])


async def test_A_before_canonical_commit_failure_changes_nothing(redis_client, monkeypatch):
    seeded = await _seed(redis_client, "A")
    binding = TaskRequeueAuthorityBinding(redis_client)
    await binding.initialise()
    entered = asyncio.Event()
    release = asyncio.Event()

    async def fail_before_commit(_plan):
        entered.set()
        await release.wait()
        raise RuntimeError("forced precommit failure")

    monkeypatch.setattr(binding.store, "commit", fail_before_commit)
    task = asyncio.create_task(_call(binding, seeded))
    await asyncio.wait_for(entered.wait(), timeout=2)
    snap = await _snapshot(seeded)
    assert snap is not None and snap.revision == 3 and snap.state == "running"
    assert _text(await redis_client.get(DagRedisKey.task_state(seeded["task_id"]))) == "running"
    assert await redis_client.xlen(DagRedisKey.completion_stream(seeded["tenant_id"])) == 0
    release.set()
    with pytest.raises(TaskRequeueAuthorityError):
        await task


async def test_B_commit_succeeds_projection_fails_replay_keeps_one_revision(redis_client, monkeypatch):
    seeded = await _seed(redis_client, "B")
    binding = TaskRequeueAuthorityBinding(redis_client)
    await binding.initialise()
    entered = asyncio.Event()
    release = asyncio.Event()
    original = binding.projection_manager.project

    async def fail_projection(_projection):
        entered.set()
        await release.wait()
        raise TaskRequeueAuthorityError(
            status=TASK_REQUEUE_PROJECTION_PENDING_STATUS,
            detail="forced projection failure",
            canonical_commit_durable=True,
        )

    monkeypatch.setattr(binding.projection_manager, "project", fail_projection)
    task = asyncio.create_task(_call(binding, seeded))
    await asyncio.wait_for(entered.wait(), timeout=2)
    snap = await _snapshot(seeded)
    assert snap is not None and snap.revision == 4 and snap.state == "ready"
    assert _text(await redis_client.get(DagRedisKey.task_state(seeded["task_id"]))) == "running"
    release.set()
    with pytest.raises(TaskRequeueAuthorityError):
        await task
    monkeypatch.setattr(binding.projection_manager, "project", original)
    replay = await _call(binding, seeded, at_ms=8000)
    assert replay.canonical_revision == 4
    assert _text(await redis_client.get(DagRedisKey.task_state(seeded["task_id"]))) == "ready"


async def test_C_projection_succeeds_delivery_fails_then_retries_only_delivery(redis_client, monkeypatch):
    seeded = await _seed(redis_client, "C")
    binding = TaskRequeueAuthorityBinding(redis_client)
    await binding.initialise()
    entered = asyncio.Event()
    release = asyncio.Event()
    original = binding.projection_manager.deliver

    async def fail_delivery(_projection):
        entered.set()
        await release.wait()
        raise TaskRequeueAuthorityError(
            status=TASK_REQUEUE_DELIVERY_PENDING_STATUS,
            detail="forced delivery failure",
            canonical_commit_durable=True,
        )

    monkeypatch.setattr(binding.projection_manager, "deliver", fail_delivery)
    task = asyncio.create_task(_call(binding, seeded))
    await asyncio.wait_for(entered.wait(), timeout=2)
    snap = await _snapshot(seeded)
    assert snap is not None and snap.revision == 4 and snap.state == "ready"
    assert _text(await redis_client.get(DagRedisKey.task_state(seeded["task_id"]))) == "ready"
    assert await redis_client.xlen(DagRedisKey.completion_stream(seeded["tenant_id"])) == 0
    release.set()
    with pytest.raises(TaskRequeueAuthorityError):
        await task
    monkeypatch.setattr(binding.projection_manager, "deliver", original)
    replay = await _call(binding, seeded, at_ms=9000)
    assert replay.canonical_revision == 4
    assert await redis_client.xlen(DagRedisKey.completion_stream(seeded["tenant_id"])) == 1


async def test_D_delivery_succeeds_ack_lost_replay_does_not_duplicate(redis_client, monkeypatch):
    seeded = await _seed(redis_client, "D")
    binding = TaskRequeueAuthorityBinding(redis_client)
    await binding.initialise()
    entered = asyncio.Event()
    release = asyncio.Event()
    original = binding.projection_manager.deliver

    async def lose_ack(projection):
        await original(projection)
        entered.set()
        await release.wait()
        raise RuntimeError("delivery acknowledgement lost")

    monkeypatch.setattr(binding.projection_manager, "deliver", lose_ack)
    task = asyncio.create_task(_call(binding, seeded))
    await asyncio.wait_for(entered.wait(), timeout=2)
    snap = await _snapshot(seeded)
    assert snap is not None and snap.revision == 4
    assert await redis_client.xlen(DagRedisKey.completion_stream(seeded["tenant_id"])) == 1
    release.set()
    with pytest.raises(TaskRequeueAuthorityError):
        await task
    monkeypatch.setattr(binding.projection_manager, "deliver", original)
    replay = await _call(binding, seeded, at_ms=9000)
    assert replay.canonical_revision == 4
    assert await redis_client.xlen(DagRedisKey.completion_stream(seeded["tenant_id"])) == 1


async def test_E_two_recovery_scanners_race_exactly_one_revision(redis_client, monkeypatch):
    seeded = await _seed(redis_client, "E")
    binding = TaskRequeueAuthorityBinding(redis_client)
    await binding.initialise()
    original = binding.store.commit
    both_arrived = asyncio.Event()
    release = asyncio.Event()
    arrivals = 0
    lock = asyncio.Lock()

    async def racing_commit(plan):
        nonlocal arrivals
        async with lock:
            arrivals += 1
            if arrivals == 2:
                both_arrived.set()
        await release.wait()
        return await original(plan)

    monkeypatch.setattr(binding.store, "commit", racing_commit)
    first = asyncio.create_task(_call(binding, seeded))
    second = asyncio.create_task(_call(binding, seeded))
    await asyncio.wait_for(both_arrived.wait(), timeout=2)
    release.set()
    a, b = await asyncio.gather(first, second)
    snap = await _snapshot(seeded)
    assert snap is not None and snap.revision == 4
    assert a.canonical_operation_id == b.canonical_operation_id == snap.operation_id
    assert await redis_client.xlen(DagRedisKey.completion_stream(seeded["tenant_id"])) == 1


async def test_F_terminal_commit_wins_before_stale_requeue_commit(redis_client, monkeypatch):
    seeded = await _seed(redis_client, "F")
    requeue = TaskRequeueAuthorityBinding(redis_client)
    terminal = TaskTerminalAuthorityBinding(redis_client)
    await requeue.initialise()
    await terminal.initialise()
    original = requeue.store.commit
    requeue_at_commit = asyncio.Event()
    terminal_done = asyncio.Event()

    async def delayed_requeue_commit(plan):
        requeue_at_commit.set()
        await terminal_done.wait()
        return await original(plan)

    monkeypatch.setattr(requeue.store, "commit", delayed_requeue_commit)
    requeue_task = asyncio.create_task(_call(requeue, seeded))
    await asyncio.wait_for(requeue_at_commit.wait(), timeout=2)
    terminal_result = await terminal.fail(
        task_id=seeded["task_id"],
        run_id=seeded["run_id"],
        tenant_id=seeded["tenant_id"],
        finished_at_ms=4500,
        worker_instance_id=seeded["worker_id"],
        scheduler_epoch=seeded["scheduler_epoch"],
        claim_epoch=seeded["claim_epoch"],
        reason_code="terminal_wins_race",
    )
    assert terminal_result.canonical_revision == 4
    terminal_done.set()
    with pytest.raises(TaskRequeueAuthorityError):
        await requeue_task
    snap = await _snapshot(seeded)
    assert snap is not None and snap.revision == 4 and snap.state == "failed"
    assert _text(await redis_client.get(DagRedisKey.task_state(seeded["task_id"]))) == "failed"
    assert await redis_client.xlen(DagRedisKey.completion_stream(seeded["tenant_id"])) == 0


async def test_G_redelivery_after_commit_before_projection_recovers_same_operation(redis_client, monkeypatch):
    seeded = await _seed(redis_client, "G")
    first_binding = TaskRequeueAuthorityBinding(redis_client)
    await first_binding.initialise()
    entered = asyncio.Event()
    release = asyncio.Event()

    async def stop_at_projection(_projection):
        entered.set()
        await release.wait()
        raise TaskRequeueAuthorityError(
            status=TASK_REQUEUE_PROJECTION_PENDING_STATUS,
            detail="worker lost before projection",
            canonical_commit_durable=True,
        )

    monkeypatch.setattr(first_binding.projection_manager, "project", stop_at_projection)
    first = asyncio.create_task(_call(first_binding, seeded))
    await asyncio.wait_for(entered.wait(), timeout=2)
    before = await _snapshot(seeded)
    assert before is not None and before.revision == 4 and before.state == "ready"
    operation_id = before.operation_id
    release.set()
    with pytest.raises(TaskRequeueAuthorityError):
        await first

    recovery = TaskRequeueAuthorityBinding(redis_client)
    replay = await _call(recovery, seeded, at_ms=9500)
    after = await _snapshot(seeded)
    assert after is not None and after.revision == 4 and after.operation_id == operation_id
    assert replay.canonical_operation_id == operation_id
    assert _text(await redis_client.hget(DagRedisKey.task_meta(seeded["task_id"]), "requeue_count")) == "1"
    assert await redis_client.xlen(DagRedisKey.completion_stream(seeded["tenant_id"])) == 1


async def test_H_projection_complete_authority_redelivery_is_mutable_noop(redis_client):
    seeded = await _seed(redis_client, "H")
    first = await _call(TaskRequeueAuthorityBinding(redis_client), seeded)
    before_count = _text(await redis_client.hget(DagRedisKey.task_meta(seeded["task_id"]), "requeue_count"))
    before_events = await redis_client.xlen(DagRedisKey.completion_stream(seeded["tenant_id"]))
    replay = await _call(TaskRequeueAuthorityBinding(redis_client), seeded, at_ms=10000)
    snap = await _snapshot(seeded)
    assert snap is not None and snap.revision == first.canonical_revision == replay.canonical_revision == 4
    assert replay.status == TASK_REQUEUE_DUPLICATE_STATUS
    assert _text(await redis_client.hget(DagRedisKey.task_meta(seeded["task_id"]), "requeue_count")) == before_count == "1"
    assert await redis_client.xlen(DagRedisKey.completion_stream(seeded["tenant_id"])) == before_events == 1


async def test_I_divergent_duplicate_fails_without_projection_or_delivery(redis_client):
    seeded = await _seed(redis_client, "I")
    first = await _call(TaskRequeueAuthorityBinding(redis_client), seeded)
    with pytest.raises(TaskRequeueAuthorityError) as exc:
        await _call(TaskRequeueAuthorityBinding(redis_client), seeded, at_ms=10000, reason="DIVERGENT")
    assert exc.value.status == RedisAuthorityCommitStatus.IDEMPOTENCY_CONFLICT.value
    snap = await _snapshot(seeded)
    assert snap is not None and snap.revision == first.canonical_revision == 4
    assert _text(await redis_client.hget(DagRedisKey.task_meta(seeded["task_id"]), "requeue_count")) == "1"
    assert await redis_client.xlen(DagRedisKey.completion_stream(seeded["tenant_id"])) == 1


async def test_J_exact_duplicate_returns_same_durable_truth_zero_new_revision(redis_client):
    seeded = await _seed(redis_client, "J")
    binding = TaskRequeueAuthorityBinding(redis_client)
    first = await _call(binding, seeded)
    second = await _call(binding, seeded, at_ms=12000)
    assert second.exact_retry is True
    assert second.status == TASK_REQUEUE_DUPLICATE_STATUS
    assert second.canonical_transition_id == first.canonical_transition_id
    assert second.canonical_record_hash == first.canonical_record_hash
    assert second.canonical_command_hash == first.canonical_command_hash
    assert second.canonical_revision == first.canonical_revision == 4
    assert second.canonical_operation_id == first.canonical_operation_id
    assert _text(await redis_client.hget(DagRedisKey.task_meta(seeded["task_id"]), "requeue_count")) == "1"
    assert await redis_client.zscore(DagRedisKey.tenant_ready_queue(seeded["tenant_id"]), seeded["task_id"]) == 5000.0
    events = await redis_client.xrange(DagRedisKey.completion_stream(seeded["tenant_id"]))
    assert len(events) == 1
    assert _text(events[0][1].get(b"at_ms") or events[0][1].get("at_ms")) == "5000"


async def test_retry_exhaustion_uses_existing_canonical_task_fail_not_requeue(redis_client):
    seeded = await _seed(redis_client, "exhausted", claim_epoch=4)
    manager = TaskRecoveryManager(
        redis_client,
        HeartbeatPolicy(stale_after_ms=100, max_requeue_count=3),
        canonical_task_requeue_binding=True,
    )
    await manager.initialise()
    result = await manager.requeue_stale_task(
        task_id=seeded["task_id"],
        run_id=seeded["run_id"],
        tenant_id=seeded["tenant_id"],
        now_ms=5000,
    )
    assert result.status == "TASK_RETRY_EXHAUSTED"
    snap = await _snapshot(seeded)
    assert snap is not None and snap.revision == 4 and snap.state == "failed"
    assert snap.operation_id.startswith("task-terminal:v1:")
    assert _text(await redis_client.get(DagRedisKey.task_state(seeded["task_id"]))) == "failed"
    assert await redis_client.xlen(DagRedisKey.completion_stream(seeded["tenant_id"])) == 0
