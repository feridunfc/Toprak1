"""Offline Lua execution only; these tests never connect to a Redis server."""
from __future__ import annotations

import hashlib
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import fakeredis.aioredis
import pytest
import pytest_asyncio

from hfa.authority import (
    AggregateType, AuthorityCommand, AuthorityDecisionCode, AuthorityEntryContext,
    CanonicalAggregateIdentity, OperationType, RedisAuthorityCommitStatus,
    RedisAuthorityCorruptionError, RedisCanonicalAuthorityStore, evaluate_authority_commit,
)
from hfa.config.keys import RedisKey
from hfa.dag.schema import DagRedisKey
from hfa_control.run_termination import RunTerminationCoordinator
from hfa_control.run_terminate_authority import RunTerminateAuthorityBinding
from hfa_control.task_terminal_authority import TaskTerminalAuthorityBinding, TaskTerminalAuthorityError
from tests.integration.test_task_terminal_authority_integration import _seed_claimed_task


def plan(identity, operation, before, after, revision, intents):
    command = AuthorityCommand(
        aggregate_identity=identity, operation_type=operation,
        operation_id=f"f03:{identity.sha256}:{operation.value}",
        expected_revision=revision, intended_previous_state=before,
        intended_next_state=after, authoritative_payload={},
        authoritative_metadata_changes={}, requested_child_effects={},
        requested_projection_intents=tuple({"kind": kind} for kind in intents),
    )
    evaluation = evaluate_authority_commit(
        context=AuthorityEntryContext(
            authenticated_writer_id="test/f03",
            allowed_operations=frozenset({operation}),
            target_aggregate_identity_sha256=identity.sha256,
            fence_required=True, fence_valid=True,
        ),
        command=command, current_revision=revision, current_state=before,
        receipt_probe=None, committed_at_ms=1000 + revision, correlation_id=None,
    )
    assert evaluation.decision.code is AuthorityDecisionCode.ACCEPTED
    return evaluation.commit_plan


@pytest_asyncio.fixture
async def domain():
    server = fakeredis.FakeServer(version=(7, 4))
    redis = fakeredis.aioredis.FakeRedis(server=server, decode_responses=True)
    await redis.eval("return 1", 0)
    # fakeredis 2.38 omits this Redis primitive. Supply its exact SHA1 behavior;
    # the product Lua, cjson, Redis commands and authority checks stay unchanged.
    server._lua_runtime.globals().redis.sha1hex = (
        lambda raw: hashlib.sha1(raw).hexdigest().encode("ascii")
    )
    yield await seed_domain(redis)
    await redis.aclose()


async def seed_domain(redis, *, prefix="f03"):
    store = RedisCanonicalAuthorityStore(redis)
    await store.initialise()
    run = CanonicalAggregateIdentity(aggregate_type=AggregateType.RUN, run_id=f"{prefix}-run")
    task = CanonicalAggregateIdentity(
        aggregate_type=AggregateType.TASK, run_id=run.run_id, task_id=f"{prefix}-task",
    )
    for entry in (
        plan(run, OperationType.RUN_CREATE, None, "pending", 0, ["RUN_STATUS_PROJECTION"]),
        plan(task, OperationType.TASK_ADMIT, None, "ready", 0, ["READY_QUEUE_IF_READY"]),
        plan(task, OperationType.TASK_DISPATCH, "ready", "scheduled", 1,
             ["CONTROL_NOTIFICATION", "TASK_REQUEST_MESSAGE"]),
        plan(task, OperationType.TASK_CLAIM, "scheduled", "running", 2, ["RUNNING_SET"]),
    ):
        assert (await store.commit(entry)).committed
    return redis, store, run, task


def terminal(task, state):
    return plan(
        task, OperationType.TASK_COMPLETE if state == "done" else OperationType.TASK_FAIL,
        "running", state, 3,
        ["OUTPUT_PROJECTION", "DEPENDENCY_FANOUT_INTENT"] if state == "done"
        else ["DEPENDENCY_FAILURE_FANOUT_INTENT"],
    )


async def terminate_run(store, run, state="done"):
    result = await store.commit(plan(
        run, OperationType.RUN_TERMINATE, "pending", state, 1, ["RUN_RESULT_PROJECTION"],
    ))
    assert result.committed


async def snapshot(redis, keys):
    # Pickle-backed DUMP is local fakeredis evidence, not a Redis-version claim.
    return {key: await redis.dump(key) for key in keys}


@pytest.mark.asyncio
@pytest.mark.parametrize("state", ["done", "failed"])
async def test_new_terminal_after_run_terminal_is_rejected_without_task_writes(domain, state):
    redis, store, run, task = domain
    await terminate_run(store, run)
    keys = store.keyspace(task.sha256).commit_keys()
    before = await snapshot(redis, keys)
    result = await store.commit(terminal(task, state))
    assert result.status is RedisAuthorityCommitStatus.ILLEGAL_STATE_TRANSITION
    assert result.detail == "parent_run_not_nonterminal"
    assert await snapshot(redis, keys) == before


@pytest.mark.asyncio
@pytest.mark.parametrize("state", ["done", "failed"])
async def test_run_commit_between_prevalidation_and_task_lua_cannot_slip_through(domain, state):
    redis, store, run, task = domain
    run_store = RedisCanonicalAuthorityStore(redis)
    await run_store.initialise()
    keys = store.keyspace(task.sha256).commit_keys()
    before = await snapshot(redis, keys)
    original = store._commit_loader.run
    raced = False

    async def interleave(**kwargs):
        nonlocal raced
        if not raced:
            raced = True
            await terminate_run(run_store, run)
        return await original(**kwargs)

    store._commit_loader.run = interleave
    result = await store.commit(terminal(task, state))
    assert raced
    assert result.status is RedisAuthorityCommitStatus.ILLEGAL_STATE_TRANSITION
    assert await snapshot(redis, keys) == before


@pytest.mark.asyncio
@pytest.mark.parametrize("state", ["done", "failed"])
async def test_task_first_then_run_preserves_receipt_first_exact_commit_duplicate(domain, state):
    redis, store, run, task = domain
    entry = terminal(task, state)
    assert (await store.commit(entry)).committed
    await terminate_run(store, run)
    keys = await redis.keys("*")
    before = await snapshot(redis, keys)
    duplicate = await store.commit(entry)
    assert duplicate.status is RedisAuthorityCommitStatus.ALREADY_APPLIED
    assert await snapshot(redis, keys) == before
    assert set(await redis.keys("*")) == set(keys)


@pytest.mark.asyncio
async def test_parent_proof_deleted_after_python_read_rejects_without_task_write(domain):
    redis, store, run, task = domain
    keys = store.keyspace(task.sha256).commit_keys()
    before = await snapshot(redis, keys)
    original = store._commit_loader.run

    async def interleave(**kwargs):
        await redis.delete(store.keyspace(run.sha256).receipts)
        return await original(**kwargs)

    store._commit_loader.run = interleave
    result = await store.commit(terminal(task, "done"))
    assert result.status is RedisAuthorityCommitStatus.CANONICAL_RECORD_CORRUPTION_CONFLICT
    assert await snapshot(redis, keys) == before


@pytest.mark.asyncio
async def test_missing_parent_cannot_fall_back_to_runtime_state(domain):
    redis, store, run, task = domain
    await redis.delete(*store.keyspace(run.sha256).commit_keys())
    await redis.set(f"hfa:run:state:{run.run_id}", "running")
    loader = store._commit_loader.run = AsyncMock()
    with pytest.raises(RedisAuthorityCorruptionError, match="parent canonical RUN head is missing"):
        await store.commit(terminal(task, "done"))
    loader.assert_not_awaited()


@pytest_asyncio.fixture
async def projection_domain(domain):
    redis = domain[0]
    store, task, _ = await _seed_claimed_task(
        redis, task_id=f"{domain[3].task_id}-projection", run_id=f"{domain[2].run_id}-projection",
        tenant_id="tenant", worker_id="worker", scheduler_epoch="epoch",
    )
    run = CanonicalAggregateIdentity(aggregate_type=AggregateType.RUN, run_id=task.run_id)
    binding = TaskTerminalAuthorityBinding(redis)
    return redis, store, run, task, binding


async def prepare(binding, task, state):
    return await binding.prepare_terminal(
        task_id=task.task_id, run_id=task.run_id, tenant_id="tenant",
        terminal_state=state, finished_at_ms=400, reason_code="finished",
        worker_instance_id="worker", scheduler_epoch="epoch", claim_epoch=1,
        output_data='{"value":1}' if state == "done" else "",
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("state", ["done", "failed"])
@pytest.mark.parametrize("runtime_run_state", ["running", "done"])
async def test_prior_terminal_missing_projection_is_blocked_after_run_terminal(
    projection_domain, state, runtime_run_state,
):
    redis, store, run, task, binding = projection_domain
    prepared = await prepare(binding, task, state)
    await terminate_run(store, run)
    await redis.set(RedisKey.run_state(run.run_id), runtime_run_state)
    keys = await redis.keys("*")
    before = await snapshot(redis, keys)
    with pytest.raises(TaskTerminalAuthorityError, match="run_terminal_projection_blocked"):
        await binding._apply_prepared(prepared)
    assert await snapshot(redis, keys) == before
    assert set(await redis.keys("*")) == set(keys)


@pytest.mark.asyncio
@pytest.mark.parametrize("state", ["done", "failed"])
async def test_run_terminalization_between_projection_guard_read_and_lua_is_read_only(
    projection_domain, state,
):
    redis, store, run, task, binding = projection_domain
    prepared = await prepare(binding, task, state)
    manager = binding.projection_manager
    await manager.initialise()
    original = manager._loader.run
    before = None

    async def interleave(**kwargs):
        nonlocal before
        await terminate_run(store, run)
        before = await snapshot(redis, await redis.keys("*"))
        return await original(**kwargs)

    manager._loader.run = interleave
    with pytest.raises(TaskTerminalAuthorityError, match="canonical_run_proof_conflict"):
        await binding._apply_prepared(prepared)
    assert await snapshot(redis, await redis.keys("*")) == before


@pytest.mark.asyncio
@pytest.mark.parametrize("state", ["done", "failed"])
@pytest.mark.parametrize("parent", ["pending", "done", "failed", "race_to_done", "race_to_failed"])
async def test_exact_projected_duplicate_is_read_only_through_coordinator(
    projection_domain, state, parent, monkeypatch,
):
    from hfa_worker import consumer, run_finalizing_runtime
    from hfa_worker.fake_executor import FakeExecutor

    redis, store, run, task, binding = projection_domain
    prepared = await prepare(binding, task, state)
    assert (await binding._apply_prepared(prepared)).projection_applied
    if parent in {"done", "failed"}:
        await terminate_run(store, run, parent)
    else:
        assert (await store.get_aggregate_snapshot(run)).state == "pending"
    stream = f"hfa:f03:delivery:{task.task_id}"
    await redis.xgroup_create(stream, "f03", id="0", mkstream=True)
    await redis.xadd(stream, {"task_id": task.task_id, "run_id": run.run_id})
    await redis.xreadgroup("f03", "worker", {stream: ">"}, count=1)
    pending_before = await redis.xpending(stream, "f03")
    assert pending_before["pending"] == 1
    keys = await redis.keys("*")
    deadlines = {key: await redis.pexpiretime(key) for key in keys}
    before = await snapshot(redis, keys)
    manager = binding.projection_manager
    original = manager._loader.run
    race_count = 0

    async def interleave(**kwargs):
        nonlocal before, race_count
        # Capture happened while RUN was pending. Its immutable proof is still
        # the stale argument when the exact TASK duplicate reaches Lua.
        assert json.loads(kwargs["args"][34])[10] == "pending"
        await terminate_run(store, run, parent.removeprefix("race_to_"))
        race_count += 1
        before = await snapshot(redis, await redis.keys("*"))
        return await original(**kwargs)

    if parent.startswith("race_to_"):
        manager._loader.run = interleave

    async def retry(**kwargs):
        again = await prepare(binding, task, state)
        assert again.exact_retry
        task_result = await binding._apply_prepared(again)
        assert task_result.exact_no_op is True
        return task_result

    resources = SimpleNamespace(initialise=AsyncMock(), settle_once=AsyncMock())
    run_projection = SimpleNamespace(initialise=AsyncMock(), project=AsyncMock())
    run_authority = RunTerminateAuthorityBinding(
        redis, store=store, resource_manager=resources, projection_manager=run_projection,
    )
    run_authority.terminate = AsyncMock(wraps=run_authority.terminate)
    coordinator = RunTerminationCoordinator(
        redis, SimpleNamespace(task_complete=retry), enabled=True, authority_binding=run_authority,
    )
    coordinator.finalize_run_from_tasks = AsyncMock(wraps=coordinator.finalize_run_from_tasks)
    ack = AsyncMock(side_effect=AssertionError("no ACK in C"))
    monkeypatch.setattr(consumer, "ack_message", ack)
    monkeypatch.setattr(run_finalizing_runtime, "ack_message", ack)
    execute = AsyncMock(side_effect=AssertionError("no executor in C"))
    monkeypatch.setattr(FakeExecutor, "execute", execute)
    result = await coordinator.task_complete(run_id=run.run_id, task_id=task.task_id)
    assert result.completed and result.exact_no_op and not result.ack_allowed
    coordinator.finalize_run_from_tasks.assert_not_awaited()
    run_authority.terminate.assert_not_awaited()
    resources.settle_once.assert_not_awaited()
    run_projection.project.assert_not_awaited()
    ack.assert_not_awaited()
    execute.assert_not_awaited()
    assert race_count == int(parent.startswith("race_to_"))
    assert await snapshot(redis, await redis.keys("*")) == before
    assert {key: await redis.pexpiretime(key) for key in keys} == deadlines
    assert await redis.xpending(stream, "f03") == pending_before


@pytest.mark.asyncio
@pytest.mark.parametrize("drift", ["state", "task_id", "run_id", "tenant_id", "ready", "running", "output"])
async def test_terminal_projection_drift_is_not_exact_no_op(projection_domain, drift):
    redis, store, run, task, binding = projection_domain
    prepared = await prepare(binding, task, "done")
    await binding._apply_prepared(prepared)
    await terminate_run(store, run)
    if drift == "state":
        await redis.set(DagRedisKey.task_state(task.task_id), "failed")
    elif drift in {"task_id", "run_id", "tenant_id"}:
        await redis.hset(DagRedisKey.task_meta(task.task_id), drift, "wrong")
    elif drift == "ready":
        await redis.zadd(DagRedisKey.tenant_ready_queue("tenant"), {task.task_id: 1})
    elif drift == "running":
        await redis.zadd(DagRedisKey.task_running_zset("tenant"), {task.task_id: 1})
    else:
        await redis.delete(DagRedisKey.task_output(task.task_id))
    before = await snapshot(redis, await redis.keys("*"))
    with pytest.raises(TaskTerminalAuthorityError, match="canonical_projection_conflict"):
        await binding._apply_prepared(prepared)
    assert await snapshot(redis, await redis.keys("*")) == before


@pytest.mark.asyncio
async def test_worker_bridge_never_acks_an_exact_no_op_result(domain, monkeypatch):
    # Negative reachability seam: a completion races with an already-read
    # delivery classification. The explicit no-op result must stop ACK too.
    from hfa_worker import consumer as module
    redis = domain[0]
    worker = object.__new__(module.WorkerConsumer)
    worker._redis = redis
    worker._worker_id, worker._worker_group = "worker", "group"
    worker._canonical_inflight = set()
    worker._task_consumer = SimpleNamespace(consume_once=AsyncMock(return_value=SimpleNamespace(
        claimed=SimpleNamespace(ok=True), executed=object(),
        completed=SimpleNamespace(completed=True, exact_no_op=True),
    )))
    ctx = SimpleNamespace(task_id="task", run_id="run", tenant_id="tenant")
    monkeypatch.setattr(module, "build_task_context_from_run_requested", lambda *a, **kw: ctx)
    monkeypatch.setattr(module, "classify_terminal_duplicate_delivery", AsyncMock(
        return_value=SimpleNamespace(status="not_terminal"),
    ))
    monkeypatch.setattr(module, "_verify_run_requested_task_identity", AsyncMock(
        return_value=(True, "", "run"),
    ))
    ack = AsyncMock(side_effect=AssertionError("no ACK"))
    monkeypatch.setattr(module, "ack_message", ack)
    await worker._process_message_via_task_consumer(ctx, "1-0", "stream", 0)
    worker._task_consumer.consume_once.assert_awaited_once()
    ack.assert_not_awaited()


@pytest.mark.asyncio
async def test_running_redelivery_stops_before_finalization_and_ack_on_exact_no_op(domain, monkeypatch):
    from hfa_worker import run_finalizing_runtime as module
    worker = object.__new__(module.RunFinalizingWorkerConsumer)
    worker._redis = domain[0]
    worker._worker_id, worker._worker_group = "worker", "group"
    replay = AsyncMock(return_value=SimpleNamespace(
        completed=True, terminal_state="done", exact_no_op=True,
    ))
    worker._task_terminal_authority_binding = SimpleNamespace(replay_terminal_projection=replay)
    worker._load_terminal_recovery_claim_input = AsyncMock(return_value={
        "task_id": "task", "run_id": "run", "tenant_id": "tenant",
        "worker_instance_id": "worker", "scheduler_epoch": "epoch", "claim_epoch": "1",
    })
    worker._finalize_run_and_ack = AsyncMock(side_effect=AssertionError("no finalization or ACK"))
    ctx = SimpleNamespace(task_id="task", run_id="run", tenant_id="tenant")
    monkeypatch.setattr(module, "build_task_context_from_run_requested", lambda *a, **kw: ctx)
    monkeypatch.setattr(module, "classify_terminal_duplicate_delivery", AsyncMock(
        return_value=SimpleNamespace(status="not_terminal"),
    ))
    await worker._process_message_via_task_consumer(ctx, "1-0", "stream", 0)
    replay.assert_awaited_once()
    worker._finalize_run_and_ack.assert_not_awaited()
