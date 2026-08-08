from __future__ import annotations

import asyncio
import hashlib
import json

import pytest

from hfa.authority import (
    AuthorityDecisionCode,
    RedisAuthorityCommitStatus,
    RedisCanonicalAuthorityStore,
    evaluate_authority_commit,
)
from hfa.config.keys import RedisKey
from hfa.dag.schema import DagRedisKey
from hfa_control.run_create_authority import (
    RunCreateAuthorityInput,
    build_run_create_command,
    build_run_create_context,
)
from hfa_control.run_terminate_authority import (
    ALREADY_PROJECTED_STATUS,
    NOT_READY_STATUS,
    PROJECTED_STATUS,
    RunTerminateAuthorityBinding,
    RunTerminateAuthorityError,
    RunTerminateProjectionManager,
    RunTerminateProjectionPendingError,
    TerminalAggregateProofManager,
    build_run_terminate_command,
    build_run_terminate_context,
    run_terminate_identity,
    run_terminate_operation_id,
)

pytestmark = [pytest.mark.asyncio, pytest.mark.integration]


async def _seed_canonical_run(
    redis_client,
    *,
    run_id: str,
    tenant_id: str,
    tasks: dict[str, str],
    legacy_run_state: str = "running",
    with_run_meta: bool = True,
) -> RedisCanonicalAuthorityStore:
    store = RedisCanonicalAuthorityStore(redis_client)
    await store.initialise()
    create = build_run_create_command(
        RunCreateAuthorityInput(
            run_id=run_id,
            tenant_id=tenant_id,
            agent_type="research",
            priority=5,
            payload={"prompt": "sprint-84.5"},
            estimated_cost_cents=100,
            preferred_region="eu-west-1",
            preferred_placement="LEAST_LOADED",
            created_at_ms=100,
            control_stream=RedisKey.stream_control(),
        )
    )
    evaluation = evaluate_authority_commit(
        context=build_run_create_context(create),
        command=create,
        current_revision=0,
        current_state=None,
        receipt_probe=None,
        committed_at_ms=100,
        correlation_id=None,
    )
    assert evaluation.decision.code is AuthorityDecisionCode.ACCEPTED
    assert evaluation.commit_plan is not None
    persisted = await store.commit(evaluation.commit_plan)
    assert persisted.status is RedisAuthorityCommitStatus.COMMITTED

    await redis_client.set(RedisKey.run_state(run_id), legacy_run_state)
    if with_run_meta:
        await redis_client.hset(
            RedisKey.run_meta(run_id),
            mapping={
                "run_id": run_id,
                "tenant_id": tenant_id,
                "state": legacy_run_state,
            },
        )
    await redis_client.zadd(RedisKey.cp_running(), {run_id: 100})
    for task_id, state in tasks.items():
        await redis_client.sadd(DagRedisKey.run_tasks(run_id), task_id)
        await redis_client.set(DagRedisKey.task_state(task_id), state)
        await redis_client.hset(
            DagRedisKey.task_meta(task_id),
            mapping={
                "task_id": task_id,
                "run_id": run_id,
                "tenant_id": tenant_id,
            },
        )
    return store


async def _set_terminal_event_migration_ready(redis_client):
    # Synthetic readiness for Sprint 84.5 regression tests only. Production
    # readiness must be created by the bounded Sprint 84.6 backfill tool.
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


async def _binding(redis_client, store=None, projection=None):
    await _set_terminal_event_migration_ready(redis_client)
    binding = RunTerminateAuthorityBinding(
        redis_client,
        store=store,
        projection_manager=projection,
    )
    await binding.initialise()
    return binding


async def _events(redis_client, run_id: str) -> list[dict[str, str]]:
    if await redis_client.type(RedisKey.stream_results()) != "stream":
        return []
    rows = await redis_client.xrange(RedisKey.stream_results(), min="-", max="+")
    return [fields for _entry, fields in rows if fields.get("run_id") == run_id]


async def _capture_and_commit_without_projection(
    redis_client,
    store: RedisCanonicalAuthorityStore,
    *,
    run_id: str,
    tenant_id: str,
    finalized_at_ms: int = 1000,
):
    proof_manager = TerminalAggregateProofManager(redis_client)
    await proof_manager.initialise()
    snapshot = await store.get_aggregate_snapshot(run_terminate_identity(run_id))
    assert snapshot is not None
    captured = await proof_manager.capture(
        run_id=run_id,
        tenant_id=tenant_id,
        finalized_at_ms=finalized_at_ms,
        worker_instance_id="worker-a",
        trigger_task_id="task-a",
        trigger_terminal_state="done",
        canonical_expected_revision=snapshot.revision,
        canonical_previous_state=snapshot.state,
    )
    assert captured.proof is not None
    proof = captured.proof
    command = build_run_terminate_command(proof)
    evaluation = evaluate_authority_commit(
        context=build_run_terminate_context(command),
        command=command,
        current_revision=snapshot.revision,
        current_state=snapshot.state,
        receipt_probe=None,
        committed_at_ms=proof.finalized_at_ms,
        correlation_id=None,
    )
    assert evaluation.decision.code is AuthorityDecisionCode.ACCEPTED
    assert evaluation.commit_plan is not None
    persisted = await store.commit(evaluation.commit_plan)
    assert persisted.status is RedisAuthorityCommitStatus.COMMITTED
    return proof, evaluation.commit_plan


async def test_success_capture_commit_projection_and_exact_retry(real_redis):
    run_id = "tenant-a:run-term-success"
    tenant_id = "tenant-a"
    store = await _seed_canonical_run(
        real_redis,
        run_id=run_id,
        tenant_id=tenant_id,
        tasks={"task-b": "done", "task-a": "done"},
    )
    binding = await _binding(real_redis, store=store)

    first = await binding.terminate(
        run_id=run_id,
        tenant_id=tenant_id,
        trigger_task_id="task-b",
        finalized_at_ms=1000,
        worker_instance_id="worker-a",
        trigger_terminal_state="done",
    )
    retry = await binding.terminate(
        run_id=run_id,
        tenant_id=tenant_id,
        trigger_task_id="task-a",
        finalized_at_ms=9999,
        worker_instance_id="worker-b",
        trigger_terminal_state="done",
    )

    assert first.status == PROJECTED_STATUS
    assert retry.status == ALREADY_PROJECTED_STATUS
    assert retry.already_finalized is True
    assert first.terminal_proof_sha256 == retry.terminal_proof_sha256
    assert first.canonical_revision == retry.canonical_revision == 2
    snapshot = await store.get_aggregate_snapshot(run_terminate_identity(run_id))
    assert snapshot is not None and snapshot.revision == 2 and snapshot.state == "done"
    proof_manager = TerminalAggregateProofManager(real_redis)
    proof = await proof_manager.load_existing(run_id)
    assert proof is not None
    assert [row.task_id for row in proof.tasks] == ["task-a", "task-b"]
    assert hashlib.sha256(proof.proof_payload_json.encode()).hexdigest() == proof.proof_sha256
    assert await real_redis.ttl(proof_manager.proof_key(run_id)) == -1
    proof_payload = json.loads(proof.proof_payload_json)
    assert "finalized_at_ms" not in proof_payload
    assert "worker_instance_id" not in proof_payload
    assert "trigger_task_id" not in proof_payload
    projection_receipt = RunTerminateProjectionManager.receipt_key(
        run_terminate_operation_id(run_id, proof.proof_sha256)
    )
    assert await real_redis.ttl(projection_receipt) == -1
    assert await real_redis.get(RedisKey.run_state(run_id)) == "done"
    assert await real_redis.zscore(RedisKey.cp_running(), run_id) is None
    events = await _events(real_redis, run_id)
    assert len(events) == 1
    assert events[0]["event_type"] == "RunCompleted"
    assert events[0]["terminal_proof_sha256"] == proof.proof_sha256
    assert events[0]["canonical_revision"] == "2"


async def test_failure_terminal_aggregate_projects_failed(real_redis):
    run_id = "tenant-a:run-term-failed"
    tenant_id = "tenant-a"
    store = await _seed_canonical_run(
        real_redis,
        run_id=run_id,
        tenant_id=tenant_id,
        tasks={"task-a": "done", "task-b": "failed", "task-c": "skipped"},
    )
    binding = await _binding(real_redis, store=store)
    result = await binding.terminate(
        run_id=run_id,
        tenant_id=tenant_id,
        trigger_task_id="task-b",
        finalized_at_ms=2000,
        worker_instance_id="worker-a",
        trigger_terminal_state="failed",
    )
    assert result.final_state == "failed"
    assert (result.task_count, result.done_count, result.failed_count, result.skipped_count) == (3, 1, 1, 1)
    stored = await real_redis.hgetall(RedisKey.run_result(run_id))
    assert stored["status"] == "failed"
    assert stored["error"] == "aggregate_task_failure"
    events = await _events(real_redis, run_id)
    assert len(events) == 1 and events[0]["event_type"] == "RunFailed"


async def test_nonterminal_sibling_is_not_ready_without_proof_or_authority_mutation(real_redis):
    run_id = "tenant-a:run-term-not-ready"
    tenant_id = "tenant-a"
    store = await _seed_canonical_run(
        real_redis,
        run_id=run_id,
        tenant_id=tenant_id,
        tasks={"task-a": "done", "task-b": "running"},
    )
    binding = await _binding(real_redis, store=store)
    result = await binding.terminate(
        run_id=run_id,
        tenant_id=tenant_id,
        trigger_task_id="task-a",
        finalized_at_ms=3000,
        trigger_terminal_state="done",
    )
    assert result.status == NOT_READY_STATUS
    assert result.finalized is False and result.ack_allowed is True
    assert await real_redis.exists(TerminalAggregateProofManager.proof_key(run_id)) == 0
    snapshot = await store.get_aggregate_snapshot(run_terminate_identity(run_id))
    assert snapshot is not None and snapshot.revision == 1 and snapshot.state == "pending"
    assert await real_redis.get(RedisKey.run_state(run_id)) == "running"
    assert await real_redis.exists(RedisKey.run_result(run_id)) == 0
    assert await _events(real_redis, run_id) == []


async def test_frozen_proof_then_task_change_conflicts_before_canonical_commit(real_redis):
    run_id = "tenant-a:run-term-proof-change"
    tenant_id = "tenant-a"
    store = await _seed_canonical_run(
        real_redis,
        run_id=run_id,
        tenant_id=tenant_id,
        tasks={"task-a": "done"},
    )
    manager = TerminalAggregateProofManager(real_redis)
    await manager.initialise()
    captured = await manager.capture(
        run_id=run_id,
        tenant_id=tenant_id,
        finalized_at_ms=4000,
        worker_instance_id="worker-a",
        trigger_task_id="task-a",
        trigger_terminal_state="done",
        canonical_expected_revision=1,
        canonical_previous_state="pending",
    )
    assert captured.proof is not None
    await real_redis.set(DagRedisKey.task_state("task-a"), "running")
    binding = await _binding(real_redis, store=store)
    with pytest.raises(RunTerminateAuthorityError, match="terminal_proof_changed_after_capture"):
        await binding.terminate(
            run_id=run_id,
            tenant_id=tenant_id,
            trigger_task_id="task-a",
            finalized_at_ms=9999,
            trigger_terminal_state="done",
        )
    snapshot = await store.get_aggregate_snapshot(run_terminate_identity(run_id))
    assert snapshot is not None and snapshot.revision == 1
    assert await _events(real_redis, run_id) == []


async def test_proof_without_canonical_receipt_never_backfills_legacy_terminal_run(real_redis):
    run_id = "tenant-a:run-term-no-backfill"
    tenant_id = "tenant-a"
    store = await _seed_canonical_run(
        real_redis,
        run_id=run_id,
        tenant_id=tenant_id,
        tasks={"task-a": "done"},
    )
    manager = TerminalAggregateProofManager(real_redis)
    await manager.initialise()
    captured = await manager.capture(
        run_id=run_id,
        tenant_id=tenant_id,
        finalized_at_ms=5000,
        worker_instance_id="worker-a",
        trigger_task_id="task-a",
        trigger_terminal_state="done",
        canonical_expected_revision=1,
        canonical_previous_state="pending",
    )
    assert captured.proof is not None
    await real_redis.set(RedisKey.run_state(run_id), "done")
    binding = await _binding(real_redis, store=store)
    with pytest.raises(RunTerminateAuthorityError, match="legacy_terminal_footprint_without_canonical_authority"):
        await binding.terminate(
            run_id=run_id,
            tenant_id=tenant_id,
            trigger_task_id="task-a",
            finalized_at_ms=5001,
            trigger_terminal_state="done",
        )
    snapshot = await store.get_aggregate_snapshot(run_terminate_identity(run_id))
    assert snapshot is not None and snapshot.revision == 1


async def test_durable_canonical_commit_recovers_projection_without_reinterpreting_tasks(real_redis):
    run_id = "tenant-a:run-term-crash-before-projection"
    tenant_id = "tenant-a"
    store = await _seed_canonical_run(
        real_redis,
        run_id=run_id,
        tenant_id=tenant_id,
        tasks={"task-a": "done"},
    )
    proof, _plan = await _capture_and_commit_without_projection(
        real_redis,
        store,
        run_id=run_id,
        tenant_id=tenant_id,
        finalized_at_ms=6000,
    )
    await real_redis.set(DagRedisKey.task_state("task-a"), "running")
    binding = await _binding(real_redis, store=store)
    result = await binding.terminate(
        run_id=run_id,
        tenant_id=tenant_id,
        trigger_task_id="different-trigger",
        finalized_at_ms=9999,
        worker_instance_id="different-worker",
        trigger_terminal_state="failed",
    )
    assert result.status == PROJECTED_STATUS
    assert result.terminal_proof_sha256 == proof.proof_sha256
    assert result.final_state == "done"
    assert await real_redis.get(RedisKey.run_state(run_id)) == "done"
    assert len(await _events(real_redis, run_id)) == 1


class _AmbiguousStore:
    def __init__(self, inner):
        self.inner = inner
        self.raise_once = True

    def __getattr__(self, name):
        return getattr(self.inner, name)

    async def commit(self, plan):
        result = await self.inner.commit(plan)
        if self.raise_once:
            self.raise_once = False
            raise RuntimeError("simulated lost canonical commit response")
        return result


async def test_ambiguous_canonical_commit_rereads_exact_receipt(real_redis):
    run_id = "tenant-a:run-term-ambiguous"
    tenant_id = "tenant-a"
    inner = await _seed_canonical_run(
        real_redis,
        run_id=run_id,
        tenant_id=tenant_id,
        tasks={"task-a": "done"},
    )
    binding = await _binding(real_redis, store=_AmbiguousStore(inner))
    result = await binding.terminate(
        run_id=run_id,
        tenant_id=tenant_id,
        trigger_task_id="task-a",
        finalized_at_ms=7000,
        trigger_terminal_state="done",
    )
    assert result.status == PROJECTED_STATUS
    snapshot = await inner.get_aggregate_snapshot(run_terminate_identity(run_id))
    assert snapshot is not None and snapshot.revision == 2
    assert len(await _events(real_redis, run_id)) == 1


class _ProjectionResponseLost:
    def __init__(self, inner):
        self.inner = inner
        self.raise_once = True

    def __getattr__(self, name):
        return getattr(self.inner, name)

    async def project(self, value):
        result = await self.inner.project(value)
        if self.raise_once:
            self.raise_once = False
            raise RuntimeError("simulated lost projection response")
        return result


async def test_projection_written_before_response_is_replay_safe(real_redis):
    run_id = "tenant-a:run-term-projection-response-lost"
    tenant_id = "tenant-a"
    store = await _seed_canonical_run(
        real_redis,
        run_id=run_id,
        tenant_id=tenant_id,
        tasks={"task-a": "done"},
    )
    projection = _ProjectionResponseLost(RunTerminateProjectionManager(real_redis))
    binding = await _binding(real_redis, store=store, projection=projection)
    with pytest.raises(RunTerminateProjectionPendingError):
        await binding.terminate(
            run_id=run_id,
            tenant_id=tenant_id,
            trigger_task_id="task-a",
            finalized_at_ms=8000,
            trigger_terminal_state="done",
        )
    retry = await binding.terminate(
        run_id=run_id,
        tenant_id=tenant_id,
        trigger_task_id="task-a",
        finalized_at_ms=9000,
        trigger_terminal_state="done",
    )
    assert retry.status == ALREADY_PROJECTED_STATUS
    assert len(await _events(real_redis, run_id)) == 1


async def test_concurrent_exact_requests_commit_one_revision_and_one_event(real_redis):
    run_id = "tenant-a:run-term-concurrent"
    tenant_id = "tenant-a"
    store = await _seed_canonical_run(
        real_redis,
        run_id=run_id,
        tenant_id=tenant_id,
        tasks={"task-a": "done", "task-b": "done"},
    )
    binding = await _binding(real_redis, store=store)

    async def call(index: int):
        return await binding.terminate(
            run_id=run_id,
            tenant_id=tenant_id,
            trigger_task_id="task-b",
            finalized_at_ms=10000 + index,
            worker_instance_id=f"worker-{index}",
            trigger_terminal_state="done",
        )

    results = await asyncio.gather(*(call(index) for index in range(8)))
    assert all(item.finalized and item.ack_allowed for item in results)
    assert len({item.terminal_proof_sha256 for item in results}) == 1
    snapshot = await store.get_aggregate_snapshot(run_terminate_identity(run_id))
    assert snapshot is not None and snapshot.revision == 2 and snapshot.state == "done"
    assert len(await _events(real_redis, run_id)) == 1


async def test_missing_run_meta_is_supported_like_legacy_finalizer(real_redis):
    run_id = "tenant-a:run-term-no-meta"
    tenant_id = "tenant-a"
    store = await _seed_canonical_run(
        real_redis,
        run_id=run_id,
        tenant_id=tenant_id,
        tasks={"task-a": "done"},
        with_run_meta=False,
    )
    binding = await _binding(real_redis, store=store)
    result = await binding.terminate(
        run_id=run_id,
        tenant_id=tenant_id,
        trigger_task_id="task-a",
        finalized_at_ms=11000,
        trigger_terminal_state="done",
    )
    assert result.status == PROJECTED_STATUS
    meta = await real_redis.hgetall(RedisKey.run_meta(run_id))
    assert meta["run_id"] == run_id and meta["tenant_id"] == tenant_id


async def test_task_identity_mismatch_fails_before_proof_or_authority(real_redis):
    run_id = "tenant-a:run-term-task-identity"
    tenant_id = "tenant-a"
    store = await _seed_canonical_run(
        real_redis,
        run_id=run_id,
        tenant_id=tenant_id,
        tasks={"task-a": "done"},
    )
    await real_redis.hset(DagRedisKey.task_meta("task-a"), "run_id", "other-run")
    binding = await _binding(real_redis, store=store)
    with pytest.raises(RunTerminateAuthorityError, match="terminal_proof_task_identity_mismatch"):
        await binding.terminate(
            run_id=run_id,
            tenant_id=tenant_id,
            trigger_task_id="task-a",
            finalized_at_ms=12000,
            trigger_terminal_state="done",
        )
    assert await real_redis.exists(TerminalAggregateProofManager.proof_key(run_id)) == 0
    snapshot = await store.get_aggregate_snapshot(run_terminate_identity(run_id))
    assert snapshot is not None and snapshot.revision == 1


async def test_projection_receipt_corruption_after_durable_commit_fails_closed(real_redis):
    run_id = "tenant-a:run-term-corrupt-projection"
    tenant_id = "tenant-a"
    store = await _seed_canonical_run(
        real_redis,
        run_id=run_id,
        tenant_id=tenant_id,
        tasks={"task-a": "done"},
    )
    proof, plan = await _capture_and_commit_without_projection(
        real_redis,
        store,
        run_id=run_id,
        tenant_id=tenant_id,
        finalized_at_ms=13000,
    )
    projection = RunTerminateProjectionManager(real_redis)
    operation_id = run_terminate_operation_id(run_id, proof.proof_sha256)
    await real_redis.set(projection.receipt_key(operation_id), "wrong-type")
    binding = await _binding(real_redis, store=store, projection=projection)
    with pytest.raises(RunTerminateProjectionPendingError):
        await binding.terminate(
            run_id=run_id,
            tenant_id=tenant_id,
            trigger_task_id="task-a",
            finalized_at_ms=14000,
            trigger_terminal_state="done",
        )
    snapshot = await store.get_aggregate_snapshot(run_terminate_identity(run_id))
    assert snapshot is not None and snapshot.revision == plan.record.to_revision == 2
    assert await _events(real_redis, run_id) == []
