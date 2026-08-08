from __future__ import annotations

import json
from pathlib import Path

import pytest

from hfa.config.keys import RedisKey
from hfa_control.run_terminal_event_evidence import (
    CONTRACT_PRODUCER_FIELD,
    CONTRACT_SCHEMA_FIELD,
    READINESS_RESULTS_STREAM_FIELD,
    READINESS_SOURCE_HISTORY_FIELD,
    READINESS_STATUS_FIELD,
    TerminalEventEvidenceError,
    backfill_terminal_event_evidence,
    ensure_terminal_event_index,
)
from hfa_control.run_terminate_authority import (
    ALREADY_PROJECTED_STATUS,
    PROJECTED_STATUS,
    RunTerminateAuthorityError,
    RunTerminateProjectionInput,
    RunTerminateProjectionManager,
    TerminalAggregateProof,
    TerminalTaskEvidence,
    run_terminate_operation_id,
)

pytestmark = [pytest.mark.asyncio, pytest.mark.integration]


def _proof(run_id: str, *, tenant_id: str = "tenant-a", final_state: str = "done", proof_hash: str = "a" * 64):
    failed = 1 if final_state == "failed" else 0
    done = 0 if failed else 1
    return TerminalAggregateProof(
        schema_version=1,
        run_id=run_id,
        tenant_id=tenant_id,
        tasks=(TerminalTaskEvidence("task-a", "failed" if failed else "done"),),
        task_count=1,
        done_count=done,
        failed_count=failed,
        skipped_count=0,
        final_state=final_state,
        proof_sha256=proof_hash,
        proof_payload_json="{}",
        finalized_at_ms=1000,
        worker_instance_id="worker-a",
        trigger_task_id="task-a",
        trigger_terminal_state="failed" if failed else "done",
        canonical_expected_revision=1,
        canonical_previous_state="pending",
    )


def _input(proof: TerminalAggregateProof):
    return RunTerminateProjectionInput(
        operation_id=run_terminate_operation_id(proof.run_id, proof.proof_sha256),
        terminal_proof_sha256=proof.proof_sha256,
        canonical_transition_id="transition-1",
        canonical_record_hash="b" * 64,
        canonical_command_hash="c" * 64,
        canonical_revision=2,
        proof=proof,
    )


async def _seed_projection_footprint(redis, run_id: str):
    await redis.set(RedisKey.run_state(run_id), "running")
    await redis.hset(RedisKey.run_meta(run_id), mapping={"run_id": run_id, "tenant_id": "tenant-a", "state": "running"})
    await redis.zadd(RedisKey.cp_running(), {run_id: 1})


async def _ready(redis):
    await ensure_terminal_event_index(redis)
    await redis.hset(
        RedisKey.run_terminal_event_index(),
        mapping={
            READINESS_STATUS_FIELD: "ready",
            READINESS_RESULTS_STREAM_FIELD: RedisKey.stream_results(),
            READINESS_SOURCE_HISTORY_FIELD: "1",
        },
    )
    await redis.persist(RedisKey.run_terminal_event_index())


def _evidence_field(run_id: str) -> str:
    return RedisKey.run_terminal_event_evidence_field(run_id)


async def _evidence(redis, run_id: str) -> dict[str, str]:
    raw = await redis.hget(RedisKey.run_terminal_event_index(), _evidence_field(run_id))
    assert raw is not None
    value = json.loads(raw)
    assert isinstance(value, dict)
    return value


async def test_migration_not_ready_is_zero_mutation(real_redis):
    proof = _proof("run-not-ready")
    await _seed_projection_footprint(real_redis, proof.run_id)
    await ensure_terminal_event_index(real_redis)
    manager = RunTerminateProjectionManager(real_redis)
    await manager.initialise()
    with pytest.raises(RunTerminateAuthorityError, match="terminal_event_migration_not_ready"):
        await manager.project(_input(proof))
    assert await real_redis.get(RedisKey.run_state(proof.run_id)) == "running"
    assert await real_redis.exists(RedisKey.run_result(proof.run_id)) == 0
    assert await real_redis.hget(RedisKey.run_terminal_event_index(), _evidence_field(proof.run_id)) is None
    assert await real_redis.xlen(RedisKey.stream_results()) == 0


async def test_first_projection_writes_event_evidence_and_receipt_atomically(real_redis):
    proof = _proof("run-first")
    await _seed_projection_footprint(real_redis, proof.run_id)
    await _ready(real_redis)
    manager = RunTerminateProjectionManager(real_redis)
    result = await manager.project(_input(proof))
    assert result.status == PROJECTED_STATUS
    evidence = await _evidence(real_redis, proof.run_id)
    assert evidence["source"] == "canonical_run_terminate"
    assert evidence["stream_entry_id"] == result.stream_entry_id
    assert await real_redis.ttl(RedisKey.run_terminal_event_index()) == -1
    receipt = await real_redis.hgetall(manager.receipt_key(_input(proof).operation_id))
    assert receipt["stream_entry_id"] == result.stream_entry_id
    assert await real_redis.xlen(RedisKey.stream_results()) == 1


async def test_exact_retry_survives_stream_trimming(real_redis):
    proof = _proof("run-trim")
    await _seed_projection_footprint(real_redis, proof.run_id)
    await _ready(real_redis)
    manager = RunTerminateProjectionManager(real_redis)
    first = await manager.project(_input(proof))
    assert await real_redis.xdel(RedisKey.stream_results(), first.stream_entry_id) == 1
    retry = await RunTerminateProjectionManager(real_redis).project(_input(proof))
    assert retry.status == ALREADY_PROJECTED_STATUS
    assert retry.stream_entry_id == first.stream_entry_id
    assert (await _evidence(real_redis, proof.run_id))["event_id"]


async def test_receipt_without_durable_evidence_fails_closed(real_redis):
    proof = _proof("run-missing-evidence")
    await _seed_projection_footprint(real_redis, proof.run_id)
    await _ready(real_redis)
    manager = RunTerminateProjectionManager(real_redis)
    await manager.project(_input(proof))
    await real_redis.hdel(RedisKey.run_terminal_event_index(), _evidence_field(proof.run_id))
    with pytest.raises(RunTerminateAuthorityError, match="terminal_event_evidence_missing"):
        await manager.project(_input(proof))
    assert await real_redis.xlen(RedisKey.stream_results()) == 1


async def test_index_eviction_removes_readiness_and_receipt_retry_fails_closed(real_redis):
    proof = _proof("run-index-evicted")
    await _seed_projection_footprint(real_redis, proof.run_id)
    await _ready(real_redis)
    manager = RunTerminateProjectionManager(real_redis)
    await manager.project(_input(proof))
    await real_redis.delete(RedisKey.run_terminal_event_index())
    with pytest.raises(RunTerminateAuthorityError, match="terminal_event_index_missing_or_wrong_type"):
        await manager.project(_input(proof))
    assert await real_redis.xlen(RedisKey.stream_results()) == 1


async def test_preexisting_historical_evidence_blocks_second_terminal_event(real_redis):
    proof = _proof("run-historical")
    await _seed_projection_footprint(real_redis, proof.run_id)
    await _ready(real_redis)
    historical = {
        "schema_version": "1",
        "run_id": proof.run_id,
        "tenant_id": proof.tenant_id,
        "event_type": "RunCompleted",
        "final_state": "done",
        "event_id": "legacy-event",
        "source": "historical_backfill",
        "stream_entry_id": "1-0",
    }
    await real_redis.hset(
        RedisKey.run_terminal_event_index(),
        _evidence_field(proof.run_id),
        json.dumps(historical, sort_keys=True, separators=(",", ":")),
    )
    manager = RunTerminateProjectionManager(real_redis)
    with pytest.raises(RunTerminateAuthorityError, match="preexisting_terminal_event_evidence"):
        await manager.project(_input(proof))
    assert await real_redis.xlen(RedisKey.stream_results()) == 0
    assert await real_redis.get(RedisKey.run_state(proof.run_id)) == "running"


async def test_changed_proof_after_projection_fails_without_second_event(real_redis):
    first_proof = _proof("run-changed-proof", proof_hash="a" * 64)
    await _seed_projection_footprint(real_redis, first_proof.run_id)
    await _ready(real_redis)
    manager = RunTerminateProjectionManager(real_redis)
    await manager.project(_input(first_proof))
    changed = _proof(first_proof.run_id, proof_hash="d" * 64)
    with pytest.raises(RunTerminateAuthorityError):
        await manager.project(_input(changed))
    assert await real_redis.xlen(RedisKey.stream_results()) == 1


async def test_backfill_is_bounded_idempotent_and_detects_historical_terminal(real_redis):
    stream = RedisKey.stream_results()
    pipe = real_redis.pipeline(transaction=False)
    for index in range(1200):
        pipe.xadd(stream, {"event_id": f"noise-{index}", "event_type": "TaskCompleted", "run_id": f"other-{index}"})
    pipe.xadd(stream, {"event_id": "legacy-terminal", "event_type": "RunFailed", "run_id": "run-legacy", "tenant_id": "tenant-a"})
    await pipe.execute()
    first = await backfill_terminal_event_evidence(real_redis, page_size=137, completed_at_ms=1000)
    assert first.status == "ready"
    assert first.scanned_entries == 1201
    assert first.terminal_events == 1
    evidence = await _evidence(real_redis, "run-legacy")
    assert evidence["event_id"] == "legacy-terminal"
    assert evidence["source"] == "historical_backfill"
    index = await real_redis.hmget(
        RedisKey.run_terminal_event_index(),
        CONTRACT_SCHEMA_FIELD,
        CONTRACT_PRODUCER_FIELD,
        READINESS_STATUS_FIELD,
    )
    assert index == ["1", "1", "ready"]
    second = await backfill_terminal_event_evidence(real_redis, page_size=97, completed_at_ms=2000)
    assert second.status == "already_ready"


async def test_backfill_rejects_malformed_preexisting_evidence(real_redis):
    await ensure_terminal_event_index(real_redis)
    await real_redis.xadd(
        RedisKey.stream_results(),
        {"event_id": "evt-valid", "event_type": "RunCompleted", "run_id": "run-malformed", "tenant_id": "tenant-a"},
    )
    await real_redis.hset(
        RedisKey.run_terminal_event_index(),
        RedisKey.run_terminal_event_evidence_field("run-malformed"),
        "{not-json",
    )
    with pytest.raises(TerminalEventEvidenceError, match="malformed"):
        await backfill_terminal_event_evidence(real_redis, page_size=2, completed_at_ms=1000)
    assert await real_redis.hget(RedisKey.run_terminal_event_index(), READINESS_STATUS_FIELD) is None


async def test_backfill_refuses_trimmed_history(real_redis):
    stream = RedisKey.stream_results()
    ids = []
    for index in range(5):
        ids.append(await real_redis.xadd(stream, {"event_id": f"evt-{index}", "event_type": "TaskCompleted"}))
    await real_redis.xdel(stream, ids[0])
    with pytest.raises(TerminalEventEvidenceError, match="trimming/deletion"):
        await backfill_terminal_event_evidence(real_redis, page_size=2, completed_at_ms=1000)
    assert await real_redis.hget(RedisKey.run_terminal_event_index(), READINESS_STATUS_FIELD) is None


async def test_large_results_stream_contract_has_no_online_full_scan(real_redis):
    source = Path("hfa-core/src/hfa/lua/run_terminate_projection.lua").read_text()
    assert 'redis.call("XRANGE", results_stream, "-", "+")' not in source
    stream = RedisKey.stream_results()
    pipe = real_redis.pipeline(transaction=False)
    for index in range(50_000):
        pipe.xadd(stream, {"event_id": f"noise-{index}", "event_type": "TaskCompleted", "run_id": f"noise-run-{index}"})
        if index and index % 2000 == 0:
            await pipe.execute()
            pipe = real_redis.pipeline(transaction=False)
    await pipe.execute()
    migration = await backfill_terminal_event_evidence(real_redis, page_size=1000, completed_at_ms=1000)
    assert migration.scanned_entries == 50_000
    proof = _proof("run-large-stream")
    await _seed_projection_footprint(real_redis, proof.run_id)
    projected = await RunTerminateProjectionManager(real_redis).project(_input(proof))
    assert projected.status == PROJECTED_STATUS


async def test_backfill_rejects_duplicate_terminal_events_for_same_run(real_redis):
    stream = RedisKey.stream_results()
    await real_redis.xadd(stream, {"event_id": "evt-a", "event_type": "RunCompleted", "run_id": "dup-run", "tenant_id": "tenant-a"})
    await real_redis.xadd(stream, {"event_id": "evt-b", "event_type": "RunCompleted", "run_id": "dup-run", "tenant_id": "tenant-a"})
    with pytest.raises(TerminalEventEvidenceError, match="multiple historical terminal events"):
        await backfill_terminal_event_evidence(real_redis, page_size=1, completed_at_ms=1000)
    assert await real_redis.hget(RedisKey.run_terminal_event_index(), READINESS_STATUS_FIELD) is None


async def test_legacy_run_terminate_writer_creates_evidence_and_blocks_canonical_duplicate(real_redis):
    from hfa.dag.schema import DagRedisKey
    from hfa_control.run_termination import RunTerminationCoordinator

    class Gateway:
        async def task_complete(self, **kwargs):  # pragma: no cover - not used
            raise AssertionError

    await ensure_terminal_event_index(real_redis)
    run_id = "run-legacy-overlap"
    tenant_id = "tenant-a"
    task_id = "task-a"
    await real_redis.set(RedisKey.run_state(run_id), "running")
    await real_redis.hset(RedisKey.run_meta(run_id), mapping={"run_id": run_id, "tenant_id": tenant_id, "state": "running"})
    await real_redis.zadd(RedisKey.cp_running(), {run_id: 1})
    await real_redis.sadd(DagRedisKey.run_tasks(run_id), task_id)
    await real_redis.set(DagRedisKey.task_state(task_id), "done")
    await real_redis.hset(DagRedisKey.task_meta(task_id), mapping={"task_id": task_id, "run_id": run_id, "tenant_id": tenant_id})

    coordinator = RunTerminationCoordinator(real_redis, Gateway(), enabled=True)
    legacy = await coordinator.finalize_run_from_tasks(
        run_id=run_id,
        tenant_id=tenant_id,
        trigger_task_id=task_id,
        finalized_at_ms=1000,
        worker_instance_id="legacy-worker",
        trigger_terminal_state="done",
    )
    assert legacy.finalized is True
    evidence = await _evidence(real_redis, run_id)
    assert evidence["source"] == "legacy_run_terminate"
    assert await real_redis.xlen(RedisKey.stream_results()) == 1

    await real_redis.hset(
        RedisKey.run_terminal_event_index(),
        mapping={
            READINESS_STATUS_FIELD: "ready",
            READINESS_RESULTS_STREAM_FIELD: RedisKey.stream_results(),
            READINESS_SOURCE_HISTORY_FIELD: "1",
        },
    )
    proof = _proof(run_id)
    with pytest.raises(RunTerminateAuthorityError, match="preexisting_terminal_event_evidence"):
        await RunTerminateProjectionManager(real_redis).project(_input(proof))
    assert await real_redis.xlen(RedisKey.stream_results()) == 1


async def test_worker_compat_writer_atomically_creates_evidence(real_redis):
    from hfa.events.schema import RunCompletedEvent
    from hfa_worker.consumer import _append_terminal_event_with_evidence

    await ensure_terminal_event_index(real_redis)
    event = RunCompletedEvent(
        event_id="worker-event-1",
        run_id="worker-compat-run",
        tenant_id="tenant-a",
        worker_id="worker-a",
    )
    await _append_terminal_event_with_evidence(real_redis, event)
    await _append_terminal_event_with_evidence(real_redis, event)
    assert await real_redis.xlen(RedisKey.stream_results()) == 1
    evidence = await _evidence(real_redis, event.run_id)
    assert evidence["source"] == "worker_consumer_compat"
    assert evidence["event_id"] == event.event_id


async def test_worker_compat_writer_fails_closed_if_index_is_missing(real_redis):
    from hfa.events.schema import RunCompletedEvent
    from hfa_worker.consumer import _append_terminal_event_with_evidence

    event = RunCompletedEvent(
        event_id="worker-event-missing-index",
        run_id="worker-missing-index",
        tenant_id="tenant-a",
        worker_id="worker-a",
    )
    with pytest.raises(RuntimeError, match="index is not prepared"):
        await _append_terminal_event_with_evidence(real_redis, event)
    assert await real_redis.xlen(RedisKey.stream_results()) == 0


async def test_wrong_index_key_type_blocks_canonical_projection_with_zero_mutation(real_redis):
    proof = _proof("run-wrong-index-type")
    await _seed_projection_footprint(real_redis, proof.run_id)
    await real_redis.set(RedisKey.run_terminal_event_index(), "corrupt")
    manager = RunTerminateProjectionManager(real_redis)
    with pytest.raises(RunTerminateAuthorityError, match="terminal_event_index_missing_or_wrong_type"):
        await manager.project(_input(proof))
    assert await real_redis.get(RedisKey.run_state(proof.run_id)) == "running"
    assert await real_redis.exists(RedisKey.run_result(proof.run_id)) == 0
    assert await real_redis.xlen(RedisKey.stream_results()) == 0


async def test_receipt_evidence_mismatch_blocks_retry_without_second_event(real_redis):
    proof = _proof("run-evidence-mismatch")
    await _seed_projection_footprint(real_redis, proof.run_id)
    await _ready(real_redis)
    manager = RunTerminateProjectionManager(real_redis)
    first = await manager.project(_input(proof))
    assert first.status == PROJECTED_STATUS
    evidence = await _evidence(real_redis, proof.run_id)
    evidence["canonical_record_hash"] = "d" * 64
    await real_redis.hset(
        RedisKey.run_terminal_event_index(),
        _evidence_field(proof.run_id),
        json.dumps(evidence, sort_keys=True, separators=(",", ":")),
    )
    with pytest.raises(RunTerminateAuthorityError, match="terminal_event_evidence_mismatch"):
        await RunTerminateProjectionManager(real_redis).project(_input(proof))
    assert await real_redis.xlen(RedisKey.stream_results()) == 1
    assert await real_redis.get(RedisKey.run_state(proof.run_id)) == "done"
