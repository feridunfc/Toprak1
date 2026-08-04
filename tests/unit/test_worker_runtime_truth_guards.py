from __future__ import annotations

import hashlib
from dataclasses import replace
from pathlib import Path

import pytest

from hfa.authority import AggregateType, CanonicalAggregateIdentity
from hfa.config.keys import RedisKey
from hfa.dag.schema import DagRedisKey
from hfa_control.dag_lua import DagLua
from hfa_control.task_claim_authority import (
    TaskClaimCanonicalProjectionInput,
)
from hfa_control.task_recovery import TaskHeartbeatManager


ROOT = Path(__file__).resolve().parents[2]
CLAIM_LUA = ROOT / "hfa-core/src/hfa/lua/task_claim_start.lua"
HEARTBEAT_LUA = ROOT / "hfa-core/src/hfa/lua/task_heartbeat.lua"
COMPLETE_LUA = ROOT / "hfa-core/src/hfa/lua/task_complete.lua"
CONSUMER = ROOT / "hfa-worker/src/hfa_worker/task_consumer.py"
HEARTBEAT_LOOP = ROOT / "hfa-worker/src/hfa_worker/task_heartbeat.py"


def _sha256(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


class _Redis:
    pass


class _Loader:
    def __init__(self, raw):
        self.raw = raw
        self.call = None

    async def run(self, *, num_keys, keys, args):
        self.call = (num_keys, keys, args)
        return self.raw


def test_each_worker_operation_binds_exact_operation_identity() -> None:
    cases = {
        CLAIM_LUA: "TASK_CLAIM",
        HEARTBEAT_LUA: "TASK_HEARTBEAT",
        COMPLETE_LUA: "TASK_COMPLETE",
    }
    for path, operation in cases.items():
        source = path.read_text(encoding="utf-8")
        assert f"'{operation}'" in source
        assert "length_prefix(OPERATION)" in source
        assert ".. length_prefix(run_id)" in source
        assert ".. length_prefix(task_id)" in source
        assert ".. length_prefix(status)" in source
        assert ".. length_prefix(detail_code)" in source
        assert ".. length_prefix(observed_run_state or '')" in source
        assert "operation=OPERATION" in source
        assert "'operation', OPERATION" in source
        assert "existing.operation ~= OPERATION" in source


def test_complete_selects_task_fail_for_failed_terminal_state() -> None:
    source = COMPLETE_LUA.read_text(encoding="utf-8")
    assert "terminal_state == 'done' and 'TASK_COMPLETE' or 'TASK_FAIL'" in source


def test_identity_validation_precedes_run_truth_and_mutation() -> None:
    checks = [
        (CLAIM_LUA, "local authoritative_identity", "local run_kind", "HINCRBY"),
        (HEARTBEAT_LUA, "local authoritative_identity", "local run_kind", "last_heartbeat_at_ms"),
        (COMPLETE_LUA, "local authoritative_identity", "local run_kind", "redis.call('SET', task_state_key, terminal_state"),
    ]
    for path, identity, run_guard, mutation in checks:
        source = path.read_text(encoding="utf-8")
        assert source.index(identity) < source.index(run_guard) < source.index(mutation)


def test_exact_runtime_truth_state_sets_are_present() -> None:
    for path in (CLAIM_LUA, HEARTBEAT_LUA, COMPLETE_LUA):
        source = path.read_text(encoding="utf-8")
        for state in ("admitted", "queued", "scheduled", "running", "rescheduled"):
            assert f"{state}=true" in source
        for state in ("done", "failed", "rejected", "dead_lettered"):
            assert f"{state}=true" in source


@pytest.mark.asyncio
async def test_dag_lua_claim_passes_run_truth_and_conflict_keys() -> None:
    dag = DagLua(_Redis())
    loader = _Loader(["run_truth_missing", "", "", "", "task-1"])
    dag._admit_loader = object()
    dag._claim_loader = loader

    result = await dag.task_claim_start(
        task_id="task-1",
        run_id="run-1",
        tenant_id="tenant-1",
        worker_instance_id="worker-1",
        claimed_at_ms=100,
        scheduler_epoch="sched-1",
    )

    assert result.status == "run_truth_missing"
    assert loader.call is not None
    num_keys, keys, args = loader.call
    assert num_keys == 9
    assert keys[6] == RedisKey.run_state("run-1")
    assert keys[7] == RedisKey.runtime_truth_conflict_index()
    assert keys[8] == RedisKey.runtime_truth_conflict_stream()
    assert args[8] == "run-1"
    assert args[9] == "0"


@pytest.mark.asyncio
async def test_dag_lua_passes_exact_canonical_claim_projection_proof() -> None:
    identity = CanonicalAggregateIdentity(
        aggregate_type=AggregateType.TASK,
        run_id="run-1",
        task_id="task-1",
    )
    dispatch_operation_id = (
        f"task-dispatch:v1:{identity.sha256}:attempt:2"
    )
    claim_operation_id = (
        f"task-claim:v1:{identity.sha256}:attempt:2"
    )
    projection = TaskClaimCanonicalProjectionInput(
        task_id="task-1",
        run_id="run-1",
        tenant_id="tenant-1",
        worker_instance_id="worker-1",
        scheduler_epoch="sched-1",
        claimed_at_ms=100,
        dispatch_attempt=2,
        dispatch_revision=4,
        previous_claim_epoch=7,
        dispatch_transition_id="dispatch-transition",
        dispatch_record_hash=_sha256("dispatch-record"),
        dispatch_command_hash=_sha256("dispatch-command"),
        dispatch_operation_id=dispatch_operation_id,
        canonical_transition_id="claim-transition",
        canonical_record_hash=_sha256("claim-record"),
        canonical_command_hash=_sha256("claim-command"),
        canonical_revision=5,
        canonical_operation_id=claim_operation_id,
        claim_epoch=8,
    )

    dag = DagLua(_Redis())
    loader = _Loader(
        ["task_claimed", "8", "sched-1", "worker-1", "task-1"]
    )
    dag._admit_loader = object()
    dag._claim_loader = loader

    result = await dag.task_claim_canonical_projection(projection)

    assert result.ok is True
    assert result.claim_epoch == "8"
    assert loader.call is not None
    num_keys, keys, args = loader.call
    assert num_keys == 9
    assert keys[6] == RedisKey.run_state("run-1")
    assert args[9:] == [
        "1",
        "tenant-1",
        "claim-transition",
        _sha256("claim-record"),
        _sha256("claim-command"),
        "5",
        claim_operation_id,
        "7",
        "8",
        "dispatch-transition",
        _sha256("dispatch-record"),
        _sha256("dispatch-command"),
        "4",
        dispatch_operation_id,
        "2",
    ]

    loader.call = None
    with pytest.raises(
        ValueError,
        match="canonical_record_hash must be a lowercase SHA-256",
    ):
        await dag.task_claim_canonical_projection(
            replace(
                projection,
                canonical_record_hash="not-a-sha256",
            )
        )
    assert loader.call is None


@pytest.mark.asyncio
async def test_dag_lua_complete_passes_run_truth_and_conflict_keys() -> None:
    dag = DagLua(_Redis())
    loader = _Loader([0, "run_truth_terminal_conflict", 0, 0])
    dag._admit_loader = object()
    dag._complete_loader = loader

    result = await dag.task_complete(
        task_id="task-1",
        run_id="run-1",
        tenant_id="tenant-1",
        terminal_state="done",
        finished_at_ms=200,
    )

    assert result.status == "run_truth_terminal_conflict"
    assert loader.call is not None
    num_keys, keys, args = loader.call
    assert num_keys == 9
    assert keys[6] == RedisKey.run_state("run-1")
    assert keys[7] == RedisKey.runtime_truth_conflict_index()
    assert keys[8] == RedisKey.runtime_truth_conflict_stream()
    assert args[1] == "run-1"


@pytest.mark.asyncio
async def test_heartbeat_manager_passes_explicit_run_truth_keys() -> None:
    manager = TaskHeartbeatManager(_Redis())
    loader = _Loader(["run_truth_missing"])
    manager._heartbeat_loader = loader

    result = await manager.record_heartbeat(
        task_id="task-1",
        run_id="run-1",
        tenant_id="tenant-1",
        worker_id="worker-1",
        claim_epoch="3",
        now_ms=300,
    )

    assert result.status == "run_truth_missing"
    assert loader.call is not None
    num_keys, keys, args = loader.call
    assert num_keys == 6
    assert keys[3] == RedisKey.run_state("run-1")
    assert keys[4] == RedisKey.runtime_truth_conflict_index()
    assert keys[5] == RedisKey.runtime_truth_conflict_stream()
    assert args == ["task-1", "run-1", "tenant-1", "worker-1", "3", "300"]


def test_task_consumer_propagates_context_run_id_to_claim_heartbeat_and_complete() -> None:
    consumer = CONSUMER.read_text(encoding="utf-8")
    heartbeat = HEARTBEAT_LOOP.read_text(encoding="utf-8")
    assert "run_id=ctx.run_id" in consumer
    assert consumer.count("run_id=ctx.run_id") >= 3
    assert "run_id=self.run_id" in heartbeat


def test_worker_truth_rejections_are_fail_closed_statuses() -> None:
    for path in (CLAIM_LUA, HEARTBEAT_LUA, COMPLETE_LUA):
        source = path.read_text(encoding="utf-8")
        for status in (
            "run_truth_missing",
            "run_truth_terminal_conflict",
            "run_truth_corruption_conflict",
            "truth_conflict_evidence_store_unavailable",
        ):
            assert status in source
