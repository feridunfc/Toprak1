from dataclasses import dataclass

import fakeredis.aioredis as faredis
import pytest

from scripts.recovery_audit import RUNNING_ZSET
from scripts.recovery_requeue import evaluate_proof, proof_gated_requeue


@dataclass
class _Result:
    ok: bool
    status: str
    requeue_count: int = 0


class _TrackingManager:
    def __init__(self, result: _Result | None = None):
        self.calls = []
        self.result = result or _Result(ok=True, status="TASK_REQUEUED", requeue_count=1)

    async def requeue_stale_task(self, **kwargs):
        self.calls.append(kwargs)
        return self.result


@pytest.mark.asyncio
async def test_recovery_requeue_blocks_missing_candidate_without_mutation():
    redis = faredis.FakeRedis()
    manager = _TrackingManager()

    result = await proof_gated_requeue(
        redis,
        run_id="missing-run",
        tenant_id="acme",
        manager=manager,
        dry_run=False,
    )

    assert result.status == "BLOCKED"
    assert result.requeue_status == "NO_RECOVERY_CANDIDATE"
    assert result.candidate_found is False
    assert result.mutation_attempted is False
    assert manager.calls == []


@pytest.mark.asyncio
async def test_recovery_requeue_blocks_dirty_proof_without_mutation():
    redis = faredis.FakeRedis()
    manager = _TrackingManager()
    run_id = "stale-run"

    await redis.zadd(RUNNING_ZSET, {run_id: 100.0})
    await redis.hset(
        f"hfa:run:meta:{run_id}",
        mapping={"run_id": run_id, "tenant_id": "acme", "admitted_at": "100.0", "state": "running"},
    )
    await redis.set(f"hfa:run:state:{run_id}", "running")

    result = await proof_gated_requeue(
        redis,
        run_id=run_id,
        tenant_id="acme",
        manager=manager,
        replay_clean=False,
        dry_run=False,
        stale_after_seconds=300,
    )

    assert result.status == "BLOCKED"
    assert result.candidate_found is True
    assert result.proof_allowed is False
    assert result.requeue_status == "PROOF_DENIED"
    assert result.mutation_attempted is False
    assert "replay_dirty" in result.proof_reason
    assert manager.calls == []


@pytest.mark.asyncio
async def test_recovery_requeue_dry_run_does_not_mutate_even_when_allowed():
    redis = faredis.FakeRedis()
    manager = _TrackingManager()
    run_id = "stale-run"

    await redis.zadd(RUNNING_ZSET, {run_id: 100.0})
    await redis.hset(
        f"hfa:run:meta:{run_id}",
        mapping={"run_id": run_id, "tenant_id": "acme", "admitted_at": "100.0", "state": "running"},
    )
    await redis.set(f"hfa:run:state:{run_id}", "running")

    result = await proof_gated_requeue(
        redis,
        run_id=run_id,
        tenant_id="acme",
        manager=manager,
        dry_run=True,
        stale_after_seconds=300,
    )

    assert result.status == "PASS"
    assert result.requeue_status == "DRY_RUN"
    assert result.candidate_found is True
    assert result.proof_allowed is True
    assert result.mutation_attempted is False
    assert manager.calls == []


@pytest.mark.asyncio
async def test_recovery_requeue_allowed_candidate_calls_manager_once():
    redis = faredis.FakeRedis()
    manager = _TrackingManager(_Result(ok=True, status="TASK_REQUEUED", requeue_count=2))
    run_id = "stale-run"

    await redis.zadd(RUNNING_ZSET, {run_id: 100.0})
    await redis.hset(
        f"hfa:run:meta:{run_id}",
        mapping={"run_id": run_id, "tenant_id": "acme", "admitted_at": "100.0", "state": "running"},
    )
    await redis.set(f"hfa:run:state:{run_id}", "running")

    result = await proof_gated_requeue(
        redis,
        run_id=run_id,
        tenant_id="acme",
        manager=manager,
        dry_run=False,
        stale_after_seconds=300,
    )

    assert result.status == "PASS"
    assert result.requeue_ok is True
    assert result.requeue_status == "TASK_REQUEUED"
    assert result.requeue_count == 2
    assert result.mutation_attempted is True
    assert manager.calls == [{"task_id": run_id, "tenant_id": "acme"}]


def test_recovery_requeue_proof_decision_fail_closed_reasons():
    result = evaluate_proof(
        replay_clean=False,
        deterministic_replay_ok=False,
        authority_artifact_ok=False,
    )

    assert result.allowed is False
    assert "replay_dirty" in result.reason
    assert "deterministic_replay_failed" in result.reason
    assert "authority_artifact_not_ok" in result.reason
