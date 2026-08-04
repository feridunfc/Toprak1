import pytest

from hfa.config.keys import RedisKey
from hfa.dag.schema import DagRedisKey
from hfa_control.task_claim import TaskClaimManager

pytestmark = pytest.mark.asyncio


async def _seed_claim_task(
    redis_client,
    *,
    task_id: str,
    run_id: str,
) -> None:
    await redis_client.set(
        DagRedisKey.task_state(task_id),
        "scheduled",
    )
    await redis_client.hset(
        DagRedisKey.task_meta(task_id),
        mapping={
            "task_id": task_id,
            "run_id": run_id,
        },
    )
    await redis_client.set(
        RedisKey.run_state(run_id),
        "running",
    )


@pytest.mark.integration
async def test_legacy_direct_claim_disabled_by_default(redis_client):
    await _seed_claim_task(
        redis_client,
        task_id="legacy-default-deny",
        run_id="run-legacy-default-deny",
    )

    mgr = TaskClaimManager(redis_client)
    result = await mgr.claim_start(
        task_id="legacy-default-deny",
        tenant_id="tenant-a",
        worker_instance_id="worker-legacy",
        claimed_at_ms=123500,
    )

    assert result.ok is False
    assert result.status == "reservation_missing"
    assert await redis_client.get(DagRedisKey.task_state("legacy-default-deny")) == "scheduled"


@pytest.mark.integration
async def test_legacy_direct_claim_requires_explicit_allow(redis_client):
    await _seed_claim_task(
        redis_client,
        task_id="legacy-explicit-allow",
        run_id="run-legacy-explicit-allow",
    )

    mgr = TaskClaimManager(redis_client)
    result = await mgr.claim_start(
        task_id="legacy-explicit-allow",
        tenant_id="tenant-a",
        worker_instance_id="worker-legacy",
        claimed_at_ms=123500,
        allow_legacy_direct_claim=True,
    )

    assert result.ok is True
    assert result.status == "task_claimed"
    assert await redis_client.get(DagRedisKey.task_state("legacy-explicit-allow")) == "running"
