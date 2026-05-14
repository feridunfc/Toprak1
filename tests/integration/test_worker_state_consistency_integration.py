
import pytest

from hfa.dag.schema import DagRedisKey
from hfa_control.worker_state_consistency import WorkerStateConsistency

pytestmark = pytest.mark.asyncio


@pytest.mark.integration
async def test_fresh_worker_survives_filter(redis_client):
    await redis_client.set(DagRedisKey.worker_load("worker-1"), "1")
    await redis_client.set(DagRedisKey.worker_capacity("worker-1"), "4")
    await redis_client.set(DagRedisKey.worker_heartbeat("worker-1"), "9500")

    checker = WorkerStateConsistency(redis_client)
    decisions = await checker.filter_eligible_workers(
        ["worker-1"],
        now_ms=10_000,
        stale_after_ms=5_000,
    )

    assert len(decisions) == 1
    assert decisions[0].eligible is True
    assert decisions[0].reason == "eligible"


@pytest.mark.integration
async def test_stale_worker_filtered_out(redis_client):
    await redis_client.set(DagRedisKey.worker_load("worker-2"), "1")
    await redis_client.set(DagRedisKey.worker_capacity("worker-2"), "4")
    await redis_client.set(DagRedisKey.worker_heartbeat("worker-2"), "1000")

    checker = WorkerStateConsistency(redis_client)
    decisions = await checker.filter_eligible_workers(
        ["worker-2"],
        now_ms=10_000,
        stale_after_ms=5_000,
    )

    assert decisions[0].eligible is False
    assert decisions[0].reason == "stale_heartbeat"


@pytest.mark.integration
async def test_capacity_full_filtered_out(redis_client):
    await redis_client.set(DagRedisKey.worker_load("worker-3"), "8")
    await redis_client.set(DagRedisKey.worker_capacity("worker-3"), "8")
    await redis_client.set(DagRedisKey.worker_heartbeat("worker-3"), "9900")

    checker = WorkerStateConsistency(redis_client)
    decisions = await checker.filter_eligible_workers(
        ["worker-3"],
        now_ms=10_000,
        stale_after_ms=5_000,
    )

    assert decisions[0].eligible is False
    assert decisions[0].reason == "capacity_full"
