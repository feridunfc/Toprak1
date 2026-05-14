
import pytest
from unittest.mock import AsyncMock

from hfa_control.worker_state_consistency import WorkerStateConsistency

pytestmark = pytest.mark.asyncio


async def test_missing_heartbeat_is_ineligible():
    redis = AsyncMock()
    redis.get.side_effect = [0, 4, None]

    checker = WorkerStateConsistency(redis)
    decision = await checker.evaluate_worker("worker-1", now_ms=10_000, stale_after_ms=5_000)

    assert decision.eligible is False
    assert decision.reason == "missing_heartbeat"


async def test_stale_heartbeat_is_ineligible():
    redis = AsyncMock()
    redis.get.side_effect = [1, 4, 1_000]

    checker = WorkerStateConsistency(redis)
    decision = await checker.evaluate_worker("worker-1", now_ms=10_000, stale_after_ms=5_000)

    assert decision.eligible is False
    assert decision.reason == "stale_heartbeat"


async def test_capacity_full_is_ineligible():
    redis = AsyncMock()
    redis.get.side_effect = [4, 4, 9_500]

    checker = WorkerStateConsistency(redis)
    decision = await checker.evaluate_worker("worker-1", now_ms=10_000, stale_after_ms=5_000)

    assert decision.eligible is False
    assert decision.reason == "capacity_full"


async def test_fresh_worker_is_eligible():
    redis = AsyncMock()
    redis.get.side_effect = [1, 4, 9_500]

    checker = WorkerStateConsistency(redis)
    decision = await checker.evaluate_worker("worker-1", now_ms=10_000, stale_after_ms=5_000)

    assert decision.eligible is True
    assert decision.reason == "eligible"
