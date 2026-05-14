"""
tests/core/test_sprint12_drain_metrics.py
IRONCLAD Sprint 12 — DrainManager metrics + lifecycle tests

Verifies:
  - drain_started counter increments
  - drain_completed counter increments when inflight reaches zero
  - drain_timeout counter increments when timeout exceeded
  - is_draining property reflects drain state
  - second call to start_drain is idempotent (no double-count)
"""

from __future__ import annotations

from unittest.mock import MagicMock

import fakeredis.aioredis as faredis
import pytest

from hfa.obs.runtime_metrics import IRONCLADMetrics as M
from hfa_worker.drain import DrainManager


@pytest.fixture(autouse=True)
def reset_metrics():
    M.reset_all()
    yield
    M.reset_all()


def _mock_consumer(inflight: int = 0):
    c = MagicMock()
    c.inflight_count = inflight
    c.stop_pulling = MagicMock()
    return c


@pytest.mark.asyncio
async def test_drain_started_counter_increments():
    redis = faredis.FakeRedis()
    consumer = _mock_consumer(inflight=0)

    drain = DrainManager(
        redis=redis,
        worker_id="w1",
        worker_group="grp",
        shards=[0],
        consumer=consumer,
    )
    await redis.set("hfa:cp:shard:owner:0", "grp")
    await drain.start_drain(reason="test", timeout=1.0)

    assert M.worker_drain_started_total.value == 1


@pytest.mark.asyncio
async def test_drain_completed_counter_when_clean():
    redis = faredis.FakeRedis()
    consumer = _mock_consumer(inflight=0)

    drain = DrainManager(
        redis=redis,
        worker_id="w1",
        worker_group="grp",
        shards=[0],
        consumer=consumer,
    )
    await redis.set("hfa:cp:shard:owner:0", "grp")
    await drain.start_drain(reason="test", timeout=2.0)

    assert M.worker_drain_completed_total.value == 1
    assert M.worker_drain_timeout_total.value == 0


@pytest.mark.asyncio
async def test_drain_timeout_counter_when_inflight_remains():
    redis = faredis.FakeRedis()
    consumer = _mock_consumer(inflight=3)  # never drains

    drain = DrainManager(
        redis=redis,
        worker_id="w1",
        worker_group="grp",
        shards=[],
        consumer=consumer,
    )
    await drain.start_drain(reason="test", timeout=0.1)

    assert M.worker_drain_timeout_total.value == 1
    assert M.worker_drain_completed_total.value == 0


@pytest.mark.asyncio
async def test_drain_idempotent_second_call_no_double_count():
    redis = faredis.FakeRedis()
    consumer = _mock_consumer(inflight=0)

    drain = DrainManager(
        redis=redis,
        worker_id="w1",
        worker_group="grp",
        shards=[],
        consumer=consumer,
    )
    await drain.start_drain(reason="test", timeout=0.5)
    await drain.start_drain(reason="test", timeout=0.5)  # second call

    # Only one started
    assert M.worker_drain_started_total.value == 1
    assert M.worker_drain_completed_total.value == 1


@pytest.mark.asyncio
async def test_is_draining_property():
    redis = faredis.FakeRedis()
    consumer = _mock_consumer(inflight=0)

    drain = DrainManager(
        redis=redis,
        worker_id="w1",
        worker_group="grp",
        shards=[],
        consumer=consumer,
    )
    assert drain.is_draining is False
    await drain.start_drain(timeout=0.1)
    assert drain.is_draining is True
