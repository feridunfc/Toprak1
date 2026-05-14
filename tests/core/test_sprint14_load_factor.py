from __future__ import annotations

import sys
import time
from pathlib import Path

import fakeredis.aioredis as faredis
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "hfa-control" / "src"))

from hfa_control.models import ControlPlaneConfig, WorkerProfile, WorkerStatus
from hfa_control.registry import WorkerRegistry


@pytest.mark.asyncio
class TestSprint14LoadFactor:
    async def test_load_factor_calculation(self):
        redis = faredis.FakeRedis()
        cfg = ControlPlaneConfig(instance_id="cp-test")
        registry = WorkerRegistry(redis, cfg)

        worker = WorkerProfile(
            worker_id="w1",
            worker_group="g1",
            region="us",
            shards=[0],
            capacity=10,
            inflight=3,
            status=WorkerStatus.HEALTHY,
            last_seen=time.time(),
        )

        assert registry._capacity_of(worker) == 10
        assert registry._inflight_of(worker) == 3
        assert registry._load_factor_of(worker) == 0.3

    async def test_full_capacity_excluded_from_schedulable(self):
        redis = faredis.FakeRedis()
        cfg = ControlPlaneConfig(instance_id="cp-test")
        registry = WorkerRegistry(redis, cfg)

        worker = WorkerProfile(
            worker_id="w1",
            worker_group="g1",
            region="us",
            shards=[0],
            capacity=10,
            inflight=10,
            status=WorkerStatus.HEALTHY,
            last_seen=time.time(),
        )

        assert registry._worker_is_schedulable(worker) is False

    async def test_zero_capacity_excluded(self):
        redis = faredis.FakeRedis()
        cfg = ControlPlaneConfig(instance_id="cp-test")
        registry = WorkerRegistry(redis, cfg)

        worker = WorkerProfile(
            worker_id="w1",
            worker_group="g1",
            region="us",
            shards=[0],
            capacity=0,
            inflight=0,
            status=WorkerStatus.HEALTHY,
            last_seen=time.time(),
        )

        assert registry._worker_is_schedulable(worker) is False

    async def test_draining_excluded(self):
        redis = faredis.FakeRedis()
        cfg = ControlPlaneConfig(instance_id="cp-test")
        registry = WorkerRegistry(redis, cfg)

        worker = WorkerProfile(
            worker_id="w1",
            worker_group="g1",
            region="us",
            shards=[0],
            capacity=10,
            inflight=3,
            status=WorkerStatus.DRAINING,
            last_seen=time.time(),
        )

        assert registry._worker_is_schedulable(worker) is False

    async def test_lower_load_factor_preferred(self):
        redis = faredis.FakeRedis()
        cfg = ControlPlaneConfig(instance_id="cp-test")
        registry = WorkerRegistry(redis, cfg)

        workers = [
            WorkerProfile(
                worker_id="w1",
                worker_group="g1",
                region="us",
                shards=[0],
                capacity=10,
                inflight=9,
                status=WorkerStatus.HEALTHY,
                last_seen=time.time(),
            ),
            WorkerProfile(
                worker_id="w2",
                worker_group="g2",
                region="us",
                shards=[1],
                capacity=2,
                inflight=1,
                status=WorkerStatus.HEALTHY,
                last_seen=time.time(),
            ),
        ]

        workers.sort(key=lambda w: registry._load_factor_of(w))
        assert workers[0].worker_id == "w2"

    async def test_dict_worker_support(self):
        redis = faredis.FakeRedis()
        cfg = ControlPlaneConfig(instance_id="cp-test")
        registry = WorkerRegistry(redis, cfg)

        worker = {
            "worker_id": "w1",
            "worker_group": "g1",
            "capacity": 10,
            "inflight": 3,
            "status": "healthy",
            "last_seen": time.time(),
        }

        assert registry._capacity_of(worker) == 10
        assert registry._inflight_of(worker) == 3
        assert registry._load_factor_of(worker) == 0.3
        assert registry._worker_is_schedulable(worker) is True
