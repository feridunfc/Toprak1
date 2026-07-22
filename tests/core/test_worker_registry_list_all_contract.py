from __future__ import annotations

import time

import pytest

from hfa.config.keys import RedisKey
from hfa_control.models import ControlPlaneConfig, WorkerStatus
from hfa_control.registry import WorkerRegistry


def _worker_hash(
    *,
    worker_id: str,
    region: str,
    last_seen: float,
    status: str = "healthy",
):
    return {
        b"worker_id": worker_id.encode(),
        b"worker_group": b"group-79",
        b"region": region.encode(),
        b"shards": b"[0]",
        b"capacity": b"2",
        b"inflight": b"0",
        b"last_seen": str(last_seen).encode(),
        b"version": b"79.7",
        b"capabilities": b'["fake"]',
        b"status": status.encode(),
    }


class RedisProjectionProbe:
    def __init__(self, rows):
        self.rows = rows
        self.patterns = []
        self.hgetall_calls = []

    async def keys(self, pattern):
        self.patterns.append(pattern)
        return list(self.rows)

    async def hgetall(self, key):
        self.hgetall_calls.append(key)
        return self.rows[key]


@pytest.mark.asyncio
async def test_list_all_workers_loads_real_projection_deterministically() -> None:
    now = time.time()
    rows = {
        b"hfa:cp:worker:worker-b": _worker_hash(
            worker_id="worker-b",
            region="region-b",
            last_seen=now,
        ),
        b"hfa:cp:worker:worker-a": _worker_hash(
            worker_id="worker-a",
            region="region-a",
            last_seen=now,
        ),
    }
    redis = RedisProjectionProbe(rows)
    registry = WorkerRegistry(
        redis,
        ControlPlaneConfig(
            instance_id="cp-registry-contract",
            worker_heartbeat_ttl=30.0,
        ),
    )

    workers = await registry.list_all_workers(region=None)

    assert redis.patterns == [RedisKey.cp_workers_scan_pattern()]
    assert [worker.worker_id for worker in workers] == [
        "worker-a",
        "worker-b",
    ]
    assert all(worker.status is WorkerStatus.HEALTHY for worker in workers)


@pytest.mark.asyncio
async def test_list_all_workers_filters_region_and_marks_stale_dead() -> None:
    now = time.time()
    rows = {
        b"hfa:cp:worker:fresh": _worker_hash(
            worker_id="fresh",
            region="target-region",
            last_seen=now,
        ),
        b"hfa:cp:worker:stale": _worker_hash(
            worker_id="stale",
            region="target-region",
            last_seen=now - 120.0,
        ),
        b"hfa:cp:worker:other": _worker_hash(
            worker_id="other",
            region="other-region",
            last_seen=now,
        ),
    }
    registry = WorkerRegistry(
        RedisProjectionProbe(rows),
        ControlPlaneConfig(
            instance_id="cp-registry-contract",
            worker_heartbeat_ttl=30.0,
        ),
    )

    workers = await registry.list_all_workers(region="target-region")

    assert [worker.worker_id for worker in workers] == ["fresh", "stale"]
    assert workers[0].status is WorkerStatus.HEALTHY
    assert workers[1].status is WorkerStatus.DEAD
