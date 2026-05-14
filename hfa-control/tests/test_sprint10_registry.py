"""
hfa-control/tests/test_sprint10_registry.py
IRONCLAD Sprint 10 — WorkerRegistry tests
"""

from __future__ import annotations

import time
import pytest
import fakeredis.aioredis as faredis

from hfa.events.schema import WorkerHeartbeatEvent, WorkerDrainingEvent
from hfa.events.codec import serialize_event
from hfa_control.registry import WorkerRegistry
from hfa_control.models import ControlPlaneConfig
from hfa_control.exceptions import WorkerNotFoundError


def _cfg(**kw) -> ControlPlaneConfig:
    return ControlPlaneConfig(
        instance_id="test",
        region="us-east-1",
        worker_heartbeat_ttl=30.0,
        registry_ttl=60,
        autoclaim_idle_ms=30_000,
        autoclaim_count=10,
        **kw,
    )


def _hb_event(
    worker_id="w0",
    group="grp-a",
    region="us-east-1",
    capacity=8,
    inflight=2,
    shards=None,
) -> WorkerHeartbeatEvent:
    return WorkerHeartbeatEvent(
        worker_id=worker_id,
        worker_group=group,
        region=region,
        capacity=capacity,
        inflight=inflight,
        shards=shards or [0, 1, 2, 3],
        version="10.0.0",
        capabilities=["python"],
    )


class TestWorkerRegistry:
    @pytest.mark.asyncio
    async def test_heartbeat_writes_worker_hash(self):
        redis = faredis.FakeRedis()
        cfg = _cfg()
        reg = WorkerRegistry(redis, cfg)
        evt = _hb_event(worker_id="w1")
        await reg._on_heartbeat(evt)

        raw = await redis.hgetall("hfa:cp:worker:w1")
        assert raw[b"worker_id"] == b"w1"
        assert raw[b"worker_group"] == b"grp-a"
        assert raw[b"capacity"] == b"8"
        assert raw[b"inflight"] == b"2"
        assert raw[b"status"] == b"healthy"

    @pytest.mark.asyncio
    async def test_heartbeat_registers_in_region_set(self):
        redis = faredis.FakeRedis()
        reg = WorkerRegistry(redis, _cfg())
        await reg._on_heartbeat(_hb_event(worker_id="w2", region="eu-west-1"))
        members = await redis.smembers("hfa:cp:workers:by_region:eu-west-1")
        assert b"w2" in members

    @pytest.mark.asyncio
    async def test_list_healthy_workers_returns_alive(self):
        redis = faredis.FakeRedis()
        reg = WorkerRegistry(redis, _cfg(worker_heartbeat_ttl=30.0))
        # Write fresh heartbeat
        evt = _hb_event(worker_id="w3")
        await reg._on_heartbeat(evt)
        workers = await reg.list_healthy_workers(region="us-east-1")
        assert any(w.worker_id == "w3" for w in workers)

    @pytest.mark.asyncio
    async def test_list_healthy_workers_excludes_dead(self):
        redis = faredis.FakeRedis()
        reg = WorkerRegistry(redis, _cfg(worker_heartbeat_ttl=0.0))  # instant TTL
        evt = _hb_event(worker_id="w4")
        await reg._on_heartbeat(evt)
        # last_seen defaults to event.timestamp; with ttl=0 any age is dead
        # Force last_seen to ancient past
        await redis.hset("hfa:cp:worker:w4", "last_seen", str(time.time() - 9999))
        workers = await reg.list_healthy_workers(region="us-east-1")
        assert not any(w.worker_id == "w4" for w in workers)

    @pytest.mark.asyncio
    async def test_draining_sets_status(self):
        redis = faredis.FakeRedis()
        reg = WorkerRegistry(redis, _cfg())
        await reg._on_heartbeat(_hb_event(worker_id="w5"))
        drain = WorkerDrainingEvent(
            worker_id="w5",
            worker_group="grp-a",
            region="us-east-1",
            drain_deadline_utc="2099-01-01T00:00:00+00:00",
            reason="SIGTERM",
        )
        await reg._on_draining(drain)
        raw = await redis.hgetall("hfa:cp:worker:w5")
        assert raw[b"status"] == b"draining"

    @pytest.mark.asyncio
    async def test_get_worker_returns_profile(self):
        redis = faredis.FakeRedis()
        reg = WorkerRegistry(redis, _cfg())
        await reg._on_heartbeat(_hb_event(worker_id="w6"))
        profile = await reg.get_worker("w6")
        assert profile.worker_id == "w6"
        assert profile.capacity == 8

    @pytest.mark.asyncio
    async def test_get_worker_raises_not_found(self):
        redis = faredis.FakeRedis()
        reg = WorkerRegistry(redis, _cfg())
        with pytest.raises(WorkerNotFoundError):
            await reg.get_worker("nonexistent")

    @pytest.mark.asyncio
    async def test_mark_dead(self):
        redis = faredis.FakeRedis()
        reg = WorkerRegistry(redis, _cfg())
        await reg._on_heartbeat(_hb_event(worker_id="w7"))
        await reg.mark_dead("w7")
        raw = await redis.hgetall("hfa:cp:worker:w7")
        assert raw[b"status"] == b"dead"

    @pytest.mark.asyncio
    async def test_registry_size(self):
        redis = faredis.FakeRedis()
        reg = WorkerRegistry(redis, _cfg())
        assert await reg.registry_size() == 0
        await reg._on_heartbeat(_hb_event(worker_id="wa"))
        await reg._on_heartbeat(_hb_event(worker_id="wb"))
        assert await reg.registry_size() == 2

    @pytest.mark.asyncio
    async def test_heartbeat_stream_handle_dispatch(self):
        redis = faredis.FakeRedis()
        reg = WorkerRegistry(redis, _cfg())
        hb = _hb_event(worker_id="w8")
        data = {k.encode(): v.encode() for k, v in serialize_event(hb).items()}
        await reg._handle(data)
        raw = await redis.hgetall("hfa:cp:worker:w8")
        assert raw[b"worker_id"] == b"w8"
