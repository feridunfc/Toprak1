"""
hfa-control/tests/test_sprint10_recovery.py
IRONCLAD Sprint 10 — RecoveryService tests
"""

from __future__ import annotations

import time
import pytest
import fakeredis.aioredis as faredis

from hfa_control.recovery import RecoveryService
from hfa_control.models import ControlPlaneConfig
from hfa_control.exceptions import DLQEntryNotFoundError, TenantMismatchError


def _cfg(**kw) -> ControlPlaneConfig:
    data = {
        "instance_id": "test",
        "stale_run_timeout": 0.1,
        "recovery_sweep_interval": 999,
        "max_reschedule_attempts": 3,
    }
    data.update(kw)
    return ControlPlaneConfig(**data)


async def _seed_running(
    redis,
    run_id,
    tenant="acme",
    agent="coder",
    group="grp-a",
    reschedule_count=0,
    admitted_offset=-700,
):
    await redis.set(f"hfa:run:state:{run_id}", "running")
    await redis.hset(
        f"hfa:run:meta:{run_id}",
        mapping={
            "run_id": run_id,
            "tenant_id": tenant,
            "agent_type": agent,
            "worker_group": group,
            "reschedule_count": str(reschedule_count),
            "admitted_at": str(time.time() + admitted_offset),
        },
    )
    await redis.zadd("hfa:cp:running", {run_id: time.time() + admitted_offset})


class TestRecoveryService:
    @pytest.mark.asyncio
    async def test_stale_run_rescheduled(self):
        redis = faredis.FakeRedis()
        recovery = RecoveryService(redis, _cfg())
        await _seed_running(redis, "run-stale-001")
        await recovery._sweep()
        state = await redis.get("hfa:run:state:run-stale-001")
        assert state.decode() == "rescheduled"

    @pytest.mark.asyncio
    async def test_reschedule_count_incremented(self):
        redis = faredis.FakeRedis()
        recovery = RecoveryService(redis, _cfg())
        await _seed_running(redis, "run-cnt-001", reschedule_count=1)
        await recovery._sweep()
        meta = await redis.hgetall("hfa:run:meta:run-cnt-001")
        assert meta[b"reschedule_count"] == b"2"

    @pytest.mark.asyncio
    async def test_max_reschedule_sends_to_dlq(self):
        redis = faredis.FakeRedis()
        recovery = RecoveryService(redis, _cfg(max_reschedule_attempts=3))
        await _seed_running(redis, "run-dlq-001", reschedule_count=3)
        await recovery._sweep()
        state = await redis.get("hfa:run:state:run-dlq-001")
        assert state.decode() == "dead_lettered"
        dlq_meta = await redis.hgetall("hfa:cp:dlq:meta:run-dlq-001")
        assert dlq_meta[b"reason"] == b"max_reschedule_exceeded"

    @pytest.mark.asyncio
    async def test_dlq_event_emitted_to_stream(self):
        redis = faredis.FakeRedis()
        recovery = RecoveryService(redis, _cfg(max_reschedule_attempts=3))
        await _seed_running(redis, "run-dlq-evt-001", reschedule_count=3)
        await recovery._sweep()
        events = await redis.xrange("hfa:stream:dlq")
        assert len(events) == 1
        assert events[0][1][b"event_type"] == b"RunDeadLettered"

    @pytest.mark.asyncio
    async def test_readmit_event_emitted_on_reschedule(self):
        redis = faredis.FakeRedis()
        recovery = RecoveryService(redis, _cfg())
        await _seed_running(redis, "run-readmit-001")
        await recovery._sweep()
        events = await redis.xrange("hfa:stream:control")
        types = [e[1].get(b"event_type", b"").decode() for e in events]
        assert "RunRescheduled" in types
        assert "RunAdmitted" in types

    @pytest.mark.asyncio
    async def test_fresh_run_not_rescheduled(self):
        redis = faredis.FakeRedis()
        recovery = RecoveryService(redis, _cfg(stale_run_timeout=9999))
        await _seed_running(redis, "run-fresh-001", admitted_offset=0)
        await recovery._sweep()
        state = await redis.get("hfa:run:state:run-fresh-001")
        assert state.decode() == "running"  # unchanged

    @pytest.mark.asyncio
    async def test_done_run_removed_from_zset(self):
        redis = faredis.FakeRedis()
        recovery = RecoveryService(redis, _cfg())
        run_id = "run-done-001"
        await redis.set(f"hfa:run:state:{run_id}", "done")
        await redis.zadd("hfa:cp:running", {run_id: time.time() - 700})
        stale = await recovery._find_stale_runs()
        assert run_id not in stale
        # verify it was cleaned from ZSET
        score = await redis.zscore("hfa:cp:running", run_id)
        assert score is None

    @pytest.mark.asyncio
    async def test_dlq_replay_re_emits_admitted(self):
        redis = faredis.FakeRedis()
        recovery = RecoveryService(redis, _cfg())
        run_id = "run-replay-001"
        # seed DLQ
        await redis.hset(
            f"hfa:cp:dlq:meta:{run_id}",
            mapping={
                "run_id": run_id,
                "tenant_id": "acme",
                "reason": "max_reschedule_exceeded",
                "reschedule_count": "3",
                "dead_lettered_at": str(time.time()),
            },
        )
        await redis.set(f"hfa:run:state:{run_id}", "dead_lettered")
        await recovery.replay_dlq_run(run_id, "acme")

        state = await redis.get(f"hfa:run:state:{run_id}")
        assert state.decode() == "admitted"
        # DLQ meta deleted
        dlq = await redis.hgetall(f"hfa:cp:dlq:meta:{run_id}")
        assert not dlq
        # RunAdmittedEvent emitted
        events = await redis.xrange("hfa:stream:control")
        types = [e[1].get(b"event_type", b"").decode() for e in events]
        assert "RunAdmitted" in types

    @pytest.mark.asyncio
    async def test_dlq_replay_not_found_raises(self):
        redis = faredis.FakeRedis()
        recovery = RecoveryService(redis, _cfg())
        with pytest.raises(DLQEntryNotFoundError):
            await recovery.replay_dlq_run("nonexistent", "acme")

    @pytest.mark.asyncio
    async def test_dlq_replay_tenant_mismatch_raises(self):
        redis = faredis.FakeRedis()
        run_id = "run-mismatch-001"
        await redis.hset(
            f"hfa:cp:dlq:meta:{run_id}",
            mapping={
                "run_id": run_id,
                "tenant_id": "other-tenant",
                "reason": "x",
                "reschedule_count": "1",
                "dead_lettered_at": "0",
            },
        )
        recovery = RecoveryService(redis, _cfg())
        with pytest.raises(TenantMismatchError):
            await recovery.replay_dlq_run(run_id, "acme")

    @pytest.mark.asyncio
    async def test_dlq_depth(self):
        redis = faredis.FakeRedis()
        recovery = RecoveryService(redis, _cfg())
        assert await recovery.dlq_depth() == 0
        await _seed_running(redis, "run-d1", reschedule_count=3)
        await recovery._sweep()
        assert await recovery.dlq_depth() == 1

    @pytest.mark.asyncio
    async def test_list_dlq_filters_by_tenant(self):
        redis = faredis.FakeRedis()
        recovery = RecoveryService(redis, _cfg(max_reschedule_attempts=3))
        await _seed_running(redis, "run-t1", tenant="acme", reschedule_count=3)
        await _seed_running(redis, "run-t2", tenant="other", reschedule_count=3)
        await recovery._sweep()
        entries = await recovery.list_dlq("acme")
        assert all(e["tenant_id"] == "acme" for e in entries)
        assert any(e["run_id"] == "run-t1" for e in entries)
        assert not any(e["run_id"] == "run-t2" for e in entries)
