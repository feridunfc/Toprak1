"""
tests/core/test_sprint12_recovery_invariants.py
IRONCLAD Sprint 12 — Recovery metrics + invariant tests

Verifies:
  - recovery_stale_detected_total increments on stale run detection
  - recovery_rescheduled_total increments on reschedule
  - recovery_dlq_total increments on DLQ placement
  - Sprint 10 reschedule semantics unchanged (state → rescheduled, re-emits RunAdmittedEvent)
  - Sprint 10 DLQ semantics unchanged (state → dead_lettered, emits RunDeadLetteredEvent)
"""

from __future__ import annotations

import time

import fakeredis.aioredis as faredis
import pytest

from hfa.obs.runtime_metrics import IRONCLADMetrics as M
from hfa_control.models import ControlPlaneConfig
from hfa_control.recovery import RecoveryService


@pytest.fixture(autouse=True)
def reset_metrics():
    M.reset_all()
    yield
    M.reset_all()


@pytest.fixture
def config():
    return ControlPlaneConfig(
        instance_id="test-cp",
        region="us-east-1",
        stale_run_timeout=5.0,
        max_reschedule_attempts=3,
        recovery_sweep_interval=999.0,  # prevent auto-sweep in tests
    )


async def _seed_stale_run(
    redis,
    run_id: str,
    tenant_id: str = "tenant-1",
    agent_type: str = "base",
    reschedule_count: int = 0,
    state: str = "running",
    age_seconds: float = 60.0,
):
    """Seed a run that appears stale in the running ZSET."""
    stale_score = time.time() - age_seconds
    await redis.zadd("hfa:cp:running", {run_id: stale_score})
    await redis.set(f"hfa:run:state:{run_id}", state, ex=86400)
    await redis.hset(
        f"hfa:run:meta:{run_id}",
        mapping={
            "run_id": run_id,
            "tenant_id": tenant_id,
            "agent_type": agent_type,
            "worker_group": "grp-a",
            "reschedule_count": str(reschedule_count),
            "admitted_at": str(stale_score),
        },
    )


# ---------------------------------------------------------------------------
# _find_stale_runs — detection
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_find_stale_runs_detects_old_running_run(config):
    redis = faredis.FakeRedis()
    recovery = RecoveryService(redis, config)

    await _seed_stale_run(redis, "run-stale-1", age_seconds=120.0)

    stale = await recovery._find_stale_runs()
    assert "run-stale-1" in stale


@pytest.mark.asyncio
async def test_find_stale_runs_ignores_recent_run(config):
    redis = faredis.FakeRedis()
    recovery = RecoveryService(redis, config)

    # Score = now (not stale)
    await redis.zadd("hfa:cp:running", {"run-fresh-1": time.time()})
    await redis.set("hfa:run:state:run-fresh-1", "running", ex=86400)

    stale = await recovery._find_stale_runs()
    assert "run-fresh-1" not in stale


@pytest.mark.asyncio
async def test_find_stale_runs_ignores_terminal_run(config):
    redis = faredis.FakeRedis()
    recovery = RecoveryService(redis, config)

    await _seed_stale_run(redis, "run-done-1", state="done", age_seconds=120.0)

    stale = await recovery._find_stale_runs()
    assert "run-done-1" not in stale


# ---------------------------------------------------------------------------
# _sweep — metrics instrumentation
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_sweep_emits_stale_detected_metric(config):
    redis = faredis.FakeRedis()
    recovery = RecoveryService(redis, config)

    await _seed_stale_run(redis, "run-sw-1", age_seconds=120.0)
    await _seed_stale_run(redis, "run-sw-2", age_seconds=120.0)

    await recovery._sweep()

    assert M.recovery_stale_detected_total.value == 2


@pytest.mark.asyncio
async def test_sweep_emits_rescheduled_metric(config):
    redis = faredis.FakeRedis()
    recovery = RecoveryService(redis, config)

    await _seed_stale_run(redis, "run-resched-1", reschedule_count=0, age_seconds=120.0)

    await recovery._sweep()

    assert M.recovery_rescheduled_total.value == 1
    assert M.recovery_dlq_total.value == 0


@pytest.mark.asyncio
async def test_sweep_emits_dlq_metric(config):
    redis = faredis.FakeRedis()
    recovery = RecoveryService(redis, config)

    # max_reschedule_attempts = 3 → at count=3 it goes to DLQ
    await _seed_stale_run(redis, "run-dlq-1", reschedule_count=3, age_seconds=120.0)

    await recovery._sweep()

    assert M.recovery_dlq_total.value == 1
    assert M.recovery_rescheduled_total.value == 0


@pytest.mark.asyncio
async def test_sweep_no_stale_no_metrics(config):
    redis = faredis.FakeRedis()
    recovery = RecoveryService(redis, config)

    await recovery._sweep()

    assert M.recovery_stale_detected_total.value == 0
    assert M.recovery_rescheduled_total.value == 0
    assert M.recovery_dlq_total.value == 0


# ---------------------------------------------------------------------------
# Sprint 10 semantics preserved — reschedule writes correct state + event
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_reschedule_sets_state_and_emits_run_admitted(config):
    redis = faredis.FakeRedis()
    recovery = RecoveryService(redis, config)

    await _seed_stale_run(redis, "run-sem-1", reschedule_count=1, age_seconds=120.0)

    result = await recovery._handle_stale("run-sem-1")

    assert result == "rescheduled"

    state_raw = await redis.get("hfa:run:state:run-sem-1")
    state = state_raw.decode() if isinstance(state_raw, bytes) else state_raw
    assert state == "rescheduled"

    # RunAdmittedEvent should be emitted to control stream
    events = await redis.xrange(config.control_stream)
    event_types = []
    for _, data in events:
        et = data.get(b"event_type", b"").decode()
        event_types.append(et)
    assert "RunAdmitted" in event_types


@pytest.mark.asyncio
async def test_dlq_sets_dead_lettered_state(config):
    redis = faredis.FakeRedis()
    recovery = RecoveryService(redis, config)

    await _seed_stale_run(redis, "run-dlq-sem-1", reschedule_count=3, age_seconds=120.0)

    result = await recovery._handle_stale("run-dlq-sem-1")

    assert result == "dlq"

    state_raw = await redis.get("hfa:run:state:run-dlq-sem-1")
    state = state_raw.decode() if isinstance(state_raw, bytes) else state_raw
    assert state == "dead_lettered"
