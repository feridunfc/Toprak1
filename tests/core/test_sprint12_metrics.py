"""
tests/core/test_sprint12_metrics.py
IRONCLAD Sprint 12 — Metrics foundation tests

Verifies:
  - IRONCLADMetrics counters/histograms/gauges increment correctly
  - reset_all() restores zero state
  - metric calls never raise (fire-and-forget contract)
  - consumer.py emits correct metric increments on execution outcomes
"""

from __future__ import annotations

import time

import fakeredis.aioredis as faredis
import pytest

from hfa.events.codec import serialize_event
from hfa.events.schema import RunRequestedEvent
from hfa.obs.runtime_metrics import IRONCLADMetrics as M
from hfa.runtime.state_store import StateStore
from hfa_worker.consumer import WorkerConsumer
from hfa_worker.executor import FakeExecutor


@pytest.fixture(autouse=True)
def reset_metrics():
    M.reset_all()
    yield
    M.reset_all()


# ---------------------------------------------------------------------------
# Unit — primitive metric types
# ---------------------------------------------------------------------------


def test_counter_increments():
    M.runs_started_total.inc()
    M.runs_started_total.inc()
    assert M.runs_started_total.value == 2


def test_counter_inc_by_amount():
    M.pending_reclaimed_total.inc(5)
    assert M.pending_reclaimed_total.value == 5


def test_histogram_records():
    M.run_execution_duration_ms.record(100.0)
    M.run_execution_duration_ms.record(200.0)
    assert M.run_execution_duration_ms.count == 2
    assert M.run_execution_duration_ms.total == 300.0


def test_gauge_inc_dec():
    M.worker_inflight.inc()
    M.worker_inflight.inc()
    assert M.worker_inflight.value == 2
    M.worker_inflight.dec()
    assert M.worker_inflight.value == 1


def test_gauge_set():
    M.worker_inflight.set(7)
    assert M.worker_inflight.value == 7


def test_reset_all_zeros_everything():
    M.runs_started_total.inc(3)
    M.run_execution_duration_ms.record(50.0)
    M.worker_inflight.inc(2)
    M.reset_all()
    assert M.runs_started_total.value == 0
    assert M.run_execution_duration_ms.count == 0
    assert M.worker_inflight.value == 0


def test_counter_never_raises_on_negative():
    # Contract: fire-and-forget — no exceptions
    M.runs_completed_total.inc(-1)  # edge case: negative amount still accepted


def test_all_sprint12_counters_exist():
    """Verify every named Sprint 12 metric is present on the class."""
    names = [
        "runs_started_total",
        "runs_completed_total",
        "runs_failed_total",
        "runs_infra_failed_total",
        "run_execution_duration_ms",
        "pending_reclaimed_total",
        "claim_renew_total",
        "claim_renew_failure_total",
        "worker_drain_started_total",
        "worker_drain_completed_total",
        "worker_drain_timeout_total",
        "worker_inflight",
        "scheduling_attempts_total",
        "scheduling_failures_total",
        "workers_excluded_draining_total",
        "recovery_stale_detected_total",
        "recovery_rescheduled_total",
        "recovery_dlq_total",
    ]
    for name in names:
        assert hasattr(M, name), f"Missing metric: {name}"


# ---------------------------------------------------------------------------
# Integration — consumer emits metrics on success
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_consumer_increments_runs_started_and_completed():
    redis = faredis.FakeRedis()
    run_id = "run-metrics-success-1"
    tenant_id = "acme"
    shard = 0

    state_store = StateStore(redis)
    await state_store.create_run_meta(
        run_id,
        {
            "run_id": run_id,
            "tenant_id": tenant_id,
            "agent_type": "test",
            "reschedule_count": "0",
            "admitted_at": str(time.time()),
            "state": "scheduled",
        },
    )
    await redis.set(f"hfa:run:state:{run_id}", "scheduled", ex=86400)

    consumer = WorkerConsumer(
        redis=redis,
        worker_id="w1",
        worker_group="grp",
        shards=[shard],
        executor=FakeExecutor(should_succeed=True, cost_cents=10, tokens_used=50),
    )

    event = RunRequestedEvent(
        run_id=run_id,
        tenant_id=tenant_id,
        agent_type="test",
        priority=5,
        payload={},
        idempotency_key=run_id,
    )
    serialized = serialize_event(event)
    redis_data = {k.encode(): v.encode() for k, v in serialized.items()}
    stream = f"hfa:stream:runs:{shard}"

    await consumer._process_message("1-0", redis_data, stream, shard)

    assert M.runs_started_total.value == 1
    assert M.runs_completed_total.value == 1
    assert M.runs_failed_total.value == 0
    assert M.run_execution_duration_ms.count == 1
    assert M.run_execution_duration_ms.total >= 0


@pytest.mark.asyncio
async def test_consumer_increments_runs_infra_failed():
    from hfa_worker.executor import FakeExecutor
    from hfa_worker.models import InfrastructureError

    redis = faredis.FakeRedis()
    run_id = "run-infra-fail-1"
    tenant_id = "acme"
    shard = 0

    state_store = StateStore(redis)
    await state_store.create_run_meta(
        run_id,
        {
            "run_id": run_id,
            "tenant_id": tenant_id,
            "agent_type": "test",
            "reschedule_count": "0",
            "admitted_at": str(time.time()),
            "state": "scheduled",
        },
    )
    await redis.set(f"hfa:run:state:{run_id}", "scheduled", ex=86400)

    class InfraExecutor(FakeExecutor):
        async def execute(self, event):
            raise InfrastructureError("disk full")

    consumer = WorkerConsumer(
        redis=redis,
        worker_id="w2",
        worker_group="grp",
        shards=[shard],
        executor=InfraExecutor(),
    )

    event = RunRequestedEvent(
        run_id=run_id,
        tenant_id=tenant_id,
        agent_type="test",
        priority=5,
        payload={},
        idempotency_key=run_id,
    )
    serialized = serialize_event(event)
    redis_data = {k.encode(): v.encode() for k, v in serialized.items()}
    stream = f"hfa:stream:runs:{shard}"

    await consumer._process_message("1-0", redis_data, stream, shard)

    assert M.runs_infra_failed_total.value == 1
    assert M.runs_completed_total.value == 0
