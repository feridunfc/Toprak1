"""Operator-owned Redis 7.4.10 gates. Worker only compiles/collects this module.

Run exclusively against an empty, disposable 127.0.0.1:6389/0 instance.
The integration fixtures FLUSHDB before/after each test. Explicit opt-in is
required before any fixture runs; this is not a deployment migration script.
"""
from __future__ import annotations

import asyncio
from collections import Counter
from dataclasses import asdict
import json
import os
import sys
import time

import pytest
import pytest_asyncio
import redis.asyncio as redis_async

from hfa.config.keys import RedisKey
from hfa.dag.schema import DagRedisKey
from hfa_control.run_terminal_event_evidence import backfill_terminal_event_evidence
from hfa_control.scheduler_snapshot import SchedulerSnapshotBuilder
from hfa_control.service import ControlPlaneService
from hfa_worker.fake_executor import FakeExecutor
from hfa_worker.heartbeat import WorkerHeartbeatPublisher
from hfa_worker.main import WorkerService
from scripts.runtime_alpha_acceptance_83_7 import (
    _build_client, _close_control, _complete_terminal_view, _control_config,
    _pending_count, _product_ready, _run_view, _stable_terminal_projection,
    _wait_until, _worker_config,
)

URL = "redis://127.0.0.1:6389/0"
pytestmark = [
    pytest.mark.asyncio,
    pytest.mark.integration,
    pytest.mark.skipif(
        os.environ.get("G0_R_OPERATOR_ALLOW_DISPOSABLE_REDIS_RESET") != "1",
        reason="operator-only gate; requires exclusive disposable Redis ownership",
    ),
]


@pytest_asyncio.fixture
async def redis_client():
    assert os.environ.get("REDIS_URL") == URL
    client = redis_async.from_url(URL, decode_responses=False)
    try:
        assert (await client.info("server"))["redis_version"] == "7.4.10"
        assert await client.dbsize() == 0, "Refuse to reset nonempty Redis"
        yield client
    finally:
        await client.aclose()


def _configure(monkeypatch):
    for name in ("RUN_CREATE", "TASK_ADMIT", "TASK_DISPATCH", "TASK_CLAIM",
                 "TASK_TERMINAL", "TASK_REQUEUE"):
        monkeypatch.setenv(f"HFA_CANONICAL_{name}_BINDING", "true")
    monkeypatch.setenv("HFA_PRODUCT_MODE", "SINGLE_TASK_ALPHA")
    monkeypatch.setenv("HFA_TENANT_IDENTITY_BOUNDARY", "TRUSTED_GATEWAY_HEADER")
    monkeypatch.setenv("HFA_STRICT_CAS_MODE", "true")


async def _migration(redis):
    # A real empty results stream, followed by the supported migration owner.
    await redis.xgroup_create(RedisKey.stream_results(), "g0-r-migration", "$", mkstream=True)
    result = await backfill_terminal_event_evidence(redis, completed_at_ms=int(time.time()*1000))
    assert result.status == "ready", asdict(result)
    return asdict(result)


def _new_worker(redis, executor, worker_id="worker-g0-r-canonical"):
    config = _worker_config(redis, worker_id=worker_id, worker_group="g0-r", executor=executor)
    config.update({f"canonical_{name}_binding": True for name in
                   ("task_admit", "task_dispatch", "task_claim", "task_terminal", "resource_settlement")})
    return WorkerService(redis, config)


async def _submit(client, key):
    response = await client.post("/control/v1/runs", headers={
        "X-Tenant-ID": "tenantg0r", "Idempotency-Key": key,
    }, json={"run_shape": "SINGLE_TASK", "payload": {"prompt": key},
             "agent_type": "sprint83-7-acceptance", "estimated_cost_cents": 0})
    assert response.status_code == 202, response.text
    assert response.json()["status"] == "ACCEPTED", response.text
    return response.json()


async def _terminal(client, run_id, status):
    return await _wait_until(lambda: _complete_terminal_view(
        client, run_id=run_id, tenant_id="tenantg0r", expected_status=status,
        expected_task_state="done" if status == "COMPLETED" else "failed",
    ), description=f"{run_id}: {status}")


async def test_migration_ready_alpha_lifecycle_and_restarts(redis_client, monkeypatch):
    _configure(monkeypatch)
    migration = await _migration(redis_client)
    config = _control_config(acceptance_id="g0r", instance_suffix="lifecycle")
    control = ControlPlaneService(redis_client, config)
    client = _build_client(control, redis_client, suffix="g0r")
    executor = FakeExecutor()
    replacement = FakeExecutor()
    worker = _new_worker(redis_client, executor)
    calls = Counter()
    original_profile = sys.getprofile()
    code = FakeExecutor.execute.__code__

    def observe(frame, event, arg):
        # FakeExecutor.execute has no await/yield: one call event per invocation.
        # Observe the real built-in implementation without overriding it.
        if event == "call" and frame.f_code is code:
            calls[id(frame.f_locals["self"])] += 1

    sys.setprofile(observe)
    try:
        await control.start()
        await worker.start()
        readiness = await _wait_until(lambda: _product_ready(client), description="alpha ready")
        assert readiness["compatible_worker_count"] == 1
        stable = {}
        for succeed, status in ((True, "COMPLETED"), (False, "FAILED")):
            executor.should_succeed = succeed
            submitted = await _submit(client, f"g0-r-{status}")
            view = await _terminal(client, submitted["run_id"], status)
            stable[submitted["run_id"]] = _stable_terminal_projection(view)
        assert calls[id(executor)] == 2
        await _close_control(control, client)
        control = ControlPlaneService(redis_client, _control_config(acceptance_id="g0r", instance_suffix="restart"))
        client = _build_client(control, redis_client, suffix="restart")
        await control.start()
        await _wait_until(lambda: _product_ready(client), description="control restart ready")
        for run_id, expected in stable.items():
            assert _stable_terminal_projection(await _run_view(client, run_id=run_id, tenant_id="tenantg0r")) == expected
        await worker.close(drain_timeout=0)
        worker = _new_worker(redis_client, replacement)
        await worker.start()
        await _wait_until(lambda: _product_ready(client), description="worker restart ready")
        await asyncio.sleep(0.5)
        for run_id, expected in stable.items():
            assert _stable_terminal_projection(await _run_view(client, run_id=run_id, tenant_id="tenantg0r")) == expected
        assert calls[id(replacement)] == 0
        pending = await _pending_count(redis_client, stream=RedisKey.stream_shard(0))
        assert pending == 0
        print(json.dumps(dict(migration=migration, success="COMPLETED", failure="FAILED",
                             control_restart_stable=True, worker_restart_stable=True,
                             executor_calls=2, replacement_executor_calls=0, pending_count=pending,
                             production_ready=False, production_cutover_authorized=False)))
    finally:
        sys.setprofile(original_profile)
        await worker.close(drain_timeout=0)
        await _close_control(control, client)


async def test_mixed_legacy_worker_excluded_from_actual_dispatch(redis_client, monkeypatch):
    _configure(monkeypatch)
    migration = await _migration(redis_client)
    config = _control_config(acceptance_id="g0r", instance_suffix="mixed")
    control = ControlPlaneService(redis_client, config)
    client = _build_client(control, redis_client, suffix="mixed")
    canonical_id, legacy_id = "worker-g0-r-canonical", "worker-g0-r-legacy"
    worker = _new_worker(redis_client, FakeExecutor(), canonical_id)
    legacy = WorkerHeartbeatPublisher(redis_client, legacy_id, "g0-r", config.region, [0], 100,
                                      lambda: 0, lambda: False, capabilities=[
                                          "product:single-task-v1", "run-finalization:v1", "executor:deterministic"])
    try:
        await control.start()
        await worker.start()
        await legacy.start()

        async def both_registered():
            return len(await control._registry.list_all_workers()) == 2

        await _wait_until(both_registered, description="mixed registry")
        readiness = await _wait_until(lambda: _product_ready(client), description="mixed readiness")
        assert readiness["compatible_worker_count"] == 1
        snapshot = await SchedulerSnapshotBuilder(redis_client, control._registry, None, None, config).build_capacity_snapshot()
        legacy_view = next(w for w in snapshot.workers if w.worker_id == legacy_id)
        assert legacy_view.capacity == 100 and not legacy_view.schedulable
        assert legacy_view.blocked_reason == "product_profile_incompatible"
        assert snapshot.total_available_slots == 1
        assert not await redis_client.exists(DagRedisKey.worker_reservation(legacy_id))
        submitted = await _submit(client, "g0-r-mixed")
        await _terminal(client, submitted["run_id"], "COMPLETED")
        dispatches = []
        for _, fields in await redis_client.xrange(RedisKey.stream_shard(0)):
            decoded = {k.decode(): v.decode() for k, v in fields.items()}
            if decoded.get("task_id") == submitted["task_id"] and decoded.get("event_type") == "TaskRequested":
                dispatches.append(decoded)
        assert dispatches and all(d.get("worker_id") == canonical_id for d in dispatches), dispatches
        assert not await redis_client.exists(DagRedisKey.worker_reservation(legacy_id))
        print(json.dumps(dict(migration=migration, compatible_worker_count=1, total_available_slots=1,
                             legacy_capacity=100, legacy_blocked_reason=legacy_view.blocked_reason,
                             dispatch_worker_id=canonical_id, terminal="COMPLETED",
                             legacy_reservation_present=False, production_ready=False,
                             production_cutover_authorized=False)))
    finally:
        await legacy.close()
        await worker.close(drain_timeout=0)
        await _close_control(control, client)
