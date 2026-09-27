"""Frozen alpha execution and operator boundaries; no live Redis required."""
from __future__ import annotations

import ast
import importlib
from dataclasses import dataclass
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import httpx
import pytest
from fastapi import FastAPI

from hfa_control.models import ControlPlaneConfig
from hfa_control.product_profile import require_alpha_deterministic_executor_mode
from hfa_control.run_submission import RunSubmissionCoordinator, SingleTaskRunSubmission
from hfa_control.scheduler import build_production_scheduler
from hfa_worker.executor_factory import build_executor
from hfa_worker.fake_executor import FakeExecutor
from hfa_worker.main import WorkerService
from hfa_worker.process_root import run_worker_process

router_module = importlib.import_module("hfa_control.api.router")
ALPHA = "SINGLE_TASK_ALPHA"
FLAGS = ("task_admit", "task_dispatch", "task_claim", "task_terminal", "resource_settlement")


class NoIoRedis:
    def __init__(self):
        self.calls = []

    def __getattr__(self, name):
        def forbidden(*args, **kwargs):
            self.calls.append(name)
            raise AssertionError(f"Unexpected Redis call: {name}")
        return forbidden


def worker_config(**changes):
    config = dict(
        product_mode=ALPHA, production=True, worker_id="g0-r-worker",
        worker_group="g0-r-group", executor_mode="fake",
        run_termination_binding_enabled=True,
        **{f"canonical_{flag}_binding": True for flag in FLAGS},
    )
    config.update(changes)
    return config


@pytest.mark.parametrize("mode", ("openai", "cognitive", "external", "configured", "unknown", ""))
def test_non_fake_alpha_modes_fail_before_external_factory_or_io(monkeypatch, mode):
    factory = Mock(side_effect=AssertionError("factory must not be called"))
    monkeypatch.setattr("hfa_worker.main.build_executor", factory)
    redis = NoIoRedis()
    with pytest.raises(ValueError, match="executor_mode=fake"):
        WorkerService(redis, worker_config(executor_mode=mode))
    factory.assert_not_called()
    assert redis.calls == []
    with pytest.raises(ValueError, match="executor_mode=fake"):
        build_executor(dict(product_mode=ALPHA, executor_mode=mode))
    with pytest.raises(ValueError, match="executor_mode=fake"):
        require_alpha_deterministic_executor_mode(product_mode=ALPHA, executor_mode=mode)


@pytest.mark.parametrize("mode", ("openai", "cognitive", "external", "configured", "unknown"))
@pytest.mark.asyncio
async def test_process_root_rejects_mode_before_redis_factory(monkeypatch, mode):
    monkeypatch.setenv("HFA_PRODUCT_MODE", ALPHA)
    monkeypatch.setenv("WORKER_GROUP", "g0-r")
    monkeypatch.setenv("WORKER_EXECUTOR_MODE", mode)
    factory = Mock(side_effect=AssertionError("Redis factory must not be called"))
    with pytest.raises(ValueError, match="executor_mode=fake"):
        await run_worker_process(redis_factory=factory)
    factory.assert_not_called()


@pytest.mark.parametrize("capability", ("executor:deterministic", "executor:external", "executor:cognitive", "executor:configured", "executor:spoofed"))
def test_injected_executor_cannot_self_attest_with_marker(capability):
    injected = SimpleNamespace(product_executor_capability=capability, execute=AsyncMock())
    redis = NoIoRedis()
    with pytest.raises(ValueError, match="built-in FakeExecutor"):
        WorkerService(redis, worker_config(executor=injected))
    injected.execute.assert_not_called()
    assert redis.calls == []


@pytest.mark.parametrize("override", ("task_executor", "subclass", "instance_execute", "wrong_capability"))
def test_fake_executor_cannot_hide_an_effectful_override(override):
    class FakeSubclass(FakeExecutor):
        async def execute(self, event):
            pytest.fail("subclass must not run")
    executor = FakeExecutor()
    config = worker_config(executor=executor)
    if override == "task_executor":
        config["task_executor"] = SimpleNamespace(execute=AsyncMock())
    elif override == "subclass":
        config["executor"] = FakeSubclass()
    elif override == "instance_execute":
        executor.execute = AsyncMock()
    else:
        executor.product_executor_capability = "executor:external"
    with pytest.raises(ValueError, match="built-in FakeExecutor"):
        WorkerService(NoIoRedis(), config)


def test_fake_alpha_owns_adapter_and_derived_marker_before_start(monkeypatch):
    execute = AsyncMock(side_effect=AssertionError("must not execute during boot"))
    monkeypatch.setattr(FakeExecutor, "execute", execute)
    redis = NoIoRedis()
    worker = WorkerService(redis, worker_config())
    assert type(worker._task_consumer._executor._executor) is FakeExecutor
    assert "executor:deterministic" in worker.runtime_capabilities
    assert "product:single-task-canonical-v1" in worker.runtime_capabilities
    assert worker._heartbeat._capabilities is worker._capabilities
    execute.assert_not_called()
    assert redis.calls == []


def test_internal_mode_keeps_explicit_executor_and_adapter():
    executor = SimpleNamespace(execute=AsyncMock(), product_executor_capability="executor:external")
    task_executor = SimpleNamespace(execute=AsyncMock())
    worker = WorkerService(NoIoRedis(), worker_config(
        product_mode="RUNTIME_INTERNAL", executor=executor, task_executor=task_executor,
    ))
    assert worker._task_consumer._executor is task_executor
    assert "executor:external" in worker.runtime_capabilities
    assert "product:single-task-canonical-v1" not in worker.runtime_capabilities
    executor.execute.assert_not_called()
    task_executor.execute.assert_not_called()
    # This verifies local compatibility, not isolation from an alpha Redis.


OPERATORS = (
    ("POST", "/runs/run-1/reschedule", None, "force_reschedule"),
    ("POST", "/dlq/run-1/replay", None, "dlq_replay"),
    ("DELETE", "/dlq/run-1", None, "dlq_delete"),
    ("POST", "/tasks/task-1/terminal-duplicate-cleanup", {}, "terminal_duplicate_cleanup"),
    ("POST", "/tasks/task-1/terminal-duplicate-cleanup",
     {"dry_run": False, "execute": True, "reason": "test", "pending_message_id": "1-0"}, "terminal_duplicate_cleanup"),
)


def operator_app(monkeypatch, mode=ALPHA):
    monkeypatch.setattr(router_module, "_require_operator", lambda token: None)
    redis = NoIoRedis()
    recovery = SimpleNamespace(_handle_stale=AsyncMock(), replay_dlq_run=AsyncMock())
    audit = SimpleNamespace(dlq_replay=AsyncMock())
    cp = SimpleNamespace(
        product_profile=SimpleNamespace(product_mode=mode), recovery=recovery,
        _audit=audit, _leader=SimpleNamespace(assert_leader=AsyncMock()),
    )
    app = FastAPI()
    app.state.cp, app.state.redis = cp, redis
    app.include_router(router_module.router)
    return app, cp, redis


@pytest.mark.parametrize("method,path,body,operation", OPERATORS)
@pytest.mark.asyncio
async def test_alpha_operator_rejected_before_owner_redis_or_cleanup_audit(monkeypatch, method, path, body, operation):
    app, cp, redis = operator_app(monkeypatch)
    cleanup = AsyncMock(side_effect=AssertionError("cleanup owner must not be called"))
    monkeypatch.setattr(router_module, "execute_terminal_duplicate_cleanup_command", cleanup)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url="http://test") as client:
        response = await client.request(method, "/control/v1" + path, json=body, headers={"X-Tenant-ID": "tenant1", "X-CP-Auth": "authorized"})
    assert response.status_code == 403, response.text
    assert response.json()["detail"] == {"code": "product_profile_mutation_forbidden", "operation": operation}
    cp.recovery._handle_stale.assert_not_called()
    cp.recovery.replay_dlq_run.assert_not_called()
    cp._leader.assert_leader.assert_not_called()
    cp._audit.dlq_replay.assert_not_called()
    cleanup.assert_not_called()
    assert redis.calls == []


@pytest.mark.parametrize("mode", (None, "TYPO"))
@pytest.mark.asyncio
async def test_missing_or_unknown_profile_cannot_enable_operator_mutation(monkeypatch, mode):
    app, cp, redis = operator_app(monkeypatch, mode)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url="http://test") as client:
        response = await client.post("/control/v1/dlq/run-1/replay", headers={"X-Tenant-ID": "tenant1"})
    assert response.status_code == 503
    cp.recovery.replay_dlq_run.assert_not_called()
    assert redis.calls == []


@pytest.mark.asyncio
async def test_read_only_terminal_evidence_and_worker_drain_preserved(monkeypatch):
    @dataclass
    class Evidence:
        status: str = "read-only"
    app, cp, redis = operator_app(monkeypatch)
    evidence = AsyncMock(return_value=Evidence())
    monkeypatch.setattr(router_module, "read_terminal_duplicate_operator_evidence", evidence)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url="http://test") as client:
        response = await client.get("/control/v1/tasks/task-1/terminal-duplicate-operator-evidence")
        assert response.status_code == 200
        assert response.json() == {"status": "read-only"}
        assert redis.calls == []
        cp.registry = SimpleNamespace(get_worker=AsyncMock(return_value=SimpleNamespace(worker_group="g", region="test")))
        cp._config = SimpleNamespace(heartbeat_stream="heartbeats")
        cp._audit = SimpleNamespace(drain_started=AsyncMock())
        redis.xadd = AsyncMock(return_value="1-0")
        response = await client.post("/control/v1/workers/w1/drain", headers={"X-Tenant-ID": "tenant1"})
        assert response.status_code == 200, response.text
        assert response.json()["status"] == "draining"
    evidence.assert_awaited_once()
    redis.xadd.assert_awaited_once()
    cp._audit.drain_started.assert_awaited_once()
    assert redis.calls == []


@pytest.mark.asyncio
async def test_internal_replay_still_delegates_to_existing_owner(monkeypatch):
    app, cp, redis = operator_app(monkeypatch, "RUNTIME_INTERNAL")
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url="http://test") as client:
        response = await client.post("/control/v1/dlq/run-1/replay", headers={"X-Tenant-ID": "tenant1"})
    assert response.status_code == 200
    cp.recovery.replay_dlq_run.assert_awaited_once_with("run-1", "tenant1")
    assert redis.calls == []


@pytest.mark.asyncio
async def test_alpha_claim_never_uses_legacy_even_with_opt_in(monkeypatch):
    monkeypatch.setenv("HFA_ALLOW_LEGACY_DIRECT_TASK_CLAIM", "true")
    worker = WorkerService(NoIoRedis(), worker_config())
    manager = worker._task_claim_manager
    legacy = AsyncMock(side_effect=AssertionError("legacy claim must not be used"))
    manager._claim_start_legacy = legacy
    manager._dag_lua.claim_task = legacy
    canonical = AsyncMock(return_value="canonical-result")
    manager._claim_start_canonical = canonical
    assert await manager.claim_start(task_id="t", run_id="r", tenant_id="tenant1", worker_instance_id="w", claimed_at_ms=1, scheduler_epoch="1") == "canonical-result"
    canonical.assert_awaited_once()
    legacy.assert_not_called()


@pytest.mark.asyncio
async def test_canonical_claim_rejects_explicit_direct_claim_before_authority():
    worker = WorkerService(NoIoRedis(), worker_config())
    manager = worker._task_claim_manager
    binding = Mock(side_effect=AssertionError("authority must not be called"))
    manager._canonical_binding = binding
    result = await manager.claim_start(task_id="t", run_id="r", tenant_id="tenant1", worker_instance_id="w", claimed_at_ms=1, scheduler_epoch="1", allow_legacy_direct_claim=True)
    assert not result.ok
    binding.assert_not_called()


@pytest.mark.parametrize("result", (False, True, "error"))
@pytest.mark.asyncio
async def test_production_scheduler_never_falls_back_with_legacy_flag(monkeypatch, result):
    for name in ("HFA_CANONICAL_TASK_ADMIT_BINDING", "HFA_CANONICAL_TASK_DISPATCH_BINDING", "HFA_ALLOW_LEGACY_INJECTED_DISPATCH"):
        monkeypatch.setenv(name, "true")
    config = ControlPlaneConfig(instance_id="g0-r", product_mode=ALPHA)
    scheduler = build_production_scheduler(redis=NoIoRedis(), config=config, registry=object(), shards=object())
    composition = scheduler.composition
    composition.dispatch_controller.dispatch_once = (
        AsyncMock(side_effect=RuntimeError("canonical dispatch failed"))
        if result == "error" else AsyncMock(return_value=result)
    )
    loop = composition.scheduler_loop
    legacy = AsyncMock(side_effect=AssertionError("legacy dispatch must not be called"))
    loop._legacy_injected_dispatch_once = legacy
    if result == "error":
        with pytest.raises(RuntimeError, match="canonical dispatch failed"):
            await loop._dispatch_once(SimpleNamespace())
    else:
        assert await loop._dispatch_once(SimpleNamespace()) is result
    legacy.assert_not_called()


@pytest.mark.asyncio
async def test_single_task_submission_cannot_import_graph_from_payload():
    seeds = []
    async def admit(request):
        return request.run_id
    async def task_admit(seed):
        seeds.append(seed)
        return SimpleNamespace(admitted=True, ready=True, task_id=seed.task_id, status="seeded_root")
    coordinator = RunSubmissionCoordinator(
        admission_controller=SimpleNamespace(admit=admit),
        dag_lua=SimpleNamespace(initialise=AsyncMock(), task_admit=task_admit),
    )
    payload = dict(task_id="injected", run_id="injected", dependency_count=2, parent_task_ids=["p"], child_task_ids=["c"], tasks=[{}, {}])
    result = await coordinator.submit(SingleTaskRunSubmission(tenant_id="tenant1", payload=payload))
    assert result.accepted
    assert len(seeds) == 1
    seed = seeds[0]
    assert seed.dependency_count == 0 and not seed.parent_task_ids and not seed.child_task_ids
    assert seed.task_id != "injected" and seed.run_id != "injected"


def test_no_other_release_module_introduces_tasks_or_calls_direct_claim():
    root = Path(__file__).resolve().parents[2]
    calls = {"task_admit": [], "claim_legacy_direct_for_compatibility": []}
    for package in ("hfa-control", "hfa-worker"):
        for path in (root / package / "src").rglob("*.py"):
            for node in ast.walk(ast.parse(path.read_text(encoding="utf-8-sig"))):
                if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) and node.func.attr in calls:
                    calls[node.func.attr].append(str(path.relative_to(root)))
    assert calls["task_admit"] == ["hfa-control/src/hfa_control/run_submission.py"]
    assert calls["claim_legacy_direct_for_compatibility"] == []


@pytest.mark.asyncio
async def test_alpha_runrequested_never_enters_legacy_completion(monkeypatch):
    consumer_module = importlib.import_module("hfa_worker.consumer")
    worker = WorkerService(NoIoRedis(), worker_config())
    consumer = worker._consumer
    event = SimpleNamespace(run_id="r", task_id="t")
    monkeypatch.setattr(consumer_module, "deserialize_run_requested", lambda raw: event)
    canonical = AsyncMock()
    consumer._process_message_via_task_consumer = canonical
    consumer._guard = SimpleNamespace(should_execute=AsyncMock(side_effect=AssertionError("legacy guard called")))
    await consumer._process_message("1-0", {"event_type": "RunRequested"}, "stream", 0)
    canonical.assert_awaited_once_with(event, "1-0", "stream", 0)
    consumer._guard.should_execute.assert_not_called()
