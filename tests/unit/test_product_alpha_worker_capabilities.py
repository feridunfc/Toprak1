from __future__ import annotations

from dataclasses import dataclass

from hfa_worker.executor import BaseExecutor
from hfa_worker.fake_executor import FakeExecutor
from hfa_worker.main import WorkerService
from hfa_worker.run_finalizing_runtime import (
    RunFinalizingTaskConsumer,
    RunFinalizingWorkerConsumer,
)


class RedisProbe:
    async def set(self, *args, **kwargs):
        return True

    async def get(self, *args, **kwargs):
        return None

    async def expire(self, *args, **kwargs):
        return 1

    async def hset(self, *args, **kwargs):
        return 1

    async def eval(self, *args, **kwargs):
        return 1


@dataclass
class ExecutionResultProbe:
    status: str = "done"
    payload: dict | None = None
    error: str = ""


class UnmarkedExecutor:
    async def execute(self, event):
        return ExecutionResultProbe(payload={})


class DeterministicExecutor:
    product_executor_capability = "executor:deterministic"

    async def execute(self, event):
        return ExecutionResultProbe(payload={})


class InvalidMarkedExecutor:
    product_executor_capability = "executor:spoofed"

    async def execute(self, event):
        return ExecutionResultProbe(payload={})


def alpha_config(executor=None, **overrides):
    values = {
        "product_mode": "SINGLE_TASK_ALPHA",
        "production": True,
        "worker_id": "worker-alpha-capabilities",
        "worker_group": "group-alpha-capabilities",
        "region": "eu-west-1",
        "shards": [0],
        "capacity": 1,
        "executor": executor or DeterministicExecutor(),
        "run_termination_binding_enabled": True,
        "capabilities": ["python", "base"],
    }
    values.update(overrides)
    return values


def internal_config(executor=None, **overrides):
    values = {
        "product_mode": "RUNTIME_INTERNAL",
        "production": True,
        "worker_id": "worker-internal-capabilities",
        "worker_group": "group-internal-capabilities",
        "region": "eu-west-1",
        "shards": [0],
        "capacity": 1,
        "executor": executor or DeterministicExecutor(),
        "run_termination_binding_enabled": False,
        "capabilities": ["base"],
    }
    values.update(overrides)
    return values


def test_base_executor_declares_safe_fallback_capability():
    assert (
        BaseExecutor.product_executor_capability
        == "executor:configured"
    )


def test_fake_executor_is_marked_deterministic():
    assert (
        FakeExecutor.product_executor_capability
        == "executor:deterministic"
    )


def test_alpha_capabilities_derive_from_actual_composition():
    service = WorkerService(
        RedisProbe(),
        alpha_config(),
    )

    assert service.runtime_capabilities == (
        "base",
        "executor:deterministic",
        "product:single-task-v1",
        "python",
        "run-finalization:v1",
    )
    assert (
        type(service._task_consumer)
        is RunFinalizingTaskConsumer
    )
    assert (
        type(service._consumer)
        is RunFinalizingWorkerConsumer
    )


def test_declared_reserved_capabilities_cannot_spoof_runtime():
    service = WorkerService(
        RedisProbe(),
        internal_config(
            capabilities=[
                "base",
                "product:single-task-v1",
                "run-finalization:v1",
                "executor:deterministic",
            ],
        ),
    )

    assert service.runtime_capabilities == (
        "base",
        "executor:deterministic",
    )
    assert "product:single-task-v1" not in (
        service.runtime_capabilities
    )
    assert "run-finalization:v1" not in (
        service.runtime_capabilities
    )


def test_unmarked_executor_uses_configured_fallback():
    service = WorkerService(
        RedisProbe(),
        alpha_config(executor=UnmarkedExecutor()),
    )

    assert "executor:configured" in service.runtime_capabilities
    assert "executor:deterministic" not in (
        service.runtime_capabilities
    )


def test_invalid_executor_marker_fails_closed_to_configured():
    service = WorkerService(
        RedisProbe(),
        alpha_config(executor=InvalidMarkedExecutor()),
    )

    assert "executor:configured" in service.runtime_capabilities
    assert "executor:spoofed" not in service.runtime_capabilities


def test_internal_worker_does_not_claim_alpha_product():
    service = WorkerService(
        RedisProbe(),
        internal_config(),
    )

    assert "product:single-task-v1" not in (
        service.runtime_capabilities
    )
    assert "run-finalization:v1" not in (
        service.runtime_capabilities
    )


def test_task_consumer_receives_derived_capabilities_in_place():
    service = WorkerService(
        RedisProbe(),
        alpha_config(),
    )

    assert (
        service._task_consumer._worker_capabilities
        is service._capabilities
    )
    assert (
        tuple(service._task_consumer._worker_capabilities)
        == service.runtime_capabilities
    )


def test_heartbeat_publishes_exact_derived_capabilities():
    service = WorkerService(
        RedisProbe(),
        alpha_config(),
    )

    assert service._heartbeat._capabilities is service._capabilities
    assert (
        tuple(service._heartbeat._capabilities)
        == service.runtime_capabilities
    )


def test_runtime_capabilities_are_sorted_and_unique():
    service = WorkerService(
        RedisProbe(),
        alpha_config(
            capabilities=["python", "base", "python", "base"],
        ),
    )

    assert service.runtime_capabilities == tuple(
        sorted(set(service.runtime_capabilities))
    )
