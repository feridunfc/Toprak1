from __future__ import annotations

from dataclasses import dataclass

import pytest

from hfa_control.dag_lua import DagLua
from hfa_control.run_terminate_authority import RunTerminateAuthorityBinding
from hfa_control.run_termination import RunTerminationCoordinator
from hfa_control.task_terminal_authority import TaskTerminalAuthorityBinding
from hfa_worker.consumer import WorkerConsumer
from hfa_worker.main import (
    WorkerService,
    _CanonicalTaskTerminalCompletionGateway,
)
from hfa_worker.run_finalizing_runtime import (
    RunFinalizingTaskConsumer,
    RunFinalizingWorkerConsumer,
)
from hfa_worker.task_consumer import TaskConsumer
from hfa_worker.task_executor import TaskExecutionResult


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
class LegacyExecutionResult:
    status: str = "done"
    payload: dict | None = None
    cost_cents: int = 0
    tokens_used: int = 0
    error: str = ""


class LegacyExecutorProbe:
    async def execute(self, event):
        return LegacyExecutionResult(payload={"run_id": getattr(event, "run_id", "")})


class CanonicalTaskExecutorProbe:
    async def execute(self, ctx):
        return TaskExecutionResult(ok=True, output={"task_id": ctx.task_id})


def _config(**overrides) -> dict:
    config = {
        "production": True,
        "worker_id": "worker-84-7d-contract",
        "worker_group": "group-84-7d-contract",
        "region": "contract",
        "shards": [0],
        "capacity": 1,
        "executor": LegacyExecutorProbe(),
        "task_executor": CanonicalTaskExecutorProbe(),
    }
    config.update(overrides)
    return config


def _canonical_task_chain(**overrides) -> dict:
    config = _config(
        canonical_task_admit_binding=True,
        canonical_task_dispatch_binding=True,
        canonical_task_claim_binding=True,
        canonical_task_terminal_binding=True,
    )
    config.update(overrides)
    return config


def test_profile_a_default_remains_dag_lua_without_run_finalization() -> None:
    service = WorkerService(RedisProbe(), _config())

    assert isinstance(service._dag_lua, DagLua)
    assert isinstance(service._task_consumer, TaskConsumer)
    assert service._task_consumer._completion_manager is service._dag_lua
    assert isinstance(service._consumer, WorkerConsumer)
    assert service._task_terminal_authority_binding is None
    assert service._run_terminate_authority_binding is None
    assert service._run_termination_coordinator is None


def test_profile_b_canonical_task_terminal_only_is_unchanged() -> None:
    service = WorkerService(RedisProbe(), _canonical_task_chain())

    assert isinstance(service._task_terminal_authority_binding, TaskTerminalAuthorityBinding)
    assert isinstance(
        service._task_terminal_completion_gateway,
        _CanonicalTaskTerminalCompletionGateway,
    )
    assert isinstance(service._task_consumer, TaskConsumer)
    assert (
        service._task_consumer._completion_manager
        is service._task_terminal_completion_gateway
    )
    assert isinstance(service._consumer, WorkerConsumer)
    assert service._run_terminate_authority_binding is None
    assert service._run_termination_coordinator is None


def test_profile_c_historical_run_finalization_remains_legacy_compatible() -> None:
    service = WorkerService(
        RedisProbe(),
        _config(run_termination_binding_enabled=True),
    )

    coordinator = service._run_termination_coordinator
    assert isinstance(coordinator, RunTerminationCoordinator)
    assert coordinator._task_completion_gateway is service._dag_lua
    assert coordinator._authority_binding is None
    assert service._run_terminate_authority_binding is None
    assert isinstance(service._task_consumer, RunFinalizingTaskConsumer)
    assert service._task_consumer._completion_manager is coordinator
    assert isinstance(service._consumer, RunFinalizingWorkerConsumer)


def test_profile_d_composes_canonical_task_and_run_authorities() -> None:
    service = WorkerService(
        RedisProbe(),
        _canonical_task_chain(run_termination_binding_enabled=True),
    )

    gateway = service._task_terminal_completion_gateway
    coordinator = service._run_termination_coordinator
    run_authority = service._run_terminate_authority_binding

    assert isinstance(gateway, _CanonicalTaskTerminalCompletionGateway)
    assert gateway._binding is service._task_terminal_authority_binding
    assert isinstance(run_authority, RunTerminateAuthorityBinding)
    assert isinstance(coordinator, RunTerminationCoordinator)
    assert coordinator._task_completion_gateway is gateway
    assert coordinator._task_completion_gateway is not service._dag_lua
    assert coordinator._authority_binding is run_authority
    assert isinstance(service._task_consumer, RunFinalizingTaskConsumer)
    assert service._task_consumer._completion_manager is coordinator
    assert isinstance(service._consumer, RunFinalizingWorkerConsumer)


def test_profile_d_advertises_existing_run_finalization_capability_only() -> None:
    service = WorkerService(
        RedisProbe(),
        _canonical_task_chain(run_termination_binding_enabled=True),
    )

    assert "run-finalization:v1" in service.runtime_capabilities
    assert not any(value.startswith("canonical-run-terminate:") for value in service.runtime_capabilities)


@pytest.mark.asyncio
async def test_profile_d_initialises_task_then_run_authority_before_consumer_groups(
    monkeypatch,
) -> None:
    service = WorkerService(
        RedisProbe(),
        _canonical_task_chain(run_termination_binding_enabled=True),
    )
    assert service._dag_lua is not None
    assert service._task_terminal_authority_binding is not None
    assert service._run_termination_coordinator is not None
    events: list[str] = []

    async def dag_initialise() -> None:
        events.append("dag.initialise")

    async def task_terminal_initialise() -> None:
        events.append("task-terminal.initialise")

    async def run_terminal_initialise() -> None:
        events.append("run-terminal.initialise")

    async def prepare_groups() -> None:
        events.append("consumer.groups")
        raise RuntimeError("stop after ordering proof")

    monkeypatch.setattr(service._dag_lua, "initialise", dag_initialise)
    monkeypatch.setattr(
        service._task_terminal_authority_binding,
        "initialise",
        task_terminal_initialise,
    )
    monkeypatch.setattr(
        service._run_termination_coordinator,
        "initialise",
        run_terminal_initialise,
    )
    monkeypatch.setattr(service, "_prepare_consumer_groups", prepare_groups)

    with pytest.raises(RuntimeError, match="stop after ordering proof"):
        await service.start()

    assert events == [
        "dag.initialise",
        "task-terminal.initialise",
        "run-terminal.initialise",
        "consumer.groups",
    ]
    assert service.is_ready is False


@pytest.mark.asyncio
async def test_profile_d_run_authority_initialisation_failure_blocks_consumer_start(
    monkeypatch,
) -> None:
    service = WorkerService(
        RedisProbe(),
        _canonical_task_chain(run_termination_binding_enabled=True),
    )
    assert service._dag_lua is not None
    assert service._task_terminal_authority_binding is not None
    assert service._run_termination_coordinator is not None
    events: list[str] = []

    async def dag_initialise() -> None:
        events.append("dag.initialise")

    async def task_terminal_initialise() -> None:
        events.append("task-terminal.initialise")

    async def run_terminal_initialise() -> None:
        events.append("run-terminal.initialise")
        raise RuntimeError("run authority unavailable")

    async def consumer_start() -> None:
        events.append("consumer.start")

    monkeypatch.setattr(service._dag_lua, "initialise", dag_initialise)
    monkeypatch.setattr(
        service._task_terminal_authority_binding,
        "initialise",
        task_terminal_initialise,
    )
    monkeypatch.setattr(
        service._run_termination_coordinator,
        "initialise",
        run_terminal_initialise,
    )
    monkeypatch.setattr(service._consumer, "start", consumer_start)

    with pytest.raises(RuntimeError, match="run authority unavailable"):
        await service.start()

    assert events == [
        "dag.initialise",
        "task-terminal.initialise",
        "run-terminal.initialise",
    ]
    assert service.is_ready is False
