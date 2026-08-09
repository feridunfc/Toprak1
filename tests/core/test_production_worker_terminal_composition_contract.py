from __future__ import annotations

from dataclasses import dataclass

import pytest

from hfa_control.dag_lua import DagLua
from hfa_control.task_terminal_authority import TaskTerminalAuthorityBinding
from hfa_control.run_terminate_authority import RunTerminateAuthorityBinding
from hfa_control.run_termination import RunTerminationCoordinator
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
        "worker_id": "worker-c2-contract",
        "worker_group": "group-c2-contract",
        "region": "contract",
        "shards": [0],
        "capacity": 1,
        "executor": LegacyExecutorProbe(),
        "task_executor": CanonicalTaskExecutorProbe(),
    }
    config.update(overrides)
    return config


def _canonical_terminal_config(**overrides) -> dict:
    config = _config(
        canonical_task_admit_binding=True,
        canonical_task_dispatch_binding=True,
        canonical_task_claim_binding=True,
        canonical_task_terminal_binding=True,
    )
    config.update(overrides)
    return config


def test_terminal_binding_defaults_false() -> None:
    service = WorkerService(RedisProbe(), _config())
    assert service.canonical_task_terminal_binding_enabled is False


def test_terminal_binding_rejects_non_boolean() -> None:
    with pytest.raises(ValueError, match="canonical_task_terminal_binding must be a boolean"):
        WorkerService(
            RedisProbe(),
            _config(canonical_task_terminal_binding="true"),
        )


def test_terminal_binding_requires_production() -> None:
    with pytest.raises(ValueError, match="canonical_task_terminal_binding requires production=True"):
        WorkerService(
            RedisProbe(),
            {
                "production": False,
                "canonical_task_terminal_binding": True,
            },
        )


def test_terminal_binding_requires_canonical_task_claim() -> None:
    with pytest.raises(ValueError, match="canonical_task_terminal_binding requires canonical_task_claim_binding=True"):
        WorkerService(
            RedisProbe(),
            _config(
                canonical_task_admit_binding=True,
                canonical_task_dispatch_binding=True,
                canonical_task_claim_binding=False,
                canonical_task_terminal_binding=True,
            ),
        )


@pytest.mark.parametrize(
    "overrides",
    [
        {
            "canonical_task_admit_binding": False,
            "canonical_task_dispatch_binding": True,
            "canonical_task_claim_binding": True,
        },
        {
            "canonical_task_admit_binding": True,
            "canonical_task_dispatch_binding": False,
            "canonical_task_claim_binding": True,
        },
    ],
)
def test_terminal_binding_requires_accepted_canonical_dependency_chain(overrides: dict) -> None:
    with pytest.raises(ValueError):
        WorkerService(
            RedisProbe(),
            _config(canonical_task_terminal_binding=True, **overrides),
        )


def test_terminal_binding_combines_with_canonical_run_termination() -> None:
    service = WorkerService(
        RedisProbe(),
        _canonical_terminal_config(run_termination_binding_enabled=True),
    )

    gateway = service._task_terminal_completion_gateway
    coordinator = service._run_termination_coordinator
    run_authority = service._run_terminate_authority_binding

    assert isinstance(gateway, _CanonicalTaskTerminalCompletionGateway)
    assert isinstance(run_authority, RunTerminateAuthorityBinding)
    assert isinstance(coordinator, RunTerminationCoordinator)
    assert coordinator._task_completion_gateway is gateway
    assert coordinator._authority_binding is run_authority
    assert isinstance(service._task_consumer, RunFinalizingTaskConsumer)
    assert service._task_consumer._completion_manager is coordinator
    assert isinstance(service._consumer, RunFinalizingWorkerConsumer)


def test_enabled_terminal_profile_constructs_binding_and_gateway() -> None:
    redis = RedisProbe()
    service = WorkerService(redis, _canonical_terminal_config())

    binding = service._task_terminal_authority_binding
    gateway = service._task_terminal_completion_gateway
    consumer = service._task_consumer

    assert service.canonical_task_terminal_binding_enabled is True
    assert isinstance(binding, TaskTerminalAuthorityBinding)
    assert binding.redis is redis
    assert isinstance(gateway, _CanonicalTaskTerminalCompletionGateway)
    assert gateway._binding is binding
    assert isinstance(consumer, TaskConsumer)
    assert consumer._completion_manager is gateway
    assert consumer._completion_manager is not service._dag_lua
    assert service._run_termination_coordinator is None


def test_disabled_terminal_profile_preserves_dag_lua_completion_manager() -> None:
    service = WorkerService(RedisProbe(), _config())
    assert isinstance(service._dag_lua, DagLua)
    assert service._task_consumer is not None
    assert service._task_consumer._completion_manager is service._dag_lua
    assert service._task_terminal_authority_binding is None
    assert service._task_terminal_completion_gateway is None


class BindingProbe:
    def __init__(self) -> None:
        self.complete_calls: list[dict] = []
        self.fail_calls: list[dict] = []

    async def complete(self, **kwargs):
        self.complete_calls.append(kwargs)
        return object()

    async def fail(self, **kwargs):
        self.fail_calls.append(kwargs)
        return object()


@pytest.mark.asyncio
async def test_gateway_routes_done_to_canonical_complete() -> None:
    binding = BindingProbe()
    gateway = _CanonicalTaskTerminalCompletionGateway(binding)

    await gateway.task_complete(
        task_id="task-done",
        run_id="run-done",
        tenant_id="tenant-done",
        terminal_state="done",
        finished_at_ms=123,
        reason_code="completed",
        worker_instance_id="worker-done",
        output_data='{"ok":true}',
        expected_scheduler_epoch="epoch-done",
        expected_claim_epoch=7,
    )

    assert binding.fail_calls == []
    assert binding.complete_calls == [
        {
            "task_id": "task-done",
            "run_id": "run-done",
            "tenant_id": "tenant-done",
            "finished_at_ms": 123,
            "worker_instance_id": "worker-done",
            "scheduler_epoch": "epoch-done",
            "claim_epoch": 7,
            "output_data": '{"ok":true}',
            "reason_code": "completed",
        }
    ]


@pytest.mark.asyncio
async def test_gateway_routes_failed_to_canonical_fail() -> None:
    binding = BindingProbe()
    gateway = _CanonicalTaskTerminalCompletionGateway(binding)

    await gateway.task_complete(
        task_id="task-fail",
        run_id="run-fail",
        tenant_id="tenant-fail",
        terminal_state="failed",
        finished_at_ms=456,
        reason_code="boom",
        worker_instance_id="worker-fail",
        output_data='{"ignored":true}',
        expected_scheduler_epoch="epoch-fail",
        expected_claim_epoch=8,
    )

    assert binding.complete_calls == []
    assert binding.fail_calls == [
        {
            "task_id": "task-fail",
            "run_id": "run-fail",
            "tenant_id": "tenant-fail",
            "finished_at_ms": 456,
            "worker_instance_id": "worker-fail",
            "scheduler_epoch": "epoch-fail",
            "claim_epoch": 8,
            "reason_code": "boom",
        }
    ]


@pytest.mark.asyncio
async def test_gateway_rejects_unknown_terminal_state() -> None:
    gateway = _CanonicalTaskTerminalCompletionGateway(BindingProbe())
    with pytest.raises(RuntimeError, match="only done or failed"):
        await gateway.task_complete(
            task_id="task-bad",
            run_id="run-bad",
            tenant_id="tenant-bad",
            terminal_state="cancelled",
            finished_at_ms=1,
            reason_code="cancelled",
            worker_instance_id="worker-bad",
            output_data="{}",
            expected_scheduler_epoch="epoch-bad",
            expected_claim_epoch=1,
        )


@pytest.mark.asyncio
async def test_terminal_authority_initialises_before_consumer_groups(monkeypatch) -> None:
    service = WorkerService(RedisProbe(), _canonical_terminal_config())
    assert service._dag_lua is not None
    assert service._task_terminal_authority_binding is not None
    events: list[str] = []

    async def dag_initialise() -> None:
        events.append("dag.initialise")

    async def terminal_initialise() -> None:
        events.append("terminal.initialise")
        raise RuntimeError("terminal store unavailable")

    async def prepare_groups() -> None:
        events.append("consumer.groups")

    monkeypatch.setattr(service._dag_lua, "initialise", dag_initialise)
    monkeypatch.setattr(
        service._task_terminal_authority_binding,
        "initialise",
        terminal_initialise,
    )
    monkeypatch.setattr(service, "_prepare_consumer_groups", prepare_groups)

    with pytest.raises(RuntimeError, match="terminal store unavailable"):
        await service.start()

    assert events == ["dag.initialise", "terminal.initialise"]
    assert service.is_ready is False
