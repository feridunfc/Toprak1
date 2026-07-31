from __future__ import annotations

from dataclasses import dataclass

import pytest

from hfa_control.run_termination import RunTerminationCoordinator
from hfa_worker.consumer import WorkerConsumer
from hfa_worker.main import WorkerService
from hfa_worker.process_root import config_from_env
from hfa_worker.run_finalizing_runtime import (
    RunFinalizingTaskConsumer,
    RunFinalizingWorkerConsumer,
)
from hfa_worker.task_consumer import TaskConsumer
from hfa_worker.task_executor import TaskExecutionResult


class RedisProbe:
    """Identity-only Redis probe for composition contracts."""

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
        return LegacyExecutionResult(
            payload={"run_id": getattr(event, "run_id", "")}
        )


class CanonicalTaskExecutorProbe:
    async def execute(self, ctx):
        return TaskExecutionResult(
            ok=True,
            output={"task_id": ctx.task_id},
        )


def _production_config(
    *,
    enabled: bool = False,
) -> dict:
    return {
        "production": True,
        "worker_id": "worker-sprint83-4",
        "worker_group": "group-sprint83-4",
        "region": "eu-west-1",
        "shards": [4],
        "capacity": 4,
        "capabilities": ["base", "python"],
        "executor": LegacyExecutorProbe(),
        "task_executor": CanonicalTaskExecutorProbe(),
        "run_termination_binding_enabled": enabled,
    }


def _build_service(
    *,
    enabled: bool = False,
) -> tuple[WorkerService, RedisProbe]:
    redis = RedisProbe()
    service = WorkerService(
        redis,
        _production_config(enabled=enabled),
    )
    return service, redis


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("1", True),
        ("true", True),
        ("TRUE", True),
        ("yes", True),
        ("on", True),
        ("0", False),
        ("false", False),
        ("FALSE", False),
        ("no", False),
        ("off", False),
    ],
)
def test_process_root_parses_explicit_run_termination_values(
    raw: str,
    expected: bool,
) -> None:
    config = config_from_env(
        {
            "WORKER_EXECUTOR_MODE": "fake",
            "WORKER_ID": "worker-sprint83-4",
            "WORKER_RUN_TERMINATION_BINDING": raw,
        }
    )

    assert config["run_termination_binding_enabled"] is expected


def test_process_root_defaults_run_termination_binding_to_disabled() -> None:
    config = config_from_env(
        {
            "WORKER_EXECUTOR_MODE": "fake",
            "WORKER_ID": "worker-sprint83-4",
        }
    )

    assert config["run_termination_binding_enabled"] is False


@pytest.mark.parametrize(
    "raw",
    ["", "2", "enabled", "disabled", "invalid"],
)
def test_process_root_rejects_unknown_run_termination_values(
    raw: str,
) -> None:
    with pytest.raises(
        RuntimeError,
        match="WORKER_RUN_TERMINATION_BINDING",
    ):
        config_from_env(
            {
                "WORKER_EXECUTOR_MODE": "fake",
                "WORKER_ID": "worker-sprint83-4",
                "WORKER_RUN_TERMINATION_BINDING": raw,
            }
        )


def test_worker_service_rejects_non_boolean_binding_config() -> None:
    config = _production_config()
    config["run_termination_binding_enabled"] = "true"

    with pytest.raises(
        ValueError,
        match="must be a boolean",
    ):
        WorkerService(RedisProbe(), config)


def test_worker_service_rejects_enabled_binding_outside_production() -> None:
    config = _production_config(enabled=True)
    config["production"] = False

    with pytest.raises(
        ValueError,
        match="requires production=True",
    ):
        WorkerService(RedisProbe(), config)


def test_disabled_binding_preserves_exact_historical_composition() -> None:
    service, _redis = _build_service(enabled=False)

    assert service.run_termination_binding_enabled is False
    assert service._run_termination_coordinator is None
    assert type(service._task_consumer) is TaskConsumer
    assert type(service._consumer) is WorkerConsumer
    assert service._task_consumer._completion_manager is service._dag_lua
    assert service._consumer._task_consumer is service._task_consumer


def test_enabled_binding_selects_finalizing_composition() -> None:
    service, redis = _build_service(enabled=True)

    coordinator = service._run_termination_coordinator

    assert service.run_termination_binding_enabled is True
    assert type(service._task_consumer) is RunFinalizingTaskConsumer
    assert type(service._consumer) is RunFinalizingWorkerConsumer
    assert isinstance(coordinator, RunTerminationCoordinator)
    assert coordinator._redis is redis
    assert coordinator._task_completion_gateway is service._dag_lua
    assert coordinator.run_termination_binding_enabled is True
    assert service._task_consumer._completion_manager is coordinator
    assert service._consumer._task_consumer is service._task_consumer


@pytest.mark.asyncio
async def test_enabled_startup_initializes_finalizer_before_work_intake() -> None:
    service, _redis = _build_service(enabled=True)
    events: list[str] = []

    async def dag_initialise() -> None:
        events.append("dag.initialise")

    async def coordinator_initialise() -> None:
        events.append("coordinator.initialise")

    async def prepare_groups() -> None:
        events.append("consumer.groups")

    async def confirm_leases() -> None:
        events.append("shard.leases")

    async def heartbeat_start() -> None:
        events.append("heartbeat.start")

    async def consumer_start() -> None:
        events.append("consumer.start")

    def start_shard_renewer() -> None:
        events.append("shard.renewer")

    def attach_observers() -> None:
        events.append("health.observers")

    service._dag_lua.initialise = dag_initialise
    service._run_termination_coordinator.initialise = (
        coordinator_initialise
    )
    service._prepare_consumer_groups = prepare_groups
    service._confirm_shard_leases = confirm_leases
    service._heartbeat.start = heartbeat_start
    service._consumer.start = consumer_start
    service._start_shard_renewer = start_shard_renewer
    service._attach_background_health_observers = attach_observers

    await service.start()

    assert events == [
        "dag.initialise",
        "coordinator.initialise",
        "consumer.groups",
        "shard.leases",
        "heartbeat.start",
        "consumer.start",
        "shard.renewer",
        "health.observers",
    ]
    assert service.is_ready is True


@pytest.mark.asyncio
async def test_disabled_startup_never_initializes_run_finalizer() -> None:
    service, _redis = _build_service(enabled=False)
    events: list[str] = []

    async def dag_initialise() -> None:
        events.append("dag.initialise")

    async def prepare_groups() -> None:
        events.append("consumer.groups")

    async def confirm_leases() -> None:
        events.append("shard.leases")

    async def heartbeat_start() -> None:
        events.append("heartbeat.start")

    async def consumer_start() -> None:
        events.append("consumer.start")

    service._dag_lua.initialise = dag_initialise
    service._prepare_consumer_groups = prepare_groups
    service._confirm_shard_leases = confirm_leases
    service._heartbeat.start = heartbeat_start
    service._consumer.start = consumer_start
    service._start_shard_renewer = lambda: None
    service._attach_background_health_observers = lambda: None

    await service.start()

    assert events == [
        "dag.initialise",
        "consumer.groups",
        "shard.leases",
        "heartbeat.start",
        "consumer.start",
    ]
    assert service._run_termination_coordinator is None
    assert service.is_ready is True


@pytest.mark.asyncio
async def test_finalizer_startup_failure_blocks_all_work_intake() -> None:
    service, _redis = _build_service(enabled=True)
    events: list[str] = []

    async def dag_initialise() -> None:
        events.append("dag.initialise")

    async def coordinator_initialise() -> None:
        events.append("coordinator.initialise")
        raise RuntimeError("RUN_TERMINATE script unavailable")

    async def prepare_groups() -> None:
        events.append("consumer.groups")

    async def heartbeat_start() -> None:
        events.append("heartbeat.start")

    async def consumer_start() -> None:
        events.append("consumer.start")

    service._dag_lua.initialise = dag_initialise
    service._run_termination_coordinator.initialise = (
        coordinator_initialise
    )
    service._prepare_consumer_groups = prepare_groups
    service._heartbeat.start = heartbeat_start
    service._consumer.start = consumer_start

    with pytest.raises(
        RuntimeError,
        match="RUN_TERMINATE script unavailable",
    ):
        await service.start()

    assert events == [
        "dag.initialise",
        "coordinator.initialise",
    ]
    assert service.is_ready is False
    assert service._started is False
    assert service.last_failure is not None
