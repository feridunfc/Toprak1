from __future__ import annotations

from dataclasses import dataclass

import pytest

from hfa.governance.admission_resource_reservation import AdmissionResourceReservationManager
from hfa_control.run_terminate_authority import RunTerminateAuthorityBinding
from hfa_worker.main import WorkerService
from hfa_worker.process_root import config_from_env
from hfa_worker.run_finalizing_runtime import RunFinalizingWorkerConsumer
from hfa_worker.task_executor import TaskExecutionResult


class RedisProbe:
    async def set(self, *args, **kwargs): return True
    async def get(self, *args, **kwargs): return None
    async def expire(self, *args, **kwargs): return 1
    async def hset(self, *args, **kwargs): return 1
    async def eval(self, *args, **kwargs): return 1


@dataclass
class LegacyExecutionResult:
    status: str = "done"
    payload: dict | None = None
    cost_cents: int = 0
    tokens_used: int = 0
    error: str = ""


class LegacyExecutor:
    async def execute(self, event):
        return LegacyExecutionResult(payload={"run_id": getattr(event, "run_id", "")})


class TaskExecutor:
    async def execute(self, ctx):
        return TaskExecutionResult(ok=True, output={"task_id": ctx.task_id})


def _base(**overrides):
    config = {
        "production": True,
        "worker_id": "worker-84-8-contract",
        "worker_group": "group-84-8-contract",
        "region": "contract",
        "shards": [0],
        "capacity": 1,
        "executor": LegacyExecutor(),
        "task_executor": TaskExecutor(),
        "canonical_task_admit_binding": True,
        "canonical_task_dispatch_binding": True,
        "canonical_task_claim_binding": True,
        "canonical_task_terminal_binding": True,
        "run_termination_binding_enabled": True,
    }
    config.update(overrides)
    return config


def test_resource_settlement_default_is_disabled_and_historical_profile_d_is_unchanged():
    service = WorkerService(RedisProbe(), _base())
    assert service.canonical_resource_settlement_binding_enabled is False
    assert service._resource_settlement_manager is None
    assert isinstance(service._run_terminate_authority_binding, RunTerminateAuthorityBinding)
    assert service._run_terminate_authority_binding.resource_manager is None
    assert isinstance(service._consumer, RunFinalizingWorkerConsumer)


def test_resource_settlement_rejects_non_boolean_config():
    with pytest.raises(ValueError, match="canonical_resource_settlement_binding must be a boolean"):
        WorkerService(RedisProbe(), _base(canonical_resource_settlement_binding="true"))


def test_resource_settlement_requires_production():
    with pytest.raises(ValueError, match="requires production=True"):
        WorkerService(
            RedisProbe(),
            _base(production=False, canonical_resource_settlement_binding=True),
        )


def test_resource_settlement_requires_canonical_task_terminal_binding():
    with pytest.raises(ValueError, match="requires canonical_task_terminal_binding=True"):
        WorkerService(
            RedisProbe(),
            _base(
                canonical_task_terminal_binding=False,
                canonical_resource_settlement_binding=True,
            ),
        )


def test_resource_settlement_requires_run_termination_binding():
    with pytest.raises(ValueError, match="requires run_termination_binding_enabled=True"):
        WorkerService(
            RedisProbe(),
            _base(
                run_termination_binding_enabled=False,
                canonical_resource_settlement_binding=True,
            ),
        )


def test_selected_profile_injects_existing_admission_resource_manager_into_run_authority():
    service = WorkerService(
        RedisProbe(),
        _base(canonical_resource_settlement_binding=True),
    )
    assert service.canonical_resource_settlement_binding_enabled is True
    assert isinstance(service._resource_settlement_manager, AdmissionResourceReservationManager)
    assert service._run_terminate_authority_binding.resource_manager is service._resource_settlement_manager
    assert service._run_termination_coordinator._authority_binding is service._run_terminate_authority_binding
    assert isinstance(service._consumer, RunFinalizingWorkerConsumer)


def test_process_root_defaults_resource_settlement_false():
    config = config_from_env({
        "WORKER_EXECUTOR_MODE": "fake",
        "WORKER_ID": "worker-84-8",
    })
    assert config["canonical_resource_settlement_binding"] is False


def test_process_root_parses_only_strict_resource_settlement_boolean():
    enabled = config_from_env({
        "WORKER_EXECUTOR_MODE": "fake",
        "WORKER_ID": "worker-84-8",
        "HFA_CANONICAL_RESOURCE_SETTLEMENT_BINDING": "true",
    })
    assert enabled["canonical_resource_settlement_binding"] is True
    with pytest.raises(RuntimeError, match="HFA_CANONICAL_RESOURCE_SETTLEMENT_BINDING"):
        config_from_env({
            "WORKER_EXECUTOR_MODE": "fake",
            "WORKER_ID": "worker-84-8",
            "HFA_CANONICAL_RESOURCE_SETTLEMENT_BINDING": "maybe",
        })
