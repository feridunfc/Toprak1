"""G0-E1 constructor regressions. No Redis server or executor may be called."""
from __future__ import annotations

from dataclasses import replace

import pytest

import hfa_control.service as control_module
import hfa_worker.main as worker_module
from hfa_control.dag_lua import DagLua
from hfa_control.models import ControlPlaneConfig
from hfa_control.product_profile import (
    validate_control_product_profile,
    validate_worker_product_profile,
)
from hfa_control.recovery import RecoveryService
from hfa_control.scheduler import build_production_scheduler
from hfa_control.service import ControlPlaneService
from hfa_worker.main import WorkerService
from hfa_worker.fake_executor import FakeExecutor
from hfa_worker.process_root import config_from_env


WORKER_BINDINGS = (
    "canonical_task_admit_binding",
    "canonical_task_dispatch_binding",
    "canonical_task_claim_binding",
    "canonical_task_terminal_binding",
    "canonical_resource_settlement_binding",
)
CONTROL_ENV_FLAGS = (
    "HFA_CANONICAL_RUN_CREATE_BINDING",
    "HFA_CANONICAL_TASK_ADMIT_BINDING",
    "HFA_CANONICAL_TASK_DISPATCH_BINDING",
    "HFA_CANONICAL_TASK_CLAIM_BINDING",
    "HFA_CANONICAL_TASK_TERMINAL_BINDING",
    "HFA_CANONICAL_TASK_REQUEUE_BINDING",
)


class NoIoRedis:
    def __getattr__(self, name):
        if name not in {"set", "get", "expire", "hset", "eval"}:
            raise AttributeError(name)

        def forbidden(*args, **kwargs):
            pytest.fail(f"Constructor attempted Redis I/O: {name}")

        return forbidden


class NoExecute:
    product_executor_capability = "executor:deterministic"

    async def execute(self, *args, **kwargs):
        pytest.fail("Executor must not run in constructor tests")


def control_config():
    return ControlPlaneConfig(
        instance_id="cp-g0-e1",
        product_mode="SINGLE_TASK_ALPHA",
        strict_cas_mode=True,
        tenant_identity_boundary="TRUSTED_GATEWAY_HEADER",
    )


def worker_config(**overrides):
    result = {
        "product_mode": "SINGLE_TASK_ALPHA",
        "production": True,
        "worker_id": "worker-g0-e1",
        "worker_group": "group-g0-e1",
        "executor": FakeExecutor(),
        "run_termination_binding_enabled": True,
        **dict.fromkeys(WORKER_BINDINGS, True),
    }
    result.update(overrides)
    return result


@pytest.fixture
def canonical_environment(monkeypatch):
    for name in CONTROL_ENV_FLAGS:
        monkeypatch.setenv(name, "true")


@pytest.mark.parametrize("missing", CONTROL_ENV_FLAGS)
def test_control_rejects_each_missing_binding_before_io(
    monkeypatch, canonical_environment, missing,
):
    monkeypatch.setenv(missing, "false")
    with pytest.raises(ValueError, match="canonical"):
        ControlPlaneService(NoIoRedis(), control_config())


def test_control_rejects_original_census_counterexample(monkeypatch):
    for name in CONTROL_ENV_FLAGS:
        monkeypatch.setenv(name, "false")
    monkeypatch.setenv("HFA_CANONICAL_TASK_ADMIT_BINDING", "true")
    with pytest.raises(ValueError, match="canonical_run_create_binding"):
        ControlPlaneService(NoIoRedis(), control_config())


@pytest.mark.parametrize("missing", WORKER_BINDINGS)
def test_worker_rejects_each_missing_binding_before_io(missing):
    with pytest.raises(ValueError, match="canonical"):
        WorkerService(NoIoRedis(), worker_config(**{missing: False}))


def test_worker_rejects_original_process_root_counterexample():
    config = config_from_env({
        "HFA_PRODUCT_MODE": "SINGLE_TASK_ALPHA",
        "WORKER_ID": "worker-g0-e1",
        "WORKER_GROUP": "group-g0-e1",
        "WORKER_EXECUTOR_MODE": "fake",
        "WORKER_RUN_TERMINATION_BINDING": "true",
    })
    config["executor"] = FakeExecutor()
    with pytest.raises(ValueError, match="SINGLE_TASK_ALPHA worker"):
        WorkerService(NoIoRedis(), config)


def test_complete_worker_process_root_config_selects_canonical_owners():
    env = {
        "HFA_PRODUCT_MODE": "SINGLE_TASK_ALPHA",
        "WORKER_ID": "worker-g0-e1",
        "WORKER_GROUP": "group-g0-e1",
        "WORKER_EXECUTOR_MODE": "fake",
        "WORKER_RUN_TERMINATION_BINDING": "true",
        **{f"HFA_{name.upper()}": "true" for name in WORKER_BINDINGS},
    }
    config = config_from_env(env)
    config["executor"] = FakeExecutor()
    worker = WorkerService(NoIoRedis(), config)
    coordinator = worker._run_termination_coordinator
    assert coordinator._authority_binding is worker._run_terminate_authority_binding
    assert worker._run_terminate_authority_binding.resource_manager is worker._resource_settlement_manager
    assert worker._task_consumer._completion_manager is coordinator
    assert coordinator._task_completion_gateway._binding is worker._task_terminal_authority_binding
    assert worker._consumer._canonical_task_claim_binding_enabled is True
    assert all(getattr(worker.product_profile, name) for name in WORKER_BINDINGS)
    assert "product:single-task-v1" in worker.runtime_capabilities


def test_complete_control_graph_selects_canonical_owners(canonical_environment):
    control = ControlPlaneService(NoIoRedis(), control_config())
    assert control.admission._run_create_authority is not None
    assert control._scheduler.composition.dag_lua._canonical_task_dispatch_binding_enabled
    recovery = control.recovery
    assert recovery._task_recovery._requeue_authority is not None
    assert recovery._task_recovery._terminal_authority is not None
    assert recovery._run_terminate_authority.resource_manager is recovery._resource_manager


@pytest.mark.parametrize("missing", (
    "HFA_CANONICAL_TASK_ADMIT_BINDING",
    "HFA_CANONICAL_TASK_DISPATCH_BINDING",
))
def test_standalone_scheduler_cannot_bypass_alpha_gate(
    monkeypatch, canonical_environment, missing,
):
    monkeypatch.setenv(missing, "false")
    with pytest.raises(ValueError, match="canonical|CANONICAL"):
        build_production_scheduler(
            redis=NoIoRedis(), config=control_config(), registry=object(), shards=object(),
        )


def test_standalone_recovery_cannot_bypass_alpha_gate(monkeypatch):
    monkeypatch.setenv("HFA_CANONICAL_TASK_REQUEUE_BINDING", "false")
    with pytest.raises(ValueError, match="SINGLE_TASK_ALPHA recovery"):
        RecoveryService(NoIoRedis(), control_config())


def test_control_checks_actual_scheduler_not_only_env(
    monkeypatch, canonical_environment,
):
    original_builder = control_module.build_production_scheduler

    def legacy_scheduler(**kwargs):
        scheduler = original_builder(**kwargs)
        scheduler._composition = replace(
            scheduler.composition, dag_lua=DagLua(kwargs["redis"]),
        )
        return scheduler

    monkeypatch.setattr(control_module, "build_production_scheduler", legacy_scheduler)
    with pytest.raises(ValueError, match="control composition.*task_dispatch_authority"):
        ControlPlaneService(NoIoRedis(), control_config())


@pytest.mark.parametrize("broken_link", ("_authority_binding", "_task_completion_gateway"))
def test_worker_rejects_disconnected_terminal_coordinator(monkeypatch, broken_link):
    original_coordinator = worker_module.RunTerminationCoordinator

    def disconnected_coordinator(*args, **kwargs):
        coordinator = original_coordinator(*args, **kwargs)
        setattr(coordinator, broken_link, None)
        return coordinator

    monkeypatch.setattr(worker_module, "RunTerminationCoordinator", disconnected_coordinator)
    with pytest.raises(ValueError, match="worker composition"):
        WorkerService(NoIoRedis(), worker_config())


def test_worker_rejects_disconnected_resource_settlement(monkeypatch):
    original_binding = worker_module.RunTerminateAuthorityBinding

    def disconnected_binding(*args, **kwargs):
        binding = original_binding(*args, **kwargs)
        binding.resource_manager = None
        return binding

    monkeypatch.setattr(worker_module, "RunTerminateAuthorityBinding", disconnected_binding)
    with pytest.raises(ValueError, match="worker composition.*resource_settlement"):
        WorkerService(NoIoRedis(), worker_config())


def test_control_rejects_disconnected_recovery_settlement(
    monkeypatch, canonical_environment,
):
    original_recovery = control_module.RecoveryService

    def disconnected_recovery(*args, **kwargs):
        recovery = original_recovery(*args, **kwargs)
        recovery._run_terminate_authority.resource_manager = None
        return recovery

    monkeypatch.setattr(control_module, "RecoveryService", disconnected_recovery)
    with pytest.raises(ValueError, match="control composition.*recovery_resource_settlement"):
        ControlPlaneService(NoIoRedis(), control_config())


def test_worker_rejects_consumer_that_drops_claim_routing(monkeypatch):
    original_consumer = worker_module.RunFinalizingWorkerConsumer

    class LegacyRoutingConsumer(original_consumer):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self._canonical_task_claim_binding_enabled = False

    monkeypatch.setattr(worker_module, "RunFinalizingWorkerConsumer", LegacyRoutingConsumer)
    with pytest.raises(ValueError, match="worker consumer.*task_consumer_routing"):
        WorkerService(NoIoRedis(), worker_config())


@pytest.mark.parametrize("binding", WORKER_BINDINGS)
@pytest.mark.parametrize("value", (False, None, 1, "true"))
def test_worker_profile_requires_explicit_true_booleans(binding, value):
    config = worker_config(**{binding: value})
    del config["executor"]
    with pytest.raises(ValueError, match=binding):
        validate_worker_product_profile(executor_configured=True, **config)


@pytest.mark.parametrize("binding", (
    "canonical_run_create_binding",
    "canonical_task_dispatch_binding",
    "canonical_task_requeue_binding",
))
@pytest.mark.parametrize("value", (False, None, 1, "true"))
def test_control_profile_requires_explicit_true_booleans(binding, value):
    config = {
        "product_mode": "SINGLE_TASK_ALPHA",
        "strict_cas_mode": True,
        "tenant_identity_boundary": "TRUSTED_GATEWAY_HEADER",
        "single_task_submission_surface": True,
        "canonical_task_admit_binding": True,
        "canonical_run_create_binding": True,
        "canonical_task_dispatch_binding": True,
        "canonical_task_requeue_binding": True,
    }
    config[binding] = value
    with pytest.raises(ValueError, match=binding):
        validate_control_product_profile(**config)


def test_runtime_internal_keeps_legacy_opt_in_defaults(monkeypatch):
    for name in CONTROL_ENV_FLAGS:
        monkeypatch.delenv(name, raising=False)
    control = ControlPlaneService(NoIoRedis(), ControlPlaneConfig(instance_id="cp-internal"))
    worker = WorkerService(NoIoRedis(), worker_config(
        product_mode="RUNTIME_INTERNAL",
        run_termination_binding_enabled=False,
        **dict.fromkeys(WORKER_BINDINGS, False),
    ))
    assert control.admission._run_create_authority is None
    assert control.recovery._task_recovery is None
    assert worker._run_terminate_authority_binding is None
    assert "product:single-task-v1" not in worker.runtime_capabilities
