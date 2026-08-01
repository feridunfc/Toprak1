from __future__ import annotations

from dataclasses import dataclass

import pytest

from hfa.config.keys import RedisTTL
from hfa_control.models import ControlPlaneConfig
from hfa_control.product_profile import (
    ProductMode,
    TenantIdentityBoundary,
    canonical_result_retention_seconds,
    parse_product_mode,
    parse_tenant_identity_boundary,
    validate_control_product_profile,
    validate_worker_product_profile,
)
from hfa_control.service import (
    ControlPlaneService,
    _config_from_env,
)
from hfa_control.task_admit_authority import FEATURE_FLAG
from hfa_worker.main import WorkerService
from hfa_worker.process_root import config_from_env
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


class ExecutorProbe:
    async def execute(self, event):
        return ExecutionResultProbe(
            payload={"run_id": getattr(event, "run_id", "")}
        )


def valid_control_profile(**overrides):
    values = {
        "product_mode": ProductMode.SINGLE_TASK_ALPHA.value,
        "tenant_identity_boundary": (
            TenantIdentityBoundary
            .TRUSTED_GATEWAY_HEADER
            .value
        ),
        "strict_cas_mode": True,
        "canonical_task_admit_binding": True,
        "single_task_submission_surface": True,
    }
    values.update(overrides)
    return validate_control_product_profile(**values)


def valid_worker_profile(**overrides):
    values = {
        "product_mode": ProductMode.SINGLE_TASK_ALPHA.value,
        "production": True,
        "worker_id": "worker-alpha",
        "worker_group": "group-alpha",
        "executor_configured": True,
        "run_termination_binding_enabled": True,
    }
    values.update(overrides)
    return validate_worker_product_profile(**values)


def alpha_worker_config(**overrides):
    values = {
        "product_mode": ProductMode.SINGLE_TASK_ALPHA.value,
        "production": True,
        "worker_id": "worker-alpha",
        "worker_group": "group-alpha",
        "region": "eu-west-1",
        "shards": [0],
        "capacity": 1,
        "executor": ExecutorProbe(),
        "run_termination_binding_enabled": True,
    }
    values.update(overrides)
    return values


def test_product_mode_defaults_to_runtime_internal():
    assert (
        parse_product_mode(None)
        is ProductMode.RUNTIME_INTERNAL
    )


def test_unsupported_product_mode_fails_closed():
    with pytest.raises(
        ValueError,
        match="unsupported product_mode",
    ):
        parse_product_mode("PUBLIC_SAAS")


def test_unknown_tenant_identity_boundary_fails_closed():
    with pytest.raises(
        ValueError,
        match="unsupported tenant_identity_boundary",
    ):
        parse_tenant_identity_boundary("UNVERIFIED_HEADER")


def test_canonical_retention_is_fixed_current_contract():
    assert canonical_result_retention_seconds() == 86_400
    assert RedisTTL.RUN_STATE == 86_400
    assert RedisTTL.RUN_META == 86_400
    assert RedisTTL.RUN_RESULT == 86_400


def test_runtime_internal_preserves_legacy_control_defaults():
    profile = validate_control_product_profile(
        product_mode=ProductMode.RUNTIME_INTERNAL.value,
        tenant_identity_boundary=(
            TenantIdentityBoundary
            .INTERNAL_UNSPECIFIED
            .value
        ),
        strict_cas_mode=False,
        canonical_task_admit_binding=False,
        single_task_submission_surface=True,
    )

    assert profile.single_task_alpha is False
    assert profile.result_retention_seconds == 86_400


def test_valid_single_task_alpha_control_profile():
    profile = valid_control_profile()

    assert profile.single_task_alpha is True
    assert (
        profile.tenant_identity_boundary
        is TenantIdentityBoundary.TRUSTED_GATEWAY_HEADER
    )
    assert profile.strict_cas_mode is True
    assert profile.canonical_task_admit_binding is True


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        (
            "strict_cas_mode",
            False,
            "requires strict_cas_mode=True",
        ),
        (
            "canonical_task_admit_binding",
            False,
            "requires canonical TASK_ADMIT binding",
        ),
        (
            "single_task_submission_surface",
            False,
            "requires the canonical single-task submission surface",
        ),
        (
            "tenant_identity_boundary",
            TenantIdentityBoundary.INTERNAL_UNSPECIFIED.value,
            "requires tenant_identity_boundary",
        ),
    ],
)
def test_incompatible_alpha_control_profile_fails_closed(
    field,
    value,
    message,
):
    with pytest.raises(ValueError, match=message):
        valid_control_profile(**{field: value})


def test_control_env_config_exposes_internal_defaults(
    monkeypatch,
):
    for name in (
        "HFA_PRODUCT_MODE",
        "HFA_TENANT_IDENTITY_BOUNDARY",
        "HFA_STRICT_CAS_MODE",
    ):
        monkeypatch.delenv(name, raising=False)

    config = _config_from_env()

    assert (
        config.product_mode
        == ProductMode.RUNTIME_INTERNAL.value
    )
    assert (
        config.tenant_identity_boundary
        == TenantIdentityBoundary
        .INTERNAL_UNSPECIFIED
        .value
    )
    assert config.strict_cas_mode is False


def test_control_env_config_parses_alpha_profile(
    monkeypatch,
):
    monkeypatch.setenv(
        "HFA_PRODUCT_MODE",
        ProductMode.SINGLE_TASK_ALPHA.value,
    )
    monkeypatch.setenv(
        "HFA_TENANT_IDENTITY_BOUNDARY",
        TenantIdentityBoundary
        .TRUSTED_GATEWAY_HEADER
        .value,
    )
    monkeypatch.setenv("HFA_STRICT_CAS_MODE", "true")

    config = _config_from_env()

    assert (
        config.product_mode
        == ProductMode.SINGLE_TASK_ALPHA.value
    )
    assert config.strict_cas_mode is True


def test_control_service_alpha_preflight_accepts_valid_profile(
    monkeypatch,
):
    monkeypatch.setenv(FEATURE_FLAG, "1")
    config = ControlPlaneConfig(
        instance_id="cp-alpha",
        strict_cas_mode=True,
        product_mode=ProductMode.SINGLE_TASK_ALPHA.value,
        tenant_identity_boundary=(
            TenantIdentityBoundary
            .TRUSTED_GATEWAY_HEADER
            .value
        ),
    )

    service = ControlPlaneService(RedisProbe(), config)

    assert service.product_profile.single_task_alpha is True
    assert (
        service.product_profile.result_retention_seconds
        == 86_400
    )


def test_control_service_alpha_preflight_rejects_legacy_task_admit(
    monkeypatch,
):
    monkeypatch.setenv(FEATURE_FLAG, "0")
    config = ControlPlaneConfig(
        instance_id="cp-alpha",
        strict_cas_mode=True,
        product_mode=ProductMode.SINGLE_TASK_ALPHA.value,
        tenant_identity_boundary=(
            TenantIdentityBoundary
            .TRUSTED_GATEWAY_HEADER
            .value
        ),
    )

    with pytest.raises(
        ValueError,
        match="canonical TASK_ADMIT binding",
    ):
        ControlPlaneService(RedisProbe(), config)


def test_runtime_internal_worker_profile_preserves_legacy_defaults():
    profile = validate_worker_product_profile(
        product_mode=ProductMode.RUNTIME_INTERNAL.value,
        production=False,
        worker_id="",
        worker_group="",
        executor_configured=True,
        run_termination_binding_enabled=False,
    )

    assert profile.single_task_alpha is False


def test_valid_single_task_alpha_worker_profile():
    profile = valid_worker_profile()

    assert profile.single_task_alpha is True
    assert profile.production is True
    assert profile.run_termination_binding_enabled is True


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        (
            "production",
            False,
            "requires production=True",
        ),
        (
            "worker_id",
            "",
            "requires a non-empty worker_id",
        ),
        (
            "worker_group",
            "",
            "requires a non-empty worker_group",
        ),
        (
            "executor_configured",
            False,
            "requires an executor",
        ),
        (
            "run_termination_binding_enabled",
            False,
            "requires run_termination_binding_enabled=True",
        ),
    ],
)
def test_incompatible_alpha_worker_profile_fails_closed(
    field,
    value,
    message,
):
    with pytest.raises(ValueError, match=message):
        valid_worker_profile(**{field: value})


def test_worker_service_alpha_uses_finalizing_composition():
    service = WorkerService(
        RedisProbe(),
        alpha_worker_config(),
    )

    assert service.product_profile.single_task_alpha is True
    assert (
        type(service._task_consumer)
        is RunFinalizingTaskConsumer
    )
    assert (
        type(service._consumer)
        is RunFinalizingWorkerConsumer
    )


def test_worker_service_alpha_rejects_disabled_finalization():
    with pytest.raises(
        ValueError,
        match="run_termination_binding_enabled=True",
    ):
        WorkerService(
            RedisProbe(),
            alpha_worker_config(
                run_termination_binding_enabled=False
            ),
        )


def test_worker_service_alpha_rejects_missing_worker_group():
    with pytest.raises(
        ValueError,
        match="non-empty worker_group",
    ):
        WorkerService(
            RedisProbe(),
            alpha_worker_config(worker_group=""),
        )


def test_process_root_preserves_internal_defaults():
    config = config_from_env(
        {
            "WORKER_EXECUTOR_MODE": "fake",
            "WORKER_ID": "worker-internal",
        }
    )

    assert (
        config["product_mode"]
        == ProductMode.RUNTIME_INTERNAL.value
    )
    assert config["run_termination_binding_enabled"] is False


def test_process_root_parses_valid_alpha_mode():
    config = config_from_env(
        {
            "HFA_PRODUCT_MODE": (
                ProductMode.SINGLE_TASK_ALPHA.value
            ),
            "WORKER_EXECUTOR_MODE": "fake",
            "WORKER_ID": "worker-alpha",
            "WORKER_GROUP": "group-alpha",
            "WORKER_RUN_TERMINATION_BINDING": "true",
        }
    )

    assert (
        config["product_mode"]
        == ProductMode.SINGLE_TASK_ALPHA.value
    )
    assert config["worker_group"] == "group-alpha"
    assert config["run_termination_binding_enabled"] is True


def test_process_root_rejects_alpha_without_worker_group():
    with pytest.raises(
        RuntimeError,
        match="WORKER_GROUP is required",
    ):
        config_from_env(
            {
                "HFA_PRODUCT_MODE": (
                    ProductMode.SINGLE_TASK_ALPHA.value
                ),
                "WORKER_EXECUTOR_MODE": "fake",
                "WORKER_ID": "worker-alpha",
                "WORKER_RUN_TERMINATION_BINDING": "true",
            }
        )


def test_process_root_rejects_unsupported_product_mode():
    with pytest.raises(
        ValueError,
        match="unsupported product_mode",
    ):
        config_from_env(
            {
                "HFA_PRODUCT_MODE": "PUBLIC_SAAS",
                "WORKER_EXECUTOR_MODE": "fake",
                "WORKER_ID": "worker-alpha",
            }
        )
