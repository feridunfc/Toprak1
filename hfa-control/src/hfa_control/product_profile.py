"""Shared Sprint 83.7 product-profile and local-preflight contracts."""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any

from hfa.config.keys import RedisTTL


class ProductMode(str, Enum):
    RUNTIME_INTERNAL = "RUNTIME_INTERNAL"
    SINGLE_TASK_ALPHA = "SINGLE_TASK_ALPHA"


class TenantIdentityBoundary(str, Enum):
    INTERNAL_UNSPECIFIED = "INTERNAL_UNSPECIFIED"
    TRUSTED_GATEWAY_HEADER = "TRUSTED_GATEWAY_HEADER"


# Composition compatibility only; this marker is not a production-ready claim.
ALPHA_CANONICAL_COMPOSITION_CAPABILITY = "product:single-task-canonical-v1"
PRODUCT_EXECUTOR_CAPABILITIES = frozenset({
    "executor:configured",
    "executor:deterministic",
    "executor:external",
    "executor:cognitive",
})


def product_worker_capabilities(values: Any) -> frozenset[str]:
    """Malformed registry evidence must not create product compatibility."""
    if not isinstance(values, (list, tuple, set, frozenset)):
        return frozenset()
    if any(not isinstance(value, str) for value in values):
        return frozenset()
    return frozenset(value.strip() for value in values if value.strip())


def alpha_worker_is_compatible(values: Any) -> bool:
    capabilities = product_worker_capabilities(values)
    composition_versions = {
        value for value in capabilities
        if value.startswith("product:single-task-canonical-")
    }
    executors = {
        value for value in capabilities if value.startswith("executor:")
    }
    return (
        composition_versions == {ALPHA_CANONICAL_COMPOSITION_CAPABILITY}
        and {"product:single-task-v1", "run-finalization:v1"} <= capabilities
        and executors == {"executor:deterministic"}
    )


def require_alpha_deterministic_executor_mode(
    *, product_mode: Any, executor_mode: Any,
) -> None:
    """The frozen first-release alpha is simulation-only."""
    if parse_product_mode(product_mode) is ProductMode.SINGLE_TASK_ALPHA:
        if str(executor_mode or "").strip().lower() != "fake":
            raise ValueError("SINGLE_TASK_ALPHA requires executor_mode=fake")


def parse_product_mode(value: Any) -> ProductMode:
    if isinstance(value, ProductMode):
        return value

    raw = (
        ProductMode.RUNTIME_INTERNAL.value
        if value is None
        else str(value).strip()
    )
    try:
        return ProductMode(raw)
    except ValueError as exc:
        allowed = ", ".join(mode.value for mode in ProductMode)
        raise ValueError(
            f"unsupported product_mode {raw!r}; expected one of: {allowed}"
        ) from exc


def parse_tenant_identity_boundary(
    value: Any,
) -> TenantIdentityBoundary:
    if isinstance(value, TenantIdentityBoundary):
        return value

    raw = (
        TenantIdentityBoundary.INTERNAL_UNSPECIFIED.value
        if value is None
        else str(value).strip()
    )
    try:
        return TenantIdentityBoundary(raw)
    except ValueError as exc:
        allowed = ", ".join(
            boundary.value for boundary in TenantIdentityBoundary
        )
        raise ValueError(
            "unsupported tenant_identity_boundary "
            f"{raw!r}; expected one of: {allowed}"
        ) from exc


def parse_strict_bool(
    value: Any,
    *,
    name: str,
    default: bool,
) -> bool:
    if value is None:
        return default
    if type(value) is bool:
        return value

    normalized = str(value).strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    raise ValueError(
        f"{name} must be one of "
        "1,true,yes,on,0,false,no,off"
    )


def canonical_result_retention_seconds() -> int:
    values = {
        int(RedisTTL.RUN_STATE),
        int(RedisTTL.RUN_META),
        int(RedisTTL.RUN_RESULT),
    }
    if len(values) != 1:
        raise RuntimeError(
            "single-task alpha requires equal canonical "
            "RUN state/meta/result retention"
        )
    retention = values.pop()
    if retention <= 0:
        raise RuntimeError(
            "canonical result retention must be greater than zero"
        )
    return retention


def require_alpha_canonical_bindings(
    *, product_mode: Any, component: str, **bindings: bool,
) -> None:
    """Reject an incomplete alpha graph; internal compatibility stays opt-in."""
    for name, value in bindings.items():
        if type(value) is not bool:
            raise ValueError(f"{name} must be a boolean")
    if parse_product_mode(product_mode) is not ProductMode.SINGLE_TASK_ALPHA:
        return
    missing = [name for name, enabled in bindings.items() if not enabled]
    if missing:
        raise ValueError(
            f"SINGLE_TASK_ALPHA {component} requires canonical bindings; "
            "disabled or missing: " + ", ".join(missing)
        )


@dataclass(frozen=True)
class ControlProductProfile:
    product_mode: ProductMode
    tenant_identity_boundary: TenantIdentityBoundary
    strict_cas_mode: bool
    canonical_task_admit_binding: bool
    single_task_submission_surface: bool
    result_retention_seconds: int
    canonical_run_create_binding: bool = False
    canonical_task_dispatch_binding: bool = False
    canonical_task_requeue_binding: bool = False

    @property
    def single_task_alpha(self) -> bool:
        return self.product_mode is ProductMode.SINGLE_TASK_ALPHA


def validate_control_product_profile(
    *,
    product_mode: Any,
    tenant_identity_boundary: Any,
    strict_cas_mode: Any,
    canonical_task_admit_binding: Any,
    single_task_submission_surface: Any,
    canonical_run_create_binding: Any = False,
    canonical_task_dispatch_binding: Any = False,
    canonical_task_requeue_binding: Any = False,
) -> ControlProductProfile:
    mode = parse_product_mode(product_mode)
    boundary = parse_tenant_identity_boundary(
        tenant_identity_boundary
    )

    if type(strict_cas_mode) is not bool:
        raise ValueError("strict_cas_mode must be a boolean")
    if type(canonical_task_admit_binding) is not bool:
        raise ValueError(
            "canonical_task_admit_binding must be a boolean"
        )
    if type(single_task_submission_surface) is not bool:
        raise ValueError(
            "single_task_submission_surface must be a boolean"
        )

    if mode is ProductMode.SINGLE_TASK_ALPHA:
        if not strict_cas_mode:
            raise ValueError(
                "SINGLE_TASK_ALPHA requires strict_cas_mode=True"
            )
        if not canonical_task_admit_binding:
            raise ValueError(
                "SINGLE_TASK_ALPHA requires "
                "canonical TASK_ADMIT binding"
            )
        if not single_task_submission_surface:
            raise ValueError(
                "SINGLE_TASK_ALPHA requires the canonical "
                "single-task submission surface"
            )
        if (
            boundary
            is not TenantIdentityBoundary.TRUSTED_GATEWAY_HEADER
        ):
            raise ValueError(
                "SINGLE_TASK_ALPHA requires "
                "tenant_identity_boundary="
                "TRUSTED_GATEWAY_HEADER"
            )

    require_alpha_canonical_bindings(
        product_mode=mode,
        component="control",
        canonical_run_create_binding=canonical_run_create_binding,
        canonical_task_dispatch_binding=canonical_task_dispatch_binding,
        canonical_task_requeue_binding=canonical_task_requeue_binding,
    )
    return ControlProductProfile(
        product_mode=mode,
        tenant_identity_boundary=boundary,
        strict_cas_mode=strict_cas_mode,
        canonical_task_admit_binding=(
            canonical_task_admit_binding
        ),
        single_task_submission_surface=(
            single_task_submission_surface
        ),
        result_retention_seconds=(
            canonical_result_retention_seconds()
        ),
        canonical_run_create_binding=canonical_run_create_binding,
        canonical_task_dispatch_binding=canonical_task_dispatch_binding,
        canonical_task_requeue_binding=canonical_task_requeue_binding,
    )


@dataclass(frozen=True)
class WorkerProductProfile:
    product_mode: ProductMode
    production: bool
    worker_id: str
    worker_group: str
    executor_configured: bool
    run_termination_binding_enabled: bool
    canonical_task_admit_binding: bool = False
    canonical_task_dispatch_binding: bool = False
    canonical_task_claim_binding: bool = False
    canonical_task_terminal_binding: bool = False
    canonical_resource_settlement_binding: bool = False

    @property
    def single_task_alpha(self) -> bool:
        return self.product_mode is ProductMode.SINGLE_TASK_ALPHA


def validate_worker_product_profile(
    *,
    product_mode: Any,
    production: Any,
    worker_id: Any,
    worker_group: Any,
    executor_configured: Any,
    run_termination_binding_enabled: Any,
    canonical_task_admit_binding: Any = False,
    canonical_task_dispatch_binding: Any = False,
    canonical_task_claim_binding: Any = False,
    canonical_task_terminal_binding: Any = False,
    canonical_resource_settlement_binding: Any = False,
) -> WorkerProductProfile:
    mode = parse_product_mode(product_mode)

    for name, value in (
        ("production", production),
        ("executor_configured", executor_configured),
        (
            "run_termination_binding_enabled",
            run_termination_binding_enabled,
        ),
    ):
        if type(value) is not bool:
            raise ValueError(f"{name} must be a boolean")

    normalized_worker_id = str(worker_id or "").strip()
    normalized_worker_group = str(worker_group or "").strip()

    if mode is ProductMode.SINGLE_TASK_ALPHA:
        if not production:
            raise ValueError(
                "SINGLE_TASK_ALPHA worker requires production=True"
            )
        if not normalized_worker_id:
            raise ValueError(
                "SINGLE_TASK_ALPHA worker requires a non-empty worker_id"
            )
        if not normalized_worker_group:
            raise ValueError(
                "SINGLE_TASK_ALPHA worker requires a non-empty worker_group"
            )
        if not executor_configured:
            raise ValueError(
                "SINGLE_TASK_ALPHA worker requires an executor"
            )
        if not run_termination_binding_enabled:
            raise ValueError(
                "SINGLE_TASK_ALPHA worker requires "
                "run_termination_binding_enabled=True"
            )

    require_alpha_canonical_bindings(
        product_mode=mode,
        component="worker",
        canonical_task_admit_binding=canonical_task_admit_binding,
        canonical_task_dispatch_binding=canonical_task_dispatch_binding,
        canonical_task_claim_binding=canonical_task_claim_binding,
        canonical_task_terminal_binding=canonical_task_terminal_binding,
        canonical_resource_settlement_binding=canonical_resource_settlement_binding,
    )
    return WorkerProductProfile(
        product_mode=mode,
        production=production,
        worker_id=normalized_worker_id,
        worker_group=normalized_worker_group,
        executor_configured=executor_configured,
        run_termination_binding_enabled=(
            run_termination_binding_enabled
        ),
        canonical_task_admit_binding=canonical_task_admit_binding,
        canonical_task_dispatch_binding=canonical_task_dispatch_binding,
        canonical_task_claim_binding=canonical_task_claim_binding,
        canonical_task_terminal_binding=canonical_task_terminal_binding,
        canonical_resource_settlement_binding=canonical_resource_settlement_binding,
    )
