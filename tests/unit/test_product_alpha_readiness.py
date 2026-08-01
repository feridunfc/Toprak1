from __future__ import annotations

from dataclasses import dataclass

import httpx
import pytest
from fastapi import FastAPI

from hfa_control.api.router import router
from hfa_control.product_profile import (
    TenantIdentityBoundary,
    validate_control_product_profile,
)
from hfa_control.service import ControlPlaneService


@dataclass
class WorkerProbe:
    capabilities: list[str]


class RedisProbe:
    def __init__(
        self,
        *,
        leader_id: str = "cp-alpha",
        ping_error: Exception | None = None,
        get_error: Exception | None = None,
    ) -> None:
        self.leader_id = leader_id
        self.ping_error = ping_error
        self.get_error = get_error
        self.calls: list[tuple] = []

    async def ping(self):
        self.calls.append(("ping",))
        if self.ping_error is not None:
            raise self.ping_error
        return True

    async def get(self, key):
        self.calls.append(("get", key))
        if self.get_error is not None:
            raise self.get_error
        return self.leader_id


class RegistryProbe:
    def __init__(
        self,
        workers: list[WorkerProbe] | None = None,
        *,
        failure: Exception | None = None,
    ) -> None:
        self.workers = list(workers or [])
        self.failure = failure
        self.calls = 0

    async def list_schedulable_workers(self):
        self.calls += 1
        if self.failure is not None:
            raise self.failure
        return list(self.workers)


class LeaderProbe:
    def __init__(self, *, is_leader: bool) -> None:
        self.is_leader = is_leader


class SchedulerProbe:
    def __init__(self, *, running: bool) -> None:
        self.running = running


def alpha_profile():
    return validate_control_product_profile(
        product_mode="SINGLE_TASK_ALPHA",
        tenant_identity_boundary=(
            TenantIdentityBoundary
            .TRUSTED_GATEWAY_HEADER
            .value
        ),
        strict_cas_mode=True,
        canonical_task_admit_binding=True,
        single_task_submission_surface=True,
    )


def internal_profile():
    return validate_control_product_profile(
        product_mode="RUNTIME_INTERNAL",
        tenant_identity_boundary="INTERNAL_UNSPECIFIED",
        strict_cas_mode=False,
        canonical_task_admit_binding=False,
        single_task_submission_surface=True,
    )


def compatible_worker(
    *,
    executor: str = "executor:deterministic",
) -> WorkerProbe:
    return WorkerProbe(
        capabilities=[
            "base",
            executor,
            "product:single-task-v1",
            "run-finalization:v1",
        ]
    )


def make_service(
    *,
    profile=None,
    redis=None,
    workers=None,
    registry_failure=None,
    is_leader: bool = True,
    scheduler_running: bool = True,
    sched_started: bool = True,
):
    service = object.__new__(ControlPlaneService)
    service._product_profile = profile or alpha_profile()
    service._redis = redis or RedisProbe()
    service._registry = RegistryProbe(
        workers=workers,
        failure=registry_failure,
    )
    service._leader = LeaderProbe(is_leader=is_leader)
    service._scheduler = SchedulerProbe(
        running=scheduler_running
    )
    service._sched_started = sched_started
    service._config = type(
        "ConfigProbe",
        (),
        {"leader_key": "hfa:cp:leader"},
    )()
    return service


@pytest.mark.asyncio
async def test_product_readiness_ready_with_compatible_worker():
    service = make_service(workers=[compatible_worker()])

    result = await service.get_product_readiness()

    assert result == {
        "product_mode": "SINGLE_TASK_ALPHA",
        "ready": True,
        "tenant_identity_boundary":
            "TRUSTED_GATEWAY_HEADER",
        "redis_reachable": True,
        "leader_available": True,
        "scheduler_running": True,
        "compatible_worker_count": 1,
        "run_finalization_available": True,
        "executor_available": True,
        "result_retention_seconds": 86_400,
    }


@pytest.mark.asyncio
async def test_infrastructure_can_be_up_while_product_not_ready():
    service = make_service(workers=[])

    result = await service.get_product_readiness()

    assert result["redis_reachable"] is True
    assert result["leader_available"] is True
    assert result["scheduler_running"] is True
    assert result["ready"] is False
    assert result["compatible_worker_count"] == 0


@pytest.mark.asyncio
async def test_incompatible_worker_does_not_make_product_ready():
    service = make_service(
        workers=[
            WorkerProbe(
                capabilities=[
                    "base",
                    "executor:deterministic",
                    "run-finalization:v1",
                ]
            )
        ]
    )

    result = await service.get_product_readiness()

    assert result["compatible_worker_count"] == 0
    assert result["executor_available"] is True
    assert result["run_finalization_available"] is True
    assert result["ready"] is False


@pytest.mark.asyncio
async def test_worker_without_executor_is_incompatible():
    service = make_service(
        workers=[
            WorkerProbe(
                capabilities=[
                    "product:single-task-v1",
                    "run-finalization:v1",
                ]
            )
        ]
    )

    result = await service.get_product_readiness()

    assert result["executor_available"] is False
    assert result["compatible_worker_count"] == 0
    assert result["ready"] is False


@pytest.mark.asyncio
async def test_redis_unavailable_is_sanitized_not_ready():
    secret = "redis://user:secret@private-host:6379"
    service = make_service(
        redis=RedisProbe(
            ping_error=RuntimeError(secret),
        ),
        workers=[compatible_worker()],
    )

    result = await service.get_product_readiness()

    assert result["redis_reachable"] is False
    assert result["leader_available"] is False
    assert result["ready"] is False
    assert secret not in repr(result)


@pytest.mark.asyncio
async def test_leader_unavailable_is_not_ready():
    service = make_service(
        redis=RedisProbe(leader_id=""),
        workers=[compatible_worker()],
    )

    result = await service.get_product_readiness()

    assert result["leader_available"] is False
    assert result["ready"] is False


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("is_leader", "sched_started", "scheduler_running"),
    [
        (False, True, True),
        (True, False, True),
        (True, True, False),
    ],
)
async def test_scheduler_must_be_running_under_local_leader(
    is_leader,
    sched_started,
    scheduler_running,
):
    service = make_service(
        workers=[compatible_worker()],
        is_leader=is_leader,
        sched_started=sched_started,
        scheduler_running=scheduler_running,
    )

    result = await service.get_product_readiness()

    assert result["scheduler_running"] is False
    assert result["ready"] is False


@pytest.mark.asyncio
async def test_registry_failure_is_sanitized_not_ready():
    secret = "worker registry connection secret"
    service = make_service(
        registry_failure=RuntimeError(secret),
    )

    result = await service.get_product_readiness()

    assert result["compatible_worker_count"] == 0
    assert result["ready"] is False
    assert secret not in repr(result)


@pytest.mark.asyncio
async def test_internal_profile_never_claims_alpha_ready():
    service = make_service(
        profile=internal_profile(),
        workers=[compatible_worker()],
    )

    result = await service.get_product_readiness()

    assert result["product_mode"] == "RUNTIME_INTERNAL"
    assert result["ready"] is False


@pytest.mark.asyncio
async def test_capability_contract_is_explicit_and_honest():
    service = make_service()

    result = await service.get_product_capabilities()

    assert result == {
        "supported_run_shapes": ["SINGLE_TASK"],
        "multi_task_result_supported": False,
        "cancel_supported": False,
        "retry_supported": False,
        "submission_idempotency_supported": True,
        "external_executor_cutover": False,
        "archive_available": False,
        "production_ready": False,
    }


class HTTPControlProbe:
    def __init__(self, readiness: dict) -> None:
        self.readiness = readiness

    async def get_product_readiness(self) -> dict:
        return dict(self.readiness)

    async def get_product_capabilities(self) -> dict:
        return {
            "supported_run_shapes": ["SINGLE_TASK"],
            "multi_task_result_supported": False,
            "cancel_supported": False,
            "retry_supported": False,
            "submission_idempotency_supported": True,
            "external_executor_cutover": False,
            "archive_available": False,
            "production_ready": False,
        }


def http_readiness(*, ready: bool) -> dict:
    return {
        "product_mode": "SINGLE_TASK_ALPHA",
        "ready": ready,
        "tenant_identity_boundary":
            "TRUSTED_GATEWAY_HEADER",
        "redis_reachable": True,
        "leader_available": True,
        "scheduler_running": True,
        "compatible_worker_count": 1 if ready else 0,
        "run_finalization_available": ready,
        "executor_available": ready,
        "result_retention_seconds": 86_400,
    }


@pytest.mark.asyncio
async def test_product_readiness_http_returns_200_when_ready():
    app = FastAPI()
    app.include_router(router)
    app.state.cp = HTTPControlProbe(
        http_readiness(ready=True)
    )

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://test",
    ) as client:
        response = await client.get(
            "/control/v1/product/readiness"
        )

    assert response.status_code == 200
    assert response.json()["ready"] is True


@pytest.mark.asyncio
async def test_product_readiness_http_returns_503_when_not_ready():
    app = FastAPI()
    app.include_router(router)
    app.state.cp = HTTPControlProbe(
        http_readiness(ready=False)
    )

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://test",
    ) as client:
        response = await client.get(
            "/control/v1/product/readiness"
        )

    assert response.status_code == 503
    body = response.json()
    assert body["ready"] is False
    assert body["compatible_worker_count"] == 0


@pytest.mark.asyncio
async def test_product_capabilities_http_is_read_only_contract():
    app = FastAPI()
    app.include_router(router)
    app.state.cp = HTTPControlProbe(
        http_readiness(ready=True)
    )

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://test",
    ) as client:
        first = await client.get(
            "/control/v1/product/capabilities"
        )
        second = await client.get(
            "/control/v1/product/capabilities"
        )

    assert first.status_code == 200
    assert second.status_code == 200
    assert first.json() == second.json()
    assert first.json()["production_ready"] is False
