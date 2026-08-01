from __future__ import annotations

import httpx
import pytest
from fastapi import FastAPI

from hfa_control.api.models import RunSubmissionRequest
from hfa_control.api.router import router
from hfa_control.run_submission import (
    RunShape,
    RunSubmissionCoordinator,
    SingleTaskRunSubmission,
)


class ForbiddenCall:
    def __init__(self, name: str) -> None:
        self.name = name
        self.calls = 0

    def __call__(self):
        self.calls += 1
        raise AssertionError(f"{self.name} must not be called")


class AdmissionProbe:
    def __init__(self) -> None:
        self.calls = 0

    async def admit(self, request):
        self.calls += 1
        raise AssertionError("RUN admission must not be called")


class DagProbe:
    def __init__(self) -> None:
        self.initialise_calls = 0
        self.task_admit_calls = 0

    async def initialise(self):
        self.initialise_calls += 1
        raise AssertionError("DAG initialise must not be called")

    async def task_admit(self, seed):
        self.task_admit_calls += 1
        raise AssertionError("TASK admission must not be called")


def coordinator_with_forbidden_side_effects():
    admission = AdmissionProbe()
    dag = DagProbe()
    uuid_factory = ForbiddenCall("uuid_factory")
    clock = ForbiddenCall("clock")
    coordinator = RunSubmissionCoordinator(
        admission_controller=admission,
        dag_lua=dag,
        uuid_factory=uuid_factory,
        clock_ms=clock,
    )
    return coordinator, admission, dag, uuid_factory, clock


@pytest.mark.parametrize(
    "run_shape",
    [
        "MULTI_TASK",
        "",
        "single_task",
    ],
)
@pytest.mark.asyncio
async def test_unsupported_shape_fails_before_every_side_effect(
    run_shape,
):
    (
        coordinator,
        admission,
        dag,
        uuid_factory,
        clock,
    ) = coordinator_with_forbidden_side_effects()

    result = await coordinator.submit(
        SingleTaskRunSubmission(
            tenant_id="tenant1",
            payload={"prompt": "must not be admitted"},
            run_shape=run_shape,
        )
    )

    assert result.status.value == "REJECTED"
    assert result.failure_code.value == (
        "UNSUPPORTED_RUN_SHAPE"
    )
    assert result.run_id == ""
    assert result.task_id == ""
    assert result.run_admitted is False
    assert result.task_admitted is False
    assert result.dispatch_possible is False
    assert result.automatic_retry is False
    assert result.automatic_rollback is False
    assert result.automatic_repair is False
    assert result.failure_type is None

    assert uuid_factory.calls == 0
    assert clock.calls == 0
    assert admission.calls == 0
    assert dag.initialise_calls == 0
    assert dag.task_admit_calls == 0


def test_run_shape_contract_contains_only_single_task():
    assert tuple(shape.value for shape in RunShape) == (
        "SINGLE_TASK",
    )


def test_http_request_defaults_to_single_task():
    request = RunSubmissionRequest()

    assert request.run_shape == "SINGLE_TASK"


def test_http_request_preserves_explicit_single_task():
    request = RunSubmissionRequest(
        run_shape="SINGLE_TASK",
        payload={"prompt": "hello"},
    )

    assert request.run_shape == "SINGLE_TASK"
    assert request.payload == {"prompt": "hello"}


class CoordinatorControlProbe:
    def __init__(self, coordinator) -> None:
        self.coordinator = coordinator
        self.requests = []

    async def submit_single_task_run(self, request):
        self.requests.append(request)
        return (
            await self.coordinator.submit(request)
        ).to_dict()


class AcceptingControlProbe:
    def __init__(self) -> None:
        self.requests = []

    async def submit_single_task_run(self, request):
        self.requests.append(request)
        return {
            "status": "ACCEPTED",
            "tenant_id": request.tenant_id,
            "run_id": f"run-{request.tenant_id}-accepted",
            "task_id": "task-accepted",
            "run_admitted": True,
            "task_admitted": True,
            "task_ready": True,
            "task_admit_status": "seeded_root",
            "failure_code": None,
            "failure_type": None,
            "dispatch_possible": True,
            "automatic_retry": False,
            "automatic_rollback": False,
            "automatic_repair": False,
        }


def app_with_control(cp):
    app = FastAPI()
    app.include_router(router)
    app.state.cp = cp
    return app


@pytest.mark.asyncio
async def test_http_multi_task_returns_400_and_zero_writes():
    (
        coordinator,
        admission,
        dag,
        uuid_factory,
        clock,
    ) = coordinator_with_forbidden_side_effects()
    cp = CoordinatorControlProbe(coordinator)

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(
            app=app_with_control(cp)
        ),
        base_url="http://test",
    ) as client:
        response = await client.post(
            "/control/v1/runs",
            headers={"X-Tenant-ID": "tenant1"},
            json={
                "run_shape": "MULTI_TASK",
                "payload": {"tasks": [1, 2]},
            },
        )

    assert response.status_code == 400
    body = response.json()
    assert body["status"] == "REJECTED"
    assert body["failure_code"] == (
        "UNSUPPORTED_RUN_SHAPE"
    )
    assert body["run_id"] == ""
    assert body["task_id"] == ""
    assert body["run_admitted"] is False
    assert body["task_admitted"] is False
    assert body["automatic_retry"] is False
    assert body["automatic_repair"] is False

    assert len(cp.requests) == 1
    assert cp.requests[0].run_shape == "MULTI_TASK"
    assert uuid_factory.calls == 0
    assert clock.calls == 0
    assert admission.calls == 0
    assert dag.initialise_calls == 0
    assert dag.task_admit_calls == 0


@pytest.mark.asyncio
async def test_http_omitted_shape_forwards_single_task():
    cp = AcceptingControlProbe()

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(
            app=app_with_control(cp)
        ),
        base_url="http://test",
    ) as client:
        response = await client.post(
            "/control/v1/runs",
            headers={"X-Tenant-ID": "tenant1"},
            json={"payload": {"prompt": "default shape"}},
        )

    assert response.status_code == 202
    assert len(cp.requests) == 1
    assert cp.requests[0].run_shape == "SINGLE_TASK"


@pytest.mark.asyncio
async def test_http_explicit_single_task_is_forwarded():
    cp = AcceptingControlProbe()

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(
            app=app_with_control(cp)
        ),
        base_url="http://test",
    ) as client:
        response = await client.post(
            "/control/v1/runs",
            headers={"X-Tenant-ID": "tenant1"},
            json={
                "run_shape": "SINGLE_TASK",
                "payload": {"prompt": "explicit shape"},
            },
        )

    assert response.status_code == 202
    assert len(cp.requests) == 1
    assert cp.requests[0].run_shape == "SINGLE_TASK"


@pytest.mark.asyncio
async def test_duplicate_unsupported_post_remains_zero_write():
    (
        coordinator,
        admission,
        dag,
        uuid_factory,
        clock,
    ) = coordinator_with_forbidden_side_effects()
    cp = CoordinatorControlProbe(coordinator)

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(
            app=app_with_control(cp)
        ),
        base_url="http://test",
    ) as client:
        first = await client.post(
            "/control/v1/runs",
            headers={"X-Tenant-ID": "tenant1"},
            json={"run_shape": "MULTI_TASK"},
        )
        second = await client.post(
            "/control/v1/runs",
            headers={"X-Tenant-ID": "tenant1"},
            json={"run_shape": "MULTI_TASK"},
        )

    assert first.status_code == 400
    assert second.status_code == 400
    assert first.json() == second.json()
    assert len(cp.requests) == 2
    assert uuid_factory.calls == 0
    assert clock.calls == 0
    assert admission.calls == 0
    assert dag.initialise_calls == 0
    assert dag.task_admit_calls == 0
