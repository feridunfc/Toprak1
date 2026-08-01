from __future__ import annotations

from dataclasses import dataclass
import json
from uuid import UUID

import pytest

from hfa_control.run_submission import (
    RunSubmissionCoordinator,
    RunSubmissionFailureCode,
    RunSubmissionStatus,
    SingleTaskRunSubmission,
)


RUN_UUID_1 = UUID(
    "11111111-1111-4111-8111-111111111111"
)
TASK_UUID_1 = UUID(
    "22222222-2222-4222-8222-222222222222"
)
RUN_UUID_2 = UUID(
    "33333333-3333-4333-8333-333333333333"
)
TASK_UUID_2 = UUID(
    "44444444-4444-4444-8444-444444444444"
)


class UUIDFactory:
    def __init__(self, *values: UUID) -> None:
        self._values = iter(values)

    def __call__(self) -> UUID:
        return next(self._values)


class AdmissionProbe:
    def __init__(
        self,
        *,
        outcome: object | None = None,
        error: Exception | None = None,
    ) -> None:
        self.outcome = outcome
        self.error = error
        self.calls: list[object] = []

    async def admit(self, request):
        self.calls.append(request)
        if self.error is not None:
            raise self.error
        if self.outcome is not None:
            return self.outcome
        return request.run_id


@dataclass(frozen=True)
class TaskAdmitProbeResult:
    admitted: bool
    ready: bool
    task_id: str
    status: str


class DagLuaProbe:
    def __init__(
        self,
        *,
        result: TaskAdmitProbeResult | None = None,
        initialise_error: Exception | None = None,
        admit_error: Exception | None = None,
    ) -> None:
        self.result = result
        self.initialise_error = initialise_error
        self.admit_error = admit_error
        self.initialise_calls = 0
        self.task_admit_calls: list[object] = []

    async def initialise(self) -> None:
        self.initialise_calls += 1
        if self.initialise_error is not None:
            raise self.initialise_error

    async def task_admit(self, seed):
        self.task_admit_calls.append(seed)
        if self.admit_error is not None:
            raise self.admit_error
        if self.result is not None:
            return self.result
        return TaskAdmitProbeResult(
            admitted=True,
            ready=True,
            task_id=seed.task_id,
            status="seeded_root",
        )


def request() -> SingleTaskRunSubmission:
    return SingleTaskRunSubmission(
        tenant_id="tenant1",
        payload={
            "prompt": "hello",
            "nested": {"b": 2, "a": 1},
        },
        agent_type="fake",
        priority=7,
        estimated_cost_cents=3,
        preferred_region="eu",
        preferred_placement="LEAST_LOADED",
        trace_parent="trace-parent",
        trace_state="trace-state",
    )


def coordinator(
    admission: AdmissionProbe,
    dag: DagLuaProbe,
    *,
    uuids: tuple[UUID, ...] = (
        RUN_UUID_1,
        TASK_UUID_1,
    ),
    clock_ms=lambda: 1_700_000_000_123,
) -> RunSubmissionCoordinator:
    return RunSubmissionCoordinator(
        admission_controller=admission,
        dag_lua=dag,
        uuid_factory=UUIDFactory(*uuids),
        clock_ms=clock_ms,
    )


@pytest.mark.asyncio
async def test_success_composes_run_then_root_task_admission():
    admission = AdmissionProbe()
    dag = DagLuaProbe()
    subject = coordinator(admission, dag)

    result = await subject.submit(request())

    assert result.status is RunSubmissionStatus.ACCEPTED
    assert result.accepted is True
    assert result.run_id == (
        "run-tenant1-"
        "11111111-1111-4111-8111-111111111111"
    )
    assert result.task_id == (
        "task-"
        "22222222-2222-4222-8222-222222222222"
    )
    assert result.run_admitted is True
    assert result.task_admitted is True
    assert result.task_ready is True
    assert result.task_admit_status == "seeded_root"
    assert result.dispatch_possible is True
    assert result.automatic_retry is False
    assert result.automatic_rollback is False
    assert result.automatic_repair is False

    assert dag.initialise_calls == 1
    assert len(admission.calls) == 1
    assert len(dag.task_admit_calls) == 1

    admitted = admission.calls[0]
    assert admitted.run_id == result.run_id
    assert admitted.tenant_id == "tenant1"
    assert admitted.agent_type == "fake"
    assert admitted.priority == 7
    assert admitted.payload == request().payload
    assert admitted.estimated_cost_cents == 3
    assert admitted.preferred_region == "eu"
    assert admitted.preferred_placement == (
        "LEAST_LOADED"
    )

    seed = dag.task_admit_calls[0]
    assert seed.task_id == result.task_id
    assert seed.run_id == result.run_id
    assert seed.tenant_id == "tenant1"
    assert seed.agent_type == "fake"
    assert seed.priority == 7
    assert seed.admitted_at == 1_700_000_000_123
    assert seed.dependency_count == 0
    assert seed.child_task_ids == ()
    assert seed.parent_task_ids == []
    assert seed.input_payload == request().payload
    assert seed.required_capabilities == []
    assert seed.region == "eu"
    assert seed.policy == "LEAST_LOADED"
    assert seed.trace_parent == "trace-parent"
    assert seed.trace_state == "trace-state"
    assert seed.payload_json == json.dumps(
        request().payload,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )


@pytest.mark.asyncio
async def test_initialisation_failure_blocks_run_admission():
    admission = AdmissionProbe()
    dag = DagLuaProbe(
        initialise_error=RuntimeError("load failed")
    )
    result = await coordinator(
        admission,
        dag,
    ).submit(request())

    assert result.status is RunSubmissionStatus.REJECTED
    assert result.failure_code is (
        RunSubmissionFailureCode
        .TASK_ADMIT_INITIALISATION_FAILED
    )
    assert result.failure_type == "RuntimeError"
    assert result.run_admitted is False
    assert admission.calls == []
    assert dag.task_admit_calls == []


@pytest.mark.asyncio
async def test_clock_failure_blocks_all_runtime_calls():
    admission = AdmissionProbe()
    dag = DagLuaProbe()
    result = await coordinator(
        admission,
        dag,
        clock_ms=lambda: 1.5,
    ).submit(request())

    assert result.status is RunSubmissionStatus.REJECTED
    assert result.failure_code is (
        RunSubmissionFailureCode.CLOCK_FAILED
    )
    assert admission.calls == []
    assert dag.initialise_calls == 0
    assert dag.task_admit_calls == []


@pytest.mark.asyncio
async def test_run_admission_exception_rejects_without_task():
    admission = AdmissionProbe(
        error=PermissionError("rejected")
    )
    dag = DagLuaProbe()
    result = await coordinator(
        admission,
        dag,
    ).submit(request())

    assert result.status is RunSubmissionStatus.REJECTED
    assert result.failure_code is (
        RunSubmissionFailureCode.RUN_ADMISSION_FAILED
    )
    assert result.failure_type == "PermissionError"
    assert result.run_admitted is False
    assert dag.task_admit_calls == []


@pytest.mark.asyncio
async def test_false_run_admission_rejects_without_task():
    admission = AdmissionProbe(outcome=False)
    dag = DagLuaProbe()
    result = await coordinator(
        admission,
        dag,
    ).submit(request())

    assert result.failure_code is (
        RunSubmissionFailureCode
        .RUN_ADMISSION_NOT_COMMITTED
    )
    assert result.run_admitted is False
    assert dag.task_admit_calls == []


@pytest.mark.asyncio
async def test_task_exception_returns_explicit_incomplete():
    admission = AdmissionProbe()
    dag = DagLuaProbe(
        admit_error=RuntimeError("task failed")
    )
    result = await coordinator(
        admission,
        dag,
    ).submit(request())

    assert result.status is (
        RunSubmissionStatus.SUBMISSION_INCOMPLETE
    )
    assert result.failure_code is (
        RunSubmissionFailureCode.TASK_ADMISSION_FAILED
    )
    assert result.run_admitted is True
    assert result.task_admitted is False
    assert result.dispatch_possible is False
    assert result.automatic_retry is False
    assert result.automatic_rollback is False
    assert result.automatic_repair is False


@pytest.mark.asyncio
async def test_already_exists_is_task_identity_collision():
    admission = AdmissionProbe()
    task_id = (
        "task-"
        "22222222-2222-4222-8222-222222222222"
    )
    dag = DagLuaProbe(
        result=TaskAdmitProbeResult(
            admitted=True,
            ready=False,
            task_id=task_id,
            status="already_exists",
        )
    )
    result = await coordinator(
        admission,
        dag,
    ).submit(request())

    assert result.status is (
        RunSubmissionStatus.SUBMISSION_INCOMPLETE
    )
    assert result.failure_code is (
        RunSubmissionFailureCode.TASK_ID_COLLISION
    )
    assert result.run_admitted is True
    assert result.dispatch_possible is False


@pytest.mark.asyncio
async def test_non_ready_root_task_is_incomplete():
    admission = AdmissionProbe()
    task_id = (
        "task-"
        "22222222-2222-4222-8222-222222222222"
    )
    dag = DagLuaProbe(
        result=TaskAdmitProbeResult(
            admitted=True,
            ready=False,
            task_id=task_id,
            status="seeded_waiting",
        )
    )
    result = await coordinator(
        admission,
        dag,
    ).submit(request())

    assert result.failure_code is (
        RunSubmissionFailureCode.TASK_NOT_READY
    )
    assert result.run_admitted is True
    assert result.task_admitted is True
    assert result.task_ready is False


@pytest.mark.asyncio
async def test_required_capabilities_fail_closed():
    admission = AdmissionProbe()
    dag = DagLuaProbe()
    unsupported = SingleTaskRunSubmission(
        tenant_id="tenant1",
        payload={"prompt": "hello"},
        required_capabilities=("gpu",),
    )
    result = await coordinator(
        admission,
        dag,
    ).submit(unsupported)

    assert result.failure_code is (
        RunSubmissionFailureCode.INVALID_REQUEST
    )
    assert admission.calls == []
    assert dag.initialise_calls == 0


@pytest.mark.asyncio
async def test_dag_initialises_once_across_submissions():
    admission = AdmissionProbe()
    dag = DagLuaProbe()
    subject = coordinator(
        admission,
        dag,
        uuids=(
            RUN_UUID_1,
            TASK_UUID_1,
            RUN_UUID_2,
            TASK_UUID_2,
        ),
    )

    first = await subject.submit(request())
    second = await subject.submit(request())

    assert first.accepted is True
    assert second.accepted is True
    assert dag.initialise_calls == 1
    assert len(admission.calls) == 2
    assert len(dag.task_admit_calls) == 2


def _patch_service_dependencies(
    monkeypatch,
    *,
    admission,
    scheduler,
):
    import hfa_control.service as service_module

    class Passive:
        def __init__(self, *args, **kwargs) -> None:
            pass

    monkeypatch.setattr(service_module, "LeaderElection", Passive)
    monkeypatch.setattr(service_module, "WorkerRegistry", Passive)
    monkeypatch.setattr(service_module, "ShardOwnershipManager", Passive)
    monkeypatch.setattr(
        service_module,
        "build_audit_logger",
        lambda redis: Passive(),
    )
    monkeypatch.setattr(
        service_module,
        "AdmissionController",
        lambda redis, config, audit=None: admission,
    )
    monkeypatch.setattr(
        service_module,
        "build_production_scheduler",
        lambda **kwargs: scheduler,
    )
    monkeypatch.setattr(service_module, "RecoveryService", Passive)
    monkeypatch.setattr(service_module, "RedisHealthMonitor", Passive)
    return service_module


@pytest.mark.asyncio
async def test_control_plane_service_binds_exact_scheduler_dag_lua(
    monkeypatch,
):
    from types import SimpleNamespace

    admission = AdmissionProbe()
    dag = DagLuaProbe()
    scheduler = SimpleNamespace(
        composition=SimpleNamespace(dag_lua=dag)
    )
    service_module = _patch_service_dependencies(
        monkeypatch,
        admission=admission,
        scheduler=scheduler,
    )
    config = service_module.ControlPlaneConfig(
        instance_id="cp-submit-test"
    )

    service = service_module.ControlPlaneService(
        object(),
        config,
    )

    assert service._scheduler is scheduler
    assert (
        service._run_submission._admission_controller
        is admission
    )
    assert service._run_submission._dag_lua is dag


@pytest.mark.asyncio
async def test_control_plane_service_submission_delegates_to_coordinator(
    monkeypatch,
):
    from types import SimpleNamespace

    admission = AdmissionProbe()
    dag = DagLuaProbe()
    scheduler = SimpleNamespace(
        composition=SimpleNamespace(dag_lua=dag)
    )
    service_module = _patch_service_dependencies(
        monkeypatch,
        admission=admission,
        scheduler=scheduler,
    )
    config = service_module.ControlPlaneConfig(
        instance_id="cp-submit-test"
    )
    service = service_module.ControlPlaneService(
        object(),
        config,
    )
    service._run_submission = coordinator(
        admission,
        dag,
    )

    result = await service.submit_single_task_run(
        request()
    )

    assert result["status"] == "ACCEPTED"
    assert result["run_admitted"] is True
    assert result["task_admitted"] is True
    assert result["task_ready"] is True
    assert result["dispatch_possible"] is True
    assert result["automatic_retry"] is False
    assert result["automatic_rollback"] is False
    assert result["automatic_repair"] is False


def test_control_plane_service_rejects_missing_scheduler_dag_lua(
    monkeypatch,
):
    from types import SimpleNamespace

    admission = AdmissionProbe()
    scheduler = SimpleNamespace(
        composition=SimpleNamespace(dag_lua=None)
    )
    service_module = _patch_service_dependencies(
        monkeypatch,
        admission=admission,
        scheduler=scheduler,
    )
    config = service_module.ControlPlaneConfig(
        instance_id="cp-submit-test"
    )

    with pytest.raises(
        RuntimeError,
        match="must expose dag_lua",
    ):
        service_module.ControlPlaneService(
            object(),
            config,
        )


class ControlPlaneApiProbe:
    def __init__(self, *, submission_result=None, status_result=None):
        self.submission_result = submission_result
        self.status_result = status_result
        self.submission_calls = []
        self.status_calls = []

    async def submit_single_task_run(self, value):
        self.submission_calls.append(value)
        return dict(self.submission_result)

    async def get_run_status_result(self, run_id):
        self.status_calls.append(run_id)
        return dict(self.status_result)


def api_request(cp):
    from types import SimpleNamespace

    return SimpleNamespace(
        app=SimpleNamespace(state=SimpleNamespace(cp=cp))
    )


def accepted_submission_payload():
    return {
        "status": "ACCEPTED",
        "tenant_id": "tenant1",
        "run_id": "run-tenant1-11111111-1111-4111-8111-111111111111",
        "task_id": "task-22222222-2222-4222-8222-222222222222",
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


def queued_status_payload():
    return {
        "schema_version": 1,
        "run_id": "run-tenant1-11111111-1111-4111-8111-111111111111",
        "status": "QUEUED",
        "terminal": False,
        "outcome": None,
        "result": None,
        "error": None,
        "submitted_at": 1.0,
        "started_at": None,
        "finished_at": None,
        "updated_at": 1.0,
        "freshness": "CURRENT",
        "completeness": "RUNNING_WITHOUT_RESULT",
        "completeness_reason": None,
        "internal_state": "admitted",
        "task_counts": {},
        "state_ttl_seconds": 100,
        "meta_ttl_seconds": 100,
        "result_ttl_seconds": -2,
        "issues": [],
    }


@pytest.mark.asyncio
async def test_http_submit_uses_header_tenant_and_returns_accepted():
    from hfa_control.api.models import RunSubmissionRequest
    from hfa_control.api.router import submit_run

    cp = ControlPlaneApiProbe(
        submission_result=accepted_submission_payload()
    )
    body = RunSubmissionRequest(
        payload={"prompt": "hello"},
        agent_type="fake",
        priority=5,
    )

    response = await submit_run(body, api_request(cp), "tenant1")

    assert response.status == "ACCEPTED"
    assert response.dispatch_possible is True
    submitted = cp.submission_calls[0]
    assert submitted.tenant_id == "tenant1"
    assert submitted.payload == {"prompt": "hello"}
    assert submitted.agent_type == "fake"


@pytest.mark.asyncio
async def test_http_submit_incomplete_returns_409():
    import json as json_module
    from fastapi.responses import JSONResponse
    from hfa_control.api.models import RunSubmissionRequest
    from hfa_control.api.router import submit_run

    data = accepted_submission_payload()
    data.update(
        {
            "status": "SUBMISSION_INCOMPLETE",
            "task_admitted": False,
            "task_ready": False,
            "task_admit_status": "",
            "failure_code": "TASK_ADMISSION_FAILED",
            "failure_type": "RuntimeError",
            "dispatch_possible": False,
        }
    )
    cp = ControlPlaneApiProbe(submission_result=data)

    response = await submit_run(
        RunSubmissionRequest(payload={"prompt": "hello"}),
        api_request(cp),
        "tenant1",
    )

    assert isinstance(response, JSONResponse)
    assert response.status_code == 409
    payload = json_module.loads(response.body)
    assert payload["status"] == "SUBMISSION_INCOMPLETE"
    assert payload["automatic_retry"] is False
    assert payload["automatic_rollback"] is False
    assert payload["automatic_repair"] is False


@pytest.mark.asyncio
async def test_http_combined_read_returns_typed_view():
    from hfa_control.api.router import run_status_result

    cp = ControlPlaneApiProbe(status_result=queued_status_payload())
    run_id = queued_status_payload()["run_id"]

    response = await run_status_result(
        run_id,
        api_request(cp),
        "tenant1",
    )

    assert response.run_id == run_id
    assert response.status == "QUEUED"
    assert response.terminal is False
    assert cp.status_calls == [run_id]


@pytest.mark.asyncio
async def test_http_combined_read_blocks_cross_tenant_before_read():
    from fastapi import HTTPException
    from hfa_control.api.router import run_status_result

    cp = ControlPlaneApiProbe(status_result=queued_status_payload())
    run_id = queued_status_payload()["run_id"]

    with pytest.raises(HTTPException) as raised:
        await run_status_result(
            run_id,
            api_request(cp),
            "tenant2",
        )

    assert raised.value.status_code == 403
    assert cp.status_calls == []


@pytest.mark.asyncio
async def test_http_combined_read_unknown_run_returns_404():
    from fastapi import HTTPException
    from hfa_control.api.router import run_status_result

    data = queued_status_payload()
    data.update(
        {
            "status": "UNKNOWN",
            "completeness": "UNKNOWN_RUN",
            "internal_state": None,
            "submitted_at": None,
            "updated_at": None,
            "freshness": "EXPIRED_OR_MISSING",
            "state_ttl_seconds": -2,
            "meta_ttl_seconds": -2,
            "result_ttl_seconds": -2,
        }
    )
    cp = ControlPlaneApiProbe(status_result=data)
    run_id = data["run_id"]

    with pytest.raises(HTTPException) as raised:
        await run_status_result(
            run_id,
            api_request(cp),
            "tenant1",
        )

    assert raised.value.status_code == 404
    assert cp.status_calls == [run_id]


def test_http_routes_are_registered_with_expected_methods():
    from hfa_control.api.router import router

    routes = {
        (
            getattr(route, "path", ""),
            frozenset(getattr(route, "methods", set())),
        ): route
        for route in router.routes
    }

    submit = routes[
        ("/control/v1/runs", frozenset({"POST"}))
    ]
    combined = routes[
        ("/control/v1/runs/{run_id}", frozenset({"GET"}))
    ]

    assert submit.status_code == 202
    assert submit.response_model.__name__ == "RunSubmissionResponse"
    assert combined.response_model.__name__ == "RunStatusResultResponse"
