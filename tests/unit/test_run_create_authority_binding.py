from __future__ import annotations

import hashlib
from dataclasses import dataclass
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from hfa.authority import (
    ReceiptProbe,
    RedisAuthorityCommitResult,
    RedisAuthorityCommitStatus,
)
from hfa.governance.admission_resource_reservation import (
    AdmissionResourceReservationReceipt,
    AdmissionResourceReservationResult,
    RESERVATION_STATE_FINALIZED,
    RESERVATION_STATE_RELEASED,
    RESERVATION_STATE_RESERVED,
    RESERVATION_STATUS_ALREADY_FINALIZED,
    RESERVATION_STATUS_FINALIZED,
    RESERVATION_STATUS_RELEASED,
    RESERVATION_STATUS_RESERVED,
)
from hfa_control.admission import AdmissionController
from hfa_control.run_create_authority import (
    RUN_CREATE_DUPLICATE_STATUS,
    RUN_CREATE_PROJECTED_STATUS,
    RunCreateAuthorityBinding,
    RunCreateProjectionPendingError,
    RunCreateProjectionResult,
)


@dataclass
class Request:
    run_id: str = "tenant-a:run-1"
    tenant_id: str = "tenant-a"
    agent_type: str = "research"
    priority: int = 5
    payload: dict = None
    estimated_cost_cents: int = 100
    preferred_region: str = ""
    preferred_placement: str = "LEAST_LOADED"

    def __post_init__(self):
        if self.payload is None:
            self.payload = {"prompt": "hello"}


class ResourceManager:
    def __init__(self, order):
        self.order = order
        self.state = None
        self.input = None
        self.release_calls = 0
        self.finalize_error = None
        self.reserve_override = None

    async def initialise(self):
        self.order.append("resource_initialise")

    async def reserve_once(self, value, **_limits):
        self.order.append("reserve")
        self.input = value
        if self.reserve_override is not None:
            return self.reserve_override
        if self.state is None:
            self.state = RESERVATION_STATE_RESERVED
            return AdmissionResourceReservationResult(
                status=RESERVATION_STATUS_RESERVED,
                operation_id=value.operation_id,
                proof_sha256=value.proof_sha256,
                resource_mutated=True,
                state=self.state,
            )
        return AdmissionResourceReservationResult(
            status=RESERVATION_STATUS_ALREADY_FINALIZED,
            operation_id=value.operation_id,
            proof_sha256=value.proof_sha256,
            resource_mutated=False,
            state=self.state,
        )

    async def get_receipt(self, value):
        self.order.append("get_resource_receipt")
        return AdmissionResourceReservationReceipt(
            operation_id=value.operation_id,
            run_id=value.run_id,
            tenant_id=value.tenant_id,
            estimated_cost_cents=value.estimated_cost_cents,
            proof_sha256=value.proof_sha256,
            reservation_version=value.reservation_version,
            state=self.state,
            created_at_ms=1234,
            finalized_at_ms=1240 if self.state == RESERVATION_STATE_FINALIZED else None,
            released_at_ms=None,
        )

    async def finalize_once(self, value):
        self.order.append("finalize")
        if self.finalize_error:
            raise self.finalize_error
        self.state = RESERVATION_STATE_FINALIZED
        return AdmissionResourceReservationResult(
            status=RESERVATION_STATUS_FINALIZED,
            operation_id=value.operation_id,
            proof_sha256=value.proof_sha256,
            resource_mutated=False,
            state=self.state,
        )

    async def release_once(self, value):
        self.order.append("release")
        self.release_calls += 1
        self.state = RESERVATION_STATE_RELEASED
        return AdmissionResourceReservationResult(
            status=RESERVATION_STATUS_RELEASED,
            operation_id=value.operation_id,
            proof_sha256=value.proof_sha256,
            resource_mutated=True,
            state=self.state,
        )


class Snapshot(SimpleNamespace):
    pass


class Keyspace:
    @staticmethod
    def operation_field(value):
        encoded = value.encode()
        return hashlib.sha256(len(encoded).to_bytes(8, "big") + encoded).hexdigest()


class Store:
    def __init__(self, order):
        self.order = order
        self.record = None
        self.receipt = None
        self.commit_mode = "success"

    async def initialise(self):
        self.order.append("store_initialise")

    async def get_aggregate_snapshot(self, _identity):
        if self.record is None:
            return None
        return Snapshot(
            operation_id=self.record.operation_id,
            transition_id=self.record.transition_id,
            canonical_command_hash=self.record.canonical_command_hash,
            canonical_record_hash=self.record.canonical_record_hash,
            revision=self.record.to_revision,
            state=self.record.next_state,
        )

    async def load_receipt_probe(self, _identity, _operation_id):
        if self.record is None:
            return None
        return ReceiptProbe(self.receipt, self.record)

    async def commit(self, plan):
        self.order.append("commit")
        if self.commit_mode == "absent_error":
            raise RuntimeError("connection lost before commit")
        self.record = plan.record
        self.receipt = plan.receipt
        if self.commit_mode == "durable_error":
            raise RuntimeError("response lost after commit")
        if self.commit_mode == "prevalidation_retry":
            self.record = None
            self.receipt = None
            return RedisAuthorityCommitResult(
                RedisAuthorityCommitStatus.PREVALIDATION_RETRY_REQUIRED,
                None,
                None,
                "proof unavailable",
            )
        return RedisAuthorityCommitResult(
            RedisAuthorityCommitStatus.COMMITTED,
            plan.record.transition_id,
            plan.record.to_revision,
        )

    async def validate_authority_head(self, *_args, **_kwargs):
        self.order.append("validate_head")
        return await self.get_aggregate_snapshot(None)

    def keyspace(self, _identity):
        return Keyspace()

    async def record_authority_conflict(self, *_args, **_kwargs):
        return RedisAuthorityCommitResult(
            RedisAuthorityCommitStatus.IDEMPOTENCY_CONFLICT,
            None,
            None,
        )


class Redis:
    async def exists(self, _key):
        return False

    async def type(self, _key):
        return "none"

    async def xrange(self, *_args, **_kwargs):
        return []


class Projection:
    def __init__(self, order):
        self.order = order
        self.mode = "first"
        self.calls = []

    async def initialise(self):
        self.order.append("projection_initialise")

    async def project(self, value):
        self.order.append("project")
        self.calls.append(value)
        if self.mode == "error":
            raise RuntimeError("projection unavailable")
        if self.mode == "duplicate":
            return RunCreateProjectionResult(
                RUN_CREATE_DUPLICATE_STATUS,
                "1-0",
                False,
            )
        return RunCreateProjectionResult(
            RUN_CREATE_PROJECTED_STATUS,
            "1-0",
            True,
        )


def subject():
    order = []
    resource = ResourceManager(order)
    store = Store(order)
    projection = Projection(order)
    binding = RunCreateAuthorityBinding(
        redis=Redis(),
        resource_manager=resource,
        control_stream="hfa:stream:control",
        store=store,
        projection_manager=projection,
    )
    return binding, resource, store, projection, order


@pytest.mark.asyncio
async def test_first_binding_order_and_stable_receipt_timestamp():
    binding, resource, store, projection, order = subject()
    result = await binding.admit(Request(), tenant_inflight_limit=5)
    assert result.status == RUN_CREATE_PROJECTED_STATUS
    assert order.index("reserve") < order.index("commit")
    assert order.index("commit") < order.index("finalize")
    assert order.index("finalize") < order.index("project")
    assert store.record.committed_at_ms == 1234
    assert store.record.authoritative_metadata_changes["created_at_ms"] == 1234
    assert projection.calls[0].reservation_proof_sha256 == resource.input.proof_sha256
    assert resource.release_calls == 0


@pytest.mark.asyncio
async def test_exact_retry_reuses_canonical_record_and_does_not_release():
    binding, resource, store, projection, _order = subject()
    first = await binding.admit(Request(), tenant_inflight_limit=5)
    projection.mode = "duplicate"
    second = await binding.admit(Request(), tenant_inflight_limit=5)
    assert first.first_projection is True
    assert second.status == RUN_CREATE_DUPLICATE_STATUS
    assert second.first_projection is False
    assert resource.release_calls == 0


@pytest.mark.asyncio
async def test_exact_retry_uses_committed_projection_config_after_runtime_drift():
    binding, resource, _store, projection, _order = subject()
    first = await binding.admit(Request(), tenant_inflight_limit=5)
    assert first.status == RUN_CREATE_PROJECTED_STATUS
    binding.control_stream = "hfa:stream:changed-after-commit"
    binding.run_state_ttl_seconds = 999
    projection.mode = "duplicate"
    retry = await binding.admit(Request(), tenant_inflight_limit=5)
    assert retry.status == RUN_CREATE_DUPLICATE_STATUS
    assert projection.calls[-1].control_stream == "hfa:stream:control"
    assert projection.calls[-1].state_ttl_seconds != 999
    assert resource.release_calls == 0


@pytest.mark.asyncio
async def test_invalid_limit_fails_before_initialisation_or_redis_touch():
    binding, _resource, _store, _projection, order = subject()
    with pytest.raises(ValueError, match="tenant_inflight_limit"):
        await binding.admit(Request(), tenant_inflight_limit=-1)
    assert order == []


@pytest.mark.asyncio
async def test_inconsistent_reservation_result_fails_closed():
    binding, resource, _store, _projection, _order = subject()
    operation_id = "run-create:v1:" + "a" * 64
    resource.reserve_override = AdmissionResourceReservationResult(
        status=RESERVATION_STATUS_RESERVED,
        operation_id=operation_id,
        proof_sha256="b" * 64,
        resource_mutated=False,
        state=RESERVATION_STATE_RESERVED,
    )
    with pytest.raises(Exception, match="status/state/mutation"):
        await binding.admit(Request(), tenant_inflight_limit=5)
    assert resource.release_calls == 0


@pytest.mark.asyncio
async def test_nondefinitive_commit_status_never_releases_resources():
    binding, resource, store, _projection, _order = subject()
    store.commit_mode = "prevalidation_retry"
    with pytest.raises(Exception, match="PREVALIDATION_RETRY_REQUIRED"):
        await binding.admit(Request(), tenant_inflight_limit=5)
    assert resource.release_calls == 0


@pytest.mark.asyncio
async def test_ambiguous_durable_commit_continues_without_release():
    binding, resource, store, _projection, _order = subject()
    store.commit_mode = "durable_error"
    result = await binding.admit(Request(), tenant_inflight_limit=5)
    assert result.status == RUN_CREATE_PROJECTED_STATUS
    assert resource.release_calls == 0


@pytest.mark.asyncio
async def test_proven_absent_commit_failure_releases_new_reservation():
    binding, resource, store, _projection, _order = subject()
    store.commit_mode = "absent_error"
    with pytest.raises(Exception, match="absence was proven"):
        await binding.admit(Request(), tenant_inflight_limit=5)
    assert resource.release_calls == 1


@pytest.mark.asyncio
async def test_finalize_failure_after_durable_commit_never_releases_or_projects():
    binding, resource, _store, projection, _order = subject()
    resource.finalize_error = RuntimeError("finalize down")
    with pytest.raises(RunCreateProjectionPendingError) as caught:
        await binding.admit(Request(), tenant_inflight_limit=5)
    assert caught.value.canonical_commit_durable is True
    assert resource.release_calls == 0
    assert projection.calls == []


@pytest.mark.asyncio
async def test_projection_failure_after_durable_commit_never_releases():
    binding, resource, _store, projection, _order = subject()
    projection.mode = "error"
    with pytest.raises(RunCreateProjectionPendingError):
        await binding.admit(Request(), tenant_inflight_limit=5)
    assert resource.release_calls == 0


@pytest.mark.asyncio
async def test_admission_default_disabled_never_touches_canonical_binding(monkeypatch):
    controller = AdmissionController(object(), SimpleNamespace())
    legacy = AsyncMock(return_value="legacy-run")
    controller._admit_legacy = legacy
    result = await controller.admit(Request())
    assert result == "legacy-run"
    legacy.assert_awaited_once()


def test_enabled_controller_requires_binding():
    with pytest.raises(ValueError, match="requires run_create_authority"):
        AdmissionController(
            object(),
            SimpleNamespace(),
            canonical_run_create_binding=True,
        )


@pytest.mark.asyncio
async def test_invalid_request_fails_before_initialisation_or_redis_touch():
    binding, _resource, _store, _projection, order = subject()
    bad = Request(priority=True)
    with pytest.raises(ValueError, match="priority"):
        await binding.admit(bad, tenant_inflight_limit=5)
    assert order == []


@pytest.mark.asyncio
async def test_canonical_controller_never_calls_legacy_resource_mutators():
    valid_run_id = "run-tenant1-123e4567-e89b-42d3-a456-426614174000"
    request = Request(run_id=valid_run_id, tenant_id="tenant1")

    class Registry:
        async def get_config(self, _tenant):
            return SimpleNamespace(
                max_inflight_runs=5,
                max_runs_per_second=None,
            )
        async def increment_inflight(self, _tenant):
            raise AssertionError("legacy increment_inflight must not be called")
        async def decrement_inflight(self, _tenant):
            raise AssertionError("legacy decrement_inflight must not be called")

    authority = SimpleNamespace(
        admit=AsyncMock(
            return_value=SimpleNamespace(
                status=RUN_CREATE_PROJECTED_STATUS,
                run_id=valid_run_id,
                first_projection=True,
            )
        )
    )
    limiter = SimpleNamespace(check_and_consume=AsyncMock())
    audit = SimpleNamespace(
        admitted=AsyncMock(side_effect=RuntimeError("audit unavailable"))
    )
    controller = AdmissionController(
        object(),
        SimpleNamespace(control_stream="hfa:stream:control"),
        tenant_registry=Registry(),
        rate_limiter=limiter,
        audit=audit,
        canonical_run_create_binding=True,
        run_create_authority=authority,
    )
    forbidden_quota = SimpleNamespace(
        check_and_increment_runs=AsyncMock(
            side_effect=AssertionError("legacy concurrent mutation")
        ),
        check_rate_limit=AsyncMock(
            side_effect=AssertionError("legacy system rate mutation")
        ),
        check_and_reserve_budget=AsyncMock(
            side_effect=AssertionError("legacy budget mutation")
        ),
        decrement_runs=AsyncMock(
            side_effect=AssertionError("legacy rollback mutation")
        ),
    )
    controller._quota = forbidden_quota
    assert await controller.admit(request) == valid_run_id
    authority.admit.assert_awaited_once()
    limiter.check_and_consume.assert_not_awaited()
    audit.admitted.assert_awaited_once()
    forbidden_quota.check_and_increment_runs.assert_not_awaited()
    forbidden_quota.check_rate_limit.assert_not_awaited()
    forbidden_quota.check_and_reserve_budget.assert_not_awaited()
    forbidden_quota.decrement_runs.assert_not_awaited()


@pytest.mark.asyncio
async def test_run_submission_reports_durable_projection_pending_without_task_admit():
    from hfa_control.run_submission import (
        RunSubmissionCoordinator,
        RunSubmissionFailureCode,
        RunSubmissionStatus,
        SingleTaskRunSubmission,
    )

    class Admission:
        async def admit(self, _request):
            raise RunCreateProjectionPendingError("projection pending")

    dag = SimpleNamespace(
        initialise=AsyncMock(),
        task_admit=AsyncMock(),
    )
    coordinator = RunSubmissionCoordinator(
        admission_controller=Admission(),
        dag_lua=dag,
        uuid_factory=lambda: __import__("uuid").UUID(
            "123e4567-e89b-42d3-a456-426614174000"
        ),
        clock_ms=lambda: 1234,
    )
    result = await coordinator.submit(
        SingleTaskRunSubmission(
            tenant_id="tenant1",
            payload={"prompt": "hello"},
        )
    )
    assert result.status is RunSubmissionStatus.SUBMISSION_INCOMPLETE
    assert result.failure_code is (
        RunSubmissionFailureCode.RUN_ADMISSION_PROJECTION_PENDING
    )
    assert result.run_admitted is True
    assert result.task_admitted is False
    assert result.task_ready is False
    assert result.dispatch_possible is False
    dag.task_admit.assert_not_awaited()


def test_service_rejects_run_create_without_task_admit(monkeypatch):
    import hfa_control.service as service_module

    monkeypatch.setenv("HFA_CANONICAL_RUN_CREATE_BINDING", "true")
    monkeypatch.setenv("HFA_CANONICAL_TASK_ADMIT_BINDING", "false")
    with pytest.raises(ValueError, match="requires canonical TASK_ADMIT"):
        service_module.ControlPlaneService(
            object(),
            service_module.ControlPlaneConfig(instance_id="cp-test"),
        )


def _patch_service_shell(monkeypatch, service_module):
    class Passive:
        def __init__(self, *args, **kwargs):
            pass

    scheduler = SimpleNamespace(composition=SimpleNamespace(dag_lua=object()))
    monkeypatch.setattr(service_module, "LeaderElection", Passive)
    monkeypatch.setattr(service_module, "WorkerRegistry", Passive)
    monkeypatch.setattr(service_module, "ShardOwnershipManager", Passive)
    monkeypatch.setattr(service_module, "build_audit_logger", lambda _redis: Passive())
    monkeypatch.setattr(
        service_module,
        "build_production_scheduler",
        lambda **_kwargs: scheduler,
    )
    monkeypatch.setattr(service_module, "RecoveryService", Passive)
    monkeypatch.setattr(service_module, "RedisHealthMonitor", Passive)
    return scheduler


def test_service_disabled_does_not_construct_run_create_dependencies(monkeypatch):
    import hfa_control.service as service_module

    monkeypatch.setenv("HFA_CANONICAL_RUN_CREATE_BINDING", "false")
    monkeypatch.setenv("HFA_CANONICAL_TASK_ADMIT_BINDING", "false")
    _patch_service_shell(monkeypatch, service_module)
    monkeypatch.setattr(
        service_module,
        "AdmissionResourceReservationManager",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("resource manager must not be constructed")
        ),
    )
    captured = {}

    def admission(*args, **kwargs):
        captured["args"] = args
        captured["kwargs"] = kwargs
        return SimpleNamespace()

    monkeypatch.setattr(service_module, "AdmissionController", admission)
    service_module.ControlPlaneService(
        object(),
        service_module.ControlPlaneConfig(instance_id="cp-disabled"),
    )
    assert captured["kwargs"] == {"audit": captured["kwargs"]["audit"]}
    assert "canonical_run_create_binding" not in captured["kwargs"]


def test_service_enabled_constructs_and_injects_exact_binding(monkeypatch):
    import hfa_control.service as service_module

    monkeypatch.setenv("HFA_CANONICAL_RUN_CREATE_BINDING", "true")
    monkeypatch.setenv("HFA_CANONICAL_TASK_ADMIT_BINDING", "true")
    _patch_service_shell(monkeypatch, service_module)
    registry = object()
    limiter = object()
    resource = object()
    authority = object()
    monkeypatch.setattr(service_module, "TenantRegistry", lambda _redis: registry)
    monkeypatch.setattr(service_module, "TenantRateLimiter", lambda _redis: limiter)
    monkeypatch.setattr(
        service_module,
        "AdmissionResourceReservationManager",
        lambda _redis: resource,
    )
    authority_args = {}

    def authority_factory(**kwargs):
        authority_args.update(kwargs)
        return authority

    monkeypatch.setattr(service_module, "RunCreateAuthorityBinding", authority_factory)
    captured = {}

    def admission(*args, **kwargs):
        captured["args"] = args
        captured["kwargs"] = kwargs
        return SimpleNamespace()

    monkeypatch.setattr(service_module, "AdmissionController", admission)
    config = service_module.ControlPlaneConfig(instance_id="cp-enabled")
    service_module.ControlPlaneService(object(), config)
    assert authority_args["resource_manager"] is resource
    assert authority_args["control_stream"] == config.control_stream
    assert captured["kwargs"]["tenant_registry"] is registry
    assert captured["kwargs"]["rate_limiter"] is limiter
    assert captured["kwargs"]["canonical_run_create_binding"] is True
    assert captured["kwargs"]["run_create_authority"] is authority
