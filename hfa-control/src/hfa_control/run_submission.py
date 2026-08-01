"""Canonical single-task RUN submission coordination for Sprint 83.5."""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
import json
import time
from typing import Any, Callable, Mapping
from uuid import UUID, uuid4

from hfa.dag.schema import DagTaskSeed


class RunSubmissionStatus(str, Enum):
    ACCEPTED = "ACCEPTED"
    REJECTED = "REJECTED"
    SUBMISSION_INCOMPLETE = "SUBMISSION_INCOMPLETE"


class RunShape(str, Enum):
    SINGLE_TASK = "SINGLE_TASK"


class UnsupportedRunShapeError(ValueError):
    pass


class RunSubmissionFailureCode(str, Enum):
    INVALID_REQUEST = "INVALID_REQUEST"
    UNSUPPORTED_RUN_SHAPE = "UNSUPPORTED_RUN_SHAPE"
    ID_GENERATION_FAILED = "ID_GENERATION_FAILED"
    CLOCK_FAILED = "CLOCK_FAILED"
    TASK_ADMIT_INITIALISATION_FAILED = "TASK_ADMIT_INITIALISATION_FAILED"
    RUN_ADMISSION_FAILED = "RUN_ADMISSION_FAILED"
    RUN_ADMISSION_NOT_COMMITTED = "RUN_ADMISSION_NOT_COMMITTED"
    RUN_ADMISSION_IDENTITY_MISMATCH = "RUN_ADMISSION_IDENTITY_MISMATCH"
    TASK_ADMISSION_FAILED = "TASK_ADMISSION_FAILED"
    TASK_ADMISSION_NOT_COMMITTED = "TASK_ADMISSION_NOT_COMMITTED"
    TASK_ID_COLLISION = "TASK_ID_COLLISION"
    TASK_NOT_READY = "TASK_NOT_READY"


@dataclass(frozen=True)
class SingleTaskRunSubmission:
    tenant_id: str
    payload: Mapping[str, Any]
    run_shape: str = RunShape.SINGLE_TASK.value
    agent_type: str = "default"
    priority: int = 5
    estimated_cost_cents: int = 0
    preferred_region: str = ""
    preferred_placement: str = "LEAST_LOADED"
    required_capabilities: tuple[str, ...] = ()
    trace_parent: str = ""
    trace_state: str = ""


@dataclass(frozen=True)
class RunSubmissionResult:
    status: RunSubmissionStatus
    tenant_id: str
    run_id: str
    task_id: str
    run_admitted: bool
    task_admitted: bool
    task_ready: bool
    task_admit_status: str = ""
    failure_code: RunSubmissionFailureCode | None = None
    failure_type: str | None = None
    dispatch_possible: bool = False
    automatic_retry: bool = False
    automatic_rollback: bool = False
    automatic_repair: bool = False

    @property
    def accepted(self) -> bool:
        return self.status is RunSubmissionStatus.ACCEPTED

    def to_dict(self) -> dict[str, Any]:
        return {
            "status": self.status.value,
            "tenant_id": self.tenant_id,
            "run_id": self.run_id,
            "task_id": self.task_id,
            "run_admitted": self.run_admitted,
            "task_admitted": self.task_admitted,
            "task_ready": self.task_ready,
            "task_admit_status": self.task_admit_status,
            "failure_code": (
                self.failure_code.value
                if self.failure_code is not None
                else None
            ),
            "failure_type": self.failure_type,
            "dispatch_possible": self.dispatch_possible,
            "automatic_retry": self.automatic_retry,
            "automatic_rollback": self.automatic_rollback,
            "automatic_repair": self.automatic_repair,
        }


@dataclass(frozen=True)
class _RunAdmissionRequest:
    run_id: str
    tenant_id: str
    agent_type: str
    priority: int
    payload: dict[str, Any]
    estimated_cost_cents: int
    preferred_region: str
    preferred_placement: str


class RunSubmissionCoordinator:
    """Compose existing RUN admission and TASK_ADMIT authorities.

    Commit order is deliberately non-atomic:

        RUN admission
        -> root TASK admission

    A TASK admission failure after successful RUN admission is returned as
    SUBMISSION_INCOMPLETE. This coordinator does not retry, roll back, repair,
    dispatch, or write lifecycle state directly.
    """

    def __init__(
        self,
        *,
        admission_controller: Any,
        dag_lua: Any,
        uuid_factory: Callable[[], UUID] = uuid4,
        clock_ms: Callable[[], int] | None = None,
    ) -> None:
        self._admission_controller = admission_controller
        self._dag_lua = dag_lua
        self._uuid_factory = uuid_factory
        self._clock_ms = clock_ms or (
            lambda: int(time.time() * 1000)
        )
        self._initialised = False

    async def initialise(self) -> None:
        if self._initialised:
            return
        initialise = getattr(self._dag_lua, "initialise", None)
        if not callable(initialise):
            raise TypeError("dag_lua.initialise must be callable")
        await initialise()
        self._initialised = True

    async def submit(
        self,
        request: SingleTaskRunSubmission,
    ) -> RunSubmissionResult:
        try:
            normalized = self._normalize_request(request)
        except UnsupportedRunShapeError:
            return self._failure(
                status=RunSubmissionStatus.REJECTED,
                tenant_id=self._safe_tenant_id(request),
                run_id="",
                task_id="",
                run_admitted=False,
                code=(
                    RunSubmissionFailureCode
                    .UNSUPPORTED_RUN_SHAPE
                ),
            )
        except Exception as exc:
            return self._failure(
                status=RunSubmissionStatus.REJECTED,
                tenant_id=self._safe_tenant_id(request),
                run_id="",
                task_id="",
                run_admitted=False,
                code=RunSubmissionFailureCode.INVALID_REQUEST,
                exc=exc,
            )

        tenant_id = normalized.tenant_id

        try:
            run_id = self._new_run_id(tenant_id)
            task_id = self._new_task_id()
        except Exception as exc:
            return self._failure(
                status=RunSubmissionStatus.REJECTED,
                tenant_id=tenant_id,
                run_id="",
                task_id="",
                run_admitted=False,
                code=RunSubmissionFailureCode.ID_GENERATION_FAILED,
                exc=exc,
            )

        try:
            admitted_at_ms = self._exact_clock_ms()
        except Exception as exc:
            return self._failure(
                status=RunSubmissionStatus.REJECTED,
                tenant_id=tenant_id,
                run_id=run_id,
                task_id=task_id,
                run_admitted=False,
                code=RunSubmissionFailureCode.CLOCK_FAILED,
                exc=exc,
            )

        try:
            await self.initialise()
        except Exception as exc:
            return self._failure(
                status=RunSubmissionStatus.REJECTED,
                tenant_id=tenant_id,
                run_id=run_id,
                task_id=task_id,
                run_admitted=False,
                code=(
                    RunSubmissionFailureCode
                    .TASK_ADMIT_INITIALISATION_FAILED
                ),
                exc=exc,
            )

        payload = dict(normalized.payload)
        admission_request = _RunAdmissionRequest(
            run_id=run_id,
            tenant_id=tenant_id,
            agent_type=normalized.agent_type,
            priority=normalized.priority,
            payload=payload,
            estimated_cost_cents=(
                normalized.estimated_cost_cents
            ),
            preferred_region=normalized.preferred_region,
            preferred_placement=(
                normalized.preferred_placement
            ),
        )

        try:
            admitted_run_id = (
                await self._admission_controller.admit(
                    admission_request
                )
            )
        except Exception as exc:
            return self._failure(
                status=RunSubmissionStatus.REJECTED,
                tenant_id=tenant_id,
                run_id=run_id,
                task_id=task_id,
                run_admitted=False,
                code=(
                    RunSubmissionFailureCode
                    .RUN_ADMISSION_FAILED
                ),
                exc=exc,
            )

        if not admitted_run_id:
            return self._failure(
                status=RunSubmissionStatus.REJECTED,
                tenant_id=tenant_id,
                run_id=run_id,
                task_id=task_id,
                run_admitted=False,
                code=(
                    RunSubmissionFailureCode
                    .RUN_ADMISSION_NOT_COMMITTED
                ),
            )

        if str(admitted_run_id) != run_id:
            return self._failure(
                status=RunSubmissionStatus.REJECTED,
                tenant_id=tenant_id,
                run_id=run_id,
                task_id=task_id,
                run_admitted=False,
                code=(
                    RunSubmissionFailureCode
                    .RUN_ADMISSION_IDENTITY_MISMATCH
                ),
            )

        payload_json = json.dumps(
            payload,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )
        task_seed = DagTaskSeed(
            task_id=task_id,
            run_id=run_id,
            tenant_id=tenant_id,
            agent_type=normalized.agent_type,
            priority=normalized.priority,
            admitted_at=admitted_at_ms,
            dependency_count=0,
            child_task_ids=(),
            input_payload=payload,
            required_capabilities=[],
            region=normalized.preferred_region,
            policy=normalized.preferred_placement,
            payload_json=payload_json,
            trace_parent=normalized.trace_parent,
            trace_state=normalized.trace_state,
            parent_task_ids=[],
        )

        try:
            task_result = await self._dag_lua.task_admit(
                task_seed
            )
        except Exception as exc:
            return self._failure(
                status=(
                    RunSubmissionStatus
                    .SUBMISSION_INCOMPLETE
                ),
                tenant_id=tenant_id,
                run_id=run_id,
                task_id=task_id,
                run_admitted=True,
                code=(
                    RunSubmissionFailureCode
                    .TASK_ADMISSION_FAILED
                ),
                exc=exc,
            )

        task_status = str(
            getattr(task_result, "status", "") or ""
        )
        task_admitted = bool(
            getattr(task_result, "admitted", False)
        )
        task_ready = bool(
            getattr(task_result, "ready", False)
        )
        returned_task_id = str(
            getattr(task_result, "task_id", "") or ""
        )

        if returned_task_id != task_id:
            return self._failure(
                status=(
                    RunSubmissionStatus
                    .SUBMISSION_INCOMPLETE
                ),
                tenant_id=tenant_id,
                run_id=run_id,
                task_id=task_id,
                run_admitted=True,
                code=(
                    RunSubmissionFailureCode
                    .TASK_ADMISSION_NOT_COMMITTED
                ),
                task_admitted=task_admitted,
                task_ready=task_ready,
                task_admit_status=task_status,
            )

        if task_status == "already_exists":
            return self._failure(
                status=(
                    RunSubmissionStatus
                    .SUBMISSION_INCOMPLETE
                ),
                tenant_id=tenant_id,
                run_id=run_id,
                task_id=task_id,
                run_admitted=True,
                code=(
                    RunSubmissionFailureCode
                    .TASK_ID_COLLISION
                ),
                task_admitted=task_admitted,
                task_ready=task_ready,
                task_admit_status=task_status,
            )

        if not task_admitted:
            return self._failure(
                status=(
                    RunSubmissionStatus
                    .SUBMISSION_INCOMPLETE
                ),
                tenant_id=tenant_id,
                run_id=run_id,
                task_id=task_id,
                run_admitted=True,
                code=(
                    RunSubmissionFailureCode
                    .TASK_ADMISSION_NOT_COMMITTED
                ),
                task_admitted=False,
                task_ready=task_ready,
                task_admit_status=task_status,
            )

        if not task_ready or task_status != "seeded_root":
            return self._failure(
                status=(
                    RunSubmissionStatus
                    .SUBMISSION_INCOMPLETE
                ),
                tenant_id=tenant_id,
                run_id=run_id,
                task_id=task_id,
                run_admitted=True,
                code=(
                    RunSubmissionFailureCode
                    .TASK_NOT_READY
                ),
                task_admitted=task_admitted,
                task_ready=task_ready,
                task_admit_status=task_status,
            )

        return RunSubmissionResult(
            status=RunSubmissionStatus.ACCEPTED,
            tenant_id=tenant_id,
            run_id=run_id,
            task_id=task_id,
            run_admitted=True,
            task_admitted=True,
            task_ready=True,
            task_admit_status=task_status,
            dispatch_possible=True,
        )

    @staticmethod
    def _safe_tenant_id(request: Any) -> str:
        return str(
            getattr(request, "tenant_id", "") or ""
        ).strip()

    @staticmethod
    def _normalize_request(
        request: SingleTaskRunSubmission,
    ) -> SingleTaskRunSubmission:
        if not isinstance(
            request,
            SingleTaskRunSubmission,
        ):
            raise TypeError(
                "request must be SingleTaskRunSubmission"
            )

        run_shape = str(
            request.run_shape or ""
        ).strip()
        if run_shape != RunShape.SINGLE_TASK.value:
            raise UnsupportedRunShapeError(
                "only SINGLE_TASK is supported"
            )

        tenant_id = str(request.tenant_id or "").strip()
        if not tenant_id:
            raise ValueError(
                "tenant_id must be a non-empty string"
            )
        if tenant_id != request.tenant_id:
            raise ValueError(
                "tenant_id must not contain surrounding whitespace"
            )

        agent_type = str(
            request.agent_type or ""
        ).strip()
        if not agent_type:
            raise ValueError(
                "agent_type must be a non-empty string"
            )

        if (
            type(request.priority) is not int
            or request.priority < 0
        ):
            raise ValueError(
                "priority must be a non-negative int"
            )

        if (
            type(request.estimated_cost_cents) is not int
            or request.estimated_cost_cents < 0
        ):
            raise ValueError(
                "estimated_cost_cents must be a non-negative int"
            )

        if not isinstance(request.payload, Mapping):
            raise TypeError("payload must be a mapping")
        payload = dict(request.payload)
        json.dumps(
            payload,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )

        capabilities = tuple(
            str(value).strip()
            for value in request.required_capabilities
            if str(value).strip()
        )
        if capabilities:
            raise ValueError(
                "required_capabilities are not yet persisted "
                "by the canonical TASK_ADMIT projection"
            )

        placement = str(
            request.preferred_placement or ""
        ).strip()
        if not placement:
            raise ValueError(
                "preferred_placement must be non-empty"
            )

        return SingleTaskRunSubmission(
            tenant_id=tenant_id,
            payload=payload,
            run_shape=RunShape.SINGLE_TASK.value,
            agent_type=agent_type,
            priority=request.priority,
            estimated_cost_cents=(
                request.estimated_cost_cents
            ),
            preferred_region=str(
                request.preferred_region or ""
            ).strip(),
            preferred_placement=placement,
            required_capabilities=(),
            trace_parent=str(
                request.trace_parent or ""
            ).strip(),
            trace_state=str(
                request.trace_state or ""
            ).strip(),
        )

    def _new_run_id(self, tenant_id: str) -> str:
        return f"run-{tenant_id}-{self._next_uuid()}"

    def _new_task_id(self) -> str:
        return f"task-{self._next_uuid()}"

    def _next_uuid(self) -> UUID:
        value = self._uuid_factory()
        if not isinstance(value, UUID):
            raise TypeError(
                "uuid_factory must return uuid.UUID"
            )
        if value.version != 4:
            raise ValueError(
                "uuid_factory must return UUIDv4"
            )
        if value.int == 0:
            raise ValueError(
                "uuid_factory must not return nil UUID"
            )
        return value

    def _exact_clock_ms(self) -> int:
        value = self._clock_ms()
        if type(value) is not int or value < 0:
            raise ValueError(
                "clock_ms must return a non-negative int"
            )
        return value

    @staticmethod
    def _failure(
        *,
        status: RunSubmissionStatus,
        tenant_id: str,
        run_id: str,
        task_id: str,
        run_admitted: bool,
        code: RunSubmissionFailureCode,
        exc: Exception | None = None,
        task_admitted: bool = False,
        task_ready: bool = False,
        task_admit_status: str = "",
    ) -> RunSubmissionResult:
        return RunSubmissionResult(
            status=status,
            tenant_id=tenant_id,
            run_id=run_id,
            task_id=task_id,
            run_admitted=run_admitted,
            task_admitted=task_admitted,
            task_ready=task_ready,
            task_admit_status=task_admit_status,
            failure_code=code,
            failure_type=(
                type(exc).__name__
                if exc is not None
                else None
            ),
            dispatch_possible=False,
            automatic_retry=False,
            automatic_rollback=False,
            automatic_repair=False,
        )
