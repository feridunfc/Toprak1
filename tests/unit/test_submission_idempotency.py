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
from hfa_control.submission_idempotency import (
    IdempotencyReservation,
    IdempotencyReservationStatus,
    SubmissionIdempotencyError,
    SubmissionIdempotencyStore,
    canonical_submission_fingerprint,
    idempotency_key_digest,
    validate_idempotency_key,
)


RUN_UUID = UUID(
    "11111111-1111-4111-8111-111111111111"
)
TASK_UUID = UUID(
    "22222222-2222-4222-8222-222222222222"
)
OWNER_UUID = UUID(
    "33333333-3333-4333-8333-333333333333"
)


class UUIDFactory:
    def __init__(self, *values: UUID) -> None:
        self._values = iter(values)
        self.calls = 0

    def __call__(self) -> UUID:
        self.calls += 1
        return next(self._values)


class LoaderProbe:
    def __init__(self, *results) -> None:
        self.results = list(results)
        self.load_calls = 0
        self.run_calls = []

    async def load(self) -> None:
        self.load_calls += 1

    async def run(self, **kwargs):
        self.run_calls.append(kwargs)
        return self.results.pop(0)


@pytest.mark.parametrize(
    "value",
    [
        "",
        " leading",
        "trailing ",
        "line\nbreak",
        "x" * 129,
        None,
    ],
)
def test_invalid_idempotency_keys_fail_closed(value):
    with pytest.raises((TypeError, ValueError)):
        validate_idempotency_key(value)


def test_key_digest_never_exposes_raw_key():
    raw = "customer-visible-request-key"
    digest = idempotency_key_digest(raw)

    assert len(digest) == 64
    assert raw not in digest


def test_fingerprint_is_canonical_and_ignores_trace_context():
    first = SingleTaskRunSubmission(
        tenant_id="tenant1",
        payload={"b": 2, "a": 1},
        agent_type="fake",
        trace_parent="trace-one",
        trace_state="state-one",
    )
    second = SingleTaskRunSubmission(
        tenant_id="tenant1",
        payload={"a": 1, "b": 2},
        agent_type="fake",
        trace_parent="trace-two",
        trace_state="state-two",
    )

    assert (
        canonical_submission_fingerprint(first)
        == canonical_submission_fingerprint(second)
    )


def test_fingerprint_changes_with_execution_payload():
    first = SingleTaskRunSubmission(
        tenant_id="tenant1",
        payload={"prompt": "one"},
    )
    second = SingleTaskRunSubmission(
        tenant_id="tenant1",
        payload={"prompt": "two"},
    )

    assert (
        canonical_submission_fingerprint(first)
        != canonical_submission_fingerprint(second)
    )


@pytest.mark.asyncio
async def test_store_maps_atomic_reservation_result():
    loader = LoaderProbe(
        [
            b"RESERVED",
            b"run-tenant1-one",
            b"task-one",
            b"",
        ]
    )
    store = SubmissionIdempotencyStore(
        object(),
        retention_seconds=86_400,
        loader=loader,
    )

    result = await store.reserve(
        tenant_id="tenant1",
        idempotency_key="key-one",
        request_fingerprint="f" * 64,
        run_id="run-tenant1-one",
        task_id="task-one",
        owner_token="owner-one",
        created_at_ms=123,
    )

    assert result.status is (
        IdempotencyReservationStatus.RESERVED
    )
    assert result.run_id == "run-tenant1-one"
    assert result.task_id == "task-one"
    assert loader.load_calls == 1
    assert loader.run_calls[0]["args"][0] == "reserve"


@pytest.mark.asyncio
async def test_store_rejects_non_finalized_mutation():
    loader = LoaderProbe(
        [b"INVALID_EVIDENCE", b"", b"", b""]
    )
    store = SubmissionIdempotencyStore(
        object(),
        retention_seconds=86_400,
        loader=loader,
    )

    with pytest.raises(SubmissionIdempotencyError):
        await store.finalize(
            tenant_id="tenant1",
            idempotency_key="key-one",
            request_fingerprint="f" * 64,
            run_id="run-tenant1-one",
            task_id="task-one",
            owner_token="owner-one",
            result_json="{}",
            updated_at_ms=124,
        )


class AdmissionProbe:
    def __init__(self) -> None:
        self.calls = []

    async def admit(self, request):
        self.calls.append(request)
        return request.run_id


@dataclass(frozen=True)
class TaskResult:
    admitted: bool
    ready: bool
    task_id: str
    status: str


class DagProbe:
    def __init__(self) -> None:
        self.initialise_calls = 0
        self.task_admit_calls = []

    async def initialise(self):
        self.initialise_calls += 1

    async def task_admit(self, seed):
        self.task_admit_calls.append(seed)
        return TaskResult(
            admitted=True,
            ready=True,
            task_id=seed.task_id,
            status="seeded_root",
        )


class StoreProbe:
    def __init__(
        self,
        reservation: IdempotencyReservation,
        *,
        finalization_error: Exception | None = None,
    ) -> None:
        self.reservation = reservation
        self.finalization_error = finalization_error
        self.initialise_calls = 0
        self.reserve_calls = []
        self.finalize_calls = []

    async def initialise(self):
        self.initialise_calls += 1

    async def reserve(self, **kwargs):
        self.reserve_calls.append(kwargs)
        return self.reservation

    async def finalize(self, **kwargs):
        self.finalize_calls.append(kwargs)
        if self.finalization_error is not None:
            raise self.finalization_error


def request(
    *,
    key: str = "key-one",
    payload=None,
) -> SingleTaskRunSubmission:
    return SingleTaskRunSubmission(
        tenant_id="tenant1",
        payload=payload or {"prompt": "hello"},
        agent_type="fake",
        idempotency_key=key,
    )


def subject(
    store: StoreProbe,
    admission: AdmissionProbe,
    dag: DagProbe,
) -> RunSubmissionCoordinator:
    return RunSubmissionCoordinator(
        admission_controller=admission,
        dag_lua=dag,
        idempotency_store=store,
        require_idempotency_key=True,
        uuid_factory=UUIDFactory(
            RUN_UUID,
            TASK_UUID,
        ),
        owner_token_factory=UUIDFactory(
            OWNER_UUID,
        ),
        clock_ms=lambda: 123,
    )


@pytest.mark.asyncio
async def test_reserved_submission_finalizes_result():
    admission = AdmissionProbe()
    dag = DagProbe()
    store = StoreProbe(
        IdempotencyReservation(
            status=IdempotencyReservationStatus.RESERVED,
            run_id=(
                "run-tenant1-"
                "11111111-1111-4111-8111-111111111111"
            ),
            task_id=(
                "task-"
                "22222222-2222-4222-8222-222222222222"
            ),
        )
    )

    result = await subject(
        store,
        admission,
        dag,
    ).submit(request())

    assert result.status is RunSubmissionStatus.ACCEPTED
    assert result.idempotent_replay is False
    assert len(admission.calls) == 1
    assert len(dag.task_admit_calls) == 1
    assert len(store.finalize_calls) == 1
    stored = json.loads(
        store.finalize_calls[0]["result_json"]
    )
    assert stored["status"] == "ACCEPTED"
    assert stored["idempotent_replay"] is False


@pytest.mark.asyncio
async def test_final_replay_returns_stored_result_without_lifecycle_calls():
    admission = AdmissionProbe()
    dag = DagProbe()
    stored = {
        "status": "ACCEPTED",
        "tenant_id": "tenant1",
        "run_id": "run-tenant1-stored",
        "task_id": "task-stored",
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
        "idempotent_replay": False,
    }
    store = StoreProbe(
        IdempotencyReservation(
            status=(
                IdempotencyReservationStatus
                .FINAL_SAME_REQUEST
            ),
            run_id="run-tenant1-stored",
            task_id="task-stored",
            result_json=json.dumps(stored),
        )
    )

    result = await subject(
        store,
        admission,
        dag,
    ).submit(request())

    assert result.accepted is True
    assert result.run_id == "run-tenant1-stored"
    assert result.idempotent_replay is True
    assert admission.calls == []
    assert dag.initialise_calls == 0
    assert dag.task_admit_calls == []
    assert store.finalize_calls == []


@pytest.mark.asyncio
async def test_in_progress_replay_fails_closed_without_lifecycle_calls():
    admission = AdmissionProbe()
    dag = DagProbe()
    store = StoreProbe(
        IdempotencyReservation(
            status=(
                IdempotencyReservationStatus
                .IN_PROGRESS_SAME_REQUEST
            ),
            run_id="run-tenant1-stored",
            task_id="task-stored",
        )
    )

    result = await subject(
        store,
        admission,
        dag,
    ).submit(request())

    assert result.failure_code is (
        RunSubmissionFailureCode
        .IDEMPOTENCY_IN_PROGRESS
    )
    assert result.run_id == "run-tenant1-stored"
    assert admission.calls == []
    assert dag.task_admit_calls == []


@pytest.mark.asyncio
async def test_different_request_reuse_fails_closed():
    admission = AdmissionProbe()
    dag = DagProbe()
    store = StoreProbe(
        IdempotencyReservation(
            status=(
                IdempotencyReservationStatus
                .DIFFERENT_REQUEST
            ),
            run_id="run-tenant1-stored",
            task_id="task-stored",
        )
    )

    result = await subject(
        store,
        admission,
        dag,
    ).submit(
        request(payload={"prompt": "changed"})
    )

    assert result.failure_code is (
        RunSubmissionFailureCode
        .IDEMPOTENCY_KEY_REUSED
    )
    assert admission.calls == []
    assert dag.task_admit_calls == []


@pytest.mark.asyncio
async def test_missing_required_key_fails_before_id_generation():
    admission = AdmissionProbe()
    dag = DagProbe()
    store = StoreProbe(
        IdempotencyReservation(
            status=IdempotencyReservationStatus.RESERVED,
            run_id="unused",
            task_id="unused",
        )
    )
    uuid_factory = UUIDFactory(
        RUN_UUID,
        TASK_UUID,
    )
    coordinator = RunSubmissionCoordinator(
        admission_controller=admission,
        dag_lua=dag,
        idempotency_store=store,
        require_idempotency_key=True,
        uuid_factory=uuid_factory,
    )

    result = await coordinator.submit(
        request(key="")
    )

    assert result.failure_code is (
        RunSubmissionFailureCode
        .INVALID_IDEMPOTENCY_KEY
    )
    assert uuid_factory.calls == 0
    assert admission.calls == []
    assert dag.initialise_calls == 0


@pytest.mark.asyncio
async def test_finalization_failure_never_claims_accepted():
    admission = AdmissionProbe()
    dag = DagProbe()
    store = StoreProbe(
        IdempotencyReservation(
            status=IdempotencyReservationStatus.RESERVED,
            run_id=(
                "run-tenant1-"
                "11111111-1111-4111-8111-111111111111"
            ),
            task_id=(
                "task-"
                "22222222-2222-4222-8222-222222222222"
            ),
        ),
        finalization_error=RuntimeError("redis failed"),
    )

    result = await subject(
        store,
        admission,
        dag,
    ).submit(request())

    assert result.status is (
        RunSubmissionStatus.SUBMISSION_INCOMPLETE
    )
    assert result.failure_code is (
        RunSubmissionFailureCode
        .IDEMPOTENCY_FINALIZATION_FAILED
    )
    assert result.run_admitted is True
    assert result.task_admitted is True
    assert result.dispatch_possible is False

@pytest.mark.parametrize(
    ("field", "corrupt_value"),
    [
        ("tenant_id", "tenant-other"),
        ("run_id", "run-tenant1-corrupt"),
        ("task_id", "task-corrupt"),
    ],
)
@pytest.mark.asyncio
async def test_final_replay_rejects_stored_identity_mismatch(
    field,
    corrupt_value,
):
    admission = AdmissionProbe()
    dag = DagProbe()
    stored = {
        "status": "ACCEPTED",
        "tenant_id": "tenant1",
        "run_id": "run-tenant1-stored",
        "task_id": "task-stored",
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
        "idempotent_replay": False,
    }
    stored[field] = corrupt_value
    store = StoreProbe(
        IdempotencyReservation(
            status=(
                IdempotencyReservationStatus
                .FINAL_SAME_REQUEST
            ),
            run_id="run-tenant1-stored",
            task_id="task-stored",
            result_json=json.dumps(stored),
        )
    )

    result = await subject(
        store,
        admission,
        dag,
    ).submit(request())

    assert result.status is RunSubmissionStatus.REJECTED
    assert result.failure_code is (
        RunSubmissionFailureCode
        .IDEMPOTENCY_STORE_FAILED
    )
    assert result.run_id == "run-tenant1-stored"
    assert result.task_id == "task-stored"
    assert admission.calls == []
    assert dag.initialise_calls == 0
    assert dag.task_admit_calls == []
    assert store.finalize_calls == []
