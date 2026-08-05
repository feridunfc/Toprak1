from __future__ import annotations

import hashlib
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from hfa.authority import (
    AggregateType,
    AuthorityCommand,
    AuthorityEntryContext,
    CanonicalAggregateIdentity,
    OperationType,
    ReceiptProbe,
    RedisAuthorityCommitResult,
    RedisAuthorityCommitStatus,
    evaluate_authority_commit,
)
from hfa.config.keys import RedisKey
from hfa.dag.schema import DagRedisKey
from hfa_control.dag_lua import DagLua, TaskClaimResult
from hfa_control.task_claim import TaskClaimManager
from hfa_control.task_claim_authority import (
    TASK_CLAIM_DUPLICATE_STATUS,
    TASK_CLAIM_EVIDENCE_CONFLICT_STATUS,
    TASK_CLAIM_PROJECTION_PENDING_STATUS,
    TASK_CLAIM_PROJECTED_STATUS,
    TaskClaimAuthorityBinding,
    parse_task_claim_binding_flag,
)


def _dispatch_evidence():
    identity = CanonicalAggregateIdentity(
        aggregate_type=AggregateType.TASK,
        run_id="run-1",
        task_id="task-1",
    )
    metadata = {
        "task_id": "task-1",
        "run_id": "run-1",
        "tenant_id": "tenant-1",
        "worker_id": "worker-1",
        "scheduler_epoch": "epoch-1",
        "dispatch_attempt": 1,
        "scheduled_at_ms": 200,
    }
    command = AuthorityCommand(
        aggregate_identity=identity,
        operation_type=OperationType.TASK_DISPATCH,
        operation_id=f"task-dispatch:v1:{identity.sha256}:attempt:1",
        expected_revision=1,
        intended_previous_state="ready",
        intended_next_state="scheduled",
        authoritative_payload=metadata,
        authoritative_metadata_changes=metadata,
        requested_child_effects={
            "worker_reservation": {
                "worker_id": "worker-1",
                "task_id": "task-1",
                "scheduler_epoch": "epoch-1",
            }
        },
        requested_projection_intents=(
            {"kind": "CONTROL_NOTIFICATION", "stream": "control"},
            {"kind": "TASK_REQUEST_MESSAGE", "stream": "shard"},
        ),
    )
    context = AuthorityEntryContext(
        authenticated_writer_id="test-dispatch-writer",
        allowed_operations=frozenset({OperationType.TASK_DISPATCH}),
        target_aggregate_identity_sha256=identity.sha256,
        fence_required=True,
        fence_valid=True,
    )
    evaluation = evaluate_authority_commit(
        context=context,
        command=command,
        current_revision=1,
        current_state="ready",
        receipt_probe=None,
        committed_at_ms=200,
        correlation_id=None,
    )
    assert evaluation.commit_plan is not None
    return identity, evaluation.commit_plan.record, evaluation.commit_plan.receipt


def _snapshot(record):
    return SimpleNamespace(
        operation_id=record.operation_id,
        transition_id=record.transition_id,
        canonical_command_hash=record.canonical_command_hash,
        canonical_record_hash=record.canonical_record_hash,
        revision=record.to_revision,
        state=record.next_state,
    )


class FakeKeyspace:
    @staticmethod
    def operation_field(value):
        return hashlib.sha256(value.encode()).hexdigest()


class FakeStore:
    def __init__(self, *, order=None):
        identity, record, receipt = _dispatch_evidence()
        self.identity = identity
        self.snapshot = _snapshot(record)
        self.probes = {record.operation_id: ReceiptProbe(receipt, record)}
        self.commit_calls = 0
        self.initialise_calls = 0
        self.validate_calls = 0
        self.order = order if order is not None else []

    async def initialise(self):
        self.initialise_calls += 1

    async def get_aggregate_snapshot(self, _identity):
        return self.snapshot

    async def load_receipt_probe(self, _identity, operation_id):
        return self.probes.get(operation_id)

    async def validate_authority_head(self, *_args, **_kwargs):
        self.validate_calls += 1
        return self.snapshot

    def keyspace(self, _identity_hash):
        return FakeKeyspace()

    async def commit(self, plan):
        self.commit_calls += 1
        self.order.append("commit")
        self.probes[plan.record.operation_id] = ReceiptProbe(
            plan.receipt, plan.record
        )
        self.snapshot = _snapshot(plan.record)
        return RedisAuthorityCommitResult(
            status=RedisAuthorityCommitStatus.COMMITTED,
            transition_id=plan.record.transition_id,
            aggregate_revision=plan.record.to_revision,
        )

    async def record_authority_conflict(self, *_args, **_kwargs):
        return RedisAuthorityCommitResult(
            status=RedisAuthorityCommitStatus.ILLEGAL_STATE_TRANSITION,
            transition_id=None,
            aggregate_revision=self.snapshot.revision,
            detail="conflict",
        )


class FakeRedis:
    def __init__(self, store: FakeStore):
        dispatch = next(iter(store.probes.values())).canonical_store_record
        self.state = "scheduled"
        self.hashes = {
            DagRedisKey.task_meta("task-1"): {
                "task_id": "task-1",
                "run_id": "run-1",
                "tenant_id": "tenant-1",
                "scheduler_epoch": "epoch-1",
                "dispatch_attempt": "1",
                "dispatch_worker_id": "worker-1",
                "claim_epoch": "0",
                "canonical_transition_id": dispatch.transition_id,
                "canonical_record_hash": dispatch.canonical_record_hash,
                "canonical_command_hash": dispatch.canonical_command_hash,
                "canonical_revision": str(dispatch.to_revision),
                "canonical_operation_id": dispatch.operation_id,
            },
            DagRedisKey.worker_reservation("worker-1"): {
                "task_id": "task-1",
                "worker_id": "worker-1",
                "scheduler_epoch": "epoch-1",
            },
            DagRedisKey.task_reservation_owner("task-1"): {
                "task_id": "task-1",
                "worker_id": "worker-1",
                "scheduler_epoch": "epoch-1",
            },
        }

    async def type(self, key):
        if key in {
            DagRedisKey.task_state("task-1"),
            RedisKey.run_state("run-1"),
        }:
            return "string"
        return "hash" if key in self.hashes else "none"

    async def get(self, key):
        if key == DagRedisKey.task_state("task-1"):
            return self.state
        if key == RedisKey.run_state("run-1"):
            return "running"
        return None

    async def hgetall(self, key):
        return dict(self.hashes.get(key, {}))


def _manager(store=None, redis=None, *, order=None, projection=None):
    store = store or FakeStore(order=order)
    redis = redis or FakeRedis(store)
    dag = DagLua(redis)
    dag.task_claim_canonical_projection = projection or AsyncMock(
        return_value=TaskClaimResult(
            ok=True,
            status=TASK_CLAIM_PROJECTED_STATUS,
            task_id="task-1",
            worker_id="worker-1",
            claim_epoch="1",
            scheduler_epoch="epoch-1",
        )
    )
    manager = TaskClaimManager(
        dag,
        object(),
        canonical_task_claim_binding=True,
        canonical_task_admit_binding=True,
        canonical_task_dispatch_binding=True,
        canonical_authority_store=store,
    )
    return manager, dag, store, redis


@pytest.mark.parametrize("value", [None, "", "0", "false", "off"])
def test_claim_flag_defaults_false(value):
    assert parse_task_claim_binding_flag(value) is False


def test_existing_manager_construction_remains_valid():
    TaskClaimManager(None, None)


def test_claim_binding_requires_admit_and_dispatch():
    with pytest.raises(ValueError, match="requires both"):
        TaskClaimManager(
            None,
            None,
            canonical_task_claim_binding=True,
            canonical_task_admit_binding=True,
            canonical_task_dispatch_binding=False,
        )


@pytest.mark.asyncio
async def test_disabled_binding_uses_legacy_and_never_touches_store(monkeypatch):
    redis = SimpleNamespace()
    dag = DagLua(redis)
    dag.task_claim_start = AsyncMock(
        return_value=TaskClaimResult(
            ok=False,
            status="task_state_conflict",
            task_id="task-1",
            worker_id="worker-1",
            claim_epoch="",
            scheduler_epoch="epoch-1",
        )
    )
    store = SimpleNamespace(
        initialise=AsyncMock(),
        commit=AsyncMock(),
    )
    manager = TaskClaimManager(
        dag,
        None,
        canonical_authority_store=store,
    )
    result = await manager.claim_start(
        task_id="task-1",
        run_id="run-1",
        tenant_id="tenant-1",
        worker_instance_id="worker-1",
        claimed_at_ms=300,
        scheduler_epoch="epoch-1",
    )
    assert result.status == "task_state_conflict"
    dag.task_claim_start.assert_awaited_once()
    store.initialise.assert_not_awaited()
    store.commit.assert_not_awaited()


@pytest.mark.asyncio
async def test_first_claim_commits_before_projection(monkeypatch):
    order = []
    projection = AsyncMock()

    async def projected(_value):
        order.append("projection")
        return TaskClaimResult(
            ok=True,
            status=TASK_CLAIM_PROJECTED_STATUS,
            task_id="task-1",
            worker_id="worker-1",
            claim_epoch="1",
            scheduler_epoch="epoch-1",
        )

    projection.side_effect = projected
    manager, dag, store, _redis = _manager(
        order=order,
        projection=projection,
    )
    event = Mock()
    monkeypatch.setattr("hfa_control.task_claim.emit_event_background", event)
    result = await manager.claim_start(
        task_id="task-1",
        run_id="run-1",
        tenant_id="tenant-1",
        worker_instance_id="worker-1",
        claimed_at_ms=300,
        scheduler_epoch="epoch-1",
    )
    assert result.ok is True
    assert order == ["commit", "projection"]
    assert store.commit_calls == 1
    event.assert_called_once()


@pytest.mark.asyncio
async def test_worker_mismatch_prevents_commit_and_projection():
    manager, dag, store, _redis = _manager()
    result = await manager.claim_start(
        task_id="task-1",
        run_id="run-1",
        tenant_id="tenant-1",
        worker_instance_id="worker-2",
        claimed_at_ms=300,
        scheduler_epoch="epoch-1",
    )
    assert result.status == TASK_CLAIM_EVIDENCE_CONFLICT_STATUS
    assert store.commit_calls == 0
    dag.task_claim_canonical_projection.assert_not_awaited()


@pytest.mark.asyncio
async def test_scheduler_epoch_mismatch_prevents_commit_and_projection():
    manager, dag, store, _redis = _manager()
    result = await manager.claim_start(
        task_id="task-1",
        run_id="run-1",
        tenant_id="tenant-1",
        worker_instance_id="worker-1",
        claimed_at_ms=300,
        scheduler_epoch="epoch-2",
    )
    assert result.status == TASK_CLAIM_EVIDENCE_CONFLICT_STATUS
    assert store.commit_calls == 0
    dag.task_claim_canonical_projection.assert_not_awaited()


@pytest.mark.asyncio
async def test_reservation_mismatch_prevents_commit_and_projection():
    store = FakeStore()
    redis = FakeRedis(store)
    redis.hashes[DagRedisKey.worker_reservation("worker-1")][
        "scheduler_epoch"
    ] = "wrong"
    manager, dag, store, _redis = _manager(store, redis)
    result = await manager.claim_start(
        task_id="task-1",
        run_id="run-1",
        tenant_id="tenant-1",
        worker_instance_id="worker-1",
        claimed_at_ms=300,
        scheduler_epoch="epoch-1",
    )
    assert result.status == TASK_CLAIM_EVIDENCE_CONFLICT_STATUS
    assert store.commit_calls == 0
    dag.task_claim_canonical_projection.assert_not_awaited()


@pytest.mark.asyncio
async def test_malformed_dispatch_proof_prevents_commit():
    store = FakeStore()
    probe = next(iter(store.probes.values()))
    record = probe.canonical_store_record
    bad = SimpleNamespace(**{
        name: getattr(record, name)
        for name in (
            "aggregate_identity", "aggregate_identity_sha256", "operation_id",
            "transition_id", "canonical_command_hash", "from_revision",
            "to_revision", "operation_type", "previous_state", "next_state",
            "authoritative_metadata_changes", "durable_projection_intents",
            "committed_at_ms",
        )
    })
    bad.canonical_record_hash = "not-a-sha256"
    bad.verify_hash = lambda: True
    bad_receipt = SimpleNamespace(
        operation_id=bad.operation_id,
        transition_id=bad.transition_id,
        canonical_command_hash=bad.canonical_command_hash,
        canonical_record_hash=bad.canonical_record_hash,
        aggregate_revision=bad.to_revision,
        operation_type=bad.operation_type,
    )
    store.probes[bad.operation_id] = SimpleNamespace(
        receipt=bad_receipt,
        canonical_store_record=bad,
    )
    store.snapshot = _snapshot(bad)
    redis = FakeRedis(store)
    redis.hashes[DagRedisKey.task_meta("task-1")][
        "canonical_record_hash"
    ] = bad.canonical_record_hash
    manager, dag, store, _redis = _manager(store, redis)
    result = await manager.claim_start(
        task_id="task-1",
        run_id="run-1",
        tenant_id="tenant-1",
        worker_instance_id="worker-1",
        claimed_at_ms=300,
        scheduler_epoch="epoch-1",
    )
    assert result.status == TASK_CLAIM_EVIDENCE_CONFLICT_STATUS
    assert store.commit_calls == 0
    dag.task_claim_canonical_projection.assert_not_awaited()


@pytest.mark.asyncio
async def test_projection_exception_returns_pending_and_does_not_emit(monkeypatch):
    projection = AsyncMock(side_effect=RuntimeError("redis unavailable"))
    manager, _dag, store, _redis = _manager(projection=projection)
    event = Mock()
    monkeypatch.setattr("hfa_control.task_claim.emit_event_background", event)
    result = await manager.claim_start(
        task_id="task-1",
        run_id="run-1",
        tenant_id="tenant-1",
        worker_instance_id="worker-1",
        claimed_at_ms=300,
        scheduler_epoch="epoch-1",
    )
    assert result.ok is False
    assert result.status == TASK_CLAIM_PROJECTION_PENDING_STATUS
    assert store.commit_calls == 1
    event.assert_not_called()


@pytest.mark.asyncio
async def test_exact_retry_reuses_timestamp_and_does_not_recommit(monkeypatch):
    manager, dag, store, _redis = _manager()
    event = Mock()
    monkeypatch.setattr("hfa_control.task_claim.emit_event_background", event)
    first = await manager.claim_start(
        task_id="task-1",
        run_id="run-1",
        tenant_id="tenant-1",
        worker_instance_id="worker-1",
        claimed_at_ms=300,
        scheduler_epoch="epoch-1",
    )
    assert first.ok is True
    first_projection = dag.task_claim_canonical_projection.await_args.args[0]

    dag.task_claim_canonical_projection.reset_mock()
    dag.task_claim_canonical_projection.return_value = TaskClaimResult(
        ok=False,
        status=TASK_CLAIM_DUPLICATE_STATUS,
        task_id="task-1",
        worker_id="worker-1",
        claim_epoch="1",
        scheduler_epoch="epoch-1",
    )
    second = await manager.claim_start(
        task_id="task-1",
        run_id="run-1",
        tenant_id="tenant-1",
        worker_instance_id="worker-1",
        claimed_at_ms=999,
        scheduler_epoch="epoch-1",
    )
    retry_projection = dag.task_claim_canonical_projection.await_args.args[0]
    assert second.status == TASK_CLAIM_DUPLICATE_STATUS
    assert retry_projection.claimed_at_ms == first_projection.claimed_at_ms == 300
    assert store.commit_calls == 1
    assert event.call_count == 1


@pytest.mark.asyncio
async def test_retry_after_projection_failure_can_allow_execution_once(monkeypatch):
    projection = AsyncMock(side_effect=RuntimeError("first projection fails"))
    manager, dag, store, _redis = _manager(projection=projection)
    event = Mock()
    monkeypatch.setattr("hfa_control.task_claim.emit_event_background", event)
    pending = await manager.claim_start(
        task_id="task-1",
        run_id="run-1",
        tenant_id="tenant-1",
        worker_instance_id="worker-1",
        claimed_at_ms=300,
        scheduler_epoch="epoch-1",
    )
    assert pending.status == TASK_CLAIM_PROJECTION_PENDING_STATUS

    dag.task_claim_canonical_projection.side_effect = None
    dag.task_claim_canonical_projection.return_value = TaskClaimResult(
        ok=True,
        status=TASK_CLAIM_PROJECTED_STATUS,
        task_id="task-1",
        worker_id="worker-1",
        claim_epoch="1",
        scheduler_epoch="epoch-1",
    )
    completed = await manager.claim_start(
        task_id="task-1",
        run_id="run-1",
        tenant_id="tenant-1",
        worker_instance_id="worker-1",
        claimed_at_ms=999,
        scheduler_epoch="epoch-1",
    )
    assert completed.ok is True
    assert store.commit_calls == 1
    event.assert_called_once()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "status",
    [
        TASK_CLAIM_DUPLICATE_STATUS,
        TASK_CLAIM_PROJECTION_PENDING_STATUS,
        "canonical_projection_conflict",
    ],
)
async def test_non_execution_statuses_never_emit(monkeypatch, status):
    projection = AsyncMock(
        return_value=TaskClaimResult(
            ok=False,
            status=status,
            task_id="task-1",
            worker_id="worker-1",
            claim_epoch="1",
            scheduler_epoch="epoch-1",
        )
    )
    manager, _dag, _store, _redis = _manager(projection=projection)
    event = Mock()
    monkeypatch.setattr("hfa_control.task_claim.emit_event_background", event)
    result = await manager.claim_start(
        task_id="task-1",
        run_id="run-1",
        tenant_id="tenant-1",
        worker_instance_id="worker-1",
        claimed_at_ms=300,
        scheduler_epoch="epoch-1",
    )
    assert result.ok is False
    event.assert_not_called()


@pytest.mark.asyncio
async def test_terminal_run_truth_prevents_commit_and_projection():
    store = FakeStore()
    redis = FakeRedis(store)
    original_get = redis.get

    async def get(key):
        if key == RedisKey.run_state("run-1"):
            return "done"
        return await original_get(key)

    redis.get = get
    manager, dag, store, _redis = _manager(store, redis)
    result = await manager.claim_start(
        task_id="task-1",
        run_id="run-1",
        tenant_id="tenant-1",
        worker_instance_id="worker-1",
        claimed_at_ms=300,
        scheduler_epoch="epoch-1",
    )
    assert result.status == TASK_CLAIM_EVIDENCE_CONFLICT_STATUS
    assert store.commit_calls == 0
    dag.task_claim_canonical_projection.assert_not_awaited()
