from __future__ import annotations

import hashlib
import json
from dataclasses import replace
from types import SimpleNamespace

import pytest

from hfa.authority import AggregateType, OperationType
from hfa_control.run_terminate_authority import (
    WRITER_ID,
    RunTerminateBindingResult,
    TerminalAggregateProof,
    TerminalAggregateProofManager,
    TerminalTaskEvidence,
    build_run_terminate_command,
    build_run_terminate_context,
    run_terminate_identity,
    run_terminate_operation_id,
)
from hfa_control.run_termination import RunTerminationCoordinator


def proof(**overrides) -> TerminalAggregateProof:
    payload = (
        '{"done_count":2,"failed_count":0,"final_state":"done",'
        '"run_id":"run-1","schema_version":1,"skipped_count":0,'
        '"task_count":2,"tasks":[{"state":"done","task_id":"task-a"},'
        '{"state":"done","task_id":"task-b"}],"tenant_id":"tenant-a"}'
    )
    base = TerminalAggregateProof(
        schema_version=1,
        run_id="run-1",
        tenant_id="tenant-a",
        tasks=(
            TerminalTaskEvidence("task-a", "done"),
            TerminalTaskEvidence("task-b", "done"),
        ),
        task_count=2,
        done_count=2,
        failed_count=0,
        skipped_count=0,
        final_state="done",
        proof_sha256=hashlib.sha256(payload.encode()).hexdigest(),
        proof_payload_json=payload,
        finalized_at_ms=1000,
        worker_instance_id="worker-a",
        trigger_task_id="task-b",
        trigger_terminal_state="done",
        canonical_expected_revision=1,
        canonical_previous_state="pending",
    )
    return replace(base, **overrides)


def test_run_terminate_identity_is_run_aggregate():
    identity = run_terminate_identity("run-1")
    assert identity.aggregate_type is AggregateType.RUN
    assert identity.run_id == "run-1"
    assert identity.task_id is None


def test_operation_identity_binds_only_run_and_immutable_proof():
    first = proof()
    changed_metadata = replace(
        first,
        finalized_at_ms=9999,
        worker_instance_id="worker-b",
        trigger_task_id="task-a",
    )
    assert run_terminate_operation_id(first.run_id, first.proof_sha256) == (
        run_terminate_operation_id(changed_metadata.run_id, changed_metadata.proof_sha256)
    )
    assert run_terminate_operation_id(first.run_id, "0" * 64) != (
        run_terminate_operation_id(first.run_id, first.proof_sha256)
    )


def test_command_is_canonical_run_terminate_revision_two_from_pending():
    value = proof()
    command = build_run_terminate_command(value)
    assert command.operation_type is OperationType.RUN_TERMINATE
    assert command.expected_revision == 1
    assert command.intended_previous_state == "pending"
    assert command.intended_next_state == "done"
    assert command.operation_id == run_terminate_operation_id(value.run_id, value.proof_sha256)
    assert tuple(intent["kind"] for intent in command.requested_projection_intents) == (
        "RUN_RESULT_PROJECTION",
    )
    assert command.authoritative_metadata_changes["terminal_evidence"]["terminal_proof_sha256"] == value.proof_sha256
    assert command.authoritative_metadata_changes["terminal_evidence"]["tasks"][0]["task_id"] == "task-a"


def test_context_is_narrow_run_terminate_writer():
    command = build_run_terminate_command(proof())
    context = build_run_terminate_context(command)
    assert context.authenticated_writer_id == WRITER_ID
    assert context.allowed_operations == frozenset({OperationType.RUN_TERMINATE})
    assert context.target_aggregate_identity_sha256 == command.aggregate_identity.sha256
    assert context.fence_required is False
    assert context.fence_valid is True


def test_changed_terminal_proof_changes_operation_and_command():
    first = proof()
    changed_payload = json.loads(first.proof_payload_json)
    changed_payload["tasks"][1]["state"] = "failed"
    changed_payload["done_count"] = 1
    changed_payload["failed_count"] = 1
    changed_payload["final_state"] = "failed"
    payload = json.dumps(changed_payload, separators=(",", ":"), sort_keys=True)
    second = replace(
        first,
        tasks=(TerminalTaskEvidence("task-a", "done"), TerminalTaskEvidence("task-b", "failed")),
        done_count=1,
        failed_count=1,
        final_state="failed",
        proof_payload_json=payload,
        proof_sha256=hashlib.sha256(payload.encode()).hexdigest(),
    )
    a = build_run_terminate_command(first)
    b = build_run_terminate_command(second)
    assert a.operation_id != b.operation_id
    assert a.canonical_command_hash != b.canonical_command_hash


def test_proof_parser_rejects_payload_hash_mismatch():
    raw = {
        "schema_version": "1",
        "run_id": "run-1",
        "tenant_id": "tenant-a",
        "proof_sha256": "0" * 64,
        "proof_payload_json": proof().proof_payload_json,
        "task_count": "2",
        "done_count": "2",
        "failed_count": "0",
        "skipped_count": "0",
        "final_state": "done",
        "finalized_at_ms": "1000",
        "worker_instance_id": "worker-a",
        "trigger_task_id": "task-b",
        "trigger_terminal_state": "done",
        "canonical_expected_revision": "1",
        "canonical_previous_state": "pending",
    }
    with pytest.raises(RuntimeError, match="SHA-256"):
        TerminalAggregateProofManager._proof_from_mapping(raw)


@pytest.mark.asyncio
async def test_coordinator_uses_canonical_binding_only_when_injected():
    class Gateway:
        pass

    class Binding:
        def __init__(self):
            self.initialized = False
            self.calls = []

        async def initialise(self):
            self.initialized = True

        async def terminate(self, **kwargs):
            self.calls.append(kwargs)
            return RunTerminateBindingResult(
                finalized=True,
                status="canonical_run_terminate_projected",
                run_id=kwargs["run_id"],
                final_state="done",
                task_count=1,
                done_count=1,
                ack_allowed=True,
            )

    class LegacyLoader:
        async def load(self):
            return None

    binding = Binding()
    coordinator = RunTerminationCoordinator(
        redis=SimpleNamespace(),
        task_completion_gateway=Gateway(),
        enabled=True,
        authority_binding=binding,
    )
    coordinator._loader = LegacyLoader()
    result = await coordinator.finalize_run_from_tasks(
        run_id="run-1",
        tenant_id="tenant-a",
        trigger_task_id="task-a",
        finalized_at_ms=1000,
        worker_instance_id="worker-a",
        trigger_terminal_state="done",
    )
    assert result.finalized is True
    assert result.status == "canonical_run_terminate_projected"
    assert result.ack_allowed is True
    assert binding.calls[0]["run_id"] == "run-1"


@pytest.mark.asyncio
async def test_coordinator_legacy_path_remains_default_without_binding():
    class Loader:
        async def run(self, **_kwargs):
            return [1, "finalized", "done", 1, 1, 0, 0, 0, 1]

    coordinator = RunTerminationCoordinator(
        redis=SimpleNamespace(),
        task_completion_gateway=SimpleNamespace(),
        enabled=True,
    )
    coordinator._loader = Loader()
    result = await coordinator.finalize_run_from_tasks(
        run_id="run-legacy",
        tenant_id="tenant-a",
        trigger_task_id="task-a",
        finalized_at_ms=1000,
    )
    assert result.status == "finalized"
    assert result.final_state == "done"
    assert result.ack_allowed is True


class _FakeProofManager:
    def __init__(self, value: TerminalAggregateProof):
        self.value = value
        self.persisted = None
        self.capture_calls = 0

    async def initialise(self):
        return None

    async def load_existing(self, _run_id):
        return self.persisted

    async def capture(self, **_kwargs):
        from hfa_control.run_terminate_authority import TerminalProofCaptureResult

        self.capture_calls += 1
        self.persisted = self.value
        return TerminalProofCaptureResult(status="captured", proof=self.value)


class _FakeProjection:
    def __init__(self, events):
        self.events = events
        self.calls = 0
        self.fail_once = False

    async def initialise(self):
        return None

    async def project(self, value):
        from hfa_control.run_terminate_authority import RunTerminateProjectionResult

        self.calls += 1
        self.events.append("projection")
        if self.fail_once:
            self.fail_once = False
            raise RuntimeError("projection failed after canonical durability")
        return RunTerminateProjectionResult(
            status=(
                "canonical_run_terminate_projected"
                if self.calls == 1
                else "canonical_run_terminate_already_projected"
            ),
            projected=self.calls == 1,
            stream_entry_id="1-0",
        )


class _FakeKeyspace:
    @staticmethod
    def operation_field(_operation_id):
        return "0" * 64


class _FakeAuthorityStore:
    def __init__(self, events):
        self.events = events
        self.plan = None
        self.snapshot_revision = 1
        self.snapshot_state = "pending"
        self.raise_after_commit_once = False

    async def initialise(self):
        return None

    async def get_aggregate_snapshot(self, _identity):
        return SimpleNamespace(
            revision=self.snapshot_revision,
            state=self.snapshot_state,
        )

    async def load_receipt_probe(self, _identity, operation_id):
        if self.plan is None or self.plan.record.operation_id != operation_id:
            return None
        from hfa.authority import ReceiptProbe

        return ReceiptProbe(
            receipt=self.plan.receipt,
            canonical_store_record=self.plan.record,
        )

    async def commit(self, plan):
        from hfa.authority import RedisAuthorityCommitStatus

        self.events.append("canonical_commit")
        self.plan = plan
        self.snapshot_revision = plan.record.to_revision
        self.snapshot_state = plan.record.next_state
        if self.raise_after_commit_once:
            self.raise_after_commit_once = False
            raise RuntimeError("lost commit response")
        return SimpleNamespace(status=RedisAuthorityCommitStatus.COMMITTED, detail="")

    def keyspace(self, _identity_sha):
        return _FakeKeyspace()

    async def validate_authority_head(self, *_args, **_kwargs):
        return None

    @staticmethod
    def canonical_projection_intents_json(_intents):
        return "[]"


@pytest.mark.asyncio
async def test_binding_commits_canonical_authority_before_projection():
    from hfa_control.run_terminate_authority import RunTerminateAuthorityBinding

    events = []
    store = _FakeAuthorityStore(events)
    proof_manager = _FakeProofManager(proof())
    projection = _FakeProjection(events)
    binding = RunTerminateAuthorityBinding(
        SimpleNamespace(),
        store=store,
        proof_manager=proof_manager,
        projection_manager=projection,
    )
    result = await binding.terminate(
        run_id="run-1",
        tenant_id="tenant-a",
        trigger_task_id="task-b",
        finalized_at_ms=1000,
        worker_instance_id="worker-a",
        trigger_terminal_state="done",
    )
    assert result.finalized is True
    assert result.canonical_revision == 2
    assert events == ["canonical_commit", "projection"]


@pytest.mark.asyncio
async def test_durable_receipt_retry_does_not_recapture_mutable_tasks():
    from hfa_control.run_terminate_authority import RunTerminateAuthorityBinding

    events = []
    store = _FakeAuthorityStore(events)
    proof_manager = _FakeProofManager(proof())
    projection = _FakeProjection(events)
    binding = RunTerminateAuthorityBinding(
        SimpleNamespace(),
        store=store,
        proof_manager=proof_manager,
        projection_manager=projection,
    )
    await binding.terminate(
        run_id="run-1",
        tenant_id="tenant-a",
        trigger_task_id="task-b",
        finalized_at_ms=1000,
        trigger_terminal_state="done",
    )
    first_capture_count = proof_manager.capture_calls
    retry = await binding.terminate(
        run_id="run-1",
        tenant_id="tenant-a",
        trigger_task_id="different-trigger",
        finalized_at_ms=9999,
        worker_instance_id="different-worker",
        trigger_terminal_state="failed",
    )
    assert retry.canonical_revision == 2
    assert proof_manager.capture_calls == first_capture_count == 1
    assert store.snapshot_revision == 2


@pytest.mark.asyncio
async def test_ambiguous_commit_uses_exact_durable_receipt_before_projection():
    from hfa_control.run_terminate_authority import RunTerminateAuthorityBinding

    events = []
    store = _FakeAuthorityStore(events)
    store.raise_after_commit_once = True
    binding = RunTerminateAuthorityBinding(
        SimpleNamespace(),
        store=store,
        proof_manager=_FakeProofManager(proof()),
        projection_manager=_FakeProjection(events),
    )
    result = await binding.terminate(
        run_id="run-1",
        tenant_id="tenant-a",
        trigger_task_id="task-b",
        finalized_at_ms=1000,
        trigger_terminal_state="done",
    )
    assert result.finalized is True
    assert events == ["canonical_commit", "projection"]
