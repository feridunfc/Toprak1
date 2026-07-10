from __future__ import annotations

from pathlib import Path

from hfa.events.schema import RunRequestedEvent
from hfa_worker.runtime.task_context_builder import (
    TASK_ID_MAPPING_SOURCE_EXPLICIT_EVENT_TASK_ID,
    TASK_ID_MAPPING_SOURCE_RUN_ID_FALLBACK,
    build_task_context_from_run_requested,
    resolve_task_identity_from_run_requested,
)


def test_builder_maps_run_requested_event_to_task_context() -> None:
    event = RunRequestedEvent(
        task_id="task-61",
        run_id="run-61",
        tenant_id="tenant-a",
        agent_type="agent-x",
        payload={"prompt": "hello"},
        scheduler_epoch="epoch-61",
        trace_parent="trace-parent-61",
        trace_state="trace-state-61",
    )

    ctx = build_task_context_from_run_requested(
        event,
        worker_id="worker-1",
        worker_group="group-a",
        shard=7,
    )

    assert ctx.task_id == "task-61"
    assert ctx.run_id == "run-61"
    assert ctx.tenant_id == "tenant-a"
    assert ctx.agent_type == "agent-x"
    assert ctx.worker_group == "group-a"
    assert ctx.worker_instance_id == "worker-1"
    assert ctx.shard == 7
    assert ctx.payload == {"prompt": "hello"}
    assert ctx.scheduler_epoch == "epoch-61"
    assert ctx.trace_parent == "trace-parent-61"
    assert ctx.trace_state == "trace-state-61"
    assert ctx.claim_epoch == ""


def test_builder_preserves_scheduler_epoch_from_dispatch_envelope() -> None:
    event = RunRequestedEvent(run_id="run-epoch", scheduler_epoch="scheduler-epoch-1")

    ctx = build_task_context_from_run_requested(
        event,
        worker_id="worker-epoch",
        worker_group="group-epoch",
        shard=3,
    )

    assert ctx.scheduler_epoch == "scheduler-epoch-1"


def test_builder_preserves_payload_without_mutating_source_payload() -> None:
    source_payload = {"prompt": "hello", "nested": {"x": 1}}
    event = RunRequestedEvent(run_id="run-payload", payload=source_payload)

    ctx = build_task_context_from_run_requested(
        event,
        worker_id="worker-payload",
        worker_group="group-payload",
        shard=1,
    )

    assert ctx.payload == source_payload
    assert ctx.payload is not source_payload

    ctx.payload["nested"]["x"] = 2

    assert source_payload["nested"]["x"] == 1


def test_builder_preserves_trace_context_with_safe_empty_defaults() -> None:
    event = RunRequestedEvent(
        run_id="run-trace",
        trace_parent=None,
        trace_state=None,
    )

    ctx = build_task_context_from_run_requested(
        event,
        worker_id="worker-trace",
        worker_group="group-trace",
        shard=0,
    )

    assert ctx.trace_parent == ""
    assert ctx.trace_state == ""


def test_task_identity_uses_explicit_event_task_id_when_present() -> None:
    event = RunRequestedEvent(
        task_id="task-explicit-1",
        run_id="run-identity",
    )

    identity = resolve_task_identity_from_run_requested(event)

    assert identity.task_id == "task-explicit-1"
    assert identity.run_id == "run-identity"
    assert identity.mapping_source == TASK_ID_MAPPING_SOURCE_EXPLICIT_EVENT_TASK_ID
    assert identity.same_identity is False


def test_legacy_run_only_identity_does_not_synthesize_task_id() -> None:
    event = RunRequestedEvent(run_id="run-fallback")

    identity = resolve_task_identity_from_run_requested(event)

    assert identity.task_id == ""
    assert identity.run_id == "run-fallback"
    assert identity.mapping_source == TASK_ID_MAPPING_SOURCE_RUN_ID_FALLBACK
    assert identity.same_identity is False


def test_legacy_run_only_context_keeps_task_id_empty() -> None:
    event = RunRequestedEvent(
        run_id="run-context-fallback",
        tenant_id="tenant-fallback",
    )

    ctx = build_task_context_from_run_requested(
        event,
        worker_id="worker-fallback",
        worker_group="group-fallback",
        shard=4,
    )

    assert ctx.task_id == ""
    assert ctx.run_id == "run-context-fallback"


def test_task_context_carries_shard_as_additive_context_field() -> None:
    event = RunRequestedEvent(run_id="run-shard")

    ctx = build_task_context_from_run_requested(
        event,
        worker_id="worker-shard",
        worker_group="group-shard",
        shard=11,
    )

    assert ctx.shard == 11


def test_worker_consumer_uses_task_context_builder_only_behind_bridge_flag() -> None:
    source = Path("hfa-worker/src/hfa_worker/consumer.py").read_text(encoding="utf-8")

    assert "build_task_context_from_run_requested" in source
    assert "is_worker_task_consumer_bridge_enabled()" in source
    assert "await self._process_message_via_task_consumer(event, msg_id, stream, shard)" in source
    assert "started = await self._guard.try_claim_and_mark_running(" in source
    assert "result = await self._executor.execute(event)" in source
