"""
hfa_worker.runtime.task_context_builder
---------------------------------------

Sprint 61 foundation:
Convert RunRequestedEvent stream envelope objects into canonical TaskContext
objects without changing WorkerConsumer behavior.

This module intentionally does not claim, execute, complete, ack, retry, or
reclaim anything. It only performs deterministic context construction.

Important identity decision:
RunRequestedEvent currently does not define a canonical task_id field.
Until the stream envelope carries task_id explicitly, the builder uses
task_id = run_id with an explicit mapping source.
"""
from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
from typing import Any

from hfa.events.schema import RunRequestedEvent
from hfa_worker.task_context import TaskContext


TASK_ID_MAPPING_SOURCE_EXPLICIT_EVENT_TASK_ID = "explicit_event_task_id"
TASK_ID_MAPPING_SOURCE_RUN_ID_FALLBACK = "run_id_fallback"


@dataclass(frozen=True)
class TaskIdentityResolution:
    task_id: str
    run_id: str
    mapping_source: str
    same_identity: bool


def _event_str(event: Any, field_name: str) -> str:
    return str(getattr(event, field_name, "") or "")


def _event_payload(event: Any) -> dict[str, Any]:
    payload = getattr(event, "payload", {}) or {}
    if not isinstance(payload, dict):
        return {}
    return deepcopy(payload)


def resolve_task_identity_from_run_requested(
    event: RunRequestedEvent,
) -> TaskIdentityResolution:
    run_id = _event_str(event, "run_id")
    explicit_task_id = _event_str(event, "task_id")

    if explicit_task_id:
        return TaskIdentityResolution(
            task_id=explicit_task_id,
            run_id=run_id,
            mapping_source=TASK_ID_MAPPING_SOURCE_EXPLICIT_EVENT_TASK_ID,
            same_identity=(explicit_task_id == run_id),
        )

    return TaskIdentityResolution(
        task_id=run_id,
        run_id=run_id,
        mapping_source=TASK_ID_MAPPING_SOURCE_RUN_ID_FALLBACK,
        same_identity=True,
    )


def build_task_context_from_run_requested(
    event: RunRequestedEvent,
    *,
    worker_id: str,
    worker_group: str,
    shard: int,
) -> TaskContext:
    identity = resolve_task_identity_from_run_requested(event)

    return TaskContext(
        task_id=identity.task_id,
        run_id=identity.run_id,
        tenant_id=_event_str(event, "tenant_id"),
        agent_type=_event_str(event, "agent_type"),
        worker_group=worker_group,
        worker_instance_id=worker_id,
        payload=_event_payload(event),
        shard=shard,
        trace_parent=_event_str(event, "trace_parent"),
        trace_state=_event_str(event, "trace_state"),
        scheduler_epoch=_event_str(event, "scheduler_epoch"),
    )
