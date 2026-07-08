"""Append-only audit model for terminal duplicate cleanup commands.

Sprint 73 audit boundary.

This module owns exactly one persistence surface:
- append an audit event to a dedicated control-plane audit stream

It does not own cleanup policy and does not perform cleanup. It must not touch
runtime streams, task state, task metadata, task output, repair, retry, or
requeue paths.

The audit stream is intentionally dedicated and separate from worker runtime
streams.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping
from uuid import uuid4


TERMINAL_DUPLICATE_CLEANUP_AUDIT_STREAM = (
    "hfa:control:audit:terminal_duplicate_cleanup"
)

TERMINAL_DUPLICATE_CLEANUP_AUDIT_EVENT_TYPE = (
    "terminal_duplicate_cleanup_command"
)

AUDIT_PHASE_INTENT = "intent"
AUDIT_PHASE_OUTCOME = "outcome"

AUDIT_NOT_ATTEMPTED = "AUDIT_NOT_ATTEMPTED"
AUDIT_INTENT_WRITTEN = "AUDIT_INTENT_WRITTEN"
AUDIT_OUTCOME_WRITTEN = "AUDIT_OUTCOME_WRITTEN"
AUDIT_BEST_EFFORT_WRITE_FAILED = "AUDIT_BEST_EFFORT_WRITE_FAILED"
AUDIT_INTENT_WRITE_FAILED = "AUDIT_INTENT_WRITE_FAILED"
AUDIT_OUTCOME_WRITE_FAILED = "AUDIT_OUTCOME_WRITE_FAILED"


@dataclass(frozen=True)
class TerminalDuplicateCleanupAuditEvent:
    command_attempt_id: str
    event_phase: str
    task_id: str

    run_id: str = ""
    stream: str = ""
    group: str = ""
    pending_message_id: str = ""

    dry_run: bool = True
    execute_requested: bool = False
    status: str = ""

    operator_reason_present: bool = False
    operator_reason_length: int = 0

    evidence_reason: str = ""
    evidence_status: str = ""
    ack_policy: str = ""
    ack_allowed: bool = False
    cleanup_candidate: bool = False

    cleanup_executed: bool = False
    ack_executed: bool = False
    ack_count: int = 0

    audit_status: str = AUDIT_NOT_ATTEMPTED
    metadata: Mapping[str, Any] = field(default_factory=dict)
    production_ready_claim: bool = False


@dataclass(frozen=True)
class TerminalDuplicateCleanupAuditAppendResult:
    written: bool
    audit_stream: str
    audit_id: str
    audit_status: str
    audit_error: str = ""
    fields: Mapping[str, str] = field(default_factory=dict)
    production_ready_claim: bool = False


def new_cleanup_command_attempt_id() -> str:
    return f"tdc-{uuid4().hex}"


def _bool(value: bool) -> str:
    return "true" if bool(value) else "false"


def _text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return str(value)


def _phase_status_for_success(event_phase: str) -> str:
    if event_phase == AUDIT_PHASE_INTENT:
        return AUDIT_INTENT_WRITTEN
    if event_phase == AUDIT_PHASE_OUTCOME:
        return AUDIT_OUTCOME_WRITTEN
    return AUDIT_NOT_ATTEMPTED


def _phase_status_for_failure(event_phase: str, *, required_for_execute: bool) -> str:
    if not required_for_execute:
        return AUDIT_BEST_EFFORT_WRITE_FAILED
    if event_phase == AUDIT_PHASE_INTENT:
        return AUDIT_INTENT_WRITE_FAILED
    if event_phase == AUDIT_PHASE_OUTCOME:
        return AUDIT_OUTCOME_WRITE_FAILED
    return AUDIT_BEST_EFFORT_WRITE_FAILED


def terminal_duplicate_cleanup_audit_fields(
    event: TerminalDuplicateCleanupAuditEvent,
) -> dict[str, str]:
    if not event.command_attempt_id:
        raise ValueError("command_attempt_id is required for cleanup audit events")
    if event.event_phase not in {AUDIT_PHASE_INTENT, AUDIT_PHASE_OUTCOME}:
        raise ValueError(f"invalid cleanup audit event phase: {event.event_phase!r}")
    if not event.task_id:
        raise ValueError("task_id is required for cleanup audit events")

    fields = {
        "event_type": TERMINAL_DUPLICATE_CLEANUP_AUDIT_EVENT_TYPE,
        "event_phase": event.event_phase,
        "command_attempt_id": event.command_attempt_id,
        "task_id": event.task_id,
        "run_id": event.run_id,
        "stream": event.stream,
        "group": event.group,
        "pending_message_id": event.pending_message_id,
        "dry_run": _bool(event.dry_run),
        "execute_requested": _bool(event.execute_requested),
        "status": event.status,
        "operator_reason_present": _bool(event.operator_reason_present),
        "operator_reason_length": str(int(event.operator_reason_length or 0)),
        "evidence_reason": event.evidence_reason,
        "evidence_status": event.evidence_status,
        "ack_policy": event.ack_policy,
        "ack_allowed": _bool(event.ack_allowed),
        "cleanup_candidate": _bool(event.cleanup_candidate),
        "cleanup_executed": _bool(event.cleanup_executed),
        "ack_executed": _bool(event.ack_executed),
        "ack_count": str(int(event.ack_count or 0)),
        "audit_status": event.audit_status,
        "production_ready_claim": "false",
    }

    for key, value in dict(event.metadata or {}).items():
        fields[f"metadata_{_text(key)}"] = _text(value)

    return {key: _text(value) for key, value in fields.items()}


async def append_terminal_duplicate_cleanup_audit_event(
    redis: Any,
    event: TerminalDuplicateCleanupAuditEvent,
    *,
    audit_stream: str = TERMINAL_DUPLICATE_CLEANUP_AUDIT_STREAM,
    maxlen: int = 10_000,
    approximate: bool = True,
    required_for_execute: bool = False,
) -> TerminalDuplicateCleanupAuditAppendResult:
    fields = terminal_duplicate_cleanup_audit_fields(event)

    try:
        audit_id = await redis.xadd(
            audit_stream,
            fields,
            maxlen=maxlen,
            approximate=approximate,
        )
    except Exception as exc:
        return TerminalDuplicateCleanupAuditAppendResult(
            written=False,
            audit_stream=audit_stream,
            audit_id="",
            audit_status=_phase_status_for_failure(
                event.event_phase,
                required_for_execute=required_for_execute,
            ),
            audit_error=f"{type(exc).__name__}: {exc}",
            fields=fields,
            production_ready_claim=False,
        )

    return TerminalDuplicateCleanupAuditAppendResult(
        written=True,
        audit_stream=audit_stream,
        audit_id=_text(audit_id),
        audit_status=_phase_status_for_success(event.event_phase),
        audit_error="",
        fields=fields,
        production_ready_claim=False,
    )
