"""Safe terminal duplicate cleanup command boundary.

Sprint 71 product command boundary.

This module is the first controlled mutation surface for terminal duplicate
cleanup. It is deliberately narrow:

Allowed mutation:
- one XACK for one explicitly requested pending stream message
- one or more dedicated append-only audit XADD calls through the audit module

Forbidden:
- XCLAIM
- direct XADD from this command service
- runtime stream audit
- mutable audit store
- SET/HSET/DEL
- EXPIRE
- requeue
- repair
- retry
- state transition
- claim_start
- task completion
- production-ready claim

This module does not own terminal duplicate ACK policy. It consumes Sprint 70
operator evidence and only adds command preconditions around a single XACK.
"""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from typing import Any, Awaitable, Callable, Mapping, Sequence

from hfa_control.terminal_duplicate_operator_evidence import (
    EVIDENCE_READ_DEGRADED,
    NO_ACK_RUN_ID_MISMATCH,
    NO_ACK_TASK_ID_MISMATCH,
    NO_ACK_TASK_META_RUN_ID_MISSING,
    NO_ACK_TASK_META_RUN_ID_MISMATCH,
    NO_ACK_TASK_NOT_TERMINAL,
    NO_ACK_WITHOUT_EXPLICIT_TASK_IDENTITY,
    NO_PENDING_STREAM_MESSAGE,
    TerminalDuplicateOperatorEvidence,
    read_terminal_duplicate_operator_evidence,
)
from hfa_control.terminal_duplicate_cleanup_audit import (
    AUDIT_BEST_EFFORT_WRITE_FAILED,
    AUDIT_INTENT_WRITE_FAILED,
    AUDIT_NOT_ATTEMPTED,
    AUDIT_OUTCOME_WRITE_FAILED,
    AUDIT_PHASE_INTENT,
    AUDIT_PHASE_OUTCOME,
    TerminalDuplicateCleanupAuditAppendResult,
    TerminalDuplicateCleanupAuditEvent,
    append_terminal_duplicate_cleanup_audit_event,
    new_cleanup_command_attempt_id,
)


DRY_RUN_CLEANUP_CANDIDATE = "DRY_RUN_CLEANUP_CANDIDATE"
DRY_RUN_NOT_CANDIDATE = "DRY_RUN_NOT_CANDIDATE"

CLEANED = "CLEANED"

DENIED_EXECUTE_NOT_EXPLICIT = "DENIED_EXECUTE_NOT_EXPLICIT"
DENIED_EXECUTE_REASON_REQUIRED = "DENIED_EXECUTE_REASON_REQUIRED"
DENIED_PENDING_MESSAGE_ID_REQUIRED = "DENIED_PENDING_MESSAGE_ID_REQUIRED"
DENIED_CONFLICTING_EXECUTION_FLAGS = "DENIED_CONFLICTING_EXECUTION_FLAGS"

DENIED_NOT_CLEANUP_CANDIDATE = "DENIED_NOT_CLEANUP_CANDIDATE"
DENIED_FALLBACK_IDENTITY = "DENIED_FALLBACK_IDENTITY"
DENIED_TASK_ID_MISMATCH = "DENIED_TASK_ID_MISMATCH"
DENIED_RUN_ID_MISMATCH = "DENIED_RUN_ID_MISMATCH"
DENIED_TASK_META_RUN_ID_MISSING = "DENIED_TASK_META_RUN_ID_MISSING"
DENIED_TASK_META_RUN_ID_MISMATCH = "DENIED_TASK_META_RUN_ID_MISMATCH"
DENIED_TASK_NOT_TERMINAL = "DENIED_TASK_NOT_TERMINAL"
DENIED_NO_PENDING_STREAM_MESSAGE = "DENIED_NO_PENDING_STREAM_MESSAGE"
DENIED_AMBIGUOUS_PENDING_MESSAGES = "DENIED_AMBIGUOUS_PENDING_MESSAGES"
DENIED_EVIDENCE_DEGRADED = "DENIED_EVIDENCE_DEGRADED"

DENIED_AUDIT_INTENT_WRITE_FAILED = "DENIED_AUDIT_INTENT_WRITE_FAILED"
CLEANED_AUDIT_OUTCOME_WRITE_FAILED = "CLEANED_AUDIT_OUTCOME_WRITE_FAILED"

ACK_NOT_APPLIED_PENDING_MISSING = "ACK_NOT_APPLIED_PENDING_MISSING"
ACK_FAILED = "ACK_FAILED"

MUTATION_TYPE_XACK_TERMINAL_DUPLICATE_CLEANUP = (
    "xack_terminal_duplicate_cleanup"
)

EvidenceReader = Callable[
    [Any],
    Awaitable[TerminalDuplicateOperatorEvidence],
]


@dataclass(frozen=True)
class TerminalDuplicateCleanupCommand:
    task_id: str
    stream_key: str
    consumer_group: str
    pending_message_id: str = ""
    dry_run: bool = True
    execute: bool = False
    reason: str = ""
    pending_limit: int = 100
    command_attempt_id: str = ""


@dataclass(frozen=True)
class TerminalDuplicateCleanupCommandResult:
    task_id: str
    run_id: str
    task_state: str
    terminal: bool

    stream: str
    group: str
    pending_message_id: str

    message_task_id: str
    message_run_id: str
    task_meta_run_id: str

    evidence_reason: str
    evidence_status: str
    ack_policy: str
    ack_allowed: bool
    cleanup_candidate: bool

    dry_run: bool
    execute_requested: bool
    cleanup_executed: bool
    ack_executed: bool
    ack_count: int

    status: str
    denial_reason: str

    read_only_evidence_used: bool
    mutation_allowed: bool
    mutation_type: str

    operator_reason: str
    operator_action_required_before: bool
    operator_action_required_after: bool

    command_attempt_id: str = ""
    audit_enabled: bool = True
    audit_required_for_execute: bool = False
    audit_intent_written: bool = False
    audit_intent_id: str = ""
    audit_outcome_written: bool = False
    audit_outcome_id: str = ""
    audit_status: str = AUDIT_NOT_ATTEMPTED
    audit_error: str = ""

    identity_status: str = "IDENTITY_UNKNOWN"
    identity_reason: str = "identity_status_not_reported_by_evidence"
    canonical_identity_confirmed: bool = False
    fallback_identity_detected: bool = False

    operator_summary: str = ""
    evidence_snapshot: Mapping[str, Any] = field(default_factory=dict)
    command_decision: Mapping[str, Any] = field(default_factory=dict)
    command_safety: Mapping[str, Any] = field(default_factory=dict)

    production_ready_claim: bool = False
    metadata: Mapping[str, Any] = field(default_factory=dict)


def _decode(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return str(value)


def _normalize_hash(raw: Any) -> dict[str, str]:
    if not isinstance(raw, Mapping):
        return {}
    return {_decode(key): _decode(value) for key, value in raw.items()}


def _normalize_stream_fields(raw_fields: Any) -> dict[str, str]:
    if isinstance(raw_fields, Mapping):
        return _normalize_hash(raw_fields)

    if isinstance(raw_fields, Sequence) and not isinstance(raw_fields, (str, bytes, bytearray)):
        items = list(raw_fields)
        out: dict[str, str] = {}
        for idx in range(0, len(items) - 1, 2):
            out[_decode(items[idx])] = _decode(items[idx + 1])
        return out

    return {}


def _normalize_pending_entry(entry: Any) -> dict[str, Any]:
    if isinstance(entry, Mapping):
        message_id = (
            entry.get("message_id")
            or entry.get(b"message_id")
            or entry.get("id")
            or entry.get(b"id")
            or ""
        )
        return {"message_id": _decode(message_id)}

    if isinstance(entry, Sequence) and not isinstance(entry, (str, bytes, bytearray)):
        items = list(entry)
        return {"message_id": _decode(items[0]) if items else ""}

    return {"message_id": _decode(entry)}


async def _read_pending_ids(
    redis: Any,
    *,
    stream_key: str,
    consumer_group: str,
    pending_limit: int,
) -> list[str]:
    raw = await redis.xpending_range(
        stream_key,
        consumer_group,
        "-",
        "+",
        pending_limit,
    )
    ids: list[str] = []
    for entry in raw or []:
        message_id = str(_normalize_pending_entry(entry).get("message_id") or "")
        if message_id:
            ids.append(message_id)
    return ids


async def _read_message_fields(
    redis: Any,
    *,
    stream_key: str,
    pending_message_id: str,
) -> dict[str, str]:
    raw = await redis.xrange(stream_key, pending_message_id, pending_message_id)
    if not raw:
        return {}

    first = raw[0]
    if isinstance(first, Mapping):
        return _normalize_stream_fields(first)

    if isinstance(first, Sequence) and not isinstance(first, (str, bytes, bytearray)):
        items = list(first)
        if len(items) >= 2:
            return _normalize_stream_fields(items[1])

    return {}


def _denied_status_for_evidence(evidence: TerminalDuplicateOperatorEvidence) -> str:
    reason = evidence.reason
    if reason == EVIDENCE_READ_DEGRADED:
        return DENIED_EVIDENCE_DEGRADED
    if reason == NO_ACK_WITHOUT_EXPLICIT_TASK_IDENTITY:
        return DENIED_FALLBACK_IDENTITY
    if reason == NO_ACK_TASK_ID_MISMATCH:
        return DENIED_TASK_ID_MISMATCH
    if reason == NO_ACK_RUN_ID_MISMATCH:
        return DENIED_RUN_ID_MISMATCH
    if reason == NO_ACK_TASK_META_RUN_ID_MISSING:
        return DENIED_TASK_META_RUN_ID_MISSING
    if reason == NO_ACK_TASK_META_RUN_ID_MISMATCH:
        return DENIED_TASK_META_RUN_ID_MISMATCH
    if reason == NO_ACK_TASK_NOT_TERMINAL:
        return DENIED_TASK_NOT_TERMINAL
    if reason == NO_PENDING_STREAM_MESSAGE:
        return DENIED_NO_PENDING_STREAM_MESSAGE
    return DENIED_NOT_CLEANUP_CANDIDATE



def _identity_status(evidence: TerminalDuplicateOperatorEvidence) -> str:
    return str(getattr(evidence, "identity_status", "") or "IDENTITY_UNKNOWN")


def _identity_reason(evidence: TerminalDuplicateOperatorEvidence) -> str:
    return str(getattr(evidence, "identity_reason", "") or "identity_status_not_reported_by_evidence")


def _canonical_identity_confirmed(evidence: TerminalDuplicateOperatorEvidence) -> bool:
    return bool(getattr(evidence, "canonical_identity_confirmed", False))


def _fallback_identity_detected(evidence: TerminalDuplicateOperatorEvidence) -> bool:
    return bool(getattr(evidence, "fallback_identity_detected", False))


def _evidence_snapshot(evidence: TerminalDuplicateOperatorEvidence) -> dict[str, Any]:
    return {
        "reason": evidence.reason,
        "status": evidence.evidence_status,
        "ack_policy": evidence.ack_policy,
        "ack_allowed": evidence.ack_allowed,
        "cleanup_candidate": evidence.cleanup_candidate,
        "message_identity_verified": evidence.message_identity_verified,
        "terminal_evidence_verified": evidence.terminal_evidence_verified,
        "identity_status": _identity_status(evidence),
        "identity_reason": _identity_reason(evidence),
        "canonical_identity_confirmed": _canonical_identity_confirmed(evidence),
        "fallback_identity_detected": _fallback_identity_detected(evidence),
        "operator_action_required": evidence.operator_action_required,
        "production_ready_claim": False,
    }


def _command_decision_snapshot(
    *,
    command: TerminalDuplicateCleanupCommand,
    evidence: TerminalDuplicateOperatorEvidence,
    mutation_allowed: bool,
    ack_executed: bool,
) -> dict[str, Any]:
    execute_path = bool(command.execute and not command.dry_run)
    return {
        "dry_run": command.dry_run,
        "execute_requested": command.execute,
        "pending_message_id_required": execute_path,
        "pending_message_id_present": bool(command.pending_message_id),
        "reason_required": execute_path,
        "reason_present": bool(str(command.reason or "").strip()),
        "pel_reread_required": execute_path and evidence.cleanup_candidate and evidence.ack_allowed,
        "xrange_reread_required": execute_path and evidence.cleanup_candidate and evidence.ack_allowed,
        "single_xack_allowed": bool(mutation_allowed or ack_executed),
        "audit_enabled": True,
        "audit_intent_required_before_xack": execute_path,
        "command_attempt_id": command.command_attempt_id,
        "production_ready_claim": False,
    }


def _command_safety_snapshot(
    *,
    mutation_allowed: bool,
    ack_executed: bool,
    append_only_audit_attempted: bool = False,
) -> dict[str, Any]:
    return {
        "mutation_boundary": "xack_only",
        "audit_boundary": "dedicated_append_only_stream",
        "mutation_executed": bool(ack_executed),
        "xack_attempted": bool(mutation_allowed),
        "direct_xadd_attempted": False,
        "append_only_audit_attempted": bool(append_only_audit_attempted),
        "runtime_stream_audit_attempted": False,
        "mutable_audit_store_attempted": False,
        "xclaim_attempted": False,
        "xadd_attempted": False,
        "state_write_attempted": False,
        "meta_write_attempted": False,
        "output_write_attempted": False,
        "repair_attempted": False,
        "requeue_attempted": False,
        "production_ready_claim": False,
    }


def _operator_summary_for_status(
    *,
    status: str,
    evidence: TerminalDuplicateOperatorEvidence,
    denial_reason: str,
    ack_count: int,
) -> str:
    if status == DRY_RUN_CLEANUP_CANDIDATE:
        return "Dry run only: terminal duplicate cleanup candidate found; no ACK executed."
    if status == DRY_RUN_NOT_CANDIDATE:
        return "Dry run only: terminal duplicate cleanup is not currently allowed."
    if status == CLEANED:
        return f"Cleanup executed: one pending terminal duplicate message was XACKed; ack_count={ack_count}."
    if status == CLEANED_AUDIT_OUTCOME_WRITE_FAILED:
        return "Cleanup executed: one pending terminal duplicate message was XACKed, but audit outcome write failed."
    if status == DENIED_AUDIT_INTENT_WRITE_FAILED:
        return "Denied: audit intent could not be written, so unaudited cleanup was blocked."
    if status == DENIED_EXECUTE_REASON_REQUIRED:
        return "Denied: execute requires a non-empty operator reason."
    if status == DENIED_PENDING_MESSAGE_ID_REQUIRED:
        return "Denied: execute requires an explicit pending_message_id."
    if status == DENIED_CONFLICTING_EXECUTION_FLAGS:
        return "Denied: dry_run and execute cannot both be true."
    if status == DENIED_EXECUTE_NOT_EXPLICIT:
        return "Denied: cleanup execution was not explicitly requested."
    if status == DENIED_FALLBACK_IDENTITY:
        return "Denied: fallback identity is not safe to cleanup."
    if status == DENIED_TASK_ID_MISMATCH:
        return "Denied: message task_id does not match cleanup evidence."
    if status == DENIED_RUN_ID_MISMATCH:
        return "Denied: message run_id does not match cleanup evidence."
    if status == DENIED_TASK_META_RUN_ID_MISSING:
        return "Denied: task_meta.run_id evidence is missing."
    if status == DENIED_TASK_META_RUN_ID_MISMATCH:
        return "Denied: task_meta.run_id does not match cleanup evidence."
    if status == DENIED_TASK_NOT_TERMINAL:
        return "Denied: task is not terminal."
    if status == DENIED_NO_PENDING_STREAM_MESSAGE:
        return "Denied: pending message is no longer in the PEL."
    if status == DENIED_AMBIGUOUS_PENDING_MESSAGES:
        return "Denied: more than one matching pending message was found."
    if status == DENIED_EVIDENCE_DEGRADED:
        return "Denied: cleanup evidence is degraded and cannot authorize cleanup."
    if status == ACK_NOT_APPLIED_PENDING_MISSING:
        return "ACK not applied: pending message was not acknowledged, likely because it is no longer pending."
    if status == ACK_FAILED:
        return "ACK failed: Redis XACK raised an exception."
    if denial_reason:
        return f"Denied: {denial_reason}."
    if evidence.reason:
        return f"Denied: {evidence.reason}."
    return f"Command completed with status {status}."



def _base_result(
    *,
    command: TerminalDuplicateCleanupCommand,
    evidence: TerminalDuplicateOperatorEvidence,
    status: str,
    denial_reason: str = "",
    cleanup_executed: bool = False,
    ack_executed: bool = False,
    ack_count: int = 0,
    mutation_allowed: bool = False,
    operator_action_required_after: bool | None = None,
    metadata: Mapping[str, Any] | None = None,
    audit_required_for_execute: bool | None = None,
) -> TerminalDuplicateCleanupCommandResult:
    execute_path = bool(command.execute and not command.dry_run)
    return TerminalDuplicateCleanupCommandResult(
        task_id=command.task_id,
        run_id=evidence.run_id,
        task_state=evidence.task_state,
        terminal=evidence.terminal,
        stream=command.stream_key,
        group=command.consumer_group,
        pending_message_id=command.pending_message_id or evidence.pending_message_id,
        message_task_id=evidence.message_task_id,
        message_run_id=evidence.message_run_id,
        task_meta_run_id=evidence.task_meta_run_id,
        evidence_reason=evidence.reason,
        evidence_status=evidence.evidence_status,
        ack_policy=evidence.ack_policy,
        ack_allowed=evidence.ack_allowed,
        cleanup_candidate=evidence.cleanup_candidate,
        identity_status=_identity_status(evidence),
        identity_reason=_identity_reason(evidence),
        canonical_identity_confirmed=_canonical_identity_confirmed(evidence),
        fallback_identity_detected=_fallback_identity_detected(evidence),
        dry_run=command.dry_run,
        execute_requested=command.execute,
        cleanup_executed=cleanup_executed,
        ack_executed=ack_executed,
        ack_count=ack_count,
        status=status,
        denial_reason=denial_reason,
        read_only_evidence_used=True,
        mutation_allowed=mutation_allowed,
        mutation_type=(
            MUTATION_TYPE_XACK_TERMINAL_DUPLICATE_CLEANUP
            if mutation_allowed or ack_executed
            else ""
        ),
        operator_reason=command.reason,
        operator_action_required_before=evidence.operator_action_required,
        operator_action_required_after=(
            evidence.operator_action_required
            if operator_action_required_after is None
            else operator_action_required_after
        ),
        command_attempt_id=command.command_attempt_id,
        audit_enabled=True,
        audit_required_for_execute=(
            execute_path if audit_required_for_execute is None else audit_required_for_execute
        ),
        audit_intent_written=False,
        audit_intent_id="",
        audit_outcome_written=False,
        audit_outcome_id="",
        audit_status=AUDIT_NOT_ATTEMPTED,
        audit_error="",
        operator_summary=_operator_summary_for_status(
            status=status,
            evidence=evidence,
            denial_reason=denial_reason,
            ack_count=ack_count,
        ),
        evidence_snapshot=_evidence_snapshot(evidence),
        command_decision=_command_decision_snapshot(
            command=command,
            evidence=evidence,
            mutation_allowed=mutation_allowed,
            ack_executed=ack_executed,
        ),
        command_safety=_command_safety_snapshot(
            mutation_allowed=mutation_allowed,
            ack_executed=ack_executed,
            append_only_audit_attempted=False,
        ),
        production_ready_claim=False,
        metadata=dict(metadata or {}),
    )


def _audit_event_from_result(
    result: TerminalDuplicateCleanupCommandResult,
    *,
    event_phase: str,
    audit_event_status: str | None = None,
) -> TerminalDuplicateCleanupAuditEvent:
    operator_reason = str(result.operator_reason or "").strip()
    return TerminalDuplicateCleanupAuditEvent(
        command_attempt_id=result.command_attempt_id,
        event_phase=event_phase,
        task_id=result.task_id,
        run_id=result.run_id,
        stream=result.stream,
        group=result.group,
        pending_message_id=result.pending_message_id,
        dry_run=result.dry_run,
        execute_requested=result.execute_requested,
        status=audit_event_status or result.status,
        operator_reason_present=bool(operator_reason),
        operator_reason_length=len(operator_reason),
        evidence_reason=result.evidence_reason,
        evidence_status=result.evidence_status,
        ack_policy=result.ack_policy,
        ack_allowed=result.ack_allowed,
        cleanup_candidate=result.cleanup_candidate,
        cleanup_executed=result.cleanup_executed,
        ack_executed=result.ack_executed,
        ack_count=result.ack_count,
        audit_status=result.audit_status,
        metadata={
            "mutation_type": result.mutation_type,
            "denial_reason_present": bool(result.denial_reason),
        },
        production_ready_claim=False,
    )


def _with_audit_append_result(
    result: TerminalDuplicateCleanupCommandResult,
    append_result: TerminalDuplicateCleanupAuditAppendResult,
    *,
    event_phase: str,
) -> TerminalDuplicateCleanupCommandResult:
    safety = dict(result.command_safety)
    safety["append_only_audit_attempted"] = True
    safety["runtime_stream_audit_attempted"] = False
    safety["mutable_audit_store_attempted"] = False
    safety["audit_stream"] = append_result.audit_stream

    updates: dict[str, Any] = {
        "audit_status": append_result.audit_status,
        "audit_error": append_result.audit_error,
        "command_safety": safety,
    }

    if event_phase == AUDIT_PHASE_INTENT:
        updates["audit_intent_written"] = append_result.written
        updates["audit_intent_id"] = append_result.audit_id
    elif event_phase == AUDIT_PHASE_OUTCOME:
        updates["audit_outcome_written"] = append_result.written
        updates["audit_outcome_id"] = append_result.audit_id

    updated = replace(result, **updates)

    if (
        event_phase == AUDIT_PHASE_OUTCOME
        and not append_result.written
        and result.cleanup_executed
        and result.ack_executed
    ):
        updated = replace(
            updated,
            status=CLEANED_AUDIT_OUTCOME_WRITE_FAILED,
            operator_summary=(
                "Cleanup executed: one pending terminal duplicate message was XACKed, "
                "but audit outcome write failed."
            ),
        )

    return updated


async def _append_outcome_audit(
    redis: Any,
    result: TerminalDuplicateCleanupCommandResult,
    *,
    required_for_execute: bool = False,
) -> TerminalDuplicateCleanupCommandResult:
    append_result = await append_terminal_duplicate_cleanup_audit_event(
        redis,
        _audit_event_from_result(result, event_phase=AUDIT_PHASE_OUTCOME),
        required_for_execute=required_for_execute,
    )
    return _with_audit_append_result(
        result,
        append_result,
        event_phase=AUDIT_PHASE_OUTCOME,
    )


def _message_body_still_matches(
    *,
    fields: Mapping[str, str],
    command: TerminalDuplicateCleanupCommand,
    evidence: TerminalDuplicateOperatorEvidence,
) -> tuple[bool, str]:
    message_task_id = fields.get("task_id", "")
    message_run_id = fields.get("run_id", "")

    if not message_task_id or message_task_id != command.task_id:
        return False, DENIED_TASK_ID_MISMATCH
    if not message_run_id or message_run_id != evidence.task_meta_run_id:
        return False, DENIED_RUN_ID_MISMATCH
    if message_task_id != evidence.message_task_id:
        return False, DENIED_TASK_ID_MISMATCH
    if message_run_id != evidence.message_run_id:
        return False, DENIED_RUN_ID_MISMATCH
    return True, ""


async def _matching_pending_count(
    redis: Any,
    *,
    command: TerminalDuplicateCleanupCommand,
    evidence: TerminalDuplicateOperatorEvidence,
) -> int:
    pending_ids = await _read_pending_ids(
        redis,
        stream_key=command.stream_key,
        consumer_group=command.consumer_group,
        pending_limit=command.pending_limit,
    )
    count = 0
    for message_id in pending_ids:
        fields = await _read_message_fields(
            redis,
            stream_key=command.stream_key,
            pending_message_id=message_id,
        )
        ok, _reason = _message_body_still_matches(
            fields=fields,
            command=command,
            evidence=evidence,
        )
        if ok:
            count += 1
    return count


async def execute_terminal_duplicate_cleanup_command(
    redis: Any,
    *,
    task_id: str,
    stream_key: str,
    consumer_group: str,
    pending_message_id: str = "",
    dry_run: bool = True,
    execute: bool = False,
    reason: str = "",
    pending_limit: int = 100,
    evidence_reader: Callable[..., Awaitable[TerminalDuplicateOperatorEvidence]] = (
        read_terminal_duplicate_operator_evidence
    ),
) -> TerminalDuplicateCleanupCommandResult:
    command = TerminalDuplicateCleanupCommand(
        task_id=task_id,
        stream_key=stream_key,
        consumer_group=consumer_group,
        pending_message_id=pending_message_id,
        dry_run=dry_run,
        execute=execute,
        reason=reason,
        pending_limit=pending_limit,
        command_attempt_id=new_cleanup_command_attempt_id(),
    )

    evidence = await evidence_reader(
        redis,
        task_id=task_id,
        stream_key=stream_key,
        consumer_group=consumer_group,
        pending_limit=pending_limit,
    )

    async def finish(
        result: TerminalDuplicateCleanupCommandResult,
        *,
        outcome_required_for_execute: bool = False,
    ) -> TerminalDuplicateCleanupCommandResult:
        return await _append_outcome_audit(
            redis,
            result,
            required_for_execute=outcome_required_for_execute,
        )

    if dry_run and execute:
        return await finish(
            _base_result(
                command=command,
                evidence=evidence,
                status=DENIED_CONFLICTING_EXECUTION_FLAGS,
                denial_reason="dry_run_and_execute_cannot_both_be_true",
            )
        )

    if dry_run:
        status = (
            DRY_RUN_CLEANUP_CANDIDATE
            if evidence.cleanup_candidate and evidence.ack_allowed
            else DRY_RUN_NOT_CANDIDATE
        )
        return await finish(_base_result(command=command, evidence=evidence, status=status))

    if not execute:
        return await finish(
            _base_result(
                command=command,
                evidence=evidence,
                status=DENIED_EXECUTE_NOT_EXPLICIT,
                denial_reason="execute_must_be_true_when_dry_run_is_false",
            )
        )

    if not str(reason or "").strip():
        return await finish(
            _base_result(
                command=command,
                evidence=evidence,
                status=DENIED_EXECUTE_REASON_REQUIRED,
                denial_reason="operator_reason_required_for_execute",
            )
        )

    if not pending_message_id:
        return await finish(
            _base_result(
                command=command,
                evidence=evidence,
                status=DENIED_PENDING_MESSAGE_ID_REQUIRED,
                denial_reason="pending_message_id_required_for_execute",
            )
        )

    if not (
        evidence.cleanup_candidate
        and evidence.ack_allowed
        and evidence.message_identity_verified
        and evidence.terminal_evidence_verified
    ):
        status = _denied_status_for_evidence(evidence)
        return await finish(
            _base_result(
                command=command,
                evidence=evidence,
                status=status,
                denial_reason=evidence.reason,
            )
        )

    if pending_message_id != evidence.pending_message_id:
        return await finish(
            _base_result(
                command=command,
                evidence=evidence,
                status=DENIED_NOT_CLEANUP_CANDIDATE,
                denial_reason="pending_message_id_does_not_match_evidence",
            )
        )

    pending_execution_result = _base_result(
        command=command,
        evidence=evidence,
        status="PENDING_EXECUTION",
        audit_required_for_execute=True,
    )
    intent_append = await append_terminal_duplicate_cleanup_audit_event(
        redis,
        _audit_event_from_result(
            pending_execution_result,
            event_phase=AUDIT_PHASE_INTENT,
            audit_event_status="PENDING_EXECUTION",
        ),
        required_for_execute=True,
    )

    if not intent_append.written:
        denied = _base_result(
            command=command,
            evidence=evidence,
            status=DENIED_AUDIT_INTENT_WRITE_FAILED,
            denial_reason=intent_append.audit_error,
            audit_required_for_execute=True,
        )
        return _with_audit_append_result(
            denied,
            intent_append,
            event_phase=AUDIT_PHASE_INTENT,
        )

    def with_intent(
        result: TerminalDuplicateCleanupCommandResult,
    ) -> TerminalDuplicateCleanupCommandResult:
        return _with_audit_append_result(
            result,
            intent_append,
            event_phase=AUDIT_PHASE_INTENT,
        )

    pending_ids = await _read_pending_ids(
        redis,
        stream_key=stream_key,
        consumer_group=consumer_group,
        pending_limit=pending_limit,
    )
    if pending_message_id not in pending_ids:
        return await finish(
            with_intent(
                _base_result(
                    command=command,
                    evidence=evidence,
                    status=DENIED_NO_PENDING_STREAM_MESSAGE,
                    denial_reason="pending_message_id_not_in_pel",
                    audit_required_for_execute=True,
                )
            )
        )

    fields = await _read_message_fields(
        redis,
        stream_key=stream_key,
        pending_message_id=pending_message_id,
    )
    matches, mismatch_status = _message_body_still_matches(
        fields=fields,
        command=command,
        evidence=evidence,
    )
    if not matches:
        return await finish(
            with_intent(
                _base_result(
                    command=command,
                    evidence=evidence,
                    status=mismatch_status,
                    denial_reason="message_body_no_longer_matches_evidence",
                    metadata={"message_fields": dict(fields)},
                    audit_required_for_execute=True,
                )
            )
        )

    matching_count = await _matching_pending_count(
        redis,
        command=command,
        evidence=evidence,
    )
    if matching_count != 1:
        return await finish(
            with_intent(
                _base_result(
                    command=command,
                    evidence=evidence,
                    status=DENIED_AMBIGUOUS_PENDING_MESSAGES,
                    denial_reason="expected_exactly_one_matching_pending_message",
                    metadata={"matching_pending_messages": matching_count},
                    audit_required_for_execute=True,
                )
            )
        )

    try:
        ack_count_raw = await redis.xack(stream_key, consumer_group, pending_message_id)
        ack_count = int(ack_count_raw or 0)
    except Exception as exc:
        return await finish(
            with_intent(
                _base_result(
                    command=command,
                    evidence=evidence,
                    status=ACK_FAILED,
                    denial_reason=f"{type(exc).__name__}: {exc}",
                    mutation_allowed=True,
                    metadata={"ack_error": str(exc)},
                    audit_required_for_execute=True,
                )
            )
        )

    if ack_count <= 0:
        return await finish(
            with_intent(
                _base_result(
                    command=command,
                    evidence=evidence,
                    status=ACK_NOT_APPLIED_PENDING_MISSING,
                    denial_reason="xack_returned_zero",
                    ack_count=0,
                    mutation_allowed=True,
                    audit_required_for_execute=True,
                )
            )
        )

    return await finish(
        with_intent(
            _base_result(
                command=command,
                evidence=evidence,
                status=CLEANED,
                cleanup_executed=True,
                ack_executed=True,
                ack_count=ack_count,
                mutation_allowed=True,
                operator_action_required_after=False,
                audit_required_for_execute=True,
            )
        ),
        outcome_required_for_execute=True,
    )
