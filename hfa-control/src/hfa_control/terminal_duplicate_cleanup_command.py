"""Safe terminal duplicate cleanup command boundary.

Sprint 71 product command boundary.

This module is the first controlled mutation surface for terminal duplicate
cleanup. It is deliberately narrow:

Allowed mutation:
- one XACK for one explicitly requested pending stream message

Forbidden:
- XCLAIM
- XADD
- SET/HSET/DEL
- EXPIRE
- requeue
- repair
- retry
- state transition
- claim_start
- task completion
- persistent audit
- production-ready claim

This module does not own terminal duplicate ACK policy. It consumes Sprint 70
operator evidence and only adds command preconditions around a single XACK.
"""

from __future__ import annotations

from dataclasses import dataclass, field
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
) -> TerminalDuplicateCleanupCommandResult:
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
        production_ready_claim=False,
        metadata=dict(metadata or {}),
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
    )

    evidence = await evidence_reader(
        redis,
        task_id=task_id,
        stream_key=stream_key,
        consumer_group=consumer_group,
        pending_limit=pending_limit,
    )

    if dry_run and execute:
        return _base_result(
            command=command,
            evidence=evidence,
            status=DENIED_CONFLICTING_EXECUTION_FLAGS,
            denial_reason="dry_run_and_execute_cannot_both_be_true",
        )

    if dry_run:
        status = (
            DRY_RUN_CLEANUP_CANDIDATE
            if evidence.cleanup_candidate and evidence.ack_allowed
            else DRY_RUN_NOT_CANDIDATE
        )
        return _base_result(command=command, evidence=evidence, status=status)

    if not execute:
        return _base_result(
            command=command,
            evidence=evidence,
            status=DENIED_EXECUTE_NOT_EXPLICIT,
            denial_reason="execute_must_be_true_when_dry_run_is_false",
        )

    if not str(reason or "").strip():
        return _base_result(
            command=command,
            evidence=evidence,
            status=DENIED_EXECUTE_REASON_REQUIRED,
            denial_reason="operator_reason_required_for_execute",
        )

    if not pending_message_id:
        return _base_result(
            command=command,
            evidence=evidence,
            status=DENIED_PENDING_MESSAGE_ID_REQUIRED,
            denial_reason="pending_message_id_required_for_execute",
        )

    if not (
        evidence.cleanup_candidate
        and evidence.ack_allowed
        and evidence.message_identity_verified
        and evidence.terminal_evidence_verified
    ):
        status = _denied_status_for_evidence(evidence)
        return _base_result(
            command=command,
            evidence=evidence,
            status=status,
            denial_reason=evidence.reason,
        )

    if pending_message_id != evidence.pending_message_id:
        return _base_result(
            command=command,
            evidence=evidence,
            status=DENIED_NOT_CLEANUP_CANDIDATE,
            denial_reason="pending_message_id_does_not_match_evidence",
        )

    pending_ids = await _read_pending_ids(
        redis,
        stream_key=stream_key,
        consumer_group=consumer_group,
        pending_limit=pending_limit,
    )
    if pending_message_id not in pending_ids:
        return _base_result(
            command=command,
            evidence=evidence,
            status=DENIED_NO_PENDING_STREAM_MESSAGE,
            denial_reason="pending_message_id_not_in_pel",
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
        return _base_result(
            command=command,
            evidence=evidence,
            status=mismatch_status,
            denial_reason="message_body_no_longer_matches_evidence",
            metadata={"message_fields": dict(fields)},
        )

    matching_count = await _matching_pending_count(
        redis,
        command=command,
        evidence=evidence,
    )
    if matching_count != 1:
        return _base_result(
            command=command,
            evidence=evidence,
            status=DENIED_AMBIGUOUS_PENDING_MESSAGES,
            denial_reason="expected_exactly_one_matching_pending_message",
            metadata={"matching_pending_messages": matching_count},
        )

    try:
        ack_count_raw = await redis.xack(stream_key, consumer_group, pending_message_id)
        ack_count = int(ack_count_raw or 0)
    except Exception as exc:
        return _base_result(
            command=command,
            evidence=evidence,
            status=ACK_FAILED,
            denial_reason=f"{type(exc).__name__}: {exc}",
            mutation_allowed=True,
            metadata={"ack_error": str(exc)},
        )

    if ack_count <= 0:
        return _base_result(
            command=command,
            evidence=evidence,
            status=ACK_NOT_APPLIED_PENDING_MISSING,
            denial_reason="xack_returned_zero",
            ack_count=0,
            mutation_allowed=True,
        )

    return _base_result(
        command=command,
        evidence=evidence,
        status=CLEANED,
        cleanup_executed=True,
        ack_executed=True,
        ack_count=ack_count,
        mutation_allowed=True,
        operator_action_required_after=False,
    )
