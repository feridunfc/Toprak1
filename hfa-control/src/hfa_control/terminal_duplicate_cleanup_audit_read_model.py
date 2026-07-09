"""Read-only audit read model for terminal duplicate cleanup commands.

Sprint 74 read boundary.

This module reads the dedicated control-plane terminal duplicate cleanup audit
stream and returns a task-centered operator timeline. It does not own cleanup
policy and it does not recommend cleanup actions.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping, Sequence

from hfa_control.terminal_duplicate_cleanup_audit import (
    AUDIT_PHASE_INTENT,
    AUDIT_PHASE_OUTCOME,
    TERMINAL_DUPLICATE_CLEANUP_AUDIT_EVENT_TYPE,
    TERMINAL_DUPLICATE_CLEANUP_AUDIT_STREAM,
)


AUDIT_READ_OK = "AUDIT_READ_OK"
AUDIT_READ_EMPTY = "AUDIT_READ_EMPTY"
AUDIT_READ_DEGRADED = "AUDIT_READ_DEGRADED"
AUDIT_READ_FAILED = "AUDIT_READ_FAILED"
AUDIT_READ_SAFETY_VIOLATION = "AUDIT_READ_SAFETY_VIOLATION"

DEFAULT_AUDIT_READ_LIMIT = 100
MAX_AUDIT_READ_LIMIT = 500
DEFAULT_AUDIT_SCAN_LIMIT = 500
MAX_AUDIT_SCAN_LIMIT = 5000

_SENSITIVE_REASON_FIELDS = {
    "operator_reason",
    "operator_reason_text",
    "operator_reason_full_text",
    "full_operator_reason",
    "reason_text",
}


@dataclass(frozen=True)
class TerminalDuplicateCleanupAuditEntry:
    audit_id: str
    event_type: str
    event_phase: str
    command_attempt_id: str
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

    audit_status: str = ""
    production_ready_claim: bool = False


@dataclass(frozen=True)
class TerminalDuplicateCleanupAuditReadResult:
    task_id: str
    audit_stream: str

    entries: tuple[TerminalDuplicateCleanupAuditEntry, ...] = ()
    entry_count: int = 0
    command_attempt_count: int = 0

    latest_command_attempt_id: str = ""
    latest_status: str = ""
    latest_cleanup_executed: bool = False
    latest_ack_executed: bool = False
    latest_ack_count: int = 0

    has_intent_without_outcome: bool = False
    has_operator_reason_text_exposure: bool = False

    read_status: str = AUDIT_READ_EMPTY
    read_error: str = ""
    operator_summary: str = ""

    limit: int = DEFAULT_AUDIT_READ_LIMIT
    limit_applied: int = DEFAULT_AUDIT_READ_LIMIT
    scan_limit: int = DEFAULT_AUDIT_SCAN_LIMIT
    scan_limit_applied: int = DEFAULT_AUDIT_SCAN_LIMIT
    raw_entries_scanned: int = 0
    possibly_truncated: bool = False

    command_safety: Mapping[str, Any] = field(default_factory=dict)
    production_ready_claim: bool = False


def _text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return str(value)


def _normalize_fields(fields: Any) -> dict[str, str]:
    if isinstance(fields, Mapping):
        return {_text(key): _text(value) for key, value in fields.items()}

    if isinstance(fields, (list, tuple)):
        items = list(fields)
        normalized: dict[str, str] = {}
        for index in range(0, len(items) - 1, 2):
            normalized[_text(items[index])] = _text(items[index + 1])
        return normalized

    return {}


def _normalize_stream_row(row: Any) -> tuple[str, dict[str, str]]:
    if isinstance(row, Mapping):
        audit_id = _text(row.get("id") or row.get("message_id") or "")
        fields = _normalize_fields(row.get("fields") or row.get("data") or {})
        return audit_id, fields

    if isinstance(row, (list, tuple)) and len(row) >= 2:
        return _text(row[0]), _normalize_fields(row[1])

    return "", {}


def _parse_bool(value: Any, *, default: bool = False) -> bool:
    text = _text(value).strip().lower()
    if text in {"true", "1", "yes", "y"}:
        return True
    if text in {"false", "0", "no", "n", ""}:
        return False
    raise ValueError(f"invalid bool value: {value!r}")


def _parse_int(value: Any, *, default: int = 0) -> int:
    text = _text(value).strip()
    if not text:
        return default
    return int(text)


def _cap(value: int, *, default: int, minimum: int, maximum: int) -> int:
    try:
        parsed = int(value)
    except Exception:
        parsed = default
    return max(minimum, min(parsed, maximum))


def _stream_id_sort_key(audit_id: str) -> tuple[int, int, str]:
    try:
        left, right = _text(audit_id).split("-", 1)
        return int(left), int(right), _text(audit_id)
    except Exception:
        return 0, 0, _text(audit_id)


def _command_safety() -> dict[str, Any]:
    return {
        "read_only": True,
        "audit_stream_read": True,
        "dedicated_audit_stream_only": True,
        "runtime_stream_read_attempted": False,
        "runtime_stream_write_attempted": False,
        "xadd_attempted": False,
        "xack_attempted": False,
        "xclaim_attempted": False,
        "task_state_mutation_attempted": False,
        "task_meta_mutation_attempted": False,
        "task_output_mutation_attempted": False,
        "cleanup_recommendation_emitted": False,
        "dashboard_action_emitted": False,
        "production_ready_claim": False,
    }


def _operator_summary(
    *,
    read_status: str,
    entry_count: int,
    latest_status: str,
    has_intent_without_outcome: bool,
    has_operator_reason_text_exposure: bool,
) -> str:
    if read_status == AUDIT_READ_FAILED:
        return "Cleanup audit read failed; no cleanup action is recommended."
    if has_operator_reason_text_exposure:
        return (
            "Audit safety violation: operator reason text was present in an audit "
            "event and was not returned."
        )
    if entry_count <= 0:
        return "No terminal duplicate cleanup audit entries found within the scan window."
    if has_intent_without_outcome:
        return (
            "Cleanup audit found: an intent event exists without a matching outcome "
            "event within the scan window."
        )
    return f"Cleanup audit found: latest audit status is {latest_status}."


def _entry_from_fields(
    *,
    audit_id: str,
    fields: Mapping[str, str],
) -> tuple[TerminalDuplicateCleanupAuditEntry | None, tuple[str, ...]]:
    errors: list[str] = []

    def parse_bool_field(name: str, default: bool = False) -> bool:
        try:
            return _parse_bool(fields.get(name, ""), default=default)
        except Exception as exc:
            errors.append(f"{name}: {exc}")
            return default

    def parse_int_field(name: str, default: int = 0) -> int:
        try:
            return _parse_int(fields.get(name, ""), default=default)
        except Exception as exc:
            errors.append(f"{name}: {exc}")
            return default

    event_type = _text(fields.get("event_type"))
    event_phase = _text(fields.get("event_phase"))
    command_attempt_id = _text(fields.get("command_attempt_id"))
    task_id = _text(fields.get("task_id"))

    if event_type != TERMINAL_DUPLICATE_CLEANUP_AUDIT_EVENT_TYPE:
        errors.append("event_type: unexpected")
    if event_phase not in {AUDIT_PHASE_INTENT, AUDIT_PHASE_OUTCOME}:
        errors.append("event_phase: unexpected")
    if not command_attempt_id:
        errors.append("command_attempt_id: missing")
    if not task_id:
        errors.append("task_id: missing")

    entry = TerminalDuplicateCleanupAuditEntry(
        audit_id=_text(audit_id),
        event_type=event_type,
        event_phase=event_phase,
        command_attempt_id=command_attempt_id,
        task_id=task_id,
        run_id=_text(fields.get("run_id")),
        stream=_text(fields.get("stream")),
        group=_text(fields.get("group")),
        pending_message_id=_text(fields.get("pending_message_id")),
        dry_run=parse_bool_field("dry_run", True),
        execute_requested=parse_bool_field("execute_requested", False),
        status=_text(fields.get("status")),
        operator_reason_present=parse_bool_field("operator_reason_present", False),
        operator_reason_length=parse_int_field("operator_reason_length", 0),
        evidence_reason=_text(fields.get("evidence_reason")),
        evidence_status=_text(fields.get("evidence_status")),
        ack_policy=_text(fields.get("ack_policy")),
        ack_allowed=parse_bool_field("ack_allowed", False),
        cleanup_candidate=parse_bool_field("cleanup_candidate", False),
        cleanup_executed=parse_bool_field("cleanup_executed", False),
        ack_executed=parse_bool_field("ack_executed", False),
        ack_count=parse_int_field("ack_count", 0),
        audit_status=_text(fields.get("audit_status")),
        production_ready_claim=parse_bool_field("production_ready_claim", False),
    )

    return entry, tuple(errors)


def _has_sensitive_reason_field(fields: Mapping[str, str]) -> bool:
    return any(key in _SENSITIVE_REASON_FIELDS for key in fields.keys())


async def read_terminal_duplicate_cleanup_audit(
    redis: Any,
    *,
    task_id: str,
    audit_stream: str = TERMINAL_DUPLICATE_CLEANUP_AUDIT_STREAM,
    limit: int = DEFAULT_AUDIT_READ_LIMIT,
    scan_limit: int = DEFAULT_AUDIT_SCAN_LIMIT,
    include_entries: bool = True,
) -> TerminalDuplicateCleanupAuditReadResult:
    limit_applied = _cap(
        limit,
        default=DEFAULT_AUDIT_READ_LIMIT,
        minimum=1,
        maximum=MAX_AUDIT_READ_LIMIT,
    )
    scan_limit_applied = _cap(
        scan_limit,
        default=DEFAULT_AUDIT_SCAN_LIMIT,
        minimum=limit_applied,
        maximum=MAX_AUDIT_SCAN_LIMIT,
    )

    base_kwargs = {
        "task_id": task_id,
        "audit_stream": audit_stream,
        "limit": limit,
        "limit_applied": limit_applied,
        "scan_limit": scan_limit,
        "scan_limit_applied": scan_limit_applied,
        "command_safety": _command_safety(),
        "production_ready_claim": False,
    }

    try:
        rows = await redis.xrevrange(
            audit_stream,
            max="+",
            min="-",
            count=scan_limit_applied,
        )
    except Exception as exc:
        return TerminalDuplicateCleanupAuditReadResult(
            **base_kwargs,
            read_status=AUDIT_READ_FAILED,
            read_error=f"{type(exc).__name__}: {exc}",
            operator_summary=_operator_summary(
                read_status=AUDIT_READ_FAILED,
                entry_count=0,
                latest_status="",
                has_intent_without_outcome=False,
                has_operator_reason_text_exposure=False,
            ),
        )

    raw_rows: Sequence[Any] = tuple(rows or ())
    normalized_rows = [_normalize_stream_row(row) for row in raw_rows]
    raw_entries_scanned = len(normalized_rows)

    matching_entries: list[TerminalDuplicateCleanupAuditEntry] = []
    parse_errors: list[str] = []
    sensitive_exposure = False

    for audit_id, fields in normalized_rows:
        if _text(fields.get("task_id")) != task_id:
            continue

        if _has_sensitive_reason_field(fields):
            sensitive_exposure = True

        entry, errors = _entry_from_fields(audit_id=audit_id, fields=fields)
        if errors:
            parse_errors.extend(f"{audit_id}: {error}" for error in errors)
        if entry is not None:
            matching_entries.append(entry)

    matching_entries.sort(key=lambda entry: _stream_id_sort_key(entry.audit_id))

    command_attempt_ids = {
        entry.command_attempt_id for entry in matching_entries if entry.command_attempt_id
    }

    phases_by_attempt: dict[str, set[str]] = {}
    for entry in matching_entries:
        if not entry.command_attempt_id:
            continue
        phases_by_attempt.setdefault(entry.command_attempt_id, set()).add(entry.event_phase)

    has_intent_without_outcome = any(
        AUDIT_PHASE_INTENT in phases and AUDIT_PHASE_OUTCOME not in phases
        for phases in phases_by_attempt.values()
    )

    latest = matching_entries[-1] if matching_entries else None
    latest_status = latest.status if latest else ""

    if sensitive_exposure:
        read_status = AUDIT_READ_SAFETY_VIOLATION
    elif parse_errors:
        read_status = AUDIT_READ_DEGRADED
    elif matching_entries:
        read_status = AUDIT_READ_OK
    else:
        read_status = AUDIT_READ_EMPTY

    returned_entries: tuple[TerminalDuplicateCleanupAuditEntry, ...]
    if include_entries:
        returned_entries = tuple(matching_entries[-limit_applied:])
    else:
        returned_entries = ()

    read_error = "; ".join(parse_errors[:10])

    return TerminalDuplicateCleanupAuditReadResult(
        **base_kwargs,
        entries=returned_entries,
        entry_count=len(matching_entries),
        command_attempt_count=len(command_attempt_ids),
        latest_command_attempt_id=latest.command_attempt_id if latest else "",
        latest_status=latest_status,
        latest_cleanup_executed=latest.cleanup_executed if latest else False,
        latest_ack_executed=latest.ack_executed if latest else False,
        latest_ack_count=latest.ack_count if latest else 0,
        has_intent_without_outcome=has_intent_without_outcome,
        has_operator_reason_text_exposure=sensitive_exposure,
        read_status=read_status,
        read_error=read_error,
        operator_summary=_operator_summary(
            read_status=read_status,
            entry_count=len(matching_entries),
            latest_status=latest_status,
            has_intent_without_outcome=has_intent_without_outcome,
            has_operator_reason_text_exposure=sensitive_exposure,
        ),
        raw_entries_scanned=raw_entries_scanned,
        possibly_truncated=raw_entries_scanned >= scan_limit_applied,
    )
