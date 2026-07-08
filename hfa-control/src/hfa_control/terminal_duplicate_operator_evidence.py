"""Read-only operator evidence for terminal duplicate cleanup candidates.

Sprint 70 product read-model.

This module answers one operator question:

    Is this pending terminal duplicate stream message a safe cleanup candidate,
    or does it require operator attention because identity/evidence is unclear?

It intentionally does not mutate runtime state.

Forbidden here:
- XACK
- XCLAIM
- XADD
- SET/HSET/DEL
- EXPIRE
- requeue
- repair
- cleanup execution
- production-ready claim

Important distinctions:

    ack_allowed != cleanup_executed
    cleanup_candidate != cleanup_done
    operator_action_required != mutation_allowed
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping, Sequence

from hfa.dag.schema import DagRedisKey


TERMINAL_STATES = frozenset(
    {
        "done",
        "failed",
        "cancelled",
        "dead_lettered",
        "rejected",
        "completed",
    }
)

EXPLICIT_TERMINAL_DUPLICATE_CLEANUP_CANDIDATE = (
    "EXPLICIT_TERMINAL_DUPLICATE_CLEANUP_CANDIDATE"
)
NO_ACK_WITHOUT_EXPLICIT_TASK_IDENTITY = "NO_ACK_WITHOUT_EXPLICIT_TASK_IDENTITY"
NO_ACK_TASK_ID_MISMATCH = "NO_ACK_TASK_ID_MISMATCH"
NO_ACK_RUN_ID_MISMATCH = "NO_ACK_RUN_ID_MISMATCH"
NO_ACK_TASK_META_RUN_ID_MISSING = "NO_ACK_TASK_META_RUN_ID_MISSING"
NO_ACK_TASK_META_RUN_ID_MISMATCH = "NO_ACK_TASK_META_RUN_ID_MISMATCH"
NO_ACK_TASK_NOT_TERMINAL = "NO_ACK_TASK_NOT_TERMINAL"
NO_PENDING_STREAM_MESSAGE = "NO_PENDING_STREAM_MESSAGE"
EVIDENCE_READ_DEGRADED = "EVIDENCE_READ_DEGRADED"

ACK_POLICY_NOT_TERMINAL = "not_terminal"
ACK_POLICY_NO_ACK_WITHOUT_EXPLICIT_TASK_IDENTITY = (
    "no_ack_without_explicit_task_identity"
)
ACK_POLICY_NO_ACK_WITHOUT_RUN_ID_MATCH = "no_ack_without_run_id_match"
ACK_POLICY_NO_ACK_WITHOUT_TERMINAL_EVIDENCE = "no_ack_without_terminal_evidence"
ACK_POLICY_ACK_EXPLICIT_TASK_RUN_TERMINAL_EVIDENCE = (
    "ack_explicit_task_run_terminal_evidence"
)


@dataclass(frozen=True)
class PendingTerminalDuplicateMessageEvidence:
    message_id: str
    message_task_id: str = ""
    message_run_id: str = ""
    consumer: str = ""
    idle_ms: int = 0
    deliveries: int = 0
    raw_fields_found: bool = False


@dataclass(frozen=True)
class TerminalDuplicateOperatorEvidence:
    task_id: str
    run_id: str
    task_state: str
    terminal: bool

    stream_pending: bool
    pending_message_id: str

    message_task_id: str
    message_run_id: str
    task_meta_run_id: str

    message_identity_verified: bool
    terminal_evidence_verified: bool

    ack_allowed: bool
    ack_policy: str

    cleanup_candidate: bool
    cleanup_done: bool
    cleanup_executed: bool
    operator_action_required: bool
    reason: str
    evidence_status: str

    read_only: bool = True
    mutation_allowed: bool = False
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
    out: dict[str, str] = {}
    for key, value in raw.items():
        out[_decode(key)] = _decode(value)
    return out


def _field(fields: Mapping[Any, Any], name: str) -> str:
    return _decode(fields.get(name) or fields.get(name.encode("utf-8")) or "")


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
        consumer = entry.get("consumer") or entry.get(b"consumer") or ""
        idle_ms = (
            entry.get("time_since_delivered")
            or entry.get(b"time_since_delivered")
            or entry.get("idle")
            or entry.get(b"idle")
            or entry.get("idle_ms")
            or entry.get(b"idle_ms")
            or 0
        )
        deliveries = (
            entry.get("times_delivered")
            or entry.get(b"times_delivered")
            or entry.get("deliveries")
            or entry.get(b"deliveries")
            or entry.get("delivery_count")
            or entry.get(b"delivery_count")
            or 0
        )
        return {
            "message_id": _decode(message_id),
            "consumer": _decode(consumer),
            "idle_ms": int(idle_ms or 0),
            "deliveries": int(deliveries or 0),
        }

    if isinstance(entry, Sequence) and not isinstance(entry, (str, bytes, bytearray)):
        items = list(entry)
        return {
            "message_id": _decode(items[0]) if len(items) > 0 else "",
            "consumer": _decode(items[1]) if len(items) > 1 else "",
            "idle_ms": int(items[2] or 0) if len(items) > 2 else 0,
            "deliveries": int(items[3] or 0) if len(items) > 3 else 0,
        }

    return {
        "message_id": _decode(entry),
        "consumer": "",
        "idle_ms": 0,
        "deliveries": 0,
    }


async def _read_pending_entries(
    redis: Any,
    *,
    stream_key: str,
    consumer_group: str,
    limit: int,
) -> list[dict[str, Any]]:
    raw = await redis.xpending_range(stream_key, consumer_group, "-", "+", limit)
    return [_normalize_pending_entry(entry) for entry in raw or []]


async def _read_message_fields(
    redis: Any,
    *,
    stream_key: str,
    message_id: str,
) -> dict[str, str]:
    if not message_id:
        return {}

    raw = await redis.xrange(stream_key, message_id, message_id)
    if not raw:
        return {}

    first = raw[0]
    if isinstance(first, Sequence) and not isinstance(first, (str, bytes, bytearray)):
        items = list(first)
        if len(items) >= 2:
            return _normalize_stream_fields(items[1])

    if isinstance(first, Mapping):
        return _normalize_stream_fields(first)

    return {}


def _matches_task_or_run(
    *,
    fields: Mapping[str, str],
    task_id: str,
    task_meta_run_id: str,
) -> bool:
    message_task_id = fields.get("task_id", "")
    message_run_id = fields.get("run_id", "")

    if message_task_id and message_task_id == task_id:
        return True
    if message_run_id and task_meta_run_id and message_run_id == task_meta_run_id:
        return True
    if message_run_id and message_run_id == task_id:
        return True
    return False


async def _find_pending_message_for_task(
    redis: Any,
    *,
    stream_key: str,
    consumer_group: str,
    task_id: str,
    task_meta_run_id: str,
    limit: int,
) -> PendingTerminalDuplicateMessageEvidence | None:
    for pending in await _read_pending_entries(
        redis,
        stream_key=stream_key,
        consumer_group=consumer_group,
        limit=limit,
    ):
        message_id = str(pending.get("message_id") or "")
        fields = await _read_message_fields(redis, stream_key=stream_key, message_id=message_id)
        if not _matches_task_or_run(
            fields=fields,
            task_id=task_id,
            task_meta_run_id=task_meta_run_id,
        ):
            continue

        return PendingTerminalDuplicateMessageEvidence(
            message_id=message_id,
            message_task_id=fields.get("task_id", ""),
            message_run_id=fields.get("run_id", ""),
            consumer=str(pending.get("consumer") or ""),
            idle_ms=int(pending.get("idle_ms") or 0),
            deliveries=int(pending.get("deliveries") or 0),
            raw_fields_found=bool(fields),
        )

    return None


def evaluate_terminal_duplicate_operator_evidence(
    *,
    task_id: str,
    task_state: str,
    task_meta_run_id: str,
    pending_message: PendingTerminalDuplicateMessageEvidence | None,
) -> TerminalDuplicateOperatorEvidence:
    terminal = task_state in TERMINAL_STATES
    stream_pending = pending_message is not None

    message_id = pending_message.message_id if pending_message else ""
    message_task_id = pending_message.message_task_id if pending_message else ""
    message_run_id = pending_message.message_run_id if pending_message else ""

    run_id = task_meta_run_id or message_run_id or task_id

    message_task_identity_verified = bool(message_task_id) and message_task_id == task_id
    message_run_identity_verified = bool(message_run_id) and bool(task_meta_run_id) and (
        message_run_id == task_meta_run_id
    )
    message_identity_verified = (
        message_task_identity_verified and message_run_identity_verified
    )
    terminal_evidence_verified = terminal and bool(task_meta_run_id)

    if not stream_pending:
        reason = NO_PENDING_STREAM_MESSAGE
        ack_allowed = False
        ack_policy = "no_pending_stream_message"
        cleanup_candidate = False
        operator_action_required = False
        evidence_status = "no_pending_message"
    elif not terminal:
        reason = NO_ACK_TASK_NOT_TERMINAL
        ack_allowed = False
        ack_policy = ACK_POLICY_NOT_TERMINAL
        cleanup_candidate = False
        operator_action_required = True
        evidence_status = "operator_attention_required"
    elif not message_task_id:
        reason = NO_ACK_WITHOUT_EXPLICIT_TASK_IDENTITY
        ack_allowed = False
        ack_policy = ACK_POLICY_NO_ACK_WITHOUT_EXPLICIT_TASK_IDENTITY
        cleanup_candidate = False
        operator_action_required = True
        evidence_status = "operator_attention_required"
    elif message_task_id != task_id:
        reason = NO_ACK_TASK_ID_MISMATCH
        ack_allowed = False
        ack_policy = ACK_POLICY_NO_ACK_WITHOUT_EXPLICIT_TASK_IDENTITY
        cleanup_candidate = False
        operator_action_required = True
        evidence_status = "operator_attention_required"
    elif not message_run_id:
        reason = NO_ACK_RUN_ID_MISMATCH
        ack_allowed = False
        ack_policy = ACK_POLICY_NO_ACK_WITHOUT_RUN_ID_MATCH
        cleanup_candidate = False
        operator_action_required = True
        evidence_status = "operator_attention_required"
    elif not task_meta_run_id:
        reason = NO_ACK_TASK_META_RUN_ID_MISSING
        ack_allowed = False
        ack_policy = ACK_POLICY_NO_ACK_WITHOUT_TERMINAL_EVIDENCE
        cleanup_candidate = False
        operator_action_required = True
        evidence_status = "operator_attention_required"
    elif message_run_id != task_meta_run_id:
        reason = NO_ACK_TASK_META_RUN_ID_MISMATCH
        ack_allowed = False
        ack_policy = ACK_POLICY_NO_ACK_WITHOUT_RUN_ID_MATCH
        cleanup_candidate = False
        operator_action_required = True
        evidence_status = "operator_attention_required"
    else:
        reason = EXPLICIT_TERMINAL_DUPLICATE_CLEANUP_CANDIDATE
        ack_allowed = True
        ack_policy = ACK_POLICY_ACK_EXPLICIT_TASK_RUN_TERMINAL_EVIDENCE
        cleanup_candidate = True
        operator_action_required = False
        evidence_status = "cleanup_candidate"

    return TerminalDuplicateOperatorEvidence(
        task_id=task_id,
        run_id=run_id,
        task_state=task_state or "unknown",
        terminal=terminal,
        stream_pending=stream_pending,
        pending_message_id=message_id,
        message_task_id=message_task_id,
        message_run_id=message_run_id,
        task_meta_run_id=task_meta_run_id,
        message_identity_verified=message_identity_verified,
        terminal_evidence_verified=terminal_evidence_verified,
        ack_allowed=ack_allowed,
        ack_policy=ack_policy,
        cleanup_candidate=cleanup_candidate,
        cleanup_done=False,
        cleanup_executed=False,
        operator_action_required=operator_action_required,
        reason=reason,
        evidence_status=evidence_status,
        read_only=True,
        mutation_allowed=False,
        production_ready_claim=False,
        metadata={
            "read_model": "terminal_duplicate_operator_evidence",
            "pending_message_consumer": pending_message.consumer if pending_message else "",
            "pending_message_idle_ms": pending_message.idle_ms if pending_message else 0,
            "pending_message_deliveries": pending_message.deliveries if pending_message else 0,
            "pending_message_fields_found": (
                pending_message.raw_fields_found if pending_message else False
            ),
        },
    )


async def read_terminal_duplicate_operator_evidence(
    redis: Any,
    *,
    task_id: str,
    stream_key: str,
    consumer_group: str,
    pending_limit: int = 100,
) -> TerminalDuplicateOperatorEvidence:
    try:
        state_raw = await redis.get(DagRedisKey.task_state(task_id))
        meta_raw = await redis.hgetall(DagRedisKey.task_meta(task_id))

        task_state = _decode(state_raw)
        meta = _normalize_hash(meta_raw)
        task_meta_run_id = meta.get("run_id", "")

        pending_message = await _find_pending_message_for_task(
            redis,
            stream_key=stream_key,
            consumer_group=consumer_group,
            task_id=task_id,
            task_meta_run_id=task_meta_run_id,
            limit=pending_limit,
        )

        return evaluate_terminal_duplicate_operator_evidence(
            task_id=task_id,
            task_state=task_state,
            task_meta_run_id=task_meta_run_id,
            pending_message=pending_message,
        )
    except Exception as exc:
        return TerminalDuplicateOperatorEvidence(
            task_id=task_id,
            run_id="",
            task_state="unknown",
            terminal=False,
            stream_pending=False,
            pending_message_id="",
            message_task_id="",
            message_run_id="",
            task_meta_run_id="",
            message_identity_verified=False,
            terminal_evidence_verified=False,
            ack_allowed=False,
            ack_policy="evidence_read_degraded",
            cleanup_candidate=False,
            cleanup_done=False,
            cleanup_executed=False,
            operator_action_required=True,
            reason=EVIDENCE_READ_DEGRADED,
            evidence_status="degraded",
            read_only=True,
            mutation_allowed=False,
            production_ready_claim=False,
            metadata={
                "read_model": "terminal_duplicate_operator_evidence",
                "degraded_reason": str(exc),
            },
        )
