
from __future__ import annotations

from typing import Any

from hfa.config.keys import RedisKey
from hfa_control.api.task_evidence import read_task_evidence


DEFAULT_CONSUMER_GROUP = "worker_consumers"


def _decode(value: Any) -> str:
    if isinstance(value, bytes):
        return value.decode("utf-8")
    if value is None:
        return ""
    return str(value)


def _entry_value_contains_task_id(fields: Any, task_id: str) -> bool:
    if not isinstance(fields, dict):
        return task_id in _decode(fields)

    for key, value in fields.items():
        decoded_key = _decode(key)
        decoded_value = _decode(value)

        if decoded_key in {"task_id", "run_id"} and decoded_value == task_id:
            return True

        if task_id in decoded_value:
            return True

    return False


def _normalize_pending_entry(entry: Any) -> dict[str, Any]:
    if isinstance(entry, dict):
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
            or 0
        )
        deliveries = (
            entry.get("times_delivered")
            or entry.get(b"times_delivered")
            or entry.get("deliveries")
            or entry.get(b"deliveries")
            or 0
        )
        return {
            "message_id": _decode(message_id),
            "consumer": _decode(consumer),
            "idle_ms": int(idle_ms or 0),
            "deliveries": int(deliveries or 0),
        }

    if isinstance(entry, (list, tuple)):
        return {
            "message_id": _decode(entry[0]) if len(entry) > 0 else "",
            "consumer": _decode(entry[1]) if len(entry) > 1 else "",
            "idle_ms": int(entry[2] or 0) if len(entry) > 2 else 0,
            "deliveries": int(entry[3] or 0) if len(entry) > 3 else 0,
        }

    return {
        "message_id": _decode(entry),
        "consumer": "",
        "idle_ms": 0,
        "deliveries": 0,
    }


async def _pending_entries(redis, stream: str, group: str, limit: int) -> list[dict[str, Any]]:
    try:
        raw = await redis.xpending_range(stream, group, "-", "+", limit)
    except Exception as exc:
        return [
            {
                "message_id": "",
                "consumer": "",
                "idle_ms": 0,
                "deliveries": 0,
                "error": f"{type(exc).__name__}: {exc}",
            }
        ]

    return [_normalize_pending_entry(entry) for entry in raw]


async def _stream_entry_matches(redis, stream: str, message_id: str, task_id: str) -> bool:
    if not message_id:
        return False

    try:
        entries = await redis.xrange(stream, min=message_id, max=message_id, count=1)
    except Exception:
        return False

    for _entry_id, fields in entries:
        if _entry_value_contains_task_id(fields, task_id):
            return True

    return False


async def read_crash_boundary_evidence(
    redis,
    task_id: str,
    *,
    shard: int = 0,
    stream: str | None = None,
    group: str = DEFAULT_CONSUMER_GROUP,
    pending_limit: int = 100,
) -> dict[str, Any]:
    """
    Read-only diagnostic for the dangerous boundary:

    task completion is already terminal, but the worker stream message is still
    pending/unacknowledged.

    This function only reads evidence. It does not acknowledge, reclaim, requeue,
    retry, repair, or mutate runtime state.
    """
    stream_name = stream or RedisKey.stream_shard(shard)
    task = await read_task_evidence(redis, task_id)
    pending = await _pending_entries(redis, stream_name, group, pending_limit)

    valid_pending = [entry for entry in pending if entry.get("message_id")]
    matching_ids: list[str] = []

    for entry in valid_pending:
        message_id = str(entry.get("message_id", ""))
        if await _stream_entry_matches(redis, stream_name, message_id, task_id):
            matching_ids.append(message_id)

    task_terminal = bool(task.get("terminal_state")) or task.get("state") in {
        "done",
        "failed",
        "cancelled",
        "rejected",
        "dead_lettered",
    }
    task_done = task.get("state") == "done" or task.get("terminal_state") == "done"
    matching_pending = bool(matching_ids)

    terminal_task_with_pending_message = task_terminal and matching_pending
    ack_missing = terminal_task_with_pending_message
    duplicate_execution_risk_visible = terminal_task_with_pending_message

    return {
        "task_id": task_id,
        "stream": stream_name,
        "group": group,
        "task_evidence": task,
        "pending": {
            "count": len(valid_pending),
            "entries": valid_pending,
            "matching_task_message_ids": matching_ids,
            "matching_task_message_pending": matching_pending,
        },
        "boundary": {
            "task_terminal": task_terminal,
            "task_done": task_done,
            "terminal_task_with_pending_message": terminal_task_with_pending_message,
            "ack_missing": ack_missing,
            "duplicate_execution_risk_visible": duplicate_execution_risk_visible,
            "operator_action_required": terminal_task_with_pending_message,
        },
        "safety": {
            "read_only": True,
            "ack_attempted": False,
            "reclaim_attempted": False,
            "requeue_attempted": False,
            "retry_attempted": False,
            "runtime_repair_attempted": False,
            "production_ready_claim": False,
        },
    }
