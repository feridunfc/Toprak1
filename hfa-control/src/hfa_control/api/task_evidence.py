
"""
Read-only task evidence reader.

Sprint 66 scope:
- expose evidence already written by Redis/Lua runtime
- do not mutate Redis
- do not trigger retry/reclaim
- do not claim production readiness
"""
from __future__ import annotations

import json
from typing import Any

from hfa.dag.schema import DagRedisKey


def _decode(value: Any) -> str:
    if isinstance(value, bytes):
        return value.decode("utf-8")
    if value is None:
        return ""
    return str(value)


def _normalize_hash(raw: dict[Any, Any]) -> dict[str, str]:
    return {_decode(k): _decode(v) for k, v in (raw or {}).items()}


def _json_or_raw(value: str) -> Any:
    if not value:
        return None
    try:
        return json.loads(value)
    except Exception:
        return value


async def read_task_evidence(redis, task_id: str) -> dict[str, Any]:
    """
    Read existing task evidence only.

    Allowed Redis commands:
    - GET task state
    - HGETALL task metadata
    - GET task output

    Forbidden here:
    - SET/HSET/DEL/XADD/XACK/XCLAIM/retry/reclaim/repair
    """
    state_raw = await redis.get(DagRedisKey.task_state(task_id))
    meta_raw = await redis.hgetall(DagRedisKey.task_meta(task_id))
    output_raw = await redis.get(DagRedisKey.task_output(task_id))

    state = _decode(state_raw)
    meta = _normalize_hash(meta_raw)
    output_text = _decode(output_raw)

    found = bool(state or meta or output_text)

    return {
        "task_id": task_id,
        "run_id": meta.get("run_id", task_id),
        "tenant_id": meta.get("tenant_id", ""),
        "found": found,
        "state": state or "unknown",
        "worker_instance_id": meta.get("worker_instance_id", ""),
        "scheduler_epoch": meta.get("scheduler_epoch", ""),
        "claim_epoch": meta.get("claim_epoch", ""),
        "terminal_state": state if state in {"done", "failed", "cancelled"} else "",
        "completed_at_ms": meta.get("completed_at_ms", ""),
        "reason_code": meta.get("reason_code", ""),
        "output_found": bool(output_text),
        "output": _json_or_raw(output_text),
        "evidence": {
            "state_key": DagRedisKey.task_state(task_id),
            "meta_key": DagRedisKey.task_meta(task_id),
            "output_key": DagRedisKey.task_output(task_id),
            "meta_found": bool(meta),
            "state_found": bool(state),
        },
        "safety": {
            "read_only": True,
            "redis_mutation_attempted": False,
            "runtime_mutation_attempted": False,
            "retry_or_reclaim_attempted": False,
            "production_ready_claim": False,
        },
    }
