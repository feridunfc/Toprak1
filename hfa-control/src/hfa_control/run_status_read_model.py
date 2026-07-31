"""Typed, deterministic, read-only RUN status/result adapter for Sprint 83.3."""
from __future__ import annotations

from dataclasses import asdict, dataclass
from enum import Enum
import json
from typing import Any

from hfa.config.keys import RedisKey


class ExternalRunStatus(str, Enum):
    QUEUED = "QUEUED"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"
    UNKNOWN = "UNKNOWN"


class ProjectionCompleteness(str, Enum):
    RUNNING_WITHOUT_RESULT = "RUNNING_WITHOUT_RESULT"
    TERMINAL_WITH_RESULT = "TERMINAL_WITH_RESULT"
    TERMINAL_WITHOUT_RESULT = "TERMINAL_WITHOUT_RESULT"
    UNKNOWN_RUN = "UNKNOWN_RUN"
    INCOMPLETE_PROJECTION = "INCOMPLETE_PROJECTION"
    CONFLICTING_EVIDENCE = "CONFLICTING_EVIDENCE"
    RESULT_EXPIRED = "RESULT_EXPIRED"
    EVIDENCE_INCOMPLETE = "EVIDENCE_INCOMPLETE"


class ProjectionFreshness(str, Enum):
    CURRENT = "CURRENT"
    EXPIRING = "EXPIRING"
    EXPIRED_OR_MISSING = "EXPIRED_OR_MISSING"
    UNKNOWN = "UNKNOWN"


_INTERNAL_TO_EXTERNAL = {
    "admitted": ExternalRunStatus.QUEUED,
    "queued": ExternalRunStatus.QUEUED,
    "pending": ExternalRunStatus.QUEUED,
    "scheduled": ExternalRunStatus.QUEUED,
    "rescheduled": ExternalRunStatus.QUEUED,
    "running": ExternalRunStatus.RUNNING,
    "done": ExternalRunStatus.COMPLETED,
    "failed": ExternalRunStatus.FAILED,
    "rejected": ExternalRunStatus.FAILED,
    "dead_lettered": ExternalRunStatus.FAILED,
    "cancelled": ExternalRunStatus.CANCELLED,
}
_TERMINAL = {
    ExternalRunStatus.COMPLETED,
    ExternalRunStatus.FAILED,
    ExternalRunStatus.CANCELLED,
}


def _decode(value: Any) -> str:
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return "" if value is None else str(value)


def _decode_mapping(raw: dict[Any, Any] | None) -> dict[str, str]:
    return {_decode(key): _decode(value) for key, value in (raw or {}).items()}


def _safe_int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(_decode(value))
    except (TypeError, ValueError):
        return None


def _safe_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(_decode(value))
    except (TypeError, ValueError):
        return None


def _freshness(ttls: tuple[int, ...], *, exists: bool) -> ProjectionFreshness:
    if not exists:
        return ProjectionFreshness.EXPIRED_OR_MISSING
    positive = [ttl for ttl in ttls if ttl >= 0]
    if not positive:
        return ProjectionFreshness.UNKNOWN
    if min(positive) <= 60:
        return ProjectionFreshness.EXPIRING
    return ProjectionFreshness.CURRENT


@dataclass(frozen=True)
class RunErrorView:
    code: str | None
    summary: str | None


@dataclass(frozen=True)
class RunResultView:
    payload: Any
    cost_cents: int | None
    tokens_used: int | None
    completed_at: float | None
    result_event_id: str | None


@dataclass(frozen=True)
class RunStatusResultView:
    schema_version: int
    run_id: str
    status: ExternalRunStatus
    terminal: bool
    outcome: str | None
    result: RunResultView | None
    error: RunErrorView | None
    submitted_at: float | None
    started_at: float | None
    finished_at: float | None
    updated_at: float | None
    canonical_revision: int | None
    source_transition_id: str | None
    projection_revision: int
    freshness: ProjectionFreshness
    completeness: ProjectionCompleteness
    internal_state: str | None
    task_counts: dict[str, int]
    conflicts: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["status"] = self.status.value
        data["freshness"] = self.freshness.value
        data["completeness"] = self.completeness.value
        return data

    def to_canonical_json(self) -> str:
        return json.dumps(
            self.to_dict(),
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
        )


class DurableRunStatusResultReader:
    """Combine existing durable RUN records without performing any Redis write."""

    def __init__(self, redis: Any) -> None:
        self._redis = redis

    async def read(self, run_id: str) -> RunStatusResultView:
        if not isinstance(run_id, str) or not run_id.strip():
            raise ValueError("run_id must be a non-empty string")
        run_id = run_id.strip()
        state_key = RedisKey.run_state(run_id)
        meta_key = RedisKey.run_meta(run_id)
        result_key = RedisKey.run_result(run_id)

        state_kind, meta_kind, result_kind = [
            _decode(value)
            for value in await self._redis.mget_types(state_key, meta_key, result_key)
        ] if hasattr(self._redis, "mget_types") else (
            _decode(await self._redis.type(state_key)),
            _decode(await self._redis.type(meta_key)),
            _decode(await self._redis.type(result_key)),
        )

        conflicts: list[str] = []
        if state_kind == "string":
            internal_state = _decode(await self._redis.get(state_key)).strip()
            if not internal_state:
                conflicts.append("RUN_STATE_EMPTY")
        elif state_kind == "none":
            internal_state = ""
        else:
            internal_state = ""
            conflicts.append("RUN_STATE_WRONG_TYPE")

        if meta_kind == "hash":
            meta = _decode_mapping(await self._redis.hgetall(meta_key))
        elif meta_kind == "none":
            meta = {}
        else:
            meta = {}
            conflicts.append("RUN_META_WRONG_TYPE")

        if result_kind == "hash":
            result_record = _decode_mapping(await self._redis.hgetall(result_key))
        elif result_kind == "none":
            result_record = {}
        else:
            result_record = {}
            conflicts.append("RUN_RESULT_WRONG_TYPE")

        known = any(kind != "none" for kind in (state_kind, meta_kind, result_kind))
        status = _INTERNAL_TO_EXTERNAL.get(internal_state, ExternalRunStatus.UNKNOWN)
        if internal_state and status is ExternalRunStatus.UNKNOWN:
            conflicts.append("RUN_STATE_UNKNOWN")
        terminal = status in _TERMINAL

        result_view: RunResultView | None = None
        error_view: RunErrorView | None = None
        result_status = result_record.get("status", "")
        payload: Any = None
        if result_record:
            try:
                payload = json.loads(result_record.get("payload", "{}"))
            except (TypeError, ValueError, json.JSONDecodeError):
                conflicts.append("RUN_RESULT_PAYLOAD_INVALID")
                payload = None
            result_view = RunResultView(
                payload=payload,
                cost_cents=_safe_int(result_record.get("cost_cents")),
                tokens_used=_safe_int(result_record.get("tokens_used")),
                completed_at=_safe_float(result_record.get("completed_at")),
                result_event_id=result_record.get("result_event_id") or None,
            )
            error_text = result_record.get("error", "").strip()
            if error_text:
                error_view = RunErrorView(code=error_text, summary=error_text)

        if result_record and not terminal:
            conflicts.append("TERMINAL_RESULT_BEFORE_TERMINAL_STATE")
        if result_status and internal_state and result_status != internal_state:
            conflicts.append("RUN_STATE_RESULT_STATUS_MISMATCH")
        if terminal and result_record and not result_record.get("result_event_id"):
            conflicts.append("TERMINAL_RESULT_EVENT_ID_MISSING")

        result_expected = bool(meta.get("result_event_id"))
        if conflicts:
            completeness = ProjectionCompleteness.CONFLICTING_EVIDENCE
        elif not known:
            completeness = ProjectionCompleteness.UNKNOWN_RUN
        elif terminal and result_record:
            completeness = ProjectionCompleteness.TERMINAL_WITH_RESULT
        elif terminal and result_expected:
            completeness = ProjectionCompleteness.RESULT_EXPIRED
        elif terminal:
            completeness = ProjectionCompleteness.TERMINAL_WITHOUT_RESULT
        elif status in {ExternalRunStatus.QUEUED, ExternalRunStatus.RUNNING}:
            completeness = ProjectionCompleteness.RUNNING_WITHOUT_RESULT
        else:
            completeness = ProjectionCompleteness.INCOMPLETE_PROJECTION

        state_ttl, meta_ttl, result_ttl = await self._redis.ttl(state_key), await self._redis.ttl(meta_key), await self._redis.ttl(result_key)
        freshness = _freshness((int(state_ttl), int(meta_ttl), int(result_ttl)), exists=known)
        finished_at = _safe_float(result_record.get("completed_at")) or (
            (_safe_float(meta.get("finalized_at_ms")) or 0.0) / 1000.0
            if meta.get("finalized_at_ms")
            else None
        )
        task_counts = {
            key: value
            for key in ("task_count", "done_count", "failed_count", "skipped_count")
            if (value := _safe_int(result_record.get(key) or meta.get(key))) is not None
        }
        return RunStatusResultView(
            schema_version=1,
            run_id=run_id,
            status=status,
            terminal=terminal,
            outcome=("SUCCESS" if status is ExternalRunStatus.COMPLETED else "FAILURE" if status is ExternalRunStatus.FAILED else "CANCELLED" if status is ExternalRunStatus.CANCELLED else None),
            result=result_view,
            error=error_view,
            submitted_at=_safe_float(meta.get("admitted_at") or meta.get("submitted_at")),
            started_at=_safe_float(meta.get("started_at")),
            finished_at=finished_at,
            updated_at=finished_at or _safe_float(meta.get("updated_at")),
            canonical_revision=_safe_int(meta.get("revision")),
            source_transition_id=meta.get("source_transition_id") or None,
            projection_revision=1,
            freshness=freshness,
            completeness=completeness,
            internal_state=internal_state or None,
            task_counts=task_counts,
            conflicts=tuple(sorted(set(conflicts))),
        )
