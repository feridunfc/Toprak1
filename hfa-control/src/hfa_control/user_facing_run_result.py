"""Read-only single-task output adapter for the user-facing RUN result."""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
import json
from typing import Any

from hfa.dag.schema import DagRedisKey
from hfa_control.run_status_read_model import (
    DurableRunStatusResultReader,
    ExternalRunStatus,
    ReadCompleteness,
    RunStatusResultView,
)


class TaskOutputStatus(str, Enum):
    RUN_UNKNOWN = "RUN_UNKNOWN"
    NOT_TERMINAL = "NOT_TERMINAL"
    RUN_EVIDENCE_INCOMPLETE = "RUN_EVIDENCE_INCOMPLETE"
    TASK_MEMBERSHIP_MISSING = "TASK_MEMBERSHIP_MISSING"
    TASK_MEMBERSHIP_WRONG_TYPE = "TASK_MEMBERSHIP_WRONG_TYPE"
    NOT_A_SINGLE_TASK_RUN = "NOT_A_SINGLE_TASK_RUN"
    TASK_IDENTITY_UNAVAILABLE = "TASK_IDENTITY_UNAVAILABLE"
    TASK_IDENTITY_CONFLICT = "TASK_IDENTITY_CONFLICT"
    TASK_STATE_UNAVAILABLE = "TASK_STATE_UNAVAILABLE"
    TASK_STATE_CONFLICT = "TASK_STATE_CONFLICT"
    TERMINAL_OUTPUT_MISSING = "TERMINAL_OUTPUT_MISSING"
    OUTPUT_WRONG_TYPE = "OUTPUT_WRONG_TYPE"
    OUTPUT_MALFORMED = "OUTPUT_MALFORMED"
    AVAILABLE = "AVAILABLE"


def _decode(value: Any) -> str:
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return "" if value is None else str(value)


def _decode_mapping(
    raw: dict[Any, Any] | None,
) -> dict[str, str]:
    return {
        _decode(key): _decode(value)
        for key, value in (raw or {}).items()
    }


@dataclass(frozen=True)
class UserFacingSingleTaskRunView:
    run: RunStatusResultView
    task_output_status: TaskOutputStatus
    task_id: str | None
    task_state: str | None
    task_output: Any
    task_output_issues: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        data = self.run.to_dict()
        data.update(
            {
                "task_output_status":
                    self.task_output_status.value,
                "task_id": self.task_id,
                "task_state": self.task_state,
                "task_output": self.task_output,
                "task_output_issues":
                    list(self.task_output_issues),
            }
        )
        return data

    def to_canonical_json(self) -> str:
        return json.dumps(
            self.to_dict(),
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
        )


class UserFacingSingleTaskRunReader:
    """Expose one canonical TASK output without mutating Redis."""

    def __init__(
        self,
        redis: Any,
        *,
        run_reader: DurableRunStatusResultReader | None = None,
    ) -> None:
        self._redis = redis
        self._run_reader = (
            run_reader
            if run_reader is not None
            else DurableRunStatusResultReader(redis)
        )

    def _view(
        self,
        run: RunStatusResultView,
        status: TaskOutputStatus,
        *,
        task_id: str | None = None,
        task_state: str | None = None,
        task_output: Any = None,
        issues: tuple[str, ...] = (),
    ) -> UserFacingSingleTaskRunView:
        return UserFacingSingleTaskRunView(
            run=run,
            task_output_status=status,
            task_id=task_id,
            task_state=task_state,
            task_output=task_output,
            task_output_issues=tuple(sorted(set(issues))),
        )

    async def read(
        self,
        run_id: str,
    ) -> UserFacingSingleTaskRunView:
        run = await self._run_reader.read(run_id)

        if (
            run.status is ExternalRunStatus.UNKNOWN
            and run.completeness
            is ReadCompleteness.UNKNOWN_RUN
        ):
            return self._view(
                run,
                TaskOutputStatus.RUN_UNKNOWN,
            )

        if not run.terminal:
            return self._view(
                run,
                TaskOutputStatus.NOT_TERMINAL,
            )

        if (
            run.completeness
            is not ReadCompleteness.TERMINAL_WITH_RESULT
        ):
            return self._view(
                run,
                TaskOutputStatus.RUN_EVIDENCE_INCOMPLETE,
                issues=(
                    "RUN_TERMINAL_EVIDENCE_NOT_COMPLETE",
                ),
            )

        membership_key = DagRedisKey.run_tasks(run.run_id)
        membership_kind = _decode(
            await self._redis.type(membership_key)
        )
        if membership_kind == "none":
            return self._view(
                run,
                TaskOutputStatus.TASK_MEMBERSHIP_MISSING,
                issues=("RUN_TASK_MEMBERSHIP_MISSING",),
            )
        if membership_kind != "set":
            return self._view(
                run,
                TaskOutputStatus.TASK_MEMBERSHIP_WRONG_TYPE,
                issues=("RUN_TASK_MEMBERSHIP_WRONG_TYPE",),
            )

        raw_members = await self._redis.smembers(
            membership_key
        )
        task_ids = sorted(
            {
                task_id
                for item in (raw_members or ())
                if (task_id := _decode(item).strip())
            }
        )
        if not task_ids:
            return self._view(
                run,
                TaskOutputStatus.TASK_MEMBERSHIP_MISSING,
                issues=("RUN_TASK_MEMBERSHIP_EMPTY",),
            )
        if len(task_ids) != 1:
            return self._view(
                run,
                TaskOutputStatus.NOT_A_SINGLE_TASK_RUN,
                issues=(
                    f"RUN_TASK_MEMBERSHIP_COUNT_{len(task_ids)}",
                ),
            )

        task_id = task_ids[0]
        meta_key = DagRedisKey.task_meta(task_id)
        state_key = DagRedisKey.task_state(task_id)
        output_key = DagRedisKey.task_output(task_id)

        meta_kind = _decode(
            await self._redis.type(meta_key)
        )
        if meta_kind != "hash":
            return self._view(
                run,
                TaskOutputStatus.TASK_IDENTITY_UNAVAILABLE,
                task_id=task_id,
                issues=(
                    "TASK_META_MISSING"
                    if meta_kind == "none"
                    else "TASK_META_WRONG_TYPE",
                ),
            )

        meta = _decode_mapping(
            await self._redis.hgetall(meta_key)
        )
        authoritative_task_id = (
            meta.get("task_id", "").strip()
        )
        authoritative_run_id = (
            meta.get("run_id", "").strip()
        )
        if (
            not authoritative_task_id
            or not authoritative_run_id
        ):
            return self._view(
                run,
                TaskOutputStatus.TASK_IDENTITY_UNAVAILABLE,
                task_id=task_id,
                issues=("TASK_IDENTITY_FIELDS_MISSING",),
            )
        if (
            authoritative_task_id != task_id
            or authoritative_run_id != run.run_id
        ):
            return self._view(
                run,
                TaskOutputStatus.TASK_IDENTITY_CONFLICT,
                task_id=task_id,
                issues=("TASK_IDENTITY_MISMATCH",),
            )

        state_kind = _decode(
            await self._redis.type(state_key)
        )
        if state_kind != "string":
            return self._view(
                run,
                TaskOutputStatus.TASK_STATE_UNAVAILABLE,
                task_id=task_id,
                issues=(
                    "TASK_STATE_MISSING"
                    if state_kind == "none"
                    else "TASK_STATE_WRONG_TYPE",
                ),
            )

        task_state = _decode(
            await self._redis.get(state_key)
        ).strip()
        expected_task_state = (
            "done"
            if run.status is ExternalRunStatus.COMPLETED
            else "failed"
        )
        if task_state != expected_task_state:
            return self._view(
                run,
                TaskOutputStatus.TASK_STATE_CONFLICT,
                task_id=task_id,
                task_state=task_state or None,
                issues=(
                    "RUN_TASK_TERMINAL_STATE_MISMATCH",
                ),
            )

        output_kind = _decode(
            await self._redis.type(output_key)
        )
        if output_kind == "none":
            return self._view(
                run,
                TaskOutputStatus.TERMINAL_OUTPUT_MISSING,
                task_id=task_id,
                task_state=task_state,
                issues=("TASK_OUTPUT_MISSING",),
            )
        if output_kind != "string":
            return self._view(
                run,
                TaskOutputStatus.OUTPUT_WRONG_TYPE,
                task_id=task_id,
                task_state=task_state,
                issues=("TASK_OUTPUT_WRONG_TYPE",),
            )

        raw_output = _decode(
            await self._redis.get(output_key)
        )
        if not raw_output.strip():
            return self._view(
                run,
                TaskOutputStatus.OUTPUT_MALFORMED,
                task_id=task_id,
                task_state=task_state,
                issues=("TASK_OUTPUT_EMPTY",),
            )
        try:
            task_output = json.loads(raw_output)
        except (
            TypeError,
            ValueError,
            json.JSONDecodeError,
        ):
            return self._view(
                run,
                TaskOutputStatus.OUTPUT_MALFORMED,
                task_id=task_id,
                task_state=task_state,
                issues=("TASK_OUTPUT_JSON_INVALID",),
            )

        return self._view(
            run,
            TaskOutputStatus.AVAILABLE,
            task_id=task_id,
            task_state=task_state,
            task_output=task_output,
        )
