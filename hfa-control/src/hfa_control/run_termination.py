"""Sprint 83.2 opt-in RUN_TERMINATE coordination boundary.

This module deliberately wraps the existing TASK completion gateway rather than
changing its default composition. RUN termination remains disabled unless this
coordinator is explicitly injected as the TaskConsumer completion manager.
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

from hfa.config.keys import RedisKey, RedisTTL
from hfa.dag.schema import DagRedisKey
from hfa.lua.loader import LuaScriptLoader


def _decode_text(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return str(value)


def _lua_path(filename: str) -> Path:
    here = Path(__file__).resolve()
    candidates = [
        here.parent.parent.parent.parent / "hfa-core" / "src" / "hfa" / "lua" / filename,
        here.parent.parent.parent / "hfa" / "lua" / filename,
    ]
    for path in candidates:
        if path.exists():
            return path
    for parent in here.parents:
        for subdir in ("hfa-core/src/hfa/lua", "hfa/lua"):
            path = parent / subdir / filename
            if path.exists():
                return path
    raise FileNotFoundError(f"Lua script not found: {filename}")


@dataclass(frozen=True)
class RunTerminationResult:
    finalized: bool
    status: str
    run_id: str
    final_state: str = ""
    task_count: int = 0
    done_count: int = 0
    failed_count: int = 0
    skipped_count: int = 0
    already_finalized: bool = False
    ack_allowed: bool = False

    @staticmethod
    def from_lua(raw: list, *, run_id: str) -> "RunTerminationResult":
        def text(index: int) -> str:
            if len(raw) <= index:
                return ""
            return _decode_text(raw[index])

        def number(index: int) -> int:
            try:
                return int(text(index) or 0)
            except (TypeError, ValueError):
                return 0

        return RunTerminationResult(
            finalized=bool(number(0)),
            status=text(1) or "unknown",
            run_id=run_id,
            final_state=text(2),
            task_count=number(3),
            done_count=number(4),
            failed_count=number(5),
            skipped_count=number(6),
            already_finalized=bool(number(7)),
            ack_allowed=bool(number(8)),
        )


@dataclass(frozen=True)
class CoordinatedTaskCompleteResult:
    completed: bool
    status: str
    unlocked_count: int = 0
    already_terminal: bool = False
    task_committed: bool = False
    ack_allowed: bool = False
    run_termination: RunTerminationResult | None = None


class RunTerminationCoordinator:
    """Compose TASK_COMPLETE with a separate idempotent RUN_TERMINATE operation."""

    def __init__(
        self,
        redis: Any,
        task_completion_gateway: Any,
        *,
        enabled: bool = False,
        authority_binding: Any | None = None,
    ) -> None:
        self._redis = redis
        self._task_completion_gateway = task_completion_gateway
        self._enabled = bool(enabled)
        self._authority_binding = authority_binding
        self._loader: Optional[LuaScriptLoader] = None

    @property
    def run_termination_binding_enabled(self) -> bool:
        return self._enabled

    async def initialise(self) -> None:
        loader = LuaScriptLoader(
            self._redis,
            _lua_path("run_terminate_from_tasks.lua"),
        )
        await loader.load()
        self._loader = loader
        if self._authority_binding is not None:
            await self._authority_binding.initialise()

    async def _ensure_initialised(self) -> None:
        if self._loader is None:
            await self.initialise()

    async def finalize_run_from_tasks(
        self,
        *,
        run_id: str,
        tenant_id: str,
        trigger_task_id: str,
        finalized_at_ms: int,
        worker_instance_id: str = "",
        trigger_terminal_state: str = "",
    ) -> RunTerminationResult:
        if not self._enabled:
            return RunTerminationResult(
                finalized=False,
                status="run_termination_binding_disabled",
                run_id=run_id,
                ack_allowed=False,
            )

        await self._ensure_initialised()
        if self._authority_binding is not None:
            canonical = await self._authority_binding.terminate(
                run_id=run_id,
                tenant_id=tenant_id,
                trigger_task_id=trigger_task_id,
                finalized_at_ms=finalized_at_ms,
                worker_instance_id=worker_instance_id,
                trigger_terminal_state=trigger_terminal_state,
            )
            return RunTerminationResult(
                finalized=bool(canonical.finalized),
                status=str(canonical.status),
                run_id=run_id,
                final_state=str(canonical.final_state),
                task_count=int(canonical.task_count),
                done_count=int(canonical.done_count),
                failed_count=int(canonical.failed_count),
                skipped_count=int(canonical.skipped_count),
                already_finalized=bool(canonical.already_finalized),
                ack_allowed=bool(canonical.ack_allowed),
            )

        assert self._loader is not None
        keys = [
            RedisKey.run_state(run_id),
            RedisKey.run_meta(run_id),
            RedisKey.run_result(run_id),
            RedisKey.cp_running(),
            DagRedisKey.run_tasks(run_id),
            RedisKey.stream_results(),
            RedisKey.runtime_truth_conflict_index(),
            RedisKey.runtime_truth_conflict_stream(),
        ]
        args = [
            run_id,
            tenant_id,
            trigger_task_id,
            str(finalized_at_ms),
            str(int(getattr(RedisTTL, "RUN_STATE", 86400))),
            str(int(getattr(RedisTTL, "RUN_META", 86400))),
            str(int(getattr(RedisTTL, "RUN_RESULT", 86400))),
            str(int(getattr(RedisTTL, "STREAM_MAXLEN", 100000))),
            DagRedisKey.task_state_prefix(),
            ":state",
            DagRedisKey.task_meta_prefix(),
            ":meta",
            worker_instance_id or "",
            trigger_terminal_state or "",
        ]
        raw = await self._loader.run(num_keys=len(keys), keys=keys, args=args)
        return RunTerminationResult.from_lua(raw, run_id=run_id)

    async def task_complete(self, **kwargs: Any) -> Any:
        task_result = await self._task_completion_gateway.task_complete(**kwargs)
        if not bool(getattr(task_result, "completed", False)) or not self._enabled:
            return task_result

        run_id = str(kwargs.get("run_id") or "").strip()
        task_id = str(kwargs.get("task_id") or "").strip()
        tenant_id = str(kwargs.get("tenant_id") or "").strip()
        finalization = await self.finalize_run_from_tasks(
            run_id=run_id,
            tenant_id=tenant_id,
            trigger_task_id=task_id,
            finalized_at_ms=int(kwargs.get("finished_at_ms") or 0),
            worker_instance_id=str(kwargs.get("worker_instance_id") or ""),
            trigger_terminal_state=str(kwargs.get("terminal_state") or ""),
        )
        coordinated = bool(finalization.ack_allowed)
        return CoordinatedTaskCompleteResult(
            completed=coordinated,
            status=(
                str(getattr(task_result, "status", "completed"))
                if coordinated
                else finalization.status
            ),
            unlocked_count=int(getattr(task_result, "unlocked_count", 0) or 0),
            already_terminal=bool(
                getattr(task_result, "already_terminal", False)
            ),
            task_committed=True,
            ack_allowed=coordinated,
            run_termination=finalization,
        )
