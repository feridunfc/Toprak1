"""
hfa_control/idempotent_dispatch.py
------------------------------------
IRONCLAD Sprint 0/1 — Idempotent dispatch guard.

Provides a pre-flight check before calling DagLua.task_dispatch_commit().
Uses IdempotencyStore to detect duplicate dispatch attempts.

Status codes align with task_dispatch_commit.lua return values:
  committed            — first dispatch, accepted
  already_scheduled    — task was already dispatched (idempotent duplicate)
  already_running      — task already claimed by a worker
  illegal_transition   — task in terminal or unexpected state
"""
from __future__ import annotations

from dataclasses import dataclass

from hfa.runtime.idempotency_store import IdempotencyStore


@dataclass(frozen=True)
class IdempotentDispatchResult:
    ok: bool
    status: str            # committed | already_scheduled | already_running | duplicate_rejected
    token_key: str = ""
    existing_value: str = ""


class IdempotentDispatchGuard:
    """
    Pre-flight idempotency check for task dispatch.

    Call guard() before calling DagLua.task_dispatch_commit().
    If guard() returns ok=False and status="already_scheduled", treat as
    success (idempotent duplicate).
    """

    def __init__(self, store: IdempotencyStore) -> None:
        self._store = store

    async def guard(
        self,
        *,
        task_id: str,
        run_id: str,
        worker_id: str,
    ) -> IdempotentDispatchResult:
        token_value = f"{run_id}:{worker_id}"
        result = await self._store.acquire_dispatch_token(
            task_id=task_id,
            token_value=token_value,
        )
        if result.accepted:
            return IdempotentDispatchResult(
                ok=True,
                status="dispatch_token_acquired",
                token_key=result.token_key,
            )
        if result.reason in ("already_exists", "duplicate"):
            return IdempotentDispatchResult(
                ok=False,
                status="already_scheduled",
                token_key=result.token_key,
                existing_value=result.existing_value,
            )
        return IdempotentDispatchResult(
            ok=False,
            status=result.reason,
            token_key=result.token_key,
            existing_value=result.existing_value,
        )
