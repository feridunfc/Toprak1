"""
hfa_control/idempotent_completion.py
--------------------------------------
IRONCLAD Sprint 2 — Idempotent completion guard with epoch-aware status mapping.

Sprint 2 change: guard() classifies the new fence-failure status codes from
task_complete.lua so callers can distinguish:
  - already_terminal      → idempotent duplicate, treat as success
  - owner_mismatch        → stale worker, reject
  - scheduler_epoch_mismatch → stale scheduler context, reject
  - claim_epoch_mismatch  → stale claim generation, reject

These are structural, not ambiguous — each maps to a clear caller action.
"""
from __future__ import annotations

from dataclasses import dataclass

from hfa.runtime.idempotency_store import IdempotencyStore

# Status codes that represent "this completion is a clean idempotent duplicate"
_IDEMPOTENT_STATUSES = frozenset({"already_terminal", "already_exists", "duplicate"})

# Status codes that represent a rejected stale/zombie completion
_FENCE_REJECTION_STATUSES = frozenset({
    "owner_mismatch",
    "scheduler_epoch_mismatch",
    "claim_epoch_mismatch",
    "illegal_transition",
})


@dataclass(frozen=True)
class IdempotentCompletionResult:
    ok: bool
    status: str
    is_duplicate: bool = False    # True = idempotent dup, treat as success
    is_fence_rejected: bool = False  # True = zombie/stale, hard reject
    token_key: str = ""
    existing_value: str = ""


class IdempotentCompletionGuard:
    """
    Pre-flight idempotency check before calling DagLua.task_complete().

    After task_complete.lua is called, its return code is the authoritative
    fence result.  This guard is a best-effort pre-check to avoid redundant
    Lua calls for known duplicates.
    """

    def __init__(self, store: IdempotencyStore) -> None:
        self._store = store

    async def guard(
        self,
        *,
        task_id: str,
        run_id: str,
        worker_id: str,
    ) -> IdempotentCompletionResult:
        token_value = f"{run_id}:{worker_id}"
        result = await self._store.acquire_completion_token(
            task_id=task_id,
            token_value=token_value,
        )
        if result.accepted:
            return IdempotentCompletionResult(
                ok=True,
                status="completion_token_acquired",
                token_key=result.token_key,
            )
        reason = result.reason
        is_dup      = reason in _IDEMPOTENT_STATUSES
        is_rejected = reason in _FENCE_REJECTION_STATUSES
        return IdempotentCompletionResult(
            ok=False,
            status=reason if not is_dup else "already_terminal",
            is_duplicate=is_dup,
            is_fence_rejected=is_rejected,
            token_key=result.token_key,
            existing_value=result.existing_value,
        )

    @staticmethod
    def classify(lua_status: str) -> IdempotentCompletionResult:
        """
        Classify the raw status string returned by DagLua.task_complete()
        into a structured result without touching the idempotency store.

        Use this after a Lua call to produce a consistent result object.
        """
        is_dup      = lua_status in _IDEMPOTENT_STATUSES
        is_rejected = lua_status in _FENCE_REJECTION_STATUSES
        ok          = lua_status == "committed"
        return IdempotentCompletionResult(
            ok=ok,
            status=lua_status,
            is_duplicate=is_dup,
            is_fence_rejected=is_rejected,
        )
