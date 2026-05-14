"""
hfa_control/task_ownership.py
-------------------------------
IRONCLAD Sprint 2 — Task ownership fencing helpers.

Sprint 2 change: OwnershipFence now understands the full fencing tuple:
  (worker_instance_id, scheduler_epoch, claim_epoch)

The authoritative fence check is inside task_complete.lua and
task_claim_start.lua (Lua scripts are the Redis-level authority).
These helpers are used for Python-side pre-flight validation and
logging only — they do NOT replace the Lua fence.
"""
from __future__ import annotations

from dataclasses import dataclass

from hfa.dag.reasons import TASK_OWNERSHIP_FENCED
from hfa.dag.schema import DagRedisKey, TaskMetaField


# ── Fence tuple ───────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class OwnershipFenceToken:
    """
    The complete fencing tuple established at claim time.

    Workers must carry this token and pass all three values to
    task_complete and heartbeat calls.
    """
    worker_instance_id: str
    scheduler_epoch: str
    claim_epoch: str


# ── Result type ───────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class OwnershipCheckResult:
    ok: bool
    status: str   # OK | TASK_OWNERSHIP_FENCED | scheduler_epoch_mismatch | claim_epoch_mismatch


# ── Fence helper ──────────────────────────────────────────────────────────────

class TaskOwnershipFence:
    """
    Stateless ownership fence check for Python pre-flight validation.

    The Lua scripts are the authoritative fence; this class exists to
    produce structured error reasons for logging and metrics before the
    Lua CAS runs.
    """

    # Canonical hash field names — match TaskMetaField constants.
    OWNER_FIELD          = TaskMetaField.WORKER_INSTANCE_ID
    SCHEDULER_EPOCH_FIELD = TaskMetaField.SCHEDULER_EPOCH
    CLAIM_EPOCH_FIELD    = TaskMetaField.CLAIM_EPOCH

    @staticmethod
    def check(current_owner: str | None, candidate_owner: str) -> OwnershipCheckResult:
        """Simple owner-only check (backward compat, used by heartbeat path)."""
        if current_owner in (None, "", candidate_owner):
            return OwnershipCheckResult(ok=True, status="OK")
        return OwnershipCheckResult(ok=False, status=TASK_OWNERSHIP_FENCED)

    @staticmethod
    def check_full(
        stored: OwnershipFenceToken,
        expected: OwnershipFenceToken,
    ) -> OwnershipCheckResult:
        """
        Full Sprint 2 fence check against all three fields.

        Empty expected fields are treated as "skip this check" for
        backward compatibility with callers that only have partial context.
        """
        if expected.worker_instance_id and stored.worker_instance_id != expected.worker_instance_id:
            return OwnershipCheckResult(ok=False, status=TASK_OWNERSHIP_FENCED)
        if expected.scheduler_epoch and stored.scheduler_epoch != expected.scheduler_epoch:
            return OwnershipCheckResult(ok=False, status="scheduler_epoch_mismatch")
        if expected.claim_epoch and stored.claim_epoch != expected.claim_epoch:
            return OwnershipCheckResult(ok=False, status="claim_epoch_mismatch")
        return OwnershipCheckResult(ok=True, status="OK")

    @staticmethod
    async def read_fence_token(redis, task_id: str) -> OwnershipFenceToken:
        """Read the current ownership fence token from Redis meta hash."""
        raw = await redis.hmget(
            DagRedisKey.task_meta(task_id),
            TaskMetaField.WORKER_INSTANCE_ID,
            TaskMetaField.SCHEDULER_EPOCH,
            TaskMetaField.CLAIM_EPOCH,
        )

        def _d(v) -> str:
            if isinstance(v, bytes):
                return v.decode("utf-8", errors="replace")
            return str(v) if v is not None else ""

        return OwnershipFenceToken(
            worker_instance_id=_d(raw[0]),
            scheduler_epoch=_d(raw[1]),
            claim_epoch=_d(raw[2]),
        )

    @staticmethod
    async def read_owner(redis, task_id: str) -> str:
        """Read only worker_instance_id (backward compat)."""
        raw = await redis.hget(DagRedisKey.task_meta(task_id), TaskMetaField.WORKER_INSTANCE_ID)
        if isinstance(raw, bytes):
            return raw.decode("utf-8", errors="replace")
        return str(raw) if raw is not None else ""
