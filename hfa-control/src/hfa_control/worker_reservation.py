
from __future__ import annotations

import secrets
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from hfa.dag.schema import DagRedisKey
from hfa.lua.loader import LuaScriptLoader

_LUA_DIR = (
    Path(__file__).resolve().parent.parent.parent.parent / "hfa-core" / "src" / "hfa" / "lua"
)
_LUA_DIR_INSTALLED = Path(__file__).resolve().parent.parent.parent / "hfa" / "lua"


def _lua_path(filename: str) -> Path:
    for base in (_LUA_DIR, _LUA_DIR_INSTALLED):
        p = base / filename
        if p.exists():
            return p
    here = Path(__file__).resolve()
    for parent in here.parents:
        for subdir in ("hfa-core/src/hfa/lua", "hfa/lua"):
            p = parent / subdir / filename
            if p.exists():
                return p
    raise FileNotFoundError(f"{filename} not found in known lua directories")


# TTL for execution ownership tokens in Redis.
# Must be longer than the maximum expected run execution time.
# Workers should reject execution if their token has expired.
_EXECUTION_TOKEN_TTL_SECONDS = 3600  # 1 hour


def _execution_token_key(run_id: str) -> str:
    """Redis key for the execution ownership token of a run."""
    return f"hfa:run:execution_token:{run_id}"


# ── Execution Ownership Token ─────────────────────────────────────────────────


def _decode_redis(value) -> str:
    return value.decode("utf-8", errors="replace") if isinstance(value, bytes) else (str(value) if value is not None else "")

@dataclass(frozen=True)
class ExecutionToken:
    """
    Execution ownership token. Proves that a specific worker was assigned
    this run by this scheduler in this epoch.

    Worker validates token before execution:
        stored = redis.hgetall(execution_token_key(run_id))
        if stored["token"] != my_token: reject()

    Fields
    ------
    run_id     : the run being assigned
    worker_id  : the worker that received the assignment
    token      : a cryptographically random 16-byte hex string (unique per dispatch)
    epoch      : the scheduler epoch at the time of dispatch
    """
    run_id: str
    worker_id: str
    token: str
    epoch: str

    @staticmethod
    def generate(*, run_id: str, worker_id: str, epoch: str) -> "ExecutionToken":
        """Generate a new execution token with a fresh random token value."""
        return ExecutionToken(
            run_id=run_id,
            worker_id=worker_id,
            token=secrets.token_hex(16),
            epoch=epoch,
        )

    def as_redis_mapping(self) -> dict[str, str]:
        """Serialize to a Redis HSET mapping."""
        return {
            "run_id": self.run_id,
            "worker_id": self.worker_id,
            "token": self.token,
            "epoch": self.epoch,
        }


# ── Reservation Result ────────────────────────────────────────────────────────

@dataclass(frozen=True)
class WorkerReservationResult:
    ok: bool
    status: str
    scheduler_epoch: str = ""
    # Execution ownership token, populated on successful reservation.
    # None if reservation failed or token storage was skipped.
    execution_token: Optional[ExecutionToken] = None


# ── Reservation Manager ───────────────────────────────────────────────────────

class WorkerReservationManager:
    def __init__(
        self,
        redis,
        reservation_ttl_seconds: int = 30,
        scheduler_id: str = "scheduler-1",
    ) -> None:
        self._redis = redis
        self._reservation_ttl_seconds = reservation_ttl_seconds
        self._scheduler_id = scheduler_id
        self._loader: Optional[LuaScriptLoader] = None

    async def initialise(self) -> None:
        path = _lua_path("reserve_worker.lua")
        self._loader = LuaScriptLoader(self._redis, path)
        await self._loader.load()

    async def reserve(
        self,
        *,
        worker_id: str,
        task_id: str,
        scheduler_epoch: str,
        reserved_at_ms: int,
    ) -> WorkerReservationResult:
        """
        Reserve a worker for a task and write an execution ownership token.

        On success:
          1. Runs reserve_worker.lua (existing behaviour, unchanged).
          2. Generates an ExecutionToken and stores it in Redis under
             hfa:run:execution_token:{task_id} as a HASH.
          3. Returns the token in WorkerReservationResult.execution_token.

        Workers MUST validate this token before starting execution.
        Token TTL is _EXECUTION_TOKEN_TTL_SECONDS.
        """
        if self._loader is None:
            await self.initialise()
        assert self._loader is not None

        result = await self._loader.run(
            num_keys=2,
            keys=[
                DagRedisKey.worker_reservation(worker_id),
                DagRedisKey.task_reservation_owner(task_id),
            ],
            args=[
                worker_id,
                task_id,
                scheduler_epoch,
                str(reserved_at_ms),
                str(self._reservation_ttl_seconds),
                self._scheduler_id,
            ],
        )

        status = result[0].decode() if isinstance(result[0], bytes) else result[0]
        epoch = (
            result[1].decode()
            if len(result) > 1 and isinstance(result[1], bytes)
            else (result[1] if len(result) > 1 else "")
        )

        if status != "reservation_created":
            return WorkerReservationResult(
                ok=False,
                status=status,
                scheduler_epoch=epoch or scheduler_epoch,
            )

        # ── Write execution ownership token ───────────────────────────────
        exec_token = ExecutionToken.generate(
            run_id=task_id,
            worker_id=worker_id,
            epoch=scheduler_epoch,
        )
        try:
            token_key = _execution_token_key(task_id)
            pipe = self._redis.pipeline()
            pipe.hset(token_key, mapping=exec_token.as_redis_mapping())
            pipe.expire(token_key, _EXECUTION_TOKEN_TTL_SECONDS)
            await pipe.execute()
        except Exception as exc:
            # Token storage failure is logged but must NOT block the reservation.
            # The reservation itself is already committed. Callers that need strict
            # token validation should treat a missing token as a rejection.
            import logging
            logging.getLogger(__name__).warning(
                "WorkerReservationManager: failed to write execution token "
                "run=%s worker=%s: %s",
                task_id,
                worker_id,
                exc,
            )
            exec_token = None  # type: ignore[assignment]

        return WorkerReservationResult(
            ok=True,
            status=status,
            scheduler_epoch=epoch or scheduler_epoch,
            execution_token=exec_token,
        )

    async def get(self, worker_id: str) -> dict[str, str]:
        return await self._redis.hgetall(DagRedisKey.worker_reservation(worker_id))

    async def get_execution_token(self, run_id: str) -> Optional[dict[str, str]]:
        """
        Read the execution ownership token for a run from Redis.

        Returns None if no token is stored (run not dispatched, or token expired).
        Workers call this to validate their assignment before executing.
        """
        try:
            raw = await self._redis.hgetall(_execution_token_key(run_id))
            if not raw:
                return None
            return {
                (k.decode() if isinstance(k, bytes) else k): (
                    v.decode() if isinstance(v, bytes) else v
                )
                for k, v in raw.items()
            }
        except Exception:
            return None

    async def release(self, worker_id: str) -> None:
        key = DagRedisKey.worker_reservation(worker_id)
        raw_task_id = await self._redis.hget(key, "task_id")
        task_id = _decode_redis(raw_task_id)

        keys_to_delete = [key]
        if task_id:
            owner_key = DagRedisKey.task_reservation_owner(task_id)
            owner_worker = _decode_redis(await self._redis.hget(owner_key, "worker_id"))
            owner_task = _decode_redis(await self._redis.hget(owner_key, "task_id"))
            if owner_worker == worker_id and owner_task == task_id:
                keys_to_delete.append(owner_key)

        await self._redis.delete(*keys_to_delete)

    async def renew(self, worker_id: str, ttl_seconds: int | None = None) -> bool:
        ttl_seconds = ttl_seconds or self._reservation_ttl_seconds
        key = DagRedisKey.worker_reservation(worker_id)
        exists = await self._redis.exists(key)
        if not exists:
            return False

        worker_ok = bool(await self._redis.expire(key, ttl_seconds))

        raw_task_id = await self._redis.hget(key, "task_id")
        task_id = _decode_redis(raw_task_id)
        if task_id:
            owner_key = DagRedisKey.task_reservation_owner(task_id)
            owner_worker = _decode_redis(await self._redis.hget(owner_key, "worker_id"))
            if owner_worker == worker_id:
                await self._redis.expire(owner_key, ttl_seconds)

        return worker_ok
