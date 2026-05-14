"""
hfa-control/src/hfa_control/leader.py
IRONCLAD Sprint 10 — Leader Election

Uses Redis SET NX EX (atomic) for single-writer Control Plane guarantee.
Fencing token (monotonic INCR counter) prevents stale leaders from writing
after losing leadership.

Algorithm
---------
1. Try SET hfa:cp:leader <instance_id> NX EX <ttl>
2. If acquired  → is_leader = True, fencing_token = INCR hfa:cp:fence
3. If not acquired:
     → GET hfa:cp:leader
     → If we already hold it: EXPIRE renew  (leadership retained)
     → Else: is_leader = False
4. Repeat every leader_renew_interval seconds.

Callers gate write operations on is_leader:
    if not self._leader.is_leader:
        raise LeadershipError(...)

IRONCLAD rules
--------------
* No print() — logging only.
* No asyncio.get_event_loop() — get_running_loop().
* close() always safe (idempotent).
"""

from __future__ import annotations

import asyncio
import logging

from typing import Optional

from hfa.obs.tracing import get_tracer  # type: ignore[attr-defined]
from hfa.config.keys import RedisKey
from hfa_control.exceptions import LeadershipError

logger = logging.getLogger(__name__)
_tracer = get_tracer("hfa.leader")

FENCE_KEY = RedisKey.cp_fence()


class LeaderElection:
    def __init__(self, redis, instance_id: str, config) -> None:
        self._redis = redis
        self._instance_id = instance_id
        self._config = config
        self._is_leader = False
        self._fencing_token: int = 0
        self._task: Optional[asyncio.Task] = None

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    @property
    def is_leader(self) -> bool:
        return self._is_leader

    @property
    def fencing_token(self) -> int:
        """
        Monotonic counter that increases every time leadership is acquired.
        Include in mutating Redis writes to detect stale leaders:
            if stored_token > self.fencing_token: skip write
        """
        return self._fencing_token

    def assert_leader(self) -> None:
        """Raise LeadershipError if this instance is not the leader."""
        if not self._is_leader:
            raise LeadershipError(
                f"Instance {self._instance_id!r} is not the Control Plane leader."
            )

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> None:
        loop = asyncio.get_running_loop()
        self._task = loop.create_task(
            self._loop(), name=f"leader.election.{self._instance_id}"
        )
        logger.info("LeaderElection started: instance=%s", self._instance_id)

    async def close(self) -> None:
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        # Release leadership if we hold it
        if self._is_leader:
            try:
                current = await self._redis.get(self._config.leader_key)
                if current and current.decode() == self._instance_id:
                    await self._redis.delete(self._config.leader_key)
                    logger.info("LeaderElection: released leadership on close")
            except Exception as exc:
                logger.error("LeaderElection.close release error: %s", exc)
        self._is_leader = False
        logger.info("LeaderElection closed: instance=%s", self._instance_id)

    # ------------------------------------------------------------------
    # Internal loop
    # ------------------------------------------------------------------

    async def _loop(self) -> None:
        while True:
            try:
                await self._try_acquire()
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.error("LeaderElection._loop error: %s", exc)
            try:
                await asyncio.sleep(self._config.leader_renew_interval)
            except asyncio.CancelledError:
                break

    async def _try_acquire(self) -> None:
        key = self._config.leader_key
        ttl = self._config.leader_ttl

        # Atomic acquire attempt
        acquired = await self._redis.set(key, self._instance_id, nx=True, ex=ttl)

        if acquired:
            if not self._is_leader:
                self._fencing_token = await self._redis.incr(FENCE_KEY)
                logger.info(
                    "LeaderElection: ACQUIRED leadership instance=%s token=%d",
                    self._instance_id,
                    self._fencing_token,
                )
            self._is_leader = True
            return

        # Already set — check if we hold it
        current = await self._redis.get(key)
        if current and current.decode() == self._instance_id:
            await self._redis.expire(key, ttl)
            self._is_leader = True
            return

        # Another instance holds leadership
        if self._is_leader:
            logger.warning(
                "LeaderElection: LOST leadership instance=%s current_leader=%s",
                self._instance_id,
                current.decode() if current else "unknown",
            )
        self._is_leader = False
