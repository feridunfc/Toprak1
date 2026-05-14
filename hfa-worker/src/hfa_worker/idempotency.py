"""
hfa-worker/src/hfa_worker/idempotency.py
IRONCLAD Sprint 11 --- Idempotency Guard with Atomic Claims
"""

from __future__ import annotations

import logging

from hfa.runtime.state_store import StateStore

logger = logging.getLogger(__name__)


class IdempotencyGuard:
    def __init__(self, redis):
        self._state = StateStore(redis)

    async def should_execute(self, run_id: str) -> bool:
        return not await self._state.is_terminal(run_id)

    async def try_claim_and_mark_running(
        self,
        run_id: str,
        worker_id: str,
        worker_group: str,
        shard: int,
    ) -> bool:
        return await self._state.mark_running(run_id, worker_id, worker_group, shard)

    async def renew_claim(self, run_id: str) -> bool:
        return await self._state.renew_claim(run_id)

    async def is_already_completed(self, run_id: str) -> bool:
        return await self._state.is_terminal(run_id)
