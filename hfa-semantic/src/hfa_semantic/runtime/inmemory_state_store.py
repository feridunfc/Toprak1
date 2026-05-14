from __future__ import annotations

import time
from typing import Any, Callable, List, Optional

from hfa_semantic.runtime.state_store import RuntimeState, StateStore


class InMemoryStateStore(StateStore):
    """
    In-memory state store for dev/test.

    Implements full StateStore ABC including delete, list_partitions,
    update (merge-function based), and optional TTL cleanup.
    """

    def __init__(self, cleanup_interval_ms: int = 0) -> None:
        # (rule_id, partition) → RuntimeState
        self._data: dict[tuple[str, str], RuntimeState] = {}
        self._cleanup_interval_ms = cleanup_interval_ms

    async def get(self, rule_id: str, partition_key: str) -> Optional[RuntimeState]:
        self._maybe_cleanup()
        entry = self._data.get((rule_id, partition_key))
        if entry is None:
            return None
        # TTL check
        if entry.ttl_ms > 0:
            age_ms = (time.time() * 1000) - entry.last_update_ms
            if age_ms > entry.ttl_ms:
                self._data.pop((rule_id, partition_key), None)
                return None
        return entry

    async def put(
        self,
        rule_id: str,
        partition_key: str,
        state: dict[str, Any],
        ttl_ms: int,
    ) -> None:
        self._data[(rule_id, partition_key)] = RuntimeState(
            rule_id=rule_id,
            partition=partition_key,
            state=dict(state),
            event_count=self._data.get((rule_id, partition_key), RuntimeState(
                rule_id=rule_id, partition=partition_key, state={}
            )).event_count + 1,
            last_update_ms=time.time() * 1000,
            ttl_ms=ttl_ms,
        )

    async def delete(self, rule_id: str, partition_key: str) -> None:
        self._data.pop((rule_id, partition_key), None)

    async def evict(self, rule_id: str, max_partitions: int) -> int:
        keys = [(r, p) for (r, p) in self._data if r == rule_id]
        if len(keys) <= max_partitions:
            return 0
        # Sort oldest first by last_update_ms
        keys.sort(key=lambda k: self._data[k].last_update_ms)
        to_remove = len(keys) - max_partitions
        for k in keys[:to_remove]:
            self._data.pop(k, None)
        return to_remove

    async def list_partitions(self, rule_id: str) -> List[str]:
        return [p for (r, p) in self._data if r == rule_id]

    async def update(
        self,
        rule_id: str,
        partition_key: str,
        updater_fn: Callable,
        ttl_ms: int,
    ) -> None:
        """Read-modify-write with merge function."""
        existing = await self.get(rule_id, partition_key)
        current_state = dict(existing.state) if existing else {}
        updated = await updater_fn(current_state)
        await self.put(rule_id, partition_key, updated, ttl_ms)

    async def close(self) -> None:
        pass

    # ── Internal ──────────────────────────────────────────────────────────────

    def _maybe_cleanup(self) -> None:
        if self._cleanup_interval_ms <= 0:
            return
        now_ms = time.time() * 1000
        expired = [
            k for k, v in self._data.items()
            if v.ttl_ms > 0 and (now_ms - v.last_update_ms) > v.ttl_ms
        ]
        for k in expired:
            self._data.pop(k, None)
