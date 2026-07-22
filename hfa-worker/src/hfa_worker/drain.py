"""
hfa-worker/src/hfa_worker/drain.py
IRONCLAD Sprint 11/12 — Graceful Drain Management with Metrics

Sprint 11 semantics preserved:
  - WorkerDrainingEvent emitted on drain start
  - consumer.stop_pulling() called immediately

Sprint 79.5 correction:
  - shard ownership is worker-group scoped
  - one worker instance must never delete the shared group lease
  - ownership expires naturally unless another group member renews it

Sprint 12 additions:
  - drain_started / drain_completed / drain_timeout counters
  - is_draining property
  - idempotent: second call to start_drain is a no-op
  - completed vs timed-out path clearly logged and counted
"""

from __future__ import annotations

import asyncio
import logging
import time

from hfa.events.codec import serialize_event
from hfa.config.keys import RedisKey
from hfa.events.schema import WorkerDrainingEvent

try:
    from hfa.obs.runtime_metrics import IRONCLADMetrics as _M
except Exception:
    _M = None  # type: ignore[assignment]

logger = logging.getLogger(__name__)


class DrainManager:
    def __init__(
        self,
        redis,
        worker_id: str,
        worker_group: str,
        shards: list[int],
        consumer,
        control_stream: str = "",
    ):
        self._redis = redis
        self._worker_id = worker_id
        self._worker_group = worker_group
        self._shards = shards
        self._consumer = consumer
        # Default to canonical key; callers may override for testing
        self._control_stream = control_stream or RedisKey.stream_control()
        self._draining = False

    @property
    def is_draining(self) -> bool:
        return self._draining

    def reset(self) -> None:
        """Reset lifecycle-local drain state after a completed service stop."""
        self._draining = False

    async def start_drain(
        self, reason: str = "shutdown", timeout: float = 30.0
    ) -> None:
        if self._draining:
            logger.warning(
                "Already draining, ignoring second drain request: worker=%s",
                self._worker_id,
            )
            return

        self._draining = True
        if _M:
            _M.worker_drain_started_total.inc()

        deadline_utc = time.strftime(
            "%Y-%m-%dT%H:%M:%SZ", time.gmtime(time.time() + timeout)
        )

        event = WorkerDrainingEvent(
            worker_id=self._worker_id,
            worker_group=self._worker_group,
            drain_deadline_utc=deadline_utc,
            reason=reason,
        )
        await self._redis.xadd(
            self._control_stream,
            serialize_event(event),
            maxlen=10000,
            approximate=True,
        )

        self._consumer.stop_pulling()

        deadline = time.time() + timeout
        while self._consumer.inflight_count > 0 and time.time() < deadline:
            await asyncio.sleep(1.0)

        timed_out = self._consumer.inflight_count > 0
        if timed_out:
            logger.warning(
                "Drain timeout: worker=%s inflight=%d remaining",
                self._worker_id,
                self._consumer.inflight_count,
            )
            if _M:
                _M.worker_drain_timeout_total.inc()
        else:
            logger.info("Drain completed cleanly: worker=%s", self._worker_id)
            if _M:
                _M.worker_drain_completed_total.inc()

        # Shard leases are worker-group scoped.  This instance stops
        # renewing through WorkerService shutdown; the Redis TTL is the only
        # safe release mechanism while sibling workers may still be alive.

    async def _release_shards(self) -> None:
        # Compatibility no-op: group leases expire by TTL, not instance stop.
        logger.debug(
            "Shard lease release deferred to TTL: worker=%s group=%s shards=%s",
            self._worker_id,
            self._worker_group,
            self._shards,
        )
