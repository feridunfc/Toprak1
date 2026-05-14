"""
hfa-control/src/hfa_control/registry.py
IRONCLAD Sprint 10 — Worker Registry

Maintains authoritative view of the worker fleet in Redis.
Workers publish WorkerHeartbeatEvent; registry writes HASH + region SET.

Redis key schema
----------------
  hfa:cp:worker:{worker_id}            HASH   TTL=registry_ttl
  hfa:cp:workers:by_region:{region}    SET    (no TTL — cleaned by mark_dead)

Consumer group
--------------
  Stream:  hfa:stream:heartbeat
  Group:   hfa-cp-registry
  Consumer per instance: registry-{instance_id}

XAUTOCLAIM is used to reclaim messages idle > autoclaim_idle_ms in PEL.
This ensures heartbeats are processed even if a previous CP instance crashed.

IRONCLAD rules
--------------
* No print() — logging only.
* No asyncio.get_event_loop() — get_running_loop().
* close() always safe.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import List, Optional

from hfa.config.keys import RedisKey
from hfa.events.schema import WorkerDrainingEvent, WorkerHeartbeatEvent

try:
    from hfa.obs.tracing import get_tracer  # type: ignore[attr-defined]

    _tracer = get_tracer("hfa.registry")
except Exception:
    _tracer = None  # graceful no-op if OTel not configured

from hfa_control.exceptions import WorkerNotFoundError
from hfa_control.models import ControlPlaneConfig, WorkerProfile, WorkerStatus

logger = logging.getLogger(__name__)

_GROUP = "hfa-cp-registry"


class WorkerRegistry:
    def __init__(self, redis, config: ControlPlaneConfig) -> None:
        self._redis = redis
        self._config = config
        self._task: Optional[asyncio.Task] = None

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> None:
        await self._ensure_group()
        loop = asyncio.get_running_loop()
        self._task = loop.create_task(
            self._consume_heartbeats(), name="registry.consume"
        )
        logger.info("WorkerRegistry started: instance=%s", self._config.instance_id)

    async def close(self) -> None:
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info("WorkerRegistry closed")

    # ------------------------------------------------------------------
    # Queries
    # ------------------------------------------------------------------

    async def list_healthy_workers(
        self, region: Optional[str] = None
    ) -> List[WorkerProfile]:
        """
        Return workers with status HEALTHY or DRAINING in the given region.
        Workers whose last_seen is older than worker_heartbeat_ttl are
        classified as DEAD and excluded.
        If region is None, returns workers across all regions.
        """
        if region:
            raw_ids = await self._redis.smembers(RedisKey.cp_workers_by_region(region))
        else:
            keys = await self._redis.keys(RedisKey.cp_workers_scan_pattern())
            raw_ids = {k.decode().split(":", 3)[3].encode() for k in keys}

        now = time.time()
        profiles = []
        for wid_b in raw_ids:
            wid = wid_b.decode() if isinstance(wid_b, bytes) else wid_b
            raw = await self._redis.hgetall(RedisKey.cp_worker(wid))
            if not raw:
                continue
            profile = WorkerProfile.from_redis_hash(raw)
            age = now - profile.last_seen
            if age > self._config.worker_heartbeat_ttl:
                profile.status = WorkerStatus.DEAD
            if profile.status in (WorkerStatus.HEALTHY, WorkerStatus.DRAINING):
                profiles.append(profile)

        return profiles

    async def list_schedulable_workers(
        self, region: Optional[str] = None
    ) -> List[WorkerProfile]:
        """
        Return only workers eligible to receive new runs.

        A worker is schedulable when ALL of the following hold:
          - status is HEALTHY (not dead, not degraded)
          - not draining
          - capacity > 0
          - inflight < capacity

        This is stricter than list_healthy_workers() and is the authoritative
        scheduling filter used to exclude draining or saturated workers.
        Safe for both object-style and dict-style worker records through
        _worker_is_schedulable().
        """
        all_workers = await self.list_healthy_workers(region=region)
        return [w for w in all_workers if self._worker_is_schedulable(w)]

    async def get_worker(self, worker_id: str) -> WorkerProfile:
        raw = await self._redis.hgetall(RedisKey.cp_worker(worker_id))
        if not raw:
            raise WorkerNotFoundError(f"Worker {worker_id!r} not registered")
        return WorkerProfile.from_redis_hash(raw)

    async def mark_dead(self, worker_id: str) -> None:
        try:
            await self._redis.hset(
                RedisKey.cp_worker(worker_id), "status", WorkerStatus.DEAD.value
            )
            logger.warning("WorkerRegistry.mark_dead: %s", worker_id)
        except Exception as exc:
            logger.error("WorkerRegistry.mark_dead error: %s", exc)

    async def registry_size(self) -> int:
        """Return total number of registered workers."""
        try:
            keys = await self._redis.keys(RedisKey.cp_workers_scan_pattern())
            return len(keys)
        except Exception:
            return 0

    # ------------------------------------------------------------------
    # Heartbeat consumer — XREADGROUP + XAUTOCLAIM
    # ------------------------------------------------------------------

    async def _ensure_group(self) -> None:
        try:
            await self._redis.xgroup_create(
                self._config.heartbeat_stream, _GROUP, id="0", mkstream=True
            )
        except Exception:
            pass  # group already exists — expected

    async def _consume_heartbeats(self) -> None:
        consumer = f"registry-{self._config.instance_id}"
        while True:
            try:
                await self._autoclaim(consumer)

                msgs = await self._redis.xreadgroup(
                    groupname=_GROUP,
                    consumername=consumer,
                    streams={self._config.heartbeat_stream: ">"},
                    count=100,
                    block=2000,
                )
                for _stream, entries in msgs or []:
                    for msg_id, data in entries:
                        await self._handle(data)
                        await self._redis.xack(
                            self._config.heartbeat_stream, _GROUP, msg_id
                        )
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.error("WorkerRegistry._consume error: %s", exc)
                await asyncio.sleep(1.0)

    async def _autoclaim(self, consumer: str) -> None:
        """Reclaim messages idle > autoclaim_idle_ms from PEL."""
        try:
            result = await self._redis.xautoclaim(
                self._config.heartbeat_stream,
                _GROUP,
                consumer,
                min_idle_time=self._config.autoclaim_idle_ms,
                start_id="0-0",
                count=self._config.autoclaim_count,
            )
            if result and result[1]:
                for msg_id, data in result[1]:
                    await self._handle(data)
                    await self._redis.xack(
                        self._config.heartbeat_stream, _GROUP, msg_id
                    )
        except Exception as exc:
            logger.debug("WorkerRegistry._autoclaim skipped: %s", exc)

    async def _handle(self, data: dict) -> None:
        et = (data.get(b"event_type") or b"").decode()
        if et == "WorkerHeartbeat":
            evt = WorkerHeartbeatEvent.from_redis(data)
            await self._on_heartbeat(evt)
        elif et == "WorkerDraining":
            evt = WorkerDrainingEvent.from_redis(data)
            await self._on_draining(evt)

    async def _on_heartbeat(self, event: WorkerHeartbeatEvent) -> None:
        key = RedisKey.cp_worker(event.worker_id)
        mapping = {
            "worker_id": event.worker_id,
            "worker_group": event.worker_group,
            "region": event.region,
            "shards": json.dumps(event.shards),
            "capacity": str(event.capacity),
            "inflight": str(event.inflight),
            "last_seen": str(event.timestamp),
            "version": event.version,
            "capabilities": json.dumps(event.capabilities),
        }

        existing_raw = await self._redis.hget(key, "status")
        existing_status = (
            existing_raw.decode() if isinstance(existing_raw, bytes) else existing_raw
        ) or ""
        if existing_status == WorkerStatus.DRAINING.value:
            mapping["status"] = WorkerStatus.DRAINING.value
        else:
            mapping["status"] = WorkerStatus.HEALTHY.value

        await self._redis.hset(key, mapping=mapping)
        await self._redis.expire(key, self._config.registry_ttl)
        await self._redis.sadd(
            RedisKey.cp_workers_by_region(event.region), event.worker_id
        )
        logger.debug(
            "Heartbeat: worker=%s inflight=%d/%d region=%s status=%s",
            event.worker_id,
            event.inflight,
            event.capacity,
            event.region,
            mapping["status"],
        )

    async def _on_draining(self, event: WorkerDrainingEvent) -> None:
        key = RedisKey.cp_worker(event.worker_id)
        await self._redis.hset(key, "status", WorkerStatus.DRAINING.value)
        await self._redis.expire(key, self._config.registry_ttl)
        logger.info(
            "Worker DRAINING: %s deadline=%s reason=%s",
            event.worker_id,
            event.drain_deadline_utc,
            event.reason,
        )

    def _status_value_of(self, worker) -> str:
        status = None

        if hasattr(worker, "status"):
            status = worker.status
        elif isinstance(worker, dict):
            status = worker.get("status")

        if status is None:
            return ""

        if hasattr(status, "value"):
            return str(status.value).lower()

        return str(status).lower()

    def _capacity_of(self, worker) -> int:
        value = None

        if hasattr(worker, "capacity"):
            value = worker.capacity
        elif isinstance(worker, dict):
            value = worker.get("capacity", 0)
        else:
            value = 0

        try:
            return int(value)
        except (TypeError, ValueError):
            return 0

    def _inflight_of(self, worker) -> int:
        value = None

        if hasattr(worker, "inflight"):
            value = worker.inflight
        elif isinstance(worker, dict):
            value = worker.get("inflight", 0)
        else:
            value = 0

        try:
            return int(value)
        except (TypeError, ValueError):
            return 0

    def _load_factor_of(self, worker) -> float:
        capacity = self._capacity_of(worker)
        if capacity <= 0:
            return float("inf")

        inflight = self._inflight_of(worker)
        if inflight >= capacity:
            return float("inf")

        return inflight / capacity

    def _worker_is_schedulable(self, worker) -> bool:
        status = self._status_value_of(worker)

        if status in {"dead", "degraded", "draining"}:
            return False

        capacity = self._capacity_of(worker)
        if capacity <= 0:
            return False

        inflight = self._inflight_of(worker)
        if inflight >= capacity:
            return False

        return True

    def _capability_matches(self, requirement, worker_capability) -> bool:
        if isinstance(worker_capability, list):
            if not isinstance(requirement, str):
                return False
            return requirement in worker_capability

        if isinstance(worker_capability, dict):
            if isinstance(requirement, str):
                return requirement in worker_capability

            if isinstance(requirement, dict):
                for req_key, req_value in requirement.items():
                    if req_key not in worker_capability:
                        return False

                    worker_value = worker_capability[req_key]

                    if req_value is None or req_value == "":
                        continue

                    if worker_value != req_value:
                        return False

                return True

        return False

    async def list_all_workers(self, region=None):
        workers = await self._load_all_worker_profiles_somehow()

        if region:
            workers = [
                w for w in workers
                if str(getattr(w, "region", "")).strip() == region
            ]

        return workers
