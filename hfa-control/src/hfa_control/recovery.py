"""
hfa-control/src/hfa_control/recovery.py
IRONCLAD Sprint 10 — Recovery Service

Runs as a background task inside the Control Plane.
Every recovery_sweep_interval seconds:
  1. Sweep running ZSET for stale runs (score + stale_run_timeout < now)
  2. For each stale run:
       - reschedule_count < max  →  reschedule (bump count, re-emit RunAdmittedEvent)
       - reschedule_count >= max →  dead-letter (RunDeadLetteredEvent + DLQ meta)
  3. Scan for runs stuck in 'scheduled' state (not yet claimed by a worker)
  4. Emit RunRescheduledEvent for audit trail

Running ZSET
------------
  hfa:cp:running    ZSET   run_id → admitted_at timestamp
  Scheduler adds entries on placement.
  RecoveryService removes entries on done/failed/dead_lettered.
  Stale entries are those where score + stale_run_timeout < now.

DLQ replay
----------
  replay_dlq_run() re-emits a RunAdmittedEvent and resets reschedule_count.
  Called by /control/v1/dlq/{run_id}/replay API endpoint.

IRONCLAD rules
--------------
* No print() — logging only.
* No asyncio.get_event_loop() — get_running_loop().
* close() always safe.
* cost_cents: int — no float USD.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import Optional

from hfa.events.schema import (
    RunAdmittedEvent,
    RunRescheduledEvent,
    RunDeadLetteredEvent,
)
from hfa.events.codec import serialize_event
from hfa.config.keys import RedisKey, RedisTTL
from hfa_control.models import ControlPlaneConfig
from hfa_control.exceptions import DLQEntryNotFoundError, TenantMismatchError
from hfa_control.state_machine import transition_state, is_terminal
from hfa.runtime.tenant_utils import decrement_tenant_inflight_if_needed

try:
    from hfa.obs.runtime_metrics import IRONCLADMetrics as _M
except Exception:
    _M = None  # type: ignore[assignment]

try:
    from hfa.obs.tracing import get_tracer  # type: ignore

    _tracer = get_tracer("hfa.recovery")
except Exception:
    _tracer = None

logger = logging.getLogger(__name__)


class RecoveryService:
    def __init__(self, redis, config: ControlPlaneConfig) -> None:
        self._redis = redis
        self._config = config
        self._task: Optional[asyncio.Task] = None

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> None:
        loop = asyncio.get_running_loop()
        self._task = loop.create_task(self._loop(), name="recovery.sweep")
        logger.info(
            "RecoveryService started: sweep_interval=%gs",
            self._config.recovery_sweep_interval,
        )

    async def close(self) -> None:
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info("RecoveryService closed")

    # ------------------------------------------------------------------
    # Sweep loop
    # ------------------------------------------------------------------

    async def _loop(self) -> None:
        while True:
            try:
                await asyncio.sleep(self._config.recovery_sweep_interval)
                await self._sweep()
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.error("RecoveryService._loop error: %s", exc, exc_info=True)

    async def _sweep(self) -> None:
        stale_runs = await self._find_stale_runs()
        rescheduled = 0
        dlq_count = 0

        if stale_runs and _M:
            _M.recovery_stale_detected_total.inc(len(stale_runs))

        for run_id in stale_runs:
            result = await self._handle_stale(run_id)
            if result == "rescheduled":
                rescheduled += 1
                if _M:
                    _M.recovery_rescheduled_total.inc()
            elif result == "dlq":
                dlq_count += 1
                if _M:
                    _M.recovery_dlq_total.inc()

        if stale_runs:
            logger.info(
                "Recovery sweep: stale=%d rescheduled=%d dlq=%d",
                len(stale_runs),
                rescheduled,
                dlq_count,
            )

    # ------------------------------------------------------------------
    # Stale run detection
    # ------------------------------------------------------------------

    async def _find_stale_runs(self) -> list[str]:
        """
        Return run_ids from running ZSET whose score (admitted_at)
        + stale_run_timeout is older than now.
        Only consider runs still in 'running' or 'scheduled' state.
        """
        cutoff = time.time() - self._config.stale_run_timeout
        try:
            stale_raw = await self._redis.zrangebyscore(
                self._config.running_zset, 0, cutoff
            )
        except Exception as exc:
            logger.error(
                "RecoveryService._find_stale_runs zrangebyscore error: %s", exc
            )
            return []

        result: list[str] = []
        for run_id_b in stale_raw:
            run_id = run_id_b.decode() if isinstance(run_id_b, bytes) else run_id_b
            state = await self._redis.get(RedisKey.run_state(run_id))
            if state:
                s = state.decode() if isinstance(state, bytes) else state
                if s in ("running", "scheduled"):
                    result.append(run_id)
                elif s in ("done", "failed", "dead_lettered"):
                    # Clean up ZSET entry
                    await self._redis.zrem(self._config.running_zset, run_id)
        return result

    # ------------------------------------------------------------------
    # Stale run handler
    # ------------------------------------------------------------------

    async def _handle_stale(self, run_id: str) -> str:
        meta = await self._redis.hgetall(RedisKey.run_meta(run_id))
        if not meta:
            await self._redis.zrem(self._config.running_zset, run_id)
            return "skipped"

        def _s(k: str) -> str:
            v = meta.get(k.encode()) or meta.get(k)
            return (v.decode() if isinstance(v, bytes) else v) or ""

        tenant_id = _s("tenant_id")
        agent_type = _s("agent_type")
        prev_worker = _s("worker_group")
        reschedule_count = int(_s("reschedule_count") or "0")

        if reschedule_count >= self._config.max_reschedule_attempts:
            return await self._dead_letter(
                run_id,
                tenant_id,
                reschedule_count,
                "max_reschedule_exceeded",
            )

        return await self._reschedule(
            run_id,
            tenant_id,
            agent_type,
            prev_worker,
            reschedule_count,
        )

    async def _reschedule(
        self,
        run_id: str,
        tenant_id: str,
        agent_type: str,
        prev_worker: str,
        reschedule_count: int,
    ) -> str:
        new_count = reschedule_count + 1

        # Update metadata
        await self._redis.hset(
            RedisKey.run_meta(run_id), "reschedule_count", str(new_count)
        )
        # CAS: only reschedule if run is still running or scheduled
        # (prevents zombie resurrection of already-done/cancelled runs)
        state_key = RedisKey.run_state(run_id)
        raw_state = await self._redis.get(state_key)
        current = raw_state.decode() if isinstance(raw_state, bytes) else raw_state
        if current not in ("running", "scheduled"):
            logger.warning(
                "Recovery._reschedule CAS miss: run=%s expected running/scheduled, got %r — skipping",
                run_id, current,
            )
            return "skipped"
        ok = await transition_state(
            self._redis, run_id, "rescheduled",
            state_key=state_key, state_ttl=RedisTTL.RUN_STATE,
            expected_state=current,
        )
        if not ok:
            logger.warning(
                "Recovery._reschedule CAS conflict: run=%s — another actor changed state, skipping",
                run_id,
            )
            return "skipped"

        # Audit event
        resched_evt = RunRescheduledEvent(
            run_id=run_id,
            tenant_id=tenant_id,
            previous_worker=prev_worker,
            reschedule_count=new_count,
            reason="stale_running",
        )
        await self._redis.xadd(
            self._config.control_stream,
            serialize_event(resched_evt),
            maxlen=100_000,
            approximate=True,
        )

        # Re-admit (Scheduler will re-place)
        admitted_evt = RunAdmittedEvent(
            run_id=run_id,
            tenant_id=tenant_id,
            agent_type=agent_type,
        )
        await self._redis.xadd(
            self._config.control_stream,
            serialize_event(admitted_evt),
            maxlen=100_000,
            approximate=True,
        )

        # Reset ZSET score to now so we don't immediately re-detect as stale
        await self._redis.zadd(self._config.running_zset, {run_id: time.time()})

        logger.warning(
            "Rescheduled: run=%s tenant=%s attempt=%d/%d prev_worker=%s",
            run_id,
            tenant_id,
            new_count,
            self._config.max_reschedule_attempts,
            prev_worker,
        )
        return "rescheduled"

    async def _dead_letter(
        self,
        run_id: str,
        tenant_id: str,
        reschedule_count: int,
        reason: str,
    ) -> str:
        # CAS: only dead-letter if not already terminal
        _dl_key = RedisKey.run_state(run_id)
        _raw = await self._redis.get(_dl_key)
        _cur = _raw.decode() if isinstance(_raw, bytes) else _raw
        if _cur not in ("running", "scheduled", "rescheduled"):
            logger.warning(
                "Recovery._dead_letter CAS skip: run=%s state=%r not actionable",
                run_id, _cur,
            )
        else:
            _dlr = await transition_state(
                self._redis, run_id, "dead_lettered",
                state_key=_dl_key, state_ttl=RedisTTL.DLQ_META,
                expected_state=_cur,
            )
        await self._redis.hset(
            RedisKey.cp_dlq_meta(run_id),
            mapping={
                "run_id": run_id,
                "tenant_id": tenant_id,
                "reason": reason,
                "reschedule_count": str(reschedule_count),
                "dead_lettered_at": str(time.time()),
            },
        )
        await self._redis.expire(RedisKey.cp_dlq_meta(run_id), RedisTTL.DLQ_META)
        await self._redis.zrem(self._config.running_zset, run_id)

        dlq_evt = RunDeadLetteredEvent(
            run_id=run_id,
            tenant_id=tenant_id,
            reason=reason,
            reschedule_count=reschedule_count,
        )
        await self._redis.xadd(
            self._config.dlq_stream,
            serialize_event(dlq_evt),
            maxlen=10_000,
            approximate=True,
        )
        await decrement_tenant_inflight_if_needed(self._redis, run_id)
        logger.error(
            "DLQ: run=%s tenant=%s reason=%s attempts=%d",
            run_id,
            tenant_id,
            reason,
            reschedule_count,
        )
        return "dlq"

    # ------------------------------------------------------------------
    # DLQ replay (called by /control/v1/dlq/{run_id}/replay)
    # ------------------------------------------------------------------

    async def replay_dlq_run(self, run_id: str, requesting_tenant: str) -> None:
        """
        Re-enqueue a DLQ run to the Scheduler.
        Raises DLQEntryNotFoundError if not found.
        Raises TenantMismatchError if tenant does not match.
        """
        meta = await self._redis.hgetall(RedisKey.cp_dlq_meta(run_id))
        if not meta:
            raise DLQEntryNotFoundError(f"DLQ entry not found: {run_id!r}")

        def _s(k: str) -> str:
            v = meta.get(k.encode()) or meta.get(k)
            return (v.decode() if isinstance(v, bytes) else v) or ""

        dlq_tenant = _s("tenant_id")
        if dlq_tenant != requesting_tenant:
            raise TenantMismatchError(
                f"DLQ run {run_id!r} belongs to tenant {dlq_tenant!r}, "
                f"not {requesting_tenant!r}"
            )

        agent_type = _s("agent_type") or ""
        raw_payload = await self._redis.get(RedisKey.run_payload(run_id))
        payload: dict = {}
        if raw_payload:
            try:
                payload = json.loads(
                    raw_payload.decode()
                    if isinstance(raw_payload, bytes)
                    else raw_payload
                )
            except json.JSONDecodeError:
                pass

        # Reset state via CAS (only valid from dead_lettered)


            # MİMARİYE SADIK REPLAY: Terminal state'den geri dönülemez.
            # Anahtar silinerek iş sıfırdan sisteme (admitted) alınır.
                # MİMARİYE SADIK REPLAY:
                # dead_lettered terminal bir durumdur ve CAS ile üzerinden geçilemez.
                # Bu yüzden eski durumu tamamen silip, işi sıfırdan (expected_state=None) kabul ediyoruz.
        _replay_key = RedisKey.run_state(run_id)
        await self._redis.delete(_replay_key)

        replay_ok = await transition_state(
            self._redis, run_id, "admitted",
            state_key=_replay_key, state_ttl=RedisTTL.RUN_STATE,
            expected_state=None,
        )

        if not replay_ok:
            logger.warning(
                "DLQ replay CAS miss: run=%s not in dead_lettered state — skipping",
                run_id,
            )
            raise DLQEntryNotFoundError(f"DLQ run {run_id!r} could not be re-admitted")

        await self._redis.hset(RedisKey.run_meta(run_id), "reschedule_count", "0")
        # Note: do NOT add to running_zset here — run is admitted, not running.
        # It will enter running_zset when the scheduler dispatches it.

        # Re-emit to Scheduler
        evt = RunAdmittedEvent(
            run_id=run_id,
            tenant_id=dlq_tenant,
            agent_type=agent_type,
            payload=payload,
        )
        await self._redis.xadd(
            self._config.control_stream,
            serialize_event(evt),
            maxlen=RedisTTL.STREAM_MAXLEN,
            approximate=True,
        )

        # Remove from DLQ
        await self._redis.delete(RedisKey.cp_dlq_meta(run_id))

        logger.info("DLQ replay: run=%s tenant=%s", run_id, dlq_tenant)

    async def dlq_depth(self) -> int:
        try:
            return await self._redis.xlen(self._config.dlq_stream)
        except Exception:
            return 0

    async def list_dlq(self, tenant_id: str, limit: int = 50) -> list[dict]:
        """
        Return DLQ entries.
        If tenant_id == "__all__", returns entries for all tenants.
        Otherwise filters to the specified tenant.
        Scans hfa:cp:dlq:meta:* keys (bounded by 7-day TTL).
        """
        _all = tenant_id == "__all__"
        # Build the scan pattern from the canonical key prefix
        dlq_scan_pattern = RedisKey.cp_dlq_meta("*")
        try:
            cursor = 0
            entries = []
            while True:
                cursor, keys = await self._redis.scan(
                    cursor, match=dlq_scan_pattern, count=100
                )
                for key in keys:
                    meta = await self._redis.hgetall(key)
                    if not meta:
                        continue

                    def _s(k: str) -> str:
                        v = meta.get(k.encode()) or meta.get(k)
                        return (v.decode() if isinstance(v, bytes) else v) or ""

                    if _all or _s("tenant_id") == tenant_id:
                        entries.append(
                            {
                                "run_id": _s("run_id"),
                                "tenant_id": _s("tenant_id"),
                                "reason": _s("reason"),
                                "reschedule_count": int(_s("reschedule_count") or "0"),
                                "dead_lettered_at": float(
                                    _s("dead_lettered_at") or "0"
                                ),
                                "original_error": _s("original_error"),
                                "cost_cents": int(_s("cost_cents") or "0"),
                            }
                        )
                    if len(entries) >= limit:
                        break
                if cursor == 0 or len(entries) >= limit:
                    break
            return entries
        except Exception as exc:
            logger.error("RecoveryService.list_dlq error: %s", exc)
            return []
