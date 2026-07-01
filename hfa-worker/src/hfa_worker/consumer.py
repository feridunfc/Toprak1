"""
hfa-worker/src/hfa_worker/consumer.py

Sprint 7.3 â€” Import Sanitization

Changes:
  * Removed: from hfa_worker.execution_types import ExecutionRequest
  * Removed: _build_execution_request() method (built ExecutionRequest adapter)
  * Added:   executor receives RunRequestedEvent directly (canonical path)
  * Errors:  ExecutionPermanentError, ExecutionTransientError now from hfa_worker.models

executor.execute() accepts RunRequestedEvent directly â€” all canonical executors
(FakeExecutor, OpenAIExecutor, CognitiveExecutor) use getattr duck-typing so
both RunRequestedEvent and the old ExecutionRequest continue to work.
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Optional, Set

from hfa.config.keys import RedisKey
from hfa.events.codec import deserialize_run_requested, serialize_event
from hfa.events.schema import RunCompletedEvent, RunFailedEvent
from hfa.runtime.state_store import StateStore
from hfa.runtime.tenant_utils import decrement_tenant_inflight_if_needed

# Sprint 7.3: import exclusively from canonical models
from hfa_worker.executor import BaseExecutor
from hfa_worker.idempotency import IdempotencyGuard
from hfa_worker.models import (
    ExecutionPermanentError,
    ExecutionTransientError,
    InfrastructureError,
    TerminalExecutionError,
)
from hfa_worker.redis_utils import ack_message, ensure_consumer_group
from hfa_worker.runtime.worker_runtime import is_worker_effect_hybrid_enabled

try:
    from hfa.obs.runtime_metrics import IRONCLADMetrics as _M
except Exception:
    _M = None  # type: ignore[assignment]

logger = logging.getLogger(__name__)
CONSUMER_GROUP = "worker_consumers"

LEGACY_STREAM_CLAIM_COMPATIBILITY_BOUNDARY = (
    "WorkerConsumer uses IdempotencyGuard.try_claim_and_mark_running / "
    "StateStore.mark_running and is not the canonical TaskConsumer.claim_start path."
)

CANONICAL_RUNTIME_CLAIM_PATH_TARGET = (
    "RunRequestedEvent -> TaskContext -> TaskConsumer.consume_once -> "
    "TaskClaimManager.claim_start"
)


class WorkerConsumer:
    """
    Legacy stream consumer compatibility boundary.

    This class still performs the older stream-consumer claim path through
    IdempotencyGuard.try_claim_and_mark_running() / StateStore.mark_running().
    It must not be mistaken for the canonical TaskConsumer.consume_once() ->
    TaskClaimManager.claim_start() path.

    Product target for a future bridge:
    RunRequestedEvent -> TaskContext -> TaskConsumer.consume_once().
    """

    def __init__(
        self,
        redis,
        worker_id: str,
        worker_group: str,
        shards: list[int],
        executor: BaseExecutor,
        reclaim_idle_ms: int = 60000,
    ):
        self._redis = redis
        self._worker_id = worker_id
        self._worker_group = worker_group
        self._shards = shards
        self._executor = executor
        self._reclaim_idle_ms = reclaim_idle_ms

        self._state = StateStore(redis)
        self._guard = IdempotencyGuard(redis)
        self._inflight: Set[str] = set()

        self._consumer_name = worker_id
        self._streams = [RedisKey.stream_shard(s) for s in shards]

        self._running = False
        self._pulling = True
        self._task: Optional[asyncio.Task] = None
        self._renewer_task: Optional[asyncio.Task] = None

    @property
    def inflight_count(self) -> int:
        return len(self._inflight)

    @property
    def is_draining(self) -> bool:
        return not self._pulling

    def stop_pulling(self) -> None:
        self._pulling = False
        logger.info("Pulling stopped: worker=%s", self._worker_id)

    async def start(self) -> None:
        for stream in self._streams:
            await ensure_consumer_group(
                self._redis,
                stream,
                CONSUMER_GROUP,
                start_id="0",
                mkstream=True,
            )

        self._running = True
        self._pulling = True

        loop = asyncio.get_running_loop()
        self._task = loop.create_task(
            self._main_lifecycle(),
            name=f"consumer.{self._worker_id}",
        )
        self._renewer_task = loop.create_task(
            self._claim_renewer(),
            name=f"renewer.{self._worker_id}",
        )

    async def close(self) -> None:
        self._running = False
        self._pulling = False

        tasks = [t for t in (self._task, self._renewer_task) if t is not None]
        for t in tasks:
            t.cancel()

        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

        self._task = None
        self._renewer_task = None

    async def _claim_renewer(self) -> None:
        while self._running:
            try:
                await asyncio.sleep(300)
                renewed = 0
                for run_id in list(self._inflight):
                    try:
                        await self._guard.renew_claim(run_id)
                        renewed += 1
                    except Exception as exc:
                        logger.warning(
                            "Claim renew failed: worker=%s run=%s error=%s",
                            self._worker_id,
                            run_id,
                            exc,
                        )
                        if _M:
                            _M.claim_renew_failure_total.inc()
                if renewed:
                    if _M:
                        _M.claim_renew_total.inc(renewed)
                    logger.debug(
                        "Claim renew: worker=%s renewed=%d",
                        self._worker_id,
                        renewed,
                    )
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.error("Claim renewer error: worker=%s %s", self._worker_id, exc)
                if _M:
                    _M.claim_renew_failure_total.inc()

    async def _main_lifecycle(self) -> None:
        await self._reclaim_pending_messages()
        await self._consume_loop()

    async def _reclaim_pending_messages(self) -> None:
        total_reclaimed = 0
        for stream in self._streams:
            try:
                pending = await self._redis.xpending_range(
                    stream,
                    CONSUMER_GROUP,
                    min="-",
                    max="+",
                    count=100,
                )

                if not pending:
                    continue

                to_claim = []
                for p in pending:
                    if isinstance(p, dict):
                        msg_id = p.get("message_id")
                        idle_ms = p.get("time_since_delivered", 0)
                    else:
                        msg_id = p[0]
                        idle_ms = p[2]

                    if msg_id is not None and idle_ms >= self._reclaim_idle_ms:
                        to_claim.append(msg_id)

                if not to_claim:
                    continue

                claimed = await self._redis.xclaim(
                    stream,
                    CONSUMER_GROUP,
                    self._consumer_name,
                    self._reclaim_idle_ms,
                    to_claim,
                )

                for msg_id, data in claimed:
                    msg_id_str = msg_id.decode() if isinstance(msg_id, bytes) else msg_id
                    shard = int(stream.split(":")[-1])
                    logger.info(
                        "Reclaimed pending message: worker=%s stream=%s msg_id=%s",
                        self._worker_id,
                        stream,
                        msg_id_str,
                    )
                    total_reclaimed += 1
                    await self._process_message(msg_id_str, data, stream, shard)

            except Exception as exc:
                logger.error("Error reclaiming pending messages: %s", exc)

        if total_reclaimed and _M:
            _M.pending_reclaimed_total.inc(total_reclaimed)

    async def _consume_loop(self) -> None:
        streams_dict = {s: ">" for s in self._streams}

        while self._running and self._pulling:
            try:
                msgs = await self._redis.xreadgroup(
                    groupname=CONSUMER_GROUP,
                    consumername=self._consumer_name,
                    streams=streams_dict,
                    count=10,
                    block=100,
                )

                if not msgs:
                    continue

                for stream_name, entries in msgs:
                    s_name = stream_name.decode() if isinstance(stream_name, bytes) else stream_name
                    shard = int(s_name.split(":")[-1])

                    for msg_id, data in entries:
                        msg_id_str = msg_id.decode() if isinstance(msg_id, bytes) else msg_id
                        await self._process_message(msg_id_str, data, s_name, shard)

            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.error("Consume loop error: %s", exc)
                await asyncio.sleep(0.1)

    async def _process_message(self, msg_id: str, data: dict, stream: str, shard: int) -> None:
        try:
            event = deserialize_run_requested(data)
            if event is None:
                logger.warning("Failed to deserialize message %s", msg_id)
                await ack_message(self._redis, stream, CONSUMER_GROUP, msg_id)
                return

            if not await self._guard.should_execute(event.run_id):
                logger.info("Skipping terminal run=%s", event.run_id)
                await ack_message(self._redis, stream, CONSUMER_GROUP, msg_id)
                return

            started = await self._guard.try_claim_and_mark_running(
                event.run_id,
                self._worker_id,
                self._worker_group,
                shard,
            )
            if not started:
                logger.info("Run already claimed elsewhere: run=%s", event.run_id)
                return

            self._inflight.add(event.run_id)
            if _M:
                _M.runs_started_total.inc()
                _M.worker_inflight.inc()

            exec_start = time.monotonic()

            try:
                # Sprint 7.3: pass RunRequestedEvent directly â€” no ExecutionRequest adapter
                # Sprint 45: normalize runtime payload before executor invocation.
                # Lua dispatch may expose payload as "payload" / "payload_json".
                existing_payload = getattr(event, "payload", None)
                if not isinstance(existing_payload, dict) or not existing_payload or "prompt" not in existing_payload:
                    import json as _json

                    decoded_payload = None
                    raw_candidates = []

                    if isinstance(data, dict):
                        raw_candidates.extend(
                            [
                                data.get(b"payload"),
                                data.get("payload"),
                                data.get(b"payload_json"),
                                data.get("payload_json"),
                            ]
                        )

                    for raw_payload in raw_candidates:
                        if not raw_payload:
                            continue
                        try:
                            if isinstance(raw_payload, bytes):
                                raw_payload = raw_payload.decode("utf-8")
                            candidate = _json.loads(raw_payload or "{}")
                            if isinstance(candidate, dict) and candidate:
                                decoded_payload = candidate
                                break
                        except Exception:
                            continue
                    if isinstance(decoded_payload, dict):
                        event.payload = decoded_payload

                result = await self._executor.execute(event)
                duration_ms = (time.monotonic() - exec_start) * 1000.0

                if result.status == "done":
                    await self._state.store_result(
                        event.run_id,
                        event.tenant_id,
                        "done",
                        result.payload,
                        result.cost_cents,
                        result.tokens_used,
                    )
                    await self._state.transition_state(event.run_id, "done")
                    await decrement_tenant_inflight_if_needed(self._redis, event.run_id)
                    evt = RunCompletedEvent(
                        run_id=event.run_id,
                        tenant_id=event.tenant_id,
                        worker_id=self._worker_id,
                        payload=result.payload,
                        cost_cents=result.cost_cents,
                        tokens_used=result.tokens_used,
                    )
                    if _M:
                        _M.runs_completed_total.inc()
                        _M.run_execution_duration_ms.record(duration_ms)
                else:
                    await self._state.store_result(
                        event.run_id,
                        event.tenant_id,
                        "failed",
                        result.payload,
                        result.cost_cents,
                        result.tokens_used,
                        error=result.error,
                    )
                    await self._state.transition_state(event.run_id, "failed")
                    await decrement_tenant_inflight_if_needed(self._redis, event.run_id)
                    evt = RunFailedEvent(
                        run_id=event.run_id,
                        tenant_id=event.tenant_id,
                        worker_id=self._worker_id,
                        error=result.error or "Terminal error",
                        cost_cents=result.cost_cents,
                        tokens_used=result.tokens_used,
                        payload=result.payload,
                    )
                    if _M:
                        _M.runs_failed_total.inc()
                        _M.run_execution_duration_ms.record(duration_ms)

                await self._redis.xadd(RedisKey.stream_results(), serialize_event(evt))
                await self._state.mark_completed(event.run_id)
                await ack_message(self._redis, stream, CONSUMER_GROUP, msg_id)

            except ExecutionPermanentError as exc:
                duration_ms = (time.monotonic() - exec_start) * 1000.0
                logger.warning("Permanent execution failure run=%s error=%s", event.run_id, exc)
                if is_worker_effect_hybrid_enabled():
                    evt = RunFailedEvent(
                        run_id=event.run_id,
                        tenant_id=event.tenant_id,
                        worker_id=self._worker_id,
                        error=str(exc),
                        cost_cents=0,
                        tokens_used=0,
                        payload={},
                    )
                    # Sprint 5: permanent failure is reported as an effect event;
                    # no direct terminal truth is authored by the worker.
                    await self._redis.xadd(RedisKey.stream_results(), serialize_event(evt))
                    await ack_message(self._redis, stream, CONSUMER_GROUP, msg_id)
                    return
                await self._state.store_result(
                    event.run_id, event.tenant_id, "failed", {}, 0, 0, error=str(exc),
                )
                await self._state.transition_state(event.run_id, "failed")
                await decrement_tenant_inflight_if_needed(self._redis, event.run_id)
                evt = RunFailedEvent(
                    run_id=event.run_id,
                    tenant_id=event.tenant_id,
                    worker_id=self._worker_id,
                    error=str(exc),
                    cost_cents=0,
                    tokens_used=0,
                    payload={},
                )
                await self._redis.xadd(RedisKey.stream_results(), serialize_event(evt))
                await self._state.mark_completed(event.run_id)
                await ack_message(self._redis, stream, CONSUMER_GROUP, msg_id)
                if _M:
                    _M.runs_failed_total.inc()
                    _M.run_execution_duration_ms.record(duration_ms)

            except ExecutionTransientError as exc:
                logger.warning("Transient execution failure run=%s error=%s", event.run_id, exc)
                await self._state.release_claim(event.run_id)
                if _M:
                    _M.runs_infra_failed_total.inc()

            except TerminalExecutionError as exc:
                duration_ms = (time.monotonic() - exec_start) * 1000.0
                logger.warning("Terminal failure run=%s error=%s", event.run_id, exc)
                if is_worker_effect_hybrid_enabled():
                    evt = RunFailedEvent(
                        run_id=event.run_id,
                        tenant_id=event.tenant_id,
                        worker_id=self._worker_id,
                        error=str(exc),
                        cost_cents=exc.cost_cents,
                        tokens_used=exc.tokens_used,
                    )
                    # Sprint 5: terminal failure is reported as an effect event;
                    # no direct terminal truth is authored by the worker.
                    await self._redis.xadd(RedisKey.stream_results(), serialize_event(evt))
                    await ack_message(self._redis, stream, CONSUMER_GROUP, msg_id)
                    return
                await self._state.store_result(
                    event.run_id, event.tenant_id, "failed", {},
                    exc.cost_cents, exc.tokens_used, error=str(exc),
                )
                await self._state.transition_state(event.run_id, "failed")
                await decrement_tenant_inflight_if_needed(self._redis, event.run_id)
                evt = RunFailedEvent(
                    run_id=event.run_id,
                    tenant_id=event.tenant_id,
                    worker_id=self._worker_id,
                    error=str(exc),
                    cost_cents=exc.cost_cents,
                    tokens_used=exc.tokens_used,
                )
                await self._redis.xadd(RedisKey.stream_results(), serialize_event(evt))
                await self._state.mark_completed(event.run_id)
                await ack_message(self._redis, stream, CONSUMER_GROUP, msg_id)
                if _M:
                    _M.runs_failed_total.inc()
                    _M.run_execution_duration_ms.record(duration_ms)

            except InfrastructureError as exc:
                logger.warning("Infrastructure crash run=%s error=%s", event.run_id, exc)
                await self._state.release_claim(event.run_id)
                if _M:
                    _M.runs_infra_failed_total.inc()

            except Exception as exc:
                logger.error(
                    "Unexpected execution crash run=%s error=%s",
                    event.run_id, exc, exc_info=True,
                )
                await self._state.release_claim(event.run_id)
                if _M:
                    _M.runs_infra_failed_total.inc()

            finally:
                self._inflight.discard(event.run_id)
                if _M:
                    _M.worker_inflight.dec()

        except Exception as exc:
            logger.error("Message process error: %s", exc, exc_info=True)

