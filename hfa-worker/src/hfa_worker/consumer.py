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
import json
import logging
import time
from typing import Any, Optional, Set

from redis.exceptions import ResponseError, WatchError

from hfa.config.keys import RedisKey
from hfa_control.dag_lua import (
    TASK_CLAIM_STATUS_RESERVATION_WORKER_MISMATCH,
)
from hfa.dag.schema import DagRedisKey
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
from hfa_worker.runtime.task_context_builder import build_task_context_from_run_requested
from hfa_worker.runtime.terminal_duplicate_delivery import (
    TERMINAL_DUPLICATE_DELIVERY,
    classify_terminal_duplicate_delivery,
)
from hfa_worker.runtime.worker_runtime import (
    is_worker_effect_hybrid_enabled,
    is_worker_task_consumer_bridge_enabled,
)

try:
    from hfa.obs.runtime_metrics import IRONCLADMetrics as _M
except Exception:
    _M = None  # type: ignore[assignment]

logger = logging.getLogger(__name__)
CONSUMER_GROUP = "worker_consumers"


def _is_nogroup_error(exc: BaseException) -> bool:
    return (
        isinstance(exc, ResponseError)
        and "NOGROUP" in str(exc).upper()
    )


def _identity_text(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return str(value)


def _identity_mapping_value(mapping: object, key: str) -> str:
    if not isinstance(mapping, dict):
        return ""
    return _identity_text(
        mapping.get(key)
        or mapping.get(key.encode("utf-8"))
        or ""
    )


def _terminal_event_evidence_fields(event: object) -> dict[str, str]:
    event_type = _identity_text(getattr(event, "event_type", ""))
    if event_type not in {"RunCompleted", "RunFailed"}:
        raise ValueError("terminal evidence helper accepts only terminal RUN events")
    run_id = _identity_text(getattr(event, "run_id", ""))
    tenant_id = _identity_text(getattr(event, "tenant_id", ""))
    event_id = _identity_text(getattr(event, "event_id", ""))
    if not run_id or not tenant_id or not event_id:
        raise ValueError("terminal event identity is incomplete")
    return {
        "schema_version": "1",
        "run_id": run_id,
        "tenant_id": tenant_id,
        "event_type": event_type,
        "final_state": "done" if event_type == "RunCompleted" else "failed",
        "event_id": event_id,
        "source": "worker_consumer_compat",
    }


async def _append_terminal_event_with_evidence(redis: object, event: object) -> None:
    """Atomically append a compatibility terminal event and durable evidence.

    The versioned evidence HASH must be explicitly prepared before patched
    terminal writers run.  Writers never recreate a missing index: if Redis
    eviction/data loss removes the index, readiness disappears with it and
    terminal emission fails closed instead of risking a duplicate event.
    """

    fields = _terminal_event_evidence_fields(event)
    index_key = RedisKey.run_terminal_event_index()
    evidence_field = RedisKey.run_terminal_event_evidence_field(fields["run_id"])
    encoded = json.dumps(fields, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    pipeline_factory = getattr(redis, "pipeline", None)
    if not callable(pipeline_factory):
        raise RuntimeError("terminal event evidence requires Redis transaction support")

    for _attempt in range(4):
        async with pipeline_factory(transaction=True) as pipe:
            try:
                await pipe.watch(index_key)
                if _identity_text(await pipe.type(index_key)) != "hash":
                    raise RuntimeError("terminal event index is not prepared")
                contract = await pipe.hmget(
                    index_key,
                    "__contract__:schema_version",
                    "__contract__:producer_contract_version",
                )
                if [_identity_text(value) for value in contract] != ["1", "1"]:
                    raise RuntimeError("terminal event index contract is invalid")
                if int(await pipe.ttl(index_key)) != -1:
                    raise RuntimeError("terminal event index must be persistent")

                existing_raw = await pipe.hget(index_key, evidence_field)
                if existing_raw is not None:
                    try:
                        existing = json.loads(_identity_text(existing_raw))
                    except json.JSONDecodeError as exc:
                        raise RuntimeError("terminal event evidence is malformed") from exc
                    identity_fields = (
                        "schema_version", "run_id", "tenant_id", "event_type",
                        "final_state", "event_id",
                    )
                    if not isinstance(existing, dict) or any(existing.get(k) != fields[k] for k in identity_fields):
                        raise RuntimeError("conflicting terminal event evidence already exists")
                    if existing.get("source") not in {"worker_consumer_compat", "historical_backfill"}:
                        raise RuntimeError("conflicting terminal event evidence source already exists")
                    await pipe.unwatch()
                    return

                pipe.multi()
                pipe.xadd(RedisKey.stream_results(), serialize_event(event))
                pipe.hset(index_key, evidence_field, encoded)
                pipe.persist(index_key)
                await pipe.execute()
                return
            except WatchError:
                continue
    raise RuntimeError("terminal event evidence transaction conflicted repeatedly")


async def _verify_run_requested_task_identity(
    redis: object,
    event: object,
) -> tuple[bool, str, str]:
    """Verify explicit message identity against authoritative task metadata."""

    message_task_id = _identity_text(getattr(event, "task_id", ""))
    message_run_id = _identity_text(getattr(event, "run_id", ""))

    if not message_task_id:
        return False, "message_task_id_missing", ""

    if not message_run_id:
        return False, "message_run_id_missing", ""

    hgetall = getattr(redis, "hgetall", None)
    if not callable(hgetall):
        return False, "authoritative_identity_unavailable", ""

    raw_meta = await hgetall(DagRedisKey.task_meta(message_task_id))
    authoritative_run_id = _identity_mapping_value(
        raw_meta,
        "run_id",
    )

    if not authoritative_run_id:
        return False, "authoritative_run_id_missing", ""

    if message_run_id != authoritative_run_id:
        return (
            False,
            "message_run_id_authoritative_mismatch",
            authoritative_run_id,
        )

    return True, "explicit_task_and_run_identity_verified", authoritative_run_id

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

    Canonical TASK_CLAIM mode explicitly routes supported request envelopes to
    TaskConsumer.consume_once(). Legacy/default mode retains the compatibility
    behavior unless the historical bridge flag is enabled.
    """

    def __init__(
        self,
        redis,
        worker_id: str,
        worker_group: str,
        shards: list[int],
        executor: BaseExecutor,
        reclaim_idle_ms: int = 60000,
        task_consumer: Any | None = None,
        canonical_task_claim_binding_enabled: bool = False,
    ):
        self._redis = redis
        self._worker_id = worker_id
        self._worker_group = worker_group
        self._shards = shards
        self._executor = executor
        self._reclaim_idle_ms = reclaim_idle_ms
        self._task_consumer = task_consumer
        if type(canonical_task_claim_binding_enabled) is not bool:
            raise ValueError(
                "canonical_task_claim_binding_enabled must be a boolean"
            )
        self._canonical_task_claim_binding_enabled = (
            canonical_task_claim_binding_enabled
        )

        self._state = StateStore(redis)
        self._guard = IdempotencyGuard(redis)
        self._inflight: Set[str] = set()
        self._canonical_inflight: Set[str] = set()

        self._consumer_name = worker_id
        self._streams = [RedisKey.stream_shard(s) for s in shards]

        self._running = False
        self._pulling = True
        self._groups_prepared = False
        self._task: Optional[asyncio.Task] = None
        self._renewer_task: Optional[asyncio.Task] = None

    @property
    def inflight_count(self) -> int:
        return len(self._inflight) + len(self._canonical_inflight)

    @property
    def is_draining(self) -> bool:
        return not self._pulling

    def stop_pulling(self) -> None:
        self._pulling = False
        logger.info("Pulling stopped: worker=%s", self._worker_id)

    def prepare_for_start(self) -> None:
        """Reset the accepting-work projection before startup registration."""
        self._pulling = True

    async def prepare_consumer_groups(self) -> None:
        # Create durable stream groups without starting background consumers.
        if self._groups_prepared:
            return

        for stream in self._streams:
            await ensure_consumer_group(
                self._redis,
                stream,
                CONSUMER_GROUP,
                start_id="0",
                mkstream=True,
            )

        self._groups_prepared = True

    async def start(self) -> None:
        if self._running or self._task is not None or self._renewer_task is not None:
            logger.warning(
                "WorkerConsumer already started: worker=%s",
                self._worker_id,
            )
            return

        await self.prepare_consumer_groups()

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
        self._groups_prepared = False

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
        try:
            await self._consume_assigned_pending_messages()
            if not self._running or not self._pulling:
                return

            await self._reclaim_pending_messages()
            if not self._running or not self._pulling:
                return

            await self._consume_loop()
        except BaseException:
            self._groups_prepared = False
            raise

    async def _consume_assigned_pending_messages(self) -> None:
        """Consume PEL entries explicitly assigned to this worker.

        A same-group worker may receive a message whose reservation belongs
        to another worker. That worker transfers the PEL entry with XCLAIM.
        The reservation owner drains those assigned entries before reading
        new stream messages.
        """
        for stream in self._streams:
            cursor = "0"
            while self._running and self._pulling:
                messages = await self._redis.xreadgroup(
                    groupname=CONSUMER_GROUP,
                    consumername=self._consumer_name,
                    streams={stream: cursor},
                    count=100,
                )
                if not messages:
                    break

                last_message_id = cursor
                advanced = False
                for stream_name, entries in messages:
                    resolved_stream = (
                        stream_name.decode()
                        if isinstance(stream_name, bytes)
                        else stream_name
                    )
                    shard = int(resolved_stream.split(":")[-1])
                    for msg_id, data in entries:
                        if not self._running or not self._pulling:
                            return

                        msg_id_text = (
                            msg_id.decode()
                            if isinstance(msg_id, bytes)
                            else str(msg_id)
                        )
                        last_message_id = msg_id_text
                        advanced = True
                        logger.info(
                            "Consuming assigned pending message: "
                            "worker=%s stream=%s msg_id=%s",
                            self._worker_id,
                            resolved_stream,
                            msg_id_text,
                        )
                        await self._process_message(
                            msg_id_text,
                            data,
                            resolved_stream,
                            shard,
                        )

                if not advanced or last_message_id == cursor:
                    break
                cursor = last_message_id

    async def _reclaim_pending_messages(self) -> None:
        total_reclaimed = 0
        for stream in self._streams:
            if not self._running or not self._pulling:
                break

            try:
                pending = await self._redis.xpending_range(
                    stream,
                    CONSUMER_GROUP,
                    min="-",
                    max="+",
                    count=100,
                )
                if not self._running or not self._pulling:
                    break

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
                if not self._running or not self._pulling:
                    break

                for msg_id, data in claimed:
                    if not self._running or not self._pulling:
                        break

                    msg_id_str = (
                        msg_id.decode()
                        if isinstance(msg_id, bytes)
                        else msg_id
                    )
                    shard = int(stream.split(":")[-1])
                    logger.info(
                        "Reclaimed pending message: "
                        "worker=%s stream=%s msg_id=%s",
                        self._worker_id,
                        stream,
                        msg_id_str,
                    )
                    total_reclaimed += 1
                    await self._process_message(
                        msg_id_str,
                        data,
                        stream,
                        shard,
                    )

                if not self._running or not self._pulling:
                    break

            except Exception as exc:
                logger.error("Error reclaiming pending messages: %s", exc)

        if total_reclaimed and _M:
            _M.pending_reclaimed_total.inc(total_reclaimed)

    async def _consume_loop(self) -> None:
        streams_dict = {s: ">" for s in self._streams}
        assigned_pending_scan_interval = 0.1
        next_assigned_pending_scan_at = 0.0

        while self._running and self._pulling:
            try:
                now = time.monotonic()
                if now >= next_assigned_pending_scan_at:
                    await self._consume_assigned_pending_messages()
                    if not self._running or not self._pulling:
                        break

                    next_assigned_pending_scan_at = (
                        time.monotonic() + assigned_pending_scan_interval
                    )

                msgs = await self._redis.xreadgroup(
                    groupname=CONSUMER_GROUP,
                    consumername=self._consumer_name,
                    streams=streams_dict,
                    count=10,
                    block=100,
                )

                if not self._running or not self._pulling:
                    break

                if not msgs:
                    continue

                for stream_name, entries in msgs:
                    if not self._running or not self._pulling:
                        break

                    s_name = (
                        stream_name.decode()
                        if isinstance(stream_name, bytes)
                        else stream_name
                    )
                    shard = int(s_name.split(":")[-1])

                    for msg_id, data in entries:
                        if not self._running or not self._pulling:
                            break

                        msg_id_str = (
                            msg_id.decode()
                            if isinstance(msg_id, bytes)
                            else msg_id
                        )
                        await self._process_message(
                            msg_id_str,
                            data,
                            s_name,
                            shard,
                        )

            except asyncio.CancelledError:
                break
            except Exception as exc:
                if _is_nogroup_error(exc):
                    self._groups_prepared = False
                    raise RuntimeError(
                        "Worker consumer group disappeared after startup"
                    ) from exc
                logger.error("Consume loop error: %s", exc)
                await asyncio.sleep(0.1)

    async def _handoff_reserved_task_message(
        self,
        *,
        task_id: str,
        msg_id: str,
        stream: str,
    ) -> bool:
        """Transfer a mismatched PEL entry to its reservation owner."""
        raw_owner = await self._redis.hgetall(
            DagRedisKey.task_reservation_owner(task_id)
        )
        target_worker = _identity_mapping_value(
            raw_owner,
            "worker_id",
        )
        indexed_task_id = _identity_mapping_value(
            raw_owner,
            "task_id",
        )

        if (
            not target_worker
            or target_worker == self._worker_id
            or (
                indexed_task_id
                and indexed_task_id != task_id
            )
        ):
            return False

        claimed = await self._redis.xclaim(
            stream,
            CONSUMER_GROUP,
            target_worker,
            0,
            [msg_id],
        )
        if not claimed:
            return False

        logger.info(
            "Transferred reserved task message: task=%s "
            "from_worker=%s to_worker=%s stream=%s msg_id=%s",
            task_id,
            self._worker_id,
            target_worker,
            stream,
            msg_id,
        )
        return True

    async def _process_message_via_task_consumer(
        self,
        event: Any,
        msg_id: str,
        stream: str,
        shard: int,
    ) -> None:
        """
        Gated Sprint 62 bridge.

        This path proves WorkerConsumer can route stream envelopes into the
        canonical TaskConsumer.consume_once() path. It deliberately does not
        redesign completion, retry, reclaim, or dashboard behavior.

        The bridge is fail-closed: when the flag is enabled but no injected
        task_consumer is configured, the message is not acked and the legacy
        claim path is not used.
        """
        if self._task_consumer is None:
            logger.error(
                "TaskConsumer bridge enabled but task_consumer is not configured: run=%s",
                getattr(event, "run_id", ""),
            )
            return

        ctx = build_task_context_from_run_requested(
            event,
            worker_id=self._worker_id,
            worker_group=self._worker_group,
            shard=shard,
        )

        duplicate_delivery = await classify_terminal_duplicate_delivery(
            self._redis,
            ctx,
            message_task_id=str(getattr(event, "task_id", "") or ""),
            message_run_id=str(getattr(event, "run_id", "") or ""),
        )
        if duplicate_delivery.status == TERMINAL_DUPLICATE_DELIVERY:
            logger.info(
                "TaskConsumer bridge suppressed terminal duplicate delivery "
                "run=%s task=%s state=%s ack_allowed=%s ack_policy=%s",
                ctx.run_id,
                ctx.task_id,
                duplicate_delivery.state,
                duplicate_delivery.ack_allowed,
                duplicate_delivery.ack_policy,
            )
            if duplicate_delivery.ack_allowed:
                await ack_message(self._redis, stream, CONSUMER_GROUP, msg_id)
            return

        (
            canonical_identity_confirmed,
            identity_reason,
            authoritative_run_id,
        ) = await _verify_run_requested_task_identity(
            self._redis,
            event,
        )

        if not canonical_identity_confirmed:
            logger.warning(
                "TaskConsumer bridge blocked non-canonical identity "
                "task=%s run=%s authoritative_run=%s reason=%s",
                getattr(event, "task_id", ""),
                getattr(event, "run_id", ""),
                authoritative_run_id,
                identity_reason,
            )
            return

        inflight_identity = ctx.task_id or ctx.run_id
        self._canonical_inflight.add(inflight_identity)
        try:
            consumed = await self._task_consumer.consume_once(
                ctx,
                claimed_at_ms=int(time.time() * 1000),
            )

            rejected_reason = str(getattr(consumed, "rejected_reason", "") or "")
            if rejected_reason:
                logger.info(
                    "TaskConsumer bridge rejected run=%s reason=%s",
                    ctx.run_id,
                    rejected_reason,
                )
                return

            claimed = getattr(consumed, "claimed", None)
            if claimed is None or not bool(getattr(claimed, "ok", False)):
                claim_status = str(
                    getattr(claimed, "status", "") or ""
                )
                if (
                    claim_status
                    == TASK_CLAIM_STATUS_RESERVATION_WORKER_MISMATCH
                    and await self._handoff_reserved_task_message(
                        task_id=ctx.task_id,
                        msg_id=msg_id,
                        stream=stream,
                    )
                ):
                    return

                logger.info(
                    "TaskConsumer bridge did not claim run=%s status=%s",
                    ctx.run_id,
                    claim_status,
                )
                return

            executed = getattr(consumed, "executed", None)
            if executed is None:
                logger.info(
                    "TaskConsumer bridge did not produce execution result run=%s",
                    ctx.run_id,
                )
                return

            completed = getattr(consumed, "completed", None)
            if completed is None:
                logger.info(
                    "TaskConsumer bridge did not produce fenced completion result run=%s",
                    ctx.run_id,
                )
                return

            if not bool(getattr(completed, "completed", False)):
                logger.info(
                    "TaskConsumer bridge completion not committed run=%s status=%s",
                    ctx.run_id,
                    getattr(completed, "status", ""),
                )
                return

            await ack_message(self._redis, stream, CONSUMER_GROUP, msg_id)
        finally:
            self._canonical_inflight.discard(inflight_identity)

    async def _process_message(self, msg_id: str, data: dict, stream: str, shard: int) -> None:
        try:
            event_type = _identity_mapping_value(data, "event_type")
            if event_type not in {"", "RunRequested", "TaskRequested"}:
                logger.warning(
                    "Unsupported worker event type=%s message=%s",
                    event_type,
                    msg_id,
                )
                return

            event = deserialize_run_requested(data)
            if event is None:
                logger.warning("Failed to deserialize message %s", msg_id)
                await ack_message(self._redis, stream, CONSUMER_GROUP, msg_id)
                return

            if (
                event_type == "TaskRequested"
                or self._canonical_task_claim_binding_enabled
                or is_worker_task_consumer_bridge_enabled()
            ):
                await self._process_message_via_task_consumer(event, msg_id, stream, shard)
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

                await _append_terminal_event_with_evidence(self._redis, evt)
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
                    await _append_terminal_event_with_evidence(self._redis, evt)
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
                await _append_terminal_event_with_evidence(self._redis, evt)
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
                    await _append_terminal_event_with_evidence(self._redis, evt)
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
                await _append_terminal_event_with_evidence(self._redis, evt)
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

