"""
hfa-core/src/hfa/runtime/state_store.py

IRONCLAD Sprint 1 — State Store (Control Plane)

TIERED STORAGE LAYER: Control State → Redis

This module owns the control plane state tier:
  * Run lifecycle state   (queued / scheduled / running / done / failed)
  * Worker reservation    (short-lived ephemeral state)
  * Virtual-runtime counters for fairness scheduling

Tiered storage overview (Sprint 1 — interfaces only)
------------------------------------------------------

  Tier        | What                | Default backend
  ------------|---------------------|----------------
  Control     | lifecycle state     | Redis  ← this file
  Payload     | task output bytes   | Local / S3  (payload_store.py)
  Lineage     | provenance graph    | Redis / pluggable (lineage_store.py)

ControlStateStore (new ABC)
    Abstract interface for control-plane state operations.
    Concrete implementations: RedisControlStateStore (default), and
    future alternatives (e.g. etcd, Postgres) can be plugged in by
    replacing this dependency in StateStore.__init__.

StateStore (unchanged public API)
    The existing concrete class. Constructor and every public method
    are 100% backward-compatible. Internally it now delegates to
    ControlStateStore, but callers see no difference.
"""

from __future__ import annotations

import hashlib
import inspect
import json
import logging
import os
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Optional

from hfa.state import transition_state
from hfa.runtime.lineage_store import LineageStore
from hfa.runtime.payload_config import get_inline_threshold_bytes
from hfa.runtime.payload_metrics import add_bytes, inc_errors, inc_inline, inc_ref
from hfa.runtime.payload_store import PayloadStore
from hfa.runtime.redis_policy import RedisCallPolicy
from hfa_control.effect_config import get_completion_effect_ttl
from hfa_control.effect_ledger import EffectLedger
from hfa_control.effect_metrics import (
    DUPLICATE_COMPLETION_SUPPRESSED,
    STALE_COMPLETION_FENCED,
    increment,
)
from hfa.events.append_service import (
    AuthoritativeEventAppendError,
    AuthoritativeEventGate,
    is_event_gate_enabled,
)
from hfa_control.event_hooks import emit_event_background
from hfa_control.event_store import EventStore
from hfa_core.events.event_types import (
    TASK_COMPLETION_REQUESTED,
    TASK_COMPLETED,
    TASK_FAILED,
)

logger = logging.getLogger(__name__)

_COMPLETION_SLICE_FALSE_VALUES = {"", "0", "false", "False", "no", "NO", "off", "OFF"}


def is_completion_slice_enabled() -> bool:
    """Return True when Sprint 2 completion slice authority is enforced."""
    return os.getenv("IRON_V3_COMPLETION_SLICE", "0") not in _COMPLETION_SLICE_FALSE_VALUES


async def _maybe_await(result):
    if inspect.isawaitable(result):
        return await result
    return result


# ── Result types ──────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class CompletionResult:
    ok: bool
    status: str
    run_id: str
    worker_id: str = ""
    duplicate: bool = False
    reason: Optional[str] = None
    committed_state: Optional[str] = None


# ── ControlStateStore — Abstract Interface ────────────────────────────────────

class ControlStateStore(ABC):
    """
    Abstract interface for control-plane state operations.

    The control plane owns:
      * Short-lived ephemeral state (worker reservations, vruntimes)
      * Task completion ownership (which worker owns a task)
      * Task lifecycle state transitions

    Default implementation: RedisControlStateStore.

    Future implementations can target etcd, Postgres, or any
    strongly-consistent KV store by subclassing this ABC.

    Contract:
      * All methods are async.
      * reserve_worker() is idempotent for the same (worker_id, run_id).
      * get_owner() / set_owner() guard against stale completions.
      * get_task_state() / set_task_state() are the authority for task
        lifecycle — they must be consistent with hfa.state transitions.
    """

    @abstractmethod
    async def reserve_worker(
        self,
        *,
        worker_id: str,
        run_id: str,
        ttl_ms: int = 5000,
    ) -> Any:
        """Reserve worker for run_id with TTL. Returns raw reservation result."""
        raise NotImplementedError

    @abstractmethod
    async def update_vruntime(self, *, tenant_id: str, delta: float) -> Any:
        """Atomically increment tenant virtual-runtime counter by delta."""
        raise NotImplementedError

    @abstractmethod
    async def get_owner(self, *, task_id: str) -> Optional[str]:
        """Return the worker_id that currently owns task_id, or None."""
        raise NotImplementedError

    @abstractmethod
    async def set_owner(self, *, task_id: str, worker_id: str) -> None:
        """Record worker_id as the owner of task_id."""
        raise NotImplementedError

    @abstractmethod
    async def get_task_state(self, *, task_id: str) -> Optional[str]:
        """Return the current lifecycle state of task_id, or None."""
        raise NotImplementedError

    @abstractmethod
    async def set_task_state(self, *, task_id: str, state: str) -> None:
        """Write lifecycle state for task_id (used for initial state only)."""
        raise NotImplementedError

    @abstractmethod
    async def set_task_output(self, *, task_id: str, record: dict) -> None:
        """Persist the task output record (inline or ref) for task_id."""
        raise NotImplementedError


# ── RedisControlStateStore — Default Implementation ───────────────────────────

class RedisControlStateStore(ControlStateStore):
    """
    Redis-backed control plane state store.

    Uses RedisCallPolicy for retry / jitter on transient failures.
    All keys follow the established hfa:* naming convention.
    """

    def __init__(
        self,
        redis_client: Any,
        lua_executor: Any,
        policy: RedisCallPolicy | None = None,
    ) -> None:
        self._redis = redis_client
        self._lua = lua_executor
        self._policy = policy or RedisCallPolicy(max_retries=3, base_delay_ms=100)

    async def reserve_worker(
        self,
        *,
        worker_id: str,
        run_id: str,
        ttl_ms: int = 5000,
    ) -> Any:
        key = f"hfa:worker:{worker_id}:reservation"
        return await self._policy.execute_with_policy(
            "reserve_worker_lua",
            self._lua.execute,
            script_name="reserve_worker",
            keys=[key],
            args=[run_id, ttl_ms],
        )

    async def update_vruntime(self, *, tenant_id: str, delta: float) -> Any:
        key = f"hfa:tenant:{tenant_id}:vruntime"
        return await self._policy.execute_with_policy(
            "update_vruntime_redis",
            self._redis.incrbyfloat,
            key,
            delta,
        )

    async def get_owner(self, *, task_id: str) -> Optional[str]:
        raw = await _maybe_await(self._redis.get(f"hfa:task:{task_id}:owner"))
        if raw is None:
            return None
        return raw.decode("utf-8") if isinstance(raw, bytes) else raw

    async def set_owner(self, *, task_id: str, worker_id: str) -> None:
        await _maybe_await(self._redis.set(f"hfa:task:{task_id}:owner", worker_id))

    async def get_task_state(self, *, task_id: str) -> Optional[str]:
        raw = await _maybe_await(
            self._redis.get(f"hfa:dag:task:{task_id}:state")
        )
        if raw is None:
            return None
        return raw.decode("utf-8") if isinstance(raw, bytes) else raw

    async def set_task_state(self, *, task_id: str, state: str) -> None:
        await _maybe_await(
            self._redis.set(f"hfa:dag:task:{task_id}:state", state)
        )

    async def set_task_output(self, *, task_id: str, record: dict) -> None:
        await _maybe_await(
            self._redis.set(f"hfa:task:{task_id}:output", json.dumps(record))
        )


# ── StateStore — Public API (unchanged) ───────────────────────────────────────

class StateStore:
    """
    Orchestrates control-plane operations across all storage tiers.

    Constructor and every public method are 100% backward-compatible
    with the previous implementation. The only change is that internal
    Redis access is now delegated to ControlStateStore, making it
    possible to swap the backend without touching callers.

    Dependencies (all optional — existing callers pass None):
      control_store   : ControlStateStore  (default: RedisControlStateStore)
      payload_store   : PayloadStore       (required for large payloads)
      lineage_store   : LineageStore       (optional provenance tracking)
      effect_ledger   : EffectLedger       (optional duplicate suppression)
      event_store     : EventStore         (optional event emission)
    """

    def __init__(
        self,
        redis_client: Any,
        lua_executor: Any,
        *,
        policy: RedisCallPolicy | None = None,
        event_store: EventStore | None = None,
        effect_ledger: EffectLedger | None = None,
        payload_store: PayloadStore | None = None,
        lineage_store: LineageStore | None = None,
        # New (Sprint 1): allow injecting a pre-built ControlStateStore.
        # If None, a RedisControlStateStore is built from redis_client + lua_executor.
        # Existing callers that do not pass this argument are unaffected.
        control_store: ControlStateStore | None = None,
    ) -> None:
        self._redis = redis_client
        self._lua = lua_executor
        _policy = policy or RedisCallPolicy(max_retries=3, base_delay_ms=100)
        self._policy = _policy
        self._event_store = event_store
        self._event_gate = AuthoritativeEventGate(event_store)
        self._effect_ledger = effect_ledger
        self._payload_store = payload_store
        self._lineage_store = lineage_store

        # Control plane store: use injected or build default Redis implementation
        if control_store is not None:
            self._control: ControlStateStore = control_store
        else:
            self._control = RedisControlStateStore(
                redis_client=redis_client,
                lua_executor=lua_executor,
                policy=_policy,
            )

    # ── Public methods (unchanged signatures) ─────────────────────────────────

    async def reserve_worker(
        self, *, worker_id: str, run_id: str, ttl_ms: int = 5000
    ) -> Any:
        return await self._control.reserve_worker(
            worker_id=worker_id, run_id=run_id, ttl_ms=ttl_ms
        )

    async def update_vruntime(self, *, tenant_id: str, delta: float) -> Any:
        return await self._control.update_vruntime(tenant_id=tenant_id, delta=delta)

    @staticmethod
    def _completion_token(
        *, run_id: str, task_id: str, worker_id: str, attempt: int
    ) -> str:
        return f"complete:{run_id}:{task_id}:worker:{worker_id}:attempt:{attempt}"

    async def store_task_output(
        self,
        *,
        task_id: str,
        run_id: str,
        payload: bytes,
        worker_id: str,
        attempt: int,
        duplicate_guard: bool = False,
    ) -> dict[str, Any]:
        if duplicate_guard:
            return {"skipped": True, "reason": "duplicate_completion_suppressed"}

        checksum = hashlib.sha256(payload).hexdigest()
        payload_size = len(payload)
        add_bytes(payload_size)

        if payload_size <= get_inline_threshold_bytes():
            record = {
                "payload_mode": "inline",
                "payload_inline": payload.decode("utf-8"),
                "payload_ref": None,
                "payload_type": "text",
                "payload_size": payload_size,
                "checksum": checksum,
                "produced_by": {"worker_id": worker_id, "attempt": attempt},
                "run_id": run_id,
            }
            inc_inline()
        else:
            if self._payload_store is None:
                raise RuntimeError("Large payload requires payload_store")
            envelope = await self._payload_store.put(payload)
            record = {
                "payload_mode": "ref",
                "payload_inline": None,
                "payload_ref": envelope.payload_ref,
                "payload_type": envelope.payload_type,
                "payload_size": envelope.payload_size,
                "checksum": envelope.checksum,
                "produced_by": {"worker_id": worker_id, "attempt": attempt},
                "run_id": run_id,
            }
            inc_ref()

        try:
            await self._control.set_task_output(task_id=task_id, record=record)
        except Exception:
            if record.get("payload_ref"):
                logger.warning(
                    "Payload orphan risk: payload_ref=%s task_id=%s",
                    record["payload_ref"],
                    task_id,
                )
            inc_errors()
            raise

        if self._lineage_store is not None:
            try:
                await self._lineage_store.record_produced_output(
                    run_id=run_id,
                    task_id=task_id,
                    output_ref=record.get("payload_ref"),
                    payload_mode=record["payload_mode"],
                    payload_size=record["payload_size"],
                    checksum=record["checksum"],
                    payload_type=record["payload_type"],
                    producer_worker_id=worker_id,
                    producer_attempt=attempt,
                )
            except Exception as exc:
                logger.warning(
                    "Lineage produced-output write failed: task_id=%s err=%s",
                    task_id,
                    exc,
                )

        return record

    async def complete_once(
        self,
        *,
        run_id: str,
        task_id: str,
        worker_id: str,
        attempt: int = 1,
        status: str = "done",
        details: dict | None = None,
    ) -> CompletionResult:
        if self._effect_ledger is not None:
            token = self._completion_token(
                run_id=run_id, task_id=task_id, worker_id=worker_id, attempt=attempt
            )
            receipt = await self._effect_ledger.acquire_effect(
                run_id=run_id,
                token=token,
                effect_type="complete",
                owner_id=worker_id,
                ttl_seconds=get_completion_effect_ttl(),
            )
            if receipt.duplicate:
                receipt.reason = "duplicate_completion_suppressed"
                increment(DUPLICATE_COMPLETION_SUPPRESSED)
                return CompletionResult(
                    ok=False,
                    status="duplicate_completion_suppressed",
                    run_id=run_id,
                    worker_id=receipt.owner_id or worker_id,
                    duplicate=True,
                    reason=receipt.reason,
                    committed_state=receipt.committed_state,
                )

        current_owner = await self._control.get_owner(task_id=task_id)
        if current_owner and current_owner != worker_id:
            increment(STALE_COMPLETION_FENCED)
            return CompletionResult(
                ok=False,
                status="stale_owner_fenced",
                run_id=run_id,
                worker_id=worker_id,
                duplicate=False,
                reason="stale_owner_fenced",
            )

        completion_slice_enabled = is_completion_slice_enabled()
        terminal_event_type = TASK_COMPLETED if status == "done" else TASK_FAILED

        event_details = dict(details or {})
        event_details.update({
            "task_id": task_id,
            "run_id": run_id,
            "attempt": attempt,
            "requested_status": status,
            "completion_slice": "IRON_V3_COMPLETION_SLICE" if completion_slice_enabled else "legacy",
        })

        # Sprint 2 CQRS pilot: record completion intent before any terminal
        # projection.  The requested event is observable but never terminal.
        if completion_slice_enabled:
            requested_gate = AuthoritativeEventGate(self._event_store, enabled=True)
            try:
                await requested_gate.append_before_authoritative_write(
                    run_id=run_id,
                    event_type=TASK_COMPLETION_REQUESTED,
                    worker_id=worker_id,
                    details=event_details,
                    authority="StateStore.complete_once.request",
                )
            except AuthoritativeEventAppendError as exc:
                return CompletionResult(
                    ok=False,
                    status="completion_request_event_blocked",
                    run_id=run_id,
                    worker_id=worker_id,
                    duplicate=False,
                    reason=str(exc),
                )

        # Final terminal projection requires a durable final event when either
        # Sprint 1 global event gate or Sprint 2 completion slice is enabled.
        final_gate = (
            AuthoritativeEventGate(self._event_store, enabled=True)
            if completion_slice_enabled
            else self._event_gate
        )
        try:
            await final_gate.append_before_authoritative_write(
                run_id=run_id,
                event_type=terminal_event_type,
                worker_id=worker_id,
                details=event_details,
                authority="StateStore.complete_once.final",
            )
        except AuthoritativeEventAppendError as exc:
            return CompletionResult(
                ok=False,
                status="event_gate_blocked",
                run_id=run_id,
                worker_id=worker_id,
                duplicate=False,
                reason=str(exc),
            )

        state_key = f"hfa:dag:task:{task_id}:state"
        current_state = await self._control.get_task_state(task_id=task_id)

        if current_state is None:
            await self._control.set_task_state(task_id=task_id, state=status)
            committed_state = status
        else:
            tr = await transition_state(
                self._redis,
                run_id=run_id,
                target_state=status,
                state_key=state_key,
                expected_state=current_state,
                lua_loader=None,
            )
            if not tr.ok:
                return CompletionResult(
                    ok=False,
                    status=tr.reason,
                    run_id=run_id,
                    worker_id=worker_id,
                    duplicate=False,
                    reason=tr.reason,
                    committed_state=tr.to_state if tr.ok else tr.from_state,
                )
            committed_state = tr.to_state

        await self._control.set_owner(task_id=task_id, worker_id=worker_id)

        if not is_event_gate_enabled() and not completion_slice_enabled:
            emit_event_background(
                self._event_store,
                run_id=run_id,
                event_type=terminal_event_type,
                worker_id=worker_id,
                details=event_details,
            )

        return CompletionResult(
            ok=True,
            status=status,
            run_id=run_id,
            worker_id=worker_id,
            duplicate=False,
            committed_state=committed_state,
        )
