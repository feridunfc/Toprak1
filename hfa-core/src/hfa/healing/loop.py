"""
hfa-core/src/hfa/healing/loop.py
IRONCLAD Sprint 4 — SelfHealingEngine

Architecture
------------
SelfHealingEngine orchestrates retry logic for agent runs:

  1. Load prior state from StateStore (persisted across restarts).
  2. Check circuit breaker — if open, fail-fast with HealingCircuitOpenError.
  3. Execute the callable (agent function).
  4. On success: clear state, close circuit.
  5. On failure:
       a. Fingerprint the error (SHA-256 of message).
       b. Increment attempt counter.
       c. If attempt >= max_attempts: open circuit for `cooldown_seconds`.
       d. Raise HealingMaxRetriesError.
  6. Exponential back-off between retries (jitter optional).

IRONCLAD rules enforced
-----------------------
* asyncio.get_running_loop() everywhere — NEVER get_event_loop().
* No print() — structured logging only.
* close() awaits all pending work.
* Integer cents for cost tracking (total_cost_cents).
* Callable signature: async (run_id: str, attempt: int) -> HealingResult
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import time
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Optional

from hfa.healing.store import LoopState, StateStore

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Result / Error types
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class HealingResult:
    """
    Returned by a successful healing run.

    Attributes:
        run_id:          Run identifier.
        attempts:        Total attempts consumed (1 = first try succeeded).
        tokens_used:     Total LLM tokens used across all attempts.
        cost_cents:      Total cost in INTEGER CENTS.
        payload:         Arbitrary output from the callable.
        recovered:       True if at least one retry was needed.
    """

    run_id: str
    attempts: int
    tokens_used: int
    cost_cents: int
    payload: Any
    recovered: bool = False


class HealingError(Exception):
    """Base class for healing engine errors."""

    def __init__(self, msg: str, run_id: str) -> None:
        super().__init__(msg)
        self.run_id = run_id


class HealingMaxRetriesError(HealingError):
    """All retry attempts exhausted without success."""

    def __init__(self, run_id: str, attempts: int, last_error: str) -> None:
        super().__init__(
            f"Max retries ({attempts}) exhausted for run {run_id}: {last_error}",
            run_id,
        )
        self.attempts = attempts
        self.last_error = last_error


class HealingCircuitOpenError(HealingError):
    """Circuit breaker is open — request rejected without attempting."""

    def __init__(self, run_id: str, open_until: float) -> None:
        wait = max(0, open_until - time.time())
        super().__init__(
            f"Circuit open for run {run_id} — retry after {wait:.1f}s",
            run_id,
        )
        self.open_until = open_until


# ---------------------------------------------------------------------------
# Attempt context passed to callable
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class AttemptContext:
    """
    Passed to the healing callable on each attempt.

    Attributes:
        run_id:      Run identifier.
        tenant_id:   Tenant identifier.
        attempt:     Current attempt number (0-based).
        prior_error: Error message from previous attempt (None on first try).
        fingerprint: SHA-256 of prior error (None on first try).
    """

    run_id: str
    tenant_id: str
    attempt: int
    prior_error: Optional[str]
    fingerprint: Optional[str]


# ---------------------------------------------------------------------------
# Token/cost update helper (passed back from callable)
# ---------------------------------------------------------------------------


@dataclass
class AttemptOutcome:
    """
    Returned by the healing callable on success.

    Attributes:
        payload:      Arbitrary output (PlanManifest, CodeChangeSet, etc.).
        tokens_used:  LLM tokens consumed in this attempt.
        cost_cents:   Cost in INTEGER CENTS for this attempt (not cumulative).
    """

    payload: Any
    tokens_used: int = 0
    cost_cents: int = 0  # ✅ integer cents — no float


# Type alias for the healing callable
HealingCallable = Callable[[AttemptContext], Awaitable[AttemptOutcome]]


# ---------------------------------------------------------------------------
# SelfHealingEngine
# ---------------------------------------------------------------------------


class SelfHealingEngine:
    """
    Stateful retry loop with circuit breaker and persistent state tracking.

    Usage
    -----
        engine = SelfHealingEngine(
            store=InMemoryStateStore(),
            max_attempts=3,
            cooldown_seconds=60.0,
            base_backoff_seconds=2.0,
        )

        async def my_agent(ctx: AttemptContext) -> AttemptOutcome:
            result = await call_llm(ctx.run_id, ctx.attempt)
            return AttemptOutcome(payload=result, tokens_used=100, cost_cents=5)

        result = await engine.run(
            callable=my_agent,
            tenant_id="acme_corp",
            run_id="run-acme_corp-abc123",
        )

    Args:
        store:              Persistent state store.
        max_attempts:       Attempts before circuit opens (default 3).
        cooldown_seconds:   Circuit-open duration after max failures.
        base_backoff_seconds: Base for exponential backoff between attempts.
        backoff_multiplier: Multiplier per attempt (attempt^multiplier).
        max_backoff_seconds: Cap on per-attempt sleep.
        jitter:             If True, add random jitter to backoff.
    """

    def __init__(
        self,
        store: StateStore,
        max_attempts: int = 3,
        cooldown_seconds: float = 60.0,
        base_backoff_seconds: float = 2.0,
        backoff_multiplier: float = 2.0,
        max_backoff_seconds: float = 30.0,
        jitter: bool = True,
    ) -> None:
        self._store = store
        self._max_attempts = max_attempts
        self._cooldown = cooldown_seconds
        self._base_backoff = base_backoff_seconds
        self._backoff_multiplier = backoff_multiplier
        self._max_backoff = max_backoff_seconds
        self._jitter = jitter
        logger.info(
            "SelfHealingEngine created: max_attempts=%d cooldown=%.1fs",
            max_attempts,
            cooldown_seconds,
        )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def run(
        self,
        callable: HealingCallable,
        tenant_id: str,
        run_id: str,
    ) -> HealingResult:
        """
        Execute `callable` with automatic retry and state persistence.

        Args:
            callable:   Async function conforming to HealingCallable.
            tenant_id:  Tenant identifier (used for state key scoping).
            run_id:     Run identifier (used for state key scoping).

        Returns:
            HealingResult on success.

        Raises:
            HealingCircuitOpenError: Circuit is open — do not retry.
            HealingMaxRetriesError:  All attempts exhausted.
        """
        state_key = self._make_key(tenant_id, run_id)
        state = await self._load_or_create(state_key)

        # ── Circuit breaker check ────────────────────────────────────────
        if state.is_circuit_open():
            logger.warning(
                "Circuit OPEN: tenant=%s run=%s open_until=%.0f",
                tenant_id,
                run_id,
                state.circuit_open_until,
            )
            raise HealingCircuitOpenError(run_id, state.circuit_open_until)

        # ── Retry loop ───────────────────────────────────────────────────

        while state.attempt < self._max_attempts:
            ctx = AttemptContext(
                run_id=run_id,
                tenant_id=tenant_id,
                attempt=state.attempt,
                prior_error=state.last_error,
                fingerprint=state.fingerprint,
            )

            logger.info(
                "Healing attempt %d/%d: tenant=%s run=%s",
                state.attempt + 1,
                self._max_attempts,
                tenant_id,
                run_id,
            )

            try:
                outcome = await callable(ctx)

                # ── SUCCESS ──────────────────────────────────────────────
                result = HealingResult(
                    run_id=run_id,
                    attempts=state.attempt + 1,
                    tokens_used=state.total_tokens_used + outcome.tokens_used,
                    cost_cents=state.total_cost_cents + outcome.cost_cents,
                    payload=outcome.payload,
                    recovered=state.attempt > 0,
                )
                await self._store.delete(state_key)
                logger.info(
                    "Healing SUCCESS: tenant=%s run=%s attempts=%d cost=%d¢",
                    tenant_id,
                    run_id,
                    result.attempts,
                    result.cost_cents,
                )
                return result

            except Exception as exc:
                # ── FAILURE ──────────────────────────────────────────────
                error_msg = str(exc)
                fingerprint = self._fingerprint(error_msg)

                logger.warning(
                    "Healing attempt %d FAILED: tenant=%s run=%s fp=%s error=%s",
                    state.attempt + 1,
                    tenant_id,
                    run_id,
                    fingerprint[:8],
                    error_msg,
                )

                state.attempt += 1
                state.last_error = error_msg
                state.fingerprint = fingerprint
                state.total_tokens_used += 0  # callable did not expose tokens on fail
                await self._store.set(state_key, state)

                if state.attempt >= self._max_attempts:
                    # Open circuit breaker
                    state.open_circuit(self._cooldown)
                    await self._store.set(state_key, state)
                    logger.error(
                        "Healing EXHAUSTED: tenant=%s run=%s circuit open for %.0fs",
                        tenant_id,
                        run_id,
                        self._cooldown,
                    )
                    raise HealingMaxRetriesError(
                        run_id, state.attempt, error_msg
                    ) from exc

                # Back-off before next attempt
                backoff = self._backoff(state.attempt)
                logger.info(
                    "Healing back-off %.2fs before attempt %d",
                    backoff,
                    state.attempt + 1,
                )
                await asyncio.sleep(backoff)

        # Should not reach here — loop exits via raise or return
        raise HealingMaxRetriesError(
            run_id, state.attempt, state.last_error or "unknown"
        )

    async def reset(self, tenant_id: str, run_id: str) -> None:
        """
        Clear stored state and close circuit (human override after review).

        Args:
            tenant_id: Tenant identifier.
            run_id:    Run identifier.
        """
        key = self._make_key(tenant_id, run_id)
        await self._store.delete(key)
        logger.info("Healing state RESET: tenant=%s run=%s", tenant_id, run_id)

    async def get_state(self, tenant_id: str, run_id: str) -> Optional[LoopState]:
        """Read current state for a run (read-only)."""
        return await self._store.get(self._make_key(tenant_id, run_id))

    async def close(self) -> None:
        """Shut down the underlying store."""
        await self._store.close()
        logger.info("SelfHealingEngine closed")

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _make_key(tenant_id: str, run_id: str) -> str:
        return f"{tenant_id}:{run_id}"

    @staticmethod
    def _fingerprint(error_msg: str) -> str:
        """SHA-256 hex of error message for dedup / triage."""
        return hashlib.sha256(error_msg.encode()).hexdigest()

    def _backoff(self, attempt: int) -> float:
        """
        Exponential back-off with optional jitter.

        Formula: min(base * multiplier^(attempt-1), max)
        With jitter: multiply by random in [0.5, 1.5].
        """
        import random

        delay = min(
            self._base_backoff * (self._backoff_multiplier ** (attempt - 1)),
            self._max_backoff,
        )
        if self._jitter:
            delay *= random.uniform(0.5, 1.5)
        return delay

    async def _load_or_create(self, key: str) -> LoopState:
        state = await self._store.get(key)
        if state is None:
            state = LoopState()
        return state
