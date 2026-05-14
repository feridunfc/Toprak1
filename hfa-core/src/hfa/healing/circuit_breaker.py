"""
hfa-core/src/hfa/healing/circuit_breaker.py
IRONCLAD Sprint 17 — Async-safe circuit breaker

Sprint 17 fix
-------------
Thread_Lock → asyncio.Lock.

Thread_Lock is NOT safe in an async event loop: Lock.acquire() blocks
the entire thread, preventing other coroutines from running. Under high
concurrency this causes the event loop to stall for the lock duration.

asyncio.Lock is cooperative: the await yields control back to the event
loop while waiting, allowing other coroutines to proceed.

States
------
  CLOSED     → normal operation
  OPEN       → fail-fast after failure_threshold consecutive failures
  HALF_OPEN  → one probe call allowed after recovery_timeout seconds

IRONCLAD rules
--------------
* asyncio.Lock for all state mutations (async methods).
* is_open(), state, failure_count are synchronous reads — checked before
  acquiring lock (benign TOCTOU: worst case is one extra OPEN check).
* No print() — logging only.
* close() is idempotent.
"""

import asyncio
import logging
import time
from enum import Enum

logger = logging.getLogger(__name__)


class CircuitState(str, Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class CircuitOpenError(Exception):
    """Raised when the circuit breaker is OPEN and requests are blocked."""


class CircuitBreaker:
    """
    Async-safe circuit breaker.

    All state mutations (record_success, record_failure) are async and
    protected by asyncio.Lock.

    is_open() is a synchronous property for fast-path checks before
    making a call — callers should also await record_success/failure.

    Args:
        name:              Logical name for logging.
        failure_threshold: Consecutive failures before tripping OPEN.
        recovery_timeout:  Seconds in OPEN before allowing a probe (HALF_OPEN).
    """

    def __init__(
        self,
        name: str,
        failure_threshold: int = 5,
        recovery_timeout: int = 60,
    ) -> None:
        self.name = name
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout

        self._state: CircuitState = CircuitState.CLOSED
        self._failure_count: int = 0
        self._last_failure_time: float = 0.0
        self._lock: asyncio.Lock = asyncio.Lock()

        logger.info(
            "CircuitBreaker(%s) initialised: threshold=%d, recovery=%ds",
            name,
            failure_threshold,
            recovery_timeout,
        )

    # ------------------------------------------------------------------
    # Synchronous fast-path (read-only, no lock)
    # ------------------------------------------------------------------

    def is_open(self) -> bool:
        """
        Return True when the circuit is fully OPEN (fail-fast mode).

        Also transitions OPEN → HALF_OPEN synchronously if recovery_timeout
        has elapsed. The actual state mutation is lock-free here; the
        competing async path will confirm under lock.
        """
        if self._state == CircuitState.OPEN:
            if time.monotonic() - self._last_failure_time >= self.recovery_timeout:
                self._state = CircuitState.HALF_OPEN
                logger.info("CircuitBreaker(%s) → HALF_OPEN (probe window)", self.name)
                return False
            return True
        return False

    @property
    def state(self) -> CircuitState:
        return self._state

    @property
    def failure_count(self) -> int:
        return self._failure_count

    # ------------------------------------------------------------------
    # Async state mutations (protected by asyncio.Lock)
    # ------------------------------------------------------------------

    async def record_success(self) -> None:
        """Reset the breaker to CLOSED after a successful operation."""
        async with self._lock:
            if self._state != CircuitState.CLOSED:
                logger.info("CircuitBreaker(%s) → CLOSED (success)", self.name)
            self._state = CircuitState.CLOSED
            self._failure_count = 0

    async def record_failure(self) -> None:
        """
        Increment failure counter. Trip breaker to OPEN at threshold.
        """
        async with self._lock:
            self._failure_count += 1
            self._last_failure_time = time.monotonic()

            if self._failure_count >= self.failure_threshold:
                if self._state != CircuitState.OPEN:
                    logger.warning(
                        "CircuitBreaker(%s) → OPEN after %d failures",
                        self.name,
                        self._failure_count,
                    )
                self._state = CircuitState.OPEN

    # ------------------------------------------------------------------
    # Synchronous compat wrappers (backward compatibility)
    # ------------------------------------------------------------------

    def record_success_sync(self) -> None:
        """
        Synchronous wrapper for contexts that cannot await.
        Creates a new event loop task if one is running, otherwise mutates
        state directly (test environments without a running loop).
        """
        try:
            loop = asyncio.get_running_loop()
            loop.create_task(self.record_success())
        except RuntimeError:
            # No running loop (sync test context)
            self._state = CircuitState.CLOSED
            self._failure_count = 0

    def record_failure_sync(self) -> None:
        """Synchronous wrapper for record_failure."""
        try:
            loop = asyncio.get_running_loop()
            loop.create_task(self.record_failure())
        except RuntimeError:
            self._failure_count += 1
            self._last_failure_time = time.monotonic()
            if self._failure_count >= self.failure_threshold:
                self._state = CircuitState.OPEN
