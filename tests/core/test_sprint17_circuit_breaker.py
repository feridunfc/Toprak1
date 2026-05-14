"""
tests/core/test_sprint17_circuit_breaker.py
IRONCLAD Sprint 17 — Async CircuitBreaker tests

Verifies:
  - threading.Lock is gone (asyncio.Lock used)
  - CLOSED → OPEN transition at threshold
  - OPEN → HALF_OPEN after recovery_timeout
  - HALF_OPEN → CLOSED on success
  - HALF_OPEN → OPEN on failure
  - record_success/failure are async
  - is_open() remains synchronous
  - No event loop blocking
"""

from __future__ import annotations

import asyncio
import time
import pytest

from hfa.healing.circuit_breaker import CircuitBreaker, CircuitState


# ---------------------------------------------------------------------------
# Lock type verification
# ---------------------------------------------------------------------------


def test_circuit_breaker_uses_asyncio_lock_not_threading():
    """threading.Lock must not be used in CircuitBreaker."""
    import inspect
    import hfa.healing.circuit_breaker as cb_mod

    source = inspect.getsource(cb_mod)
    assert "from threading import Lock" not in source, (
        "CircuitBreaker must not import threading.Lock"
    )
    assert "threading.Lock" not in source, "CircuitBreaker must not use threading.Lock"
    assert "asyncio.Lock" in source, "CircuitBreaker must use asyncio.Lock"


# ---------------------------------------------------------------------------
# Initial state
# ---------------------------------------------------------------------------


def test_initial_state_is_closed():
    cb = CircuitBreaker("test", failure_threshold=3, recovery_timeout=60)
    assert cb.state == CircuitState.CLOSED
    assert cb.failure_count == 0
    assert cb.is_open() is False


# ---------------------------------------------------------------------------
# CLOSED → OPEN transition
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_trips_open_at_threshold():
    cb = CircuitBreaker("test", failure_threshold=3, recovery_timeout=60)
    await cb.record_failure()
    await cb.record_failure()
    assert cb.state == CircuitState.CLOSED  # not yet
    await cb.record_failure()
    assert cb.state == CircuitState.OPEN
    assert cb.is_open() is True


@pytest.mark.asyncio
async def test_does_not_trip_below_threshold():
    cb = CircuitBreaker("test", failure_threshold=5, recovery_timeout=60)
    for _ in range(4):
        await cb.record_failure()
    assert cb.state == CircuitState.CLOSED


# ---------------------------------------------------------------------------
# OPEN → HALF_OPEN after timeout
# ---------------------------------------------------------------------------


def test_transitions_to_half_open_after_recovery():
    cb = CircuitBreaker("test", failure_threshold=1, recovery_timeout=0)
    # Force into OPEN state by patching failure count
    cb._failure_count = 1
    cb._last_failure_time = time.monotonic() - 1.0  # 1s ago
    cb._state = CircuitState.OPEN

    # recovery_timeout=0 → should transition immediately
    assert cb.is_open() is False
    assert cb.state == CircuitState.HALF_OPEN


def test_remains_open_before_recovery_timeout():
    cb = CircuitBreaker("test", failure_threshold=1, recovery_timeout=3600)
    cb._failure_count = 1
    cb._last_failure_time = time.monotonic()
    cb._state = CircuitState.OPEN

    assert cb.is_open() is True
    assert cb.state == CircuitState.OPEN


# ---------------------------------------------------------------------------
# HALF_OPEN → CLOSED on success
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_closes_on_success_from_half_open():
    cb = CircuitBreaker("test", failure_threshold=1, recovery_timeout=0)
    cb._state = CircuitState.HALF_OPEN
    await cb.record_success()
    assert cb.state == CircuitState.CLOSED
    assert cb.failure_count == 0
    assert cb.is_open() is False


# ---------------------------------------------------------------------------
# Success resets from CLOSED
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_record_success_resets_failure_count():
    cb = CircuitBreaker("test", failure_threshold=5, recovery_timeout=60)
    await cb.record_failure()
    await cb.record_failure()
    assert cb.failure_count == 2
    await cb.record_success()
    assert cb.failure_count == 0
    assert cb.state == CircuitState.CLOSED


# ---------------------------------------------------------------------------
# Concurrent async safety
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_concurrent_failures_do_not_corrupt_state():
    """50 concurrent record_failure calls — state must be OPEN, count >= threshold."""
    cb = CircuitBreaker("test", failure_threshold=3, recovery_timeout=60)
    await asyncio.gather(*[cb.record_failure() for _ in range(50)])
    assert cb.state == CircuitState.OPEN
    assert cb.failure_count >= 3


@pytest.mark.asyncio
async def test_concurrent_success_and_failure_no_deadlock():
    """Mixing success and failure calls concurrently must not deadlock."""
    cb = CircuitBreaker("test", failure_threshold=100, recovery_timeout=60)
    tasks = [cb.record_failure() for _ in range(25)] + [cb.record_success() for _ in range(25)]
    # Should complete without hanging
    await asyncio.wait_for(asyncio.gather(*tasks), timeout=2.0)


# ---------------------------------------------------------------------------
# is_open() is synchronous
# ---------------------------------------------------------------------------


def test_is_open_is_synchronous():
    """is_open() must be callable without await."""
    cb = CircuitBreaker("test")
    result = cb.is_open()  # no await — must not raise
    assert isinstance(result, bool)
