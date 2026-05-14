"""
hfa-control/src/hfa_control/redis_resilience.py
IRONCLAD Sprint 17 — Redis resilience: exponential backoff + retry

Design
------
Redis is the single state store for the entire control plane.
Any transient disconnection (network blip, Redis restart, failover) must
be handled gracefully — not crash the CP process.

This module provides:

1. with_redis_retry(fn, ...)
   Async decorator/context that retries a Redis call with exponential
   backoff on transient errors. Permanent errors (key type mismatch,
   script errors) are not retried.

2. RedisHealthMonitor
   Background task that continuously checks Redis reachability and
   exposes is_healthy / last_error properties for readiness endpoint.

Retry policy (defaults)
-----------------------
  max_attempts : 5
  base_delay   : 0.1s
  max_delay    : 10.0s
  backoff_factor: 2.0
  jitter       : ±20% of computed delay (prevents thundering herd)

Transient vs permanent errors
------------------------------
Transient (retried):
  - ConnectionError, ConnectionRefusedError, TimeoutError
  - redis.exceptions.ConnectionError, BusyLoadingError
  - OSError

Permanent (not retried):
  - WRONGTYPE (key type mismatch)
  - NOSCRIPT (handled by LuaScriptLoader)
  - ResponseError with non-transient messages
  - asyncio.CancelledError (always re-raised)

IRONCLAD rules
--------------
* Never swallow CancelledError.
* Jitter prevents thundering herd on mass reconnect.
* Fail-open after max_attempts — callers decide whether to raise or continue.
* No print() — logging only.
"""

from __future__ import annotations

import asyncio
import logging
import random
from typing import Any, Callable, Optional

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Retry policy defaults
# ---------------------------------------------------------------------------

DEFAULT_MAX_ATTEMPTS = 5
DEFAULT_BASE_DELAY = 0.1  # seconds
DEFAULT_MAX_DELAY = 10.0  # seconds
DEFAULT_BACKOFF_FACTOR = 2.0
DEFAULT_JITTER_FACTOR = 0.2  # ±20% jitter

# ---------------------------------------------------------------------------
# Error classification
# ---------------------------------------------------------------------------

_TRANSIENT_EXCEPTION_TYPES = (
    ConnectionError,
    ConnectionRefusedError,
    TimeoutError,
    OSError,
)

_TRANSIENT_REDIS_MESSAGES = (
    "connection",
    "timeout",
    "reset",
    "broken pipe",
    "loading",
    "ioerror",
    "socket",
    "network",
)

_PERMANENT_REDIS_MESSAGES = (
    "wrongtype",
    "noscript",
    "no matching script",
    "wrong number of arguments",
    "syntax error",
)


def _is_transient(exc: Exception) -> bool:
    """Return True if this exception is a transient Redis/network error."""
    if isinstance(exc, asyncio.CancelledError):
        return False
    if isinstance(exc, _TRANSIENT_EXCEPTION_TYPES):
        return True
    msg = str(exc).lower()
    for keyword in _PERMANENT_REDIS_MESSAGES:
        if keyword in msg:
            return False
    for keyword in _TRANSIENT_REDIS_MESSAGES:
        if keyword in msg:
            return True
    # Default: treat unknown Redis errors as potentially transient
    exc_type = type(exc).__name__.lower()
    return "redis" in exc_type or "connection" in exc_type


def _backoff_delay(
    attempt: int, base: float, factor: float, max_delay: float, jitter: float
) -> float:
    """Compute jittered exponential backoff delay."""
    raw = min(base * (factor**attempt), max_delay)
    spread = raw * jitter
    return raw + random.uniform(-spread, spread)


# ---------------------------------------------------------------------------
# Retry wrapper
# ---------------------------------------------------------------------------


async def with_redis_retry(
    fn: Callable,
    *args: Any,
    max_attempts: int = DEFAULT_MAX_ATTEMPTS,
    base_delay: float = DEFAULT_BASE_DELAY,
    max_delay: float = DEFAULT_MAX_DELAY,
    backoff_factor: float = DEFAULT_BACKOFF_FACTOR,
    jitter_factor: float = DEFAULT_JITTER_FACTOR,
    operation_name: str = "",
    **kwargs: Any,
) -> Any:
    """
    Call async fn(*args, **kwargs) with exponential backoff retry on
    transient Redis errors.

    Args:
        fn:             Async callable to retry.
        *args:          Positional args for fn.
        max_attempts:   Total attempts before raising.
        base_delay:     Initial delay in seconds.
        max_delay:      Maximum delay cap in seconds.
        backoff_factor: Multiplier per attempt.
        jitter_factor:  Random spread factor (0.2 = ±20%).
        operation_name: Label for log messages.
        **kwargs:       Keyword args for fn.

    Returns:
        Whatever fn returns on success.

    Raises:
        Last exception if all attempts fail.
        asyncio.CancelledError immediately (never retried).
    """
    label = operation_name or getattr(fn, "__name__", "redis_op")
    last_exc: Optional[Exception] = None

    for attempt in range(max_attempts):
        try:
            return await fn(*args, **kwargs)

        except asyncio.CancelledError:
            raise

        except Exception as exc:
            if not _is_transient(exc):
                raise

            last_exc = exc
            if attempt < max_attempts - 1:
                delay = _backoff_delay(
                    attempt, base_delay, backoff_factor, max_delay, jitter_factor
                )
                logger.warning(
                    "Redis transient error on %s (attempt %d/%d): %s — retrying in %.2fs",
                    label,
                    attempt + 1,
                    max_attempts,
                    exc,
                    delay,
                )
                await asyncio.sleep(delay)
            else:
                logger.error(
                    "Redis error on %s exhausted %d attempts: %s",
                    label,
                    max_attempts,
                    exc,
                )

    raise last_exc  # type: ignore[misc]


# ---------------------------------------------------------------------------
# RedisHealthMonitor
# ---------------------------------------------------------------------------


class RedisHealthMonitor:
    """
    Background task that pings Redis every interval seconds.
    Exposes is_healthy and consecutive_failures for readiness checks.

    Args:
        redis:    aioredis.Redis client.
        interval: Probe interval in seconds (default 5).
        timeout:  PING timeout in seconds (default 2).
    """

    def __init__(self, redis, interval: float = 5.0, timeout: float = 2.0) -> None:
        self._redis = redis
        self._interval = interval
        self._timeout = timeout
        self._healthy = True
        self._last_err: Optional[str] = None
        self._failures = 0
        self._task: Optional[asyncio.Task] = None

    async def start(self) -> None:
        loop = asyncio.get_running_loop()
        self._task = loop.create_task(self._loop(), name="redis.health_monitor")
        logger.info("RedisHealthMonitor started (interval=%.1fs)", self._interval)

    async def close(self) -> None:
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info("RedisHealthMonitor stopped")

    @property
    def is_healthy(self) -> bool:
        return self._healthy

    @property
    def consecutive_failures(self) -> int:
        return self._failures

    @property
    def last_error(self) -> Optional[str]:
        return self._last_err

    async def _loop(self) -> None:
        while True:
            try:
                await asyncio.wait_for(self._redis.ping(), timeout=self._timeout)
                if not self._healthy:
                    logger.info("Redis recovered after %d failure(s)", self._failures)
                self._healthy = True
                self._failures = 0
                self._last_err = None
            except asyncio.CancelledError:
                break
            except Exception as exc:
                self._healthy = False
                self._failures += 1
                self._last_err = str(exc)
                logger.warning(
                    "Redis health check failed (consecutive=%d): %s",
                    self._failures,
                    exc,
                )
            await asyncio.sleep(self._interval)
