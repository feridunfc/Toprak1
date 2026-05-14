"""
tests/core/test_sprint17_redis_resilience.py
IRONCLAD Sprint 17 — Redis resilience (retry + health monitor) tests

Verifies:
  - with_redis_retry succeeds on first attempt
  - Retries on transient errors, succeeds eventually
  - Exhausts retries and raises last exception
  - Permanent errors are NOT retried
  - CancelledError is never retried
  - Jitter produces different delays
  - RedisHealthMonitor reports healthy on ping success
  - RedisHealthMonitor reports unhealthy after ping failure
  - RedisHealthMonitor recovers on subsequent ping success
"""

from __future__ import annotations

import asyncio
import pytest
from unittest.mock import AsyncMock, patch

from hfa_control.redis_resilience import (
    with_redis_retry,
    RedisHealthMonitor,
    _is_transient,
    _backoff_delay,
)


# ---------------------------------------------------------------------------
# Error classification
# ---------------------------------------------------------------------------


def test_connection_error_is_transient():
    assert _is_transient(ConnectionError("refused")) is True


def test_timeout_error_is_transient():
    assert _is_transient(TimeoutError("timeout")) is True


def test_os_error_is_transient():
    assert _is_transient(OSError("socket reset")) is True


def test_wrongtype_is_permanent():
    exc = Exception("WRONGTYPE Operation against a key holding the wrong kind")
    assert _is_transient(exc) is False


def test_noscript_is_permanent():
    exc = Exception("NOSCRIPT No matching script")
    assert _is_transient(exc) is False


def test_cancelled_error_is_not_transient():
    assert _is_transient(asyncio.CancelledError()) is False


def test_syntax_error_is_permanent():
    exc = Exception("ERR syntax error")
    assert _is_transient(exc) is False


# ---------------------------------------------------------------------------
# Backoff delay
# ---------------------------------------------------------------------------


def test_backoff_delay_grows_exponentially():
    d0 = _backoff_delay(0, base=0.1, factor=2.0, max_delay=10.0, jitter=0.0)
    d1 = _backoff_delay(1, base=0.1, factor=2.0, max_delay=10.0, jitter=0.0)
    d2 = _backoff_delay(2, base=0.1, factor=2.0, max_delay=10.0, jitter=0.0)
    assert d0 == pytest.approx(0.1)
    assert d1 == pytest.approx(0.2)
    assert d2 == pytest.approx(0.4)


def test_backoff_delay_capped_at_max():
    d = _backoff_delay(100, base=0.1, factor=2.0, max_delay=5.0, jitter=0.0)
    assert d == pytest.approx(5.0)


def test_backoff_jitter_produces_variation():
    delays = {
        _backoff_delay(1, base=1.0, factor=2.0, max_delay=10.0, jitter=0.2) for _ in range(20)
    }
    assert len(delays) > 1, "Jitter should produce different values"


# ---------------------------------------------------------------------------
# with_redis_retry — success path
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_retry_succeeds_on_first_attempt():
    called = []

    async def fn():
        called.append(1)
        return "ok"

    result = await with_redis_retry(fn, max_attempts=3)
    assert result == "ok"
    assert len(called) == 1


@pytest.mark.asyncio
async def test_retry_succeeds_on_second_attempt():
    attempts = []

    async def fn():
        attempts.append(1)
        if len(attempts) < 2:
            raise ConnectionError("transient")
        return "recovered"

    with patch("hfa_control.redis_resilience.asyncio.sleep", new_callable=AsyncMock):
        result = await with_redis_retry(fn, max_attempts=3, base_delay=0.001)
    assert result == "recovered"
    assert len(attempts) == 2


@pytest.mark.asyncio
async def test_retry_exhausted_raises_last_exception():
    async def fn():
        raise ConnectionError("always fails")

    with patch("hfa_control.redis_resilience.asyncio.sleep", new_callable=AsyncMock):
        with pytest.raises(ConnectionError, match="always fails"):
            await with_redis_retry(fn, max_attempts=3, base_delay=0.001)


@pytest.mark.asyncio
async def test_permanent_error_not_retried():
    attempts = []

    async def fn():
        attempts.append(1)
        raise Exception("WRONGTYPE wrong key type")

    with pytest.raises(Exception, match="WRONGTYPE"):
        await with_redis_retry(fn, max_attempts=5)

    assert len(attempts) == 1, "Permanent error must not trigger retries"


@pytest.mark.asyncio
async def test_cancelled_error_propagates_immediately():
    attempts = []

    async def fn():
        attempts.append(1)
        raise asyncio.CancelledError()

    with pytest.raises(asyncio.CancelledError):
        await with_redis_retry(fn, max_attempts=5)

    assert len(attempts) == 1, "CancelledError must not be retried"


# ---------------------------------------------------------------------------
# RedisHealthMonitor
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_health_monitor_reports_healthy_on_ping():
    redis = AsyncMock()
    redis.ping = AsyncMock(return_value=True)

    monitor = RedisHealthMonitor(redis, interval=0.01, timeout=0.5)
    await monitor.start()
    await asyncio.sleep(0.05)
    await monitor.close()

    assert monitor.is_healthy is True
    assert monitor.consecutive_failures == 0
    assert monitor.last_error is None


@pytest.mark.asyncio
async def test_health_monitor_reports_unhealthy_on_ping_failure():
    redis = AsyncMock()
    redis.ping = AsyncMock(side_effect=ConnectionError("Redis down"))

    monitor = RedisHealthMonitor(redis, interval=0.01, timeout=0.5)
    await monitor.start()
    await asyncio.sleep(0.05)
    await monitor.close()

    assert monitor.is_healthy is False
    assert monitor.consecutive_failures >= 1
    assert monitor.last_error is not None


@pytest.mark.asyncio
async def test_health_monitor_recovers_after_failure():
    call_count = [0]
    redis = AsyncMock()

    async def ping_side_effect():
        call_count[0] += 1
        if call_count[0] <= 2:
            raise ConnectionError("Redis down")
        return True

    redis.ping = AsyncMock(side_effect=ping_side_effect)

    monitor = RedisHealthMonitor(redis, interval=0.01, timeout=0.5)
    await monitor.start()
    await asyncio.sleep(0.1)
    await monitor.close()

    # After recovery, should be healthy
    assert monitor.is_healthy is True
    assert monitor.consecutive_failures == 0


@pytest.mark.asyncio
async def test_health_monitor_close_is_idempotent():
    redis = AsyncMock()
    redis.ping = AsyncMock(return_value=True)
    monitor = RedisHealthMonitor(redis, interval=0.01)
    await monitor.start()
    await monitor.close()
    await monitor.close()  # second close — no exception


@pytest.mark.asyncio
async def test_health_monitor_initial_state_is_healthy():
    """Before first probe, monitor starts healthy (optimistic)."""
    redis = AsyncMock()
    monitor = RedisHealthMonitor(redis, interval=999)  # never probes
    assert monitor.is_healthy is True
    assert monitor.consecutive_failures == 0
