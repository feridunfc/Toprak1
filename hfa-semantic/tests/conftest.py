"""
hfa-semantic/tests/conftest.py

Test configuration and fixtures for semantic layer.
"""

import asyncio

import pytest
import redis.asyncio as redis
from fakeredis.aioredis import FakeRedis


@pytest.fixture
async def redis_client():
    """Fake Redis for testing."""
    client = FakeRedis(decode_responses=True)
    yield client
    await client.close()


@pytest.fixture
def event_loop():
    """Create event loop for async tests."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


@pytest.fixture
async def redis_async_client():
    """Async Redis client for tests."""
    # Using fakeredis for testing
    client = FakeRedis(decode_responses=True)
    yield client
    await client.flushdb()
    await client.close()

