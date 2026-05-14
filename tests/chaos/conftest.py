import asyncio
import inspect
import os

import pytest
import pytest_asyncio


class ChaosInjector:
    @staticmethod
    def simulate_redis_timeout():
        async def _mock_timeout(*args, **kwargs):
            await asyncio.sleep(0.05)
            raise ConnectionError("CHAOS: Redis connection timeout simulated")
        return _mock_timeout

    @staticmethod
    def simulate_connection_reset():
        async def _mock_reset(*args, **kwargs):
            await asyncio.sleep(0.01)
            raise ConnectionError("CHAOS: connection reset by peer")
        return _mock_reset


@pytest.fixture
def chaos_injector() -> ChaosInjector:
    return ChaosInjector()


@pytest_asyncio.fixture
async def redis_client():
    """Chaos/integration Redis client.

    Default: real Redis on 127.0.0.1 with readiness retry.
    Optional: USE_FAKE_REDIS=1 for local debug.
    """
    client = None
    use_fake = os.getenv("USE_FAKE_REDIS", "0") == "1"

    if use_fake:
        # 🔥 ZOMBİ İNFAZ EDİLDİ: Artık doğrudan modern FakeAsyncRedis kullanıyoruz
        from fakeredis import FakeAsyncRedis
        client = FakeAsyncRedis(decode_responses=True)
    else:
        import redis.asyncio as redis
        host = os.getenv("REDIS_HOST", "127.0.0.1")
        port = int(os.getenv("REDIS_PORT", "6379"))
        client = redis.Redis(host=host, port=port, decode_responses=True)

        last_error = None
        for _ in range(30):
            try:
                await client.ping()
                break
            except Exception as exc:
                last_error = exc
                await asyncio.sleep(0.5)
        else:
            raise last_error

    await client.flushdb()
    try:
        yield client
    finally:
        try:
            await client.flushdb()
        except Exception:
            pass

        close_fn = getattr(client, "aclose", None) or getattr(client, "close", None)
        if close_fn is not None:
            result = close_fn()
            if inspect.isawaitable(result):
                await result