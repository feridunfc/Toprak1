
import pytest

from hfa.runtime.state_store import StateStore

pytestmark = pytest.mark.asyncio


class FakeLuaExecutor:
    def __init__(self) -> None:
        self.calls = 0

    async def execute(self, *, script_name, keys, args):
        self.calls += 1
        if self.calls == 1:
            raise Exception("timeout talking to redis")
        return {"script_name": script_name, "keys": keys, "args": args}


class FakeRedis:
    def __init__(self) -> None:
        self.calls = 0

    async def incrbyfloat(self, key, delta):
        self.calls += 1
        if self.calls == 1:
            raise Exception("connection reset by peer")
        return 42.5


@pytest.mark.integration
async def test_state_store_reserve_worker_retries_and_succeeds():
    store = StateStore(redis_client=FakeRedis(), lua_executor=FakeLuaExecutor())
    result = await store.reserve_worker(worker_id="worker-1", run_id="run-1", ttl_ms=5000)
    assert result["script_name"] == "reserve_worker"
    assert result["keys"] == ["hfa:worker:worker-1:reservation"]


@pytest.mark.integration
async def test_state_store_update_vruntime_retries_and_succeeds():
    store = StateStore(redis_client=FakeRedis(), lua_executor=FakeLuaExecutor())
    result = await store.update_vruntime(tenant_id="tenant-a", delta=1.5)
    assert result == 42.5
